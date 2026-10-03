#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
"""Same-image scenario campaign; raw measurements remain outside checkouts."""

import argparse
import hashlib
import json
import math
import os
from pathlib import Path
import platform
import re
import shutil
import subprocess
import sys
import tarfile
import time
import uuid
import copy
import gzip
import socket
import stat
import urllib.request

sys.dont_write_bytecode = True
import performance as perf

IMAGE_BASE = "sha256:93ce27a88655056a51dbdd8f5f2d7ddc071c7b0070fb288a37b5a285fc83971e"
CPU_RESOLUTION_SECONDS = 0.01
MODES = ("native", "syscalls", "allocations", "copies")
UNITS = {"native_run_ns": "ns/client-run-window", "elapsed_ns": "ns/launcher-process-lifetime",
         "peak_rss_kib": "KiB/process-and-waited-descendants-peak", "user_cpu_seconds": "s/process-user-cpu",
         "system_cpu_seconds": "s/process-system-cpu", "syscall_calls": "calls/process-lifetime",
         "malloc_family_growth_bytes": "B/glibc-malloc-family-cumulative-growth",
         "malloc_family_calls": "calls/glibc-malloc-family", "heap_peak_bytes": "B/glibc-malloc-family-peak",
         "intercepted_copy_bytes": "B/intercepted-library-copies", "intercepted_copy_calls": "calls/intercepted-library-copies"}
PACKAGES = {"strace": "6.1-0.1", "valgrind": "1:3.19.0-1", "heaptrack": "1.4.0-2",
            "libc6": "2.36-9+deb12u14", "time": "1.9-0.2", "clang": "1:14.0-55.7~deb12u1"}
GAPS = ["doctest performance (functional compilation/execution only)", "effective multi-message batch formation",
        "TLS", "grouped ACKs", "concurrent traffic", "pure/SimProviders isolated scenarios",
        "explicit feature/fault isolated scenarios", "quiescent RSS/retention after drain",
        "all Rust allocations (glibc dynamic malloc-family only)", "inlined/kernel/DMA copy bytes",
        "client-only attribution of whole-lifecycle instrumented totals"]
FIXTURE_TAGS = ("apachepulsar/pulsar:latest", "apachepulsar/pulsar:4.0.4", "apachepulsar/pulsar:4.2.4",
                "apachepulsar/pulsar:5.0.0-M1", "gcavalcante8808/krb5-server:latest", "athenz/athenz-zts-server:1.12.41")
SEED_REGISTRY = "crates/magnetar-runtime-moonpool/seeds/known-failing.toml"
EXAMPLE_SOURCE = "crates/magnetar/examples/performance.rs"


def save(path, data):
    Path(path).write_text(json.dumps(data, indent=2, allow_nan=False) + "\n")


class CollectionFailure(ValueError):
    def __init__(self, state, reason, collector_exit_code=None, child_exit_code=None):
        super().__init__(reason)
        self.state = state
        self.collector_exit_code = collector_exit_code
        self.child_exit_code = child_exit_code


def write_artifact_manifest(output, name="artifacts.json", required=()):
    if any(not (output / path).is_file() for path in required):
        raise ValueError("final manifest lacks a required launcher artifact")
    save(output / name, {str(path.relative_to(output)): perf.digest(path) for path in output.rglob("*")
                        if path.is_file() and "target" not in path.parts and path != output / name
                        and (name != "artifacts.json" or path != output / "campaign-artifacts.json")})


def verify_artifact_manifest(path):
    for relative, expected in json.loads(path.read_text()).items():
        file = (path.parent / relative).resolve()
        if not file.is_relative_to(path.parent.resolve()) or perf.digest(file) != expected:
            raise ValueError(f"immutable campaign artifact differs: {relative}")


def check_image(image, expected_id, dockerfile_sha256):
    labels = image.get("Config", {}).get("Labels") or {}
    if image.get("Id") != expected_id or not re.fullmatch(r"sha256:[0-9a-f]{64}", expected_id):
        raise ValueError("an exact locally inspected image ID is required")
    if labels.get("org.opencontainers.image.base.digest") != IMAGE_BASE or labels.get("magnetar.performance.dockerfile-sha256") != dockerfile_sha256:
        raise ValueError("inspected image base/Dockerfile build labels differ from audited source")


def verify_fixture_images(images, output):
    observed = {}
    for tag, expected in images.items():
        image = json.loads(perf.read_command(["docker", "image", "inspect", tag], output))[0]
        if image["Id"] != expected["image_id"] or expected["reference"] not in (image.get("RepoDigests") or []):
            raise ValueError("fixture tag/image/registry digest differs: " + tag)
        observed[tag] = image
    return observed


def comparison_row(mode, metric, unit, base, candidate, same_elf):
    row = {"mode": mode, "metric": metric, "unit": unit, **perf.summarize(base, candidate),
           "comparison_kind": "calibration-identical-elf" if same_elf else "base-candidate",
           "product_effect": None if same_elf else "informative-cost-comparison"}
    row.update(base_display=row["base"], candidate_display=row["candidate"])
    if metric in ("user_cpu_seconds", "system_cpu_seconds"):
        row.update(cpu_resolution_seconds=CPU_RESOLUTION_SECONDS,
                   base_display="<0.01" if row["base"] == 0 else row["base"],
                   candidate_display="<0.01" if row["candidate"] == 0 else row["candidate"],
                   delta=None, relative_percent=None, verdict="quantized CPU observation; informative only")
    if same_elf:
        row["verdict"] = "calibration variation; no product effect" + ("; quantized CPU" if "cpu_resolution_seconds" in row else "")
    return row


def check_diagnostics(text):
    if re.search(r"panicked|\bEPERM\b|Operation not permitted|ptrace.*denied|error:|truncated", text, re.IGNORECASE):
        raise ValueError("collector/child reported panic, denial, error or truncation")


def parse_memusage(text):
    text = re.sub(r"\x1b\[[0-9;]*m", "", text)
    summary = re.findall(r"Memory usage summary: heap total: (\d+), heap peak: (\d+), stack peak: (\d+)", text)
    if len(summary) != 1:
        raise ValueError("missing or ambiguous glibc 2.36 memusage summary")
    total, peak, _ = map(int, summary[0])
    rows = {}
    for name, calls, amount, failed in re.findall(r"^\s*(malloc|realloc|calloc)\|\s+(\d+)\s+(\d+)\s+(\d+)", text, re.MULTILINE):
        if name in rows or int(failed):
            raise ValueError("duplicate memusage row or failed allocation")
        rows[name] = {"calls": int(calls), "bytes": int(amount), "failed": int(failed)}
    frees = re.findall(r"^\s*free\|\s+(\d+)\s+(\d+)", text, re.MULTILINE)
    if set(rows) != {"malloc", "realloc", "calloc"} or len(frees) != 1 or peak <= 0 or sum(row["bytes"] for row in rows.values()) != total:
        raise ValueError("missing or inconsistent memusage rows/peak")
    return {"malloc_family_growth_bytes": total, "malloc_family_calls": sum(row["calls"] for row in rows.values()),
            "realloc_net_growth_bytes": rows["realloc"]["bytes"], "heap_peak_bytes": peak,
            "malloc_family_rows": rows, "free_calls": int(frees[0][0])}


def check_scenario(observation, request):
    if observation.get("schema_version") != 1:
        raise ValueError("unsupported scenario observation schema")
    for key in ("scenario_id", "scenario_revision", "runtime", "seed", "payload_bytes", "batching"):
        if observation.get(key) != request[key]:
            raise ValueError(f"scenario result differs from request: {key}")
    expected = 1 if request["scenario_id"] == "idle" else request["messages"]
    if observation.get("completed") != expected or observation.get("requested") != expected:
        raise ValueError("scenario requested/completed work differs")
    phases = observation.get("phases", [])
    if [entry[0] for entry in phases] != ["setup", "warmup", "run", "drain", "finished"]:
        raise ValueError("missing or reordered scenario phases")
    stamps = [entry[1] for entry in phases]
    if any(type(stamp) is not int or stamp <= 0 for stamp in stamps) or stamps != sorted(stamps):
        raise ValueError("invalid scenario phase timestamps")
    if type(observation.get("native_run_ns")) is not int or observation["native_run_ns"] <= 0:
        raise ValueError("missing positive native run duration")
    if not observation.get("scope") or not observation.get("denominator"):
        raise ValueError("missing scenario metric scope/denominator")
    for field, scenarios in (("send_latency_ns", ("producer", "roundtrip")), ("receive_ack_latency_ns", ("consumer", "roundtrip"))):
        samples = observation.get(field)
        expected_samples = expected if request["scenario_id"] in scenarios else 0
        if not isinstance(samples, list) or len(samples) != expected_samples or any(type(value) is not int or value < 0 for value in samples):
            raise ValueError("missing or malformed native latency sample catalogue")
    if not request["scenario_id"].startswith("control-") and request["scenario_id"] != "idle":
        required = {"terminal_marker_acknowledged": True, "broker_message_after_marker": False,
                    "consumer_queue_after_drain": 0, "producer_pending_after_drain": 0, "topic_partition_count": 0}
        if any(observation.get(key) != value for key, value in required.items()):
            raise ValueError("terminal drain oracle failed or absent")
        if observation.get("drain_verified_messages") != expected or observation.get("confirmed_payload_bytes") != expected * request["payload_bytes"]:
            raise ValueError("terminal delivery/payload count differs")


def check_union(expected, shards):
    perf.reconcile_families(expected, shards)


def check_environment(launch, tools, compiler, affinity, suite=False):
    if not isinstance(launch, dict):
        raise ValueError("environment must be an inspected object")
    for field in ("image_id", "runner_identity", "cpu_affinity", "profile", "features", *( () if suite else ("broker",) )):
        if not launch.get(field):
            raise ValueError(f"missing qualified environment: {field}")
    if not re.fullmatch(r"sha256:[0-9a-f]{64}", launch["image_id"]) or not re.fullmatch(r"[0-9a-f]{64}", launch["runner_identity"]):
        raise ValueError("environment image/runner identity must be exact digests")
    if not suite:
        if not re.fullmatch(r"sha256:[0-9a-f]{64}", launch["broker"].get("image", "")) or not re.fullmatch(r"[0-9a-f]{64}", launch["broker"].get("id", "")):
            raise ValueError("missing qualified broker identity")
    expected_features = perf.axis_configuration(launch.get("axis", "workspace-all-features"), [])[2] if suite else ["moonpool", "scalable-topics"]
    if launch["profile"] != perf.PROFILE_ENV or launch["features"] != expected_features:
        raise ValueError("actual build profile/features differ from frozen launcher")
    if tools != PACKAGES or not compiler.startswith("rustc 1.98.1 "):
        raise ValueError("compiler/collector versions differ from qualified image contract")
    if sorted(affinity) != launch["cpu_affinity"]:
        raise ValueError("actual container CPU affinity differs from frozen launcher")


def copy_witness(data, count):
    totals = perf.parse_dhat(data, "copy")
    frames = data.get("ftbl", [])
    if not frames or any(not isinstance(frame, str) for frame in frames):
        raise ValueError("missing DHAT symbol table")
    for point in data["pps"]:
        if not isinstance(point.get("fs"), list) or any(type(frame) is not int or not 0 <= frame < len(frames) for frame in point["fs"]):
            raise ValueError("invalid DHAT copy frame references")
    points = [point for point in data["pps"] if any(re.search(r"forced_copy \((?:[^)]*/)?control\.c:\d+\)", frames[frame]) for frame in point["fs"])]
    if len(points) != 1 or points[0]["tb"] != count * 256 or points[0]["tbk"] != count:
        raise ValueError("forced library memcpy calibration differs")
    return {**totals, "workload_copy_bytes": points[0]["tb"], "workload_copy_calls": points[0]["tbk"],
            "runtime_copy_bytes": totals["intercepted_copy_bytes"] - points[0]["tb"],
            "runtime_copy_calls": totals["intercepted_copy_calls"] - points[0]["tbk"]}


def workload_identity(request):
    # Fixture bindings (fresh topic and endpoint) have separate provenance.
    # All other request keys determine the actual semantic work.
    return {key: value for key, value in request.items() if key not in ("topic", "service_url")}


def collect(command, cwd, prefix, mode):
    prefix = Path(prefix)
    try:
        metrics = _collect(command, cwd, prefix, mode)
    except (ValueError, OSError, json.JSONDecodeError) as error:
        status_path = Path(str(prefix) + ".exit.json")
        status = json.loads(status_path.read_text())["status"] if status_path.exists() else None
        failure = error if isinstance(error, CollectionFailure) else CollectionFailure("invalid-collection", str(error), status)
        save(str(prefix) + ".collection.json", {"state": failure.state, "reason": str(failure), "metrics": None,
             "collector": mode, "collector_exit_code": failure.collector_exit_code, "child_exit_code": failure.child_exit_code})
        if failure is error:
            raise
        raise failure from error
    save(str(prefix) + ".collection.json", {"state": "valid", "metrics": metrics, "collector": mode,
         "collector_exit_code": 0, "child_exit_code": 0})
    return metrics


def _collect(command, cwd, prefix, mode):
    prefix = Path(prefix)
    target_command = list(command)
    env = dict(os.environ, LC_ALL="C")
    if mode == "native":
        command = ["/usr/bin/time", "--quiet", "-f", "%e %M %x %U %S", "-o", str(prefix) + ".time", *command]
    elif mode == "syscalls":
        command = ["strace", "-qq", "-f", "-c", "-o", str(prefix) + ".strace", "--", *command]
    elif mode == "allocations":
        env["LD_PRELOAD"] = "/usr/lib/x86_64-linux-gnu/libmemusage.so"
    elif mode == "copies":
        command = ["valgrind", "--tool=dhat", "--mode=copy", "--dhat-out-file=" + str(prefix) + ".dhat.json", *command]
    else:
        raise ValueError("unsupported independent collection mode")
    save(str(prefix) + ".launcher.json", {"command": command, "collector": mode, "controller_pid": os.getpid(),
                                         "scope": "launched-process-threads-and-waited-children"})
    started_ns = time.perf_counter_ns()
    with Path(str(prefix) + ".stdout").open("wb") as out, Path(str(prefix) + ".stderr").open("wb") as err:
        process = subprocess.Popen(command, cwd=cwd, env=env, stdout=out, stderr=err)
        save(str(prefix) + ".identity.json", {"pid": process.pid, "command": command,
                                             "target_command": target_command,
                                             "started_epoch_ns": time.time_ns(), "binary_sha256": perf.digest(target_command[0])})
        status = process.wait()
    elapsed_ns = time.perf_counter_ns() - started_ns
    save(str(prefix) + ".exit.json", {"status": status})
    stderr = Path(str(prefix) + ".stderr").read_text()
    if status != 0:
        # GNU time returns the child's exit status; Rust's Termination error and
        # panic text identify child failures when a profiler is the launcher.
        child_failure = (mode == "native" and "cannot run" not in stderr) or bool(re.search(r"(?m)^Error:|thread .*panicked", stderr))
        raise CollectionFailure("functional-failure" if child_failure else "invalid-collection",
                                f"{mode} child/collector exited {status}; raw prefix: {prefix}", status,
                                status if child_failure else None)
    check_diagnostics(stderr)
    if mode == "native":
        fields = Path(str(prefix) + ".time").read_text().strip().split()
        if len(fields) != 5:
            raise ValueError("unknown GNU time format")
        metrics = perf.parse_time(" ".join(fields[:3]))
        if any(not re.fullmatch(r"\d+\.\d{2}", value) for value in fields[3:]):
            raise ValueError("unknown GNU time CPU granularity/format")
        metrics.update(elapsed_ns=elapsed_ns, user_cpu_seconds=float(fields[3]), system_cpu_seconds=float(fields[4]),
                       cpu_resolution_seconds=CPU_RESOLUTION_SECONDS,
                       cpu_reported_seconds={"user": fields[3], "system": fields[4]},
                       cpu_below_resolution={"user": float(fields[3]) == 0, "system": float(fields[4]) == 0})
        if any(not math.isfinite(metrics[key]) or metrics[key] < 0 for key in ("user_cpu_seconds", "system_cpu_seconds")):
            raise ValueError("invalid GNU time CPU observation")
        metrics["elapsed_resolution_ns"] = max(1, math.ceil(time.get_clock_info("perf_counter").resolution * 1_000_000_000))
        return metrics
    if mode == "syscalls":
        return perf.parse_strace(Path(str(prefix) + ".strace").read_text())
    if mode == "allocations":
        return parse_memusage(stderr)
    raw = Path(str(prefix) + ".dhat.json")
    data = json.loads(raw.read_text())
    raw.chmod(0o644)
    if data.get("pid") != process.pid or data.get("cmd") != " ".join(target_command):
        raise ValueError("DHAT target PID/command differs from trusted launcher")
    return perf.parse_dhat(data, "copy")


def qualify_collectors(output):
    directory = output / "collector-controls"
    directory.mkdir()
    source = Path(__file__).parent / "performance/control.c"
    binary = directory / "control"
    perf.run(["clang", "-g", "-O0", "-fno-builtin", str(source), "-o", str(binary)], directory, directory / "build.stdout", directory / "build.stderr")
    controls = {}
    for mode, control in (("syscalls", "syscall"), ("allocations", "allocation"), ("copies", "copy")):
        samples = []
        for count in (1000, 2000):
            sample = collect([str(binary), control, str(count)], directory, directory / f"{mode}-{count}", mode)
            if mode == "syscalls" and sample["syscalls"].get("getpid", {}).get("calls") != count:
                raise ValueError("strace calibration did not observe the exact getpid count")
            if mode == "allocations":
                rows = sample["malloc_family_rows"]
                if any(rows[name]["calls"] != count for name in ("malloc", "calloc", "realloc")) or sample["malloc_family_growth_bytes"] != count * 3072 or sample["heap_peak_bytes"] != 3072:
                    raise ValueError("glibc malloc/realloc-growth/peak calibration differs")
            if mode == "copies":
                sample = copy_witness(json.loads((directory / f"{mode}-{count}.dhat.json").read_text()), count)
            sample.update(requested_control_cycles=count, perturbation="known-workload" if count == 1000 else "declared-double-workload",
                          scope="external-tool-witness-process-lifecycle")
            samples.append(sample)
        controls[mode] = samples
    # Empty succeeds functionally; missing profiler output must still be refused.
    perf.run([str(binary), "empty", "1"], directory, directory / "empty.stdout", directory / "empty.stderr")
    try:
        perf.parse_strace("")
    except ValueError:
        controls["empty-collection-refused"] = True
    result = subprocess.run([str(binary), "invalid", "1"], cwd=directory, check=False)
    if result.returncode != 7:
        raise ValueError("invalid collector control did not fail")
    controls["invalid-child-exit"] = result.returncode
    controls["source_sha256"] = perf.digest(source)
    save(directory / "controls.json", controls)
    return controls


def scenario_pass(binary, checkout, output, request, mode, name):
    prefix = output / name
    request_path = Path(str(prefix) + ".request.json")
    save(request_path, request)
    metrics = collect([str(binary), str(request_path)], checkout, prefix, mode)
    observation = json.loads(Path(str(prefix) + ".stdout").read_text())
    check_scenario(observation, request)
    if mode == "native":
        metrics["native_run_ns"] = observation["native_run_ns"]
    # Instrumented durations are retained as raw data, never native metrics.
    return {"mode": mode, "state": "valid", "metrics": metrics, "request": request,
            "request_sha256": perf.digest(request_path), "binary_sha256": perf.digest(binary),
            "workload_identity": workload_identity(request),
            "fixture_bindings": {key: request[key] for key in ("topic", "service_url")},
            "completed": observation["completed"], "scope": observation["scope"] if mode == "native" else "scenario-process-lifecycle-including-harness",
            "denominator": observation["denominator"], "observation": observation, "artifact_prefix": name}


def check_example_allocations(samples, cycles, payload_bytes):
    for call in ("malloc", "realloc"):
        if samples[1]["malloc_family_rows"][call]["calls"] - samples[0]["malloc_family_rows"][call]["calls"] != cycles:
            raise ValueError("same-binary glibc allocation control increment differs")
    if samples[1]["realloc_net_growth_bytes"] - samples[0]["realloc_net_growth_bytes"] != cycles * payload_bytes:
        raise ValueError("same-binary glibc realloc growth increment differs")


def qualify_example(binary, checkout, output, revision):
    controls = {}
    for mode, scenario in (("syscalls", "control-syscall"), ("allocations", "control-allocation"), ("copies", "control-copy")):
        samples = []
        for multiplier in (1, 2):
            request = {"scenario_id": scenario, "scenario_revision": revision, "runtime": "none", "service_url": "",
                       "topic": "control", "messages": 1000, "payload_bytes": 256, "seed": 17,
                       "batching": False, "control_cost_multiplier": multiplier}
            samples.append(scenario_pass(binary, checkout, output, request, mode, f"example-control-{mode}-{multiplier}"))
        if mode == "syscalls" and samples[1]["metrics"]["syscalls"].get("write", {}).get("calls", 0) - samples[0]["metrics"]["syscalls"].get("write", {}).get("calls", 0) != 1000:
            raise ValueError("same-binary syscall control increment differs")
        if mode == "allocations":
            check_example_allocations([sample["metrics"] for sample in samples], request["messages"], request["payload_bytes"])
        controls[mode] = {"samples": samples, "purpose": "declared artificial cost perturbation; not product comparison",
                          "workload_interception": "unqualified: safe Rust copies may be inlined" if mode == "copies" else "known increment verified"}
    request = dict(request, scenario_id="control-empty", control_cost_multiplier=1)
    controls["empty"] = scenario_pass(binary, checkout, output, request, "native", "example-control-empty")
    request = dict(request, scenario_id="control-invalid")
    path = output / "example-control-invalid.request.json"
    save(path, request)
    try:
        collect([str(binary), str(path)], checkout, output / "example-control-invalid", "native")
    except CollectionFailure as error:
        if error.state != "functional-failure" or error.child_exit_code != 1:
            raise
        controls["invalid"] = {"state": "expected-functional-failure", "reason": str(error), "child_exit_code": error.child_exit_code}
    else:
        raise ValueError("same-binary invalid control was accepted")
    return controls


def scenario_semantic_identity(manifest, configuration, workload):
    compiled = manifest["input_manifest"]["compiler_sources"]
    # Keep the conservative full checkout closure: the example dep-info alone
    # does not establish every dependent crate's compiler inputs.
    return {"checkout_inputs": manifest["source_manifest"], "lockfile_sha256": manifest["lockfile_sha256"],
            "compiler_inputs": compiled,
            "noncompiler_guards": {path: sha for path, sha in manifest["input_manifest"]["scenario_sources"].items() if path not in compiled},
            "configuration": configuration, "workload": workload}


def unattributed_input_changes(left, right):
    changed = {path for path in set(left["checkout_inputs"]) | set(right["checkout_inputs"])
               if left["checkout_inputs"].get(path) != right["checkout_inputs"].get(path)}
    # Exact lockfiles are independently verified. Every other changed path
    # requires compiler input proof from both references, regardless of suffix.
    return sorted(changed - {"Cargo.lock"} - (set(left["compiler_inputs"]) & set(right["compiler_inputs"])))


def summarize_scenarios(manifests, observations):
    rows = []
    calibration_rows = []
    same_elf = perf.known_identity(manifests["base"].get("binary_sha256")) and manifests["base"]["binary_sha256"] == manifests["candidate"].get("binary_sha256")
    identity = manifests["base"].get("semantic_identity")
    required = ("checkout_inputs", "lockfile_sha256", "compiler_inputs", "noncompiler_guards", "configuration", "workload")
    inputs_known = all(isinstance(manifests[side].get("semantic_identity"), dict) and
                       all(isinstance(manifests[side]["semantic_identity"].get(field), dict) for field in required if field != "lockfile_sha256") and
                       all(perf.known_identity(manifests[side]["semantic_identity"].get(field)) for field in required if field != "noncompiler_guards") and
                       all(perf.known_identity(value) for value in manifests[side]["semantic_identity"]["noncompiler_guards"].values()) for side in ("base", "candidate"))
    same_inputs = inputs_known and identity == manifests["candidate"]["semantic_identity"]
    unknown_changes = unattributed_input_changes(identity, manifests["candidate"]["semantic_identity"]) if inputs_known else []
    compatible = not inputs_known or all(identity[key] == manifests["candidate"]["semantic_identity"][key] for key in ("configuration", "workload", "noncompiler_guards"))
    calibration = (same_inputs or same_elf) and compatible
    measurable = inputs_known and compatible and not unknown_changes
    kind = "calibration-identical-inputs" if same_inputs else "calibration-identical-elf" if same_elf else "base-candidate"
    for mode in MODES:
        for metric, unit in UNITS.items():
            left = [row["metrics"][metric] for row in observations["base"] if row["mode"] == mode and metric in row["metrics"]]
            right = [row["metrics"][metric] for row in observations["candidate"] if row["mode"] == mode and metric in row["metrics"]]
            if left or right:
                work = {json.dumps(row["workload_identity"], sort_keys=True) for row in observations["base"] + observations["candidate"] if row["mode"] == mode}
                if len(work) != 1:
                    raise ValueError("scenario semantic work differs between references")
                row = comparison_row(mode, metric, unit, left, right, calibration)
                row["comparison_kind"] = kind
                if not calibration and not measurable:
                    reason = "unmeasured: snapshot changes outside compiler input proof; attribution unknown" if unknown_changes else "unmeasured: incompatible configuration/workload/helper guards" if not compatible else "unmeasured: missing complete product/configuration identity"
                    row.update(product_effect=None, delta=None, relative_percent=None, verdict=reason)
                rows.append(row)
            calibrated = [row["metrics"][metric] for row in observations["calibration"] if row["mode"] == mode and metric in row["metrics"]]
            if calibrated:
                calibration_rows.append(comparison_row(mode, metric, unit, calibrated[::2], calibrated[1::2], True))
    return {"comparison": rows, "comparison_kind": kind, "elf_reproducibility": "identical" if same_elf else "different",
            "product_effect": None if calibration or not measurable else "informative-cost-comparison", "calibration_summary": calibration_rows,
            "comparison_state": "calibration" if calibration else "comparable" if measurable else "unmeasured",
            "unattributed_input_changes": unknown_changes,
            "cpu_resolution_seconds": CPU_RESOLUTION_SECONDS}


def write_scenario_report(report, output):
    rows = report["comparison"]
    calibration_rows = report["calibration_summary"]
    comparison_label = {"calibration-identical-elf": "identical ELF calibration; no product effect",
                        "calibration-identical-inputs": "identical source/configuration calibration; no product effect"}.get(report["comparison_kind"], "base/candidate informative costs")
    if report.get("comparison_state") == "unmeasured":
        comparison_label = "base/candidate unmeasured effect; input identity is not qualified"
    save(output / "scenario-report.json", report)
    text = ["# Magnetar isolated scenario measurements", "", "State: partial. Valid higher costs are informative.",
            "Comparison: " + comparison_label + "; ELF reproducibility: " + report.get("elf_reproducibility", "unrecorded"),
            "GNU time CPU granularity: 0.01 s. Values below resolution are displayed <0.01; this is not zero CPU cost or timing accuracy.",
            "Whole-lifecycle profiles include harness/setup/warmup/drain; native run metrics use the example's declared window.",
            "DHAT observes intercepted library copies; glibc malloc-family is not every Rust allocation.", "",
            "| Pass / scope | Metric / unit | Main median | PR median | Absolute delta | Relative delta | Verdict |", "| --- | --- | --- | --- | --- | --- | --- |"]
    for row in rows:
        delta = "unmeasured" if row["delta"] is None else f"{row['delta']:+g}"
        relative = "unmeasured" if row["relative_percent"] is None else f"{row['relative_percent']:+.2f}%"
        text.append(f"| {row['mode']} / {row['unit'].split('/', 1)[-1]} | {row['metric']} / {row['unit']} | {row['base_display']} | {row['candidate_display']} | {delta} | {relative} | {row['verdict']} |")
    text.extend(["", "Base/base calibration (alternating samples of the same base ELF):", "",
                 "| Mode | Metric / unit | First samples | Alternating samples | Observed range |",
                 "| --- | --- | --- | --- | --- |"])
    for row in calibration_rows:
        text.append(f"| {row['mode']} | {row['metric']} / {row['unit']} | {row['base_display']} | {row['candidate_display']} | {row['base_range']} / {row['candidate_range']} |")
    text.extend(["", "Uncovered dimensions:", "", *["- " + gap for gap in GAPS], "", "Exact inputs, scopes, calibration and raw samples: [scenario-report.json](scenario-report.json).", ""])
    (output / "scenario-report.md").write_text("\n".join(text))


def internal_scenarios(args):
    output = perf.output_directory(args.output, [args.base, args.candidate])
    launch = json.loads(args.environment.read_text())
    tools = {package: perf.read_command(["dpkg-query", "-W", "-f=${Version}", package], output) for package in PACKAGES}
    check_environment(launch, tools, perf.read_command(["rustc", "-V"], output), os.sched_getaffinity(0))
    launch.update(packages=tools, compiler=perf.read_command(["rustc", "-Vv"], output),
                  cargo=perf.read_command(["cargo", "-V"], output), kernel=platform.release(),
                  cpuinfo_sha256=perf.digest("/proc/cpuinfo"), harness_sha256=perf.digest(__file__),
                  example_sha256=perf.digest(args.candidate / "crates/magnetar/examples/performance.rs"))
    manifests = {}
    binaries = {}
    for side, checkout, expected, overlay in (("base", args.base, args.expected_base_sha, args.base_overlay),
                                               ("candidate", args.candidate, args.expected_candidate_sha, args.candidate_overlay)):
        perf.check_expected_revision(checkout, expected)
        manifest = perf.source_snapshot(checkout, overlay)
        if perf.digest(checkout / "crates/magnetar/examples/performance.rs") != launch["example_sha256"]:
            raise ValueError("base/candidate must use exactly the same audited example bytes")
        directory = output / side
        directory.mkdir()
        target = args.build_cache if args.build_cache else directory / "target"
        env = dict(os.environ, **perf.PROFILE_ENV, CARGO_TARGET_DIR=str(target))
        command = ["cargo", "build", "-p", "magnetar-driver", "--example", "performance", "--features", "moonpool,scalable-topics", "--release", "--locked"]
        try:
            perf.run(command, checkout, directory / "build.stdout", directory / "build.stderr", env)
        except RuntimeError as error:
            raise CollectionFailure("functional-failure", f"{side} build failed: {error}") from error
        dep_info = directory / "performance.dep-info"
        dep_info.write_bytes((target / "release/examples/performance.d").read_bytes())
        inputs = perf.scenario_identity(checkout, "crates/magnetar/examples/performance.rs", [], dep_info)
        binary = directory / "performance"
        shutil.copyfile(target / "release/examples/performance", binary)
        binary.chmod(0o755)
        perf.run(["readelf", "-Ws", str(binary)], checkout, directory / "symbols", directory / "symbols.stderr")
        notes = perf.read_command(["readelf", "--notes", str(binary)], checkout)
        (directory / "elf-notes").write_text(notes + "\n")
        build_id = re.findall(r"Build ID: ([0-9a-f]+)", notes)
        if len(build_id) != 1:
            raise ValueError("missing or ambiguous scenario binary build ID")
        perf.run(["ldd", str(binary)], checkout, directory / "ldd", directory / "ldd.stderr")
        if "libc.so.6" not in (directory / "ldd").read_text() or " malloc@" not in (directory / "symbols").read_text():
            raise ValueError("scenario binary lacks qualified dynamic glibc allocator symbols")
        if perf.source_snapshot(checkout, overlay) != manifest:
            raise ValueError("source changed during scenario build")
        source_tar = directory / "sources.tar"
        with tarfile.open(source_tar, "w", dereference=True) as archive:
            for path in manifest["source_manifest"]:
                archive.add(checkout / path, arcname=path, recursive=False)
        manifests[side] = {**manifest, "lockfile_sha256": perf.digest(checkout / "Cargo.lock"), "binary_sha256": perf.digest(binary),
                           "build_id": build_id[0], "allocator": "Rust System through dynamically linked glibc malloc-family",
                           "source_artifact_sha256": perf.digest(source_tar), "build_command": command,
                           "input_manifest": inputs, "dep_info_sha256": perf.digest(dep_info),
                           "input_scope": "compiler dependency closure plus conservative local test/fixture closure"}
        configuration = {key: launch[key] for key in ("compiler", "cargo", "image_id", "image_base", "dockerfile_sha256", "harness_sha256", "example_sha256", "profile", "features")}
        configuration["build_command"] = command
        work = {"scenario_id": args.scenario, "scenario_revision": launch["example_sha256"], "runtime": args.runtime,
                "messages": args.messages, "payload_bytes": args.payload_bytes, "seed": 17, "batching": False, "control_cost_multiplier": 1}
        manifests[side]["semantic_identity"] = scenario_semantic_identity(manifests[side], configuration, work)
        binaries[side] = binary
    controls = qualify_collectors(output)
    example_controls = {}
    for side, checkout in (("base", args.base), ("candidate", args.candidate)):
        example_controls[side] = qualify_example(binaries[side], checkout, output / side, launch["example_sha256"])
    observations = {"calibration": [], "base": [], "candidate": []}
    scenario_revision = launch["example_sha256"]
    # Both builds and tool controls have finished before any retained native time.
    for stage in ("calibration", "comparison"):
        if stage == "comparison" and args.calibration_only:
            continue
        for mode in MODES:
            for repetition in range(args.repetitions):
                order = ["calibration"] if stage == "calibration" else ["base", "candidate"] if repetition % 2 == 0 else ["candidate", "base"]
                for side in order:
                    reference = "base" if side == "calibration" else side
                    checkout = args.base if reference == "base" else args.candidate
                    request = {"scenario_id": args.scenario, "scenario_revision": scenario_revision, "runtime": args.runtime,
                               "service_url": args.service_url, "topic": "persistent://public/default/magnetar-perf-" + uuid.uuid4().hex,
                               "messages": args.messages, "payload_bytes": args.payload_bytes, "seed": 17, "batching": False, "control_cost_multiplier": 1}
                    result = scenario_pass(binaries[reference], checkout, output / reference, request, mode, f"{side}-{mode}-{repetition}")
                    observations[side].append(result)
                    print(f"{side}/{mode}/{repetition}: {result['completed']} completed", flush=True)
    report = {"schema_version": 1, "state": "partial", "environment": launch, "manifests": manifests,
              "controls": controls, "example_controls": example_controls, "observations": observations,
              **summarize_scenarios(manifests, observations), "uncovered": GAPS,
              "family_equivalents": [], "performance_policy": "informative; functional/invalid collection failures exit nonzero"}
    write_scenario_report(report, output)
    for side, checkout, overlay in (("base", args.base, args.base_overlay), ("candidate", args.candidate, args.candidate_overlay)):
        dep_info = output / side / "performance.dep-info"
        if perf.digest(dep_info) != manifests[side]["dep_info_sha256"] or perf.scenario_identity(checkout, "crates/magnetar/examples/performance.rs", [], dep_info) != manifests[side]["input_manifest"]:
            raise ValueError("compiled scenario input closure changed during campaign")
        snapshot = perf.source_snapshot(checkout, overlay)
        if any(snapshot[key] != manifests[side][key] for key in snapshot):
            raise ValueError("source changed during scenario campaign")
    write_artifact_manifest(output)
    print(f"scenario campaign: {output}/scenario-report.md; state=partial", flush=True)
    return 0


def suite_environment(launch, output):
    tools = {package: perf.read_command(["dpkg-query", "-W", "-f=${Version}", package], output) for package in PACKAGES}
    compiler = perf.read_command(["rustc", "-Vv"], output)
    check_environment(launch, tools, compiler, os.sched_getaffinity(0), suite=True)
    return {"image_id": launch["image_id"], "dockerfile_sha256": launch["dockerfile_sha256"],
            "runner": launch["runner_identity"], "cpu_affinity": launch["cpu_affinity"],
            "broker_digests": launch.get("fixture_image_ids", [launch["broker"]["image"]] if launch.get("broker") else []),
            "fixture_scope": launch.get("fixture_scope", "external-private-broker"), "toolchain": compiler, "features": launch["features"],
            "seed": str(launch["seed"]), "pip33_fixture": launch.get("pip33_fixture"), "campaign_environment": launch}


def internal_suite(args):
    output = perf.output_directory(args.output, [args.base, args.candidate])
    launch = json.loads(args.environment.read_text())
    if launch.get("axis", "workspace-all-features") != args.axis or launch["seed"] != args.seed:
        raise ValueError("requested axis/seed differs from launcher environment")
    environment = suite_environment(launch, output)
    # Native suite processes inherit a fixed seed; both reference builds finish first.
    os.environ["MOONPOOL_SEED"] = str(launch["seed"])
    status = perf.compare_checkouts(args, environment)
    report = json.loads((output / "report.json").read_text())
    report.update(environment=launch, uncovered=GAPS,
                  performance_policy="informative costs; functional/invalid collection failures exit nonzero")
    for side, checkout in (("base", args.base), ("candidate", args.candidate)):
        manifest = report["manifests"][side]
        source_tar = output / side / "sources.tar"
        with tarfile.open(source_tar, "w", dereference=True) as archive:
            for path in sorted(set(manifest["provenance"]["source_manifest"]) | set(manifest["compiled_input_manifest"])):
                archive.add(checkout / path, arcname=path, recursive=False)
        manifest["source_artifact_sha256"] = perf.digest(source_tar)
        save(output / side / "inventory.json", manifest)
    perf.write_report(report, output)
    write_artifact_manifest(output)
    return status


def container_execution_options(cargo_home, docker_socket=False):
    identity = {"uid": os.getuid(), "gid": os.getgid(), "home": "/performance-home",
                "cargo_home": cargo_home, "docker_socket_gid": None}
    options = ["--user", f"{identity['uid']}:{identity['gid']}", "--tmpfs", identity["home"] +
               f":rw,size=16m,mode=0700,uid={identity['uid']},gid={identity['gid']}",
               "-e", "HOME=" + identity["home"], "-e", "CARGO_HOME=" + cargo_home]
    if docker_socket:
        socket_stat = Path("/var/run/docker.sock").stat()
        if not stat.S_ISSOCK(socket_stat.st_mode):
            raise ValueError("fixture Docker endpoint is not a Unix socket")
        identity["docker_socket_gid"] = socket_stat.st_gid
        options.extend(["--group-add", str(socket_stat.st_gid)])
    return identity, options


def launch_container(args):
    for checkout, expected in ((args.base, args.expected_base_sha), (args.candidate, args.expected_candidate_sha)):
        perf.check_expected_revision(checkout, expected)
    output = perf.output_directory(args.output, [args.base, args.candidate])
    if any(output.iterdir()):
        raise ValueError("campaign output must be a new empty directory")
    image = json.loads(perf.read_command(["docker", "image", "inspect", args.image_id], output))[0]
    save(output / "image-inspect.json", image)
    dockerfile_sha256 = perf.digest(Path(__file__).parent / "performance/Dockerfile")
    check_image(image, args.image_id, dockerfile_sha256)
    broker = None
    if not args.suite:
        broker = json.loads(perf.read_command(["docker", "inspect", args.broker_container], output))[0]
        label_key, label_value = args.fixture_label.split("=", 1)
        if broker["Id"] != args.broker_container or broker["Image"] != args.broker_image_id or broker["Config"]["Labels"].get(label_key) != label_value or not broker["State"]["Running"]:
            raise ValueError("task-owned broker identity, image, label or running state differs")
        ports = broker["NetworkSettings"]["Ports"].get("6650/tcp", [])
        if not any(row["HostIp"] == "127.0.0.1" and args.service_url == "pulsar://127.0.0.1:" + row["HostPort"] for row in ports):
            raise ValueError("service endpoint is not the broker's inspected private loopback binding")
    affinity = sorted(os.sched_getaffinity(0))[:args.cpu_count]
    if len(affinity) != args.cpu_count:
        raise ValueError("requested CPU count exceeds available affinity")
    launch = {"image_id": image["Id"], "image_base": image["Config"]["Labels"]["org.opencontainers.image.base.digest"],
              "dockerfile_sha256": image["Config"]["Labels"]["magnetar.performance.dockerfile-sha256"],
              "image_inspect_sha256": perf.digest(output / "image-inspect.json"),
              "runner_identity": hashlib.sha256(Path("/etc/machine-id").read_bytes()).hexdigest(),
              "cpu_affinity": affinity, "broker": {"id": broker["Id"], "image": broker["Image"],
              "name": broker["Name"], "label": args.fixture_label, "ports": broker["NetworkSettings"]["Ports"]} if broker else None,
              "fixture_scope": "test-suite-owned-fixtures" if args.suite and args.fixture_images else "no-external-fixture" if args.suite else "external-private-broker",
              "profile": perf.PROFILE_ENV, "features": perf.axis_configuration(args.axis, args.package)[2] if args.suite else ["moonpool", "scalable-topics"],
              "axis": args.axis, "seed": args.seed}
    pip33 = None
    if args.pip33_fixture:
        pip33 = json.loads(args.pip33_fixture.read_text())
        pip33["containers"] = json.loads(perf.read_command(["docker", "inspect", *[row["Id"] for row in pip33["containers"]]], output))
        perf.check_pip33_fixture(pip33)
        if dict(binding.split("=", 1) for binding in args.binding) != pip33["bindings"]:
            raise ValueError("PIP-33 child bindings differ from inspected fixture")
        launch["pip33_fixture"] = pip33
        save(output / "pip33-before.json", pip33)
    environment = output / "environment.json"
    save(environment, launch)
    cargo_home = args.cargo_cache.resolve()
    cargo_home.mkdir(parents=True, exist_ok=True)
    if args.build_cache:
        args.build_cache = perf.output_directory(args.build_cache, [args.base, args.candidate])
    if args.docker_socket and not args.suite:
        raise ValueError("Docker socket is available only to the explicit fixture-owning suite worker")
    execution_identity, execution_options = container_execution_options("/cargo", args.docker_socket)
    launch["execution_identity"] = execution_identity
    save(environment, launch)
    mounts = {args.base, args.candidate, Path(__file__).parent.resolve()}
    for checkout in (args.base, args.candidate):
        mounts.add(Path(perf.read_command(["git", "rev-parse", "--path-format=absolute", "--git-common-dir"], checkout)))
    for overlay in (args.base_overlay, args.candidate_overlay):
        if overlay:
            mounts.add(overlay.resolve())
    command = ["docker", "create", "--network", "host", "--cpuset-cpus", ",".join(map(str, affinity)),
               "--label", "magnetar.performance.campaign=" + output.name, *execution_options]
    home_options = f"rw,size=16m,mode=0700,uid={execution_identity['uid']},gid={execution_identity['gid']}"
    for path in sorted(mounts):
        command.extend(["--mount", f"type=bind,src={path},dst={path},readonly"])
    command.extend(["--mount", f"type=bind,src={output},dst={output}", "--mount", f"type=bind,src={cargo_home},dst=/cargo",
                    "-e", "CARGO_BUILD_JOBS=" + str(args.cpu_count),
                    "-e", "PYTHONDONTWRITEBYTECODE=1", "-e", "GIT_CONFIG_GLOBAL=/dev/null", "-e", "GIT_CONFIG_SYSTEM=/dev/null",
                    "-e", "GIT_CONFIG_COUNT=1", "-e", "GIT_CONFIG_KEY_0=safe.directory", "-e", "GIT_CONFIG_VALUE_0=*",
                    args.image_id, "python3", str(Path(__file__).resolve()), "--base", str(args.base), "--candidate", str(args.candidate),
                    "--expected-base-sha", args.expected_base_sha, "--expected-candidate-sha", args.expected_candidate_sha,
                    "--output", str(output), "--environment", str(environment), "--service-url", args.service_url,
                    "--repetitions", str(args.repetitions), "--scenario", args.scenario, "--runtime", args.runtime,
                    "--messages", str(args.messages), "--payload-bytes", str(args.payload_bytes)])
    command.extend(["--axis", args.axis, "--seed", str(args.seed)])
    if args.build_cache:
        # A mutable build cache is never a retained raw/source artifact.
        # Cargo still checks each reference with its own --locked inputs.
        command[2:2] = ["--mount", f"type=bind,src={args.build_cache.resolve()},dst={args.build_cache.resolve()}"]
        command.extend(["--build-cache", str(args.build_cache.resolve())])
        if args.build_session:
            command.extend(["--build-session", args.build_session])
    if args.docker_socket:
        command[2:2] = ["--mount", "type=bind,src=/var/run/docker.sock,dst=/var/run/docker.sock"]
    for binding in args.binding:
        name, value = binding.split("=", 1)
        if name not in ("MAGNETAR_PIP33_CLUSTER_A_URL", "MAGNETAR_PIP33_CLUSTER_B_URL", "MAGNETAR_PIP33_ADMIN_B_URL"):
            raise ValueError("unknown private fixture binding")
        command[2:2] = ["-e", name + "=" + value]
    if args.fixture_images:
        fixture_images = json.loads(args.fixture_images.read_text())
        save(output / "fixture-images-before.json", verify_fixture_images(fixture_images, output))
        launch["fixture_image_ids"] = sorted({row["image_id"] for row in fixture_images.values()} | ({broker["Image"]} if broker else set()))
        launch["fixture_images"] = fixture_images
        save(environment, launch)
    for option, path in (("--base-overlay", args.base_overlay), ("--candidate-overlay", args.candidate_overlay)):
        if path:
            command.extend([option, str(path.resolve())])
    if args.calibration_only:
        command.append("--calibration-only")
    if args.suite:
        command.extend(["--suite", "--shard", str(args.shard), "--shards", str(args.shards)])
        for package in args.package:
            command.extend(["--package", package])
    container_id = perf.read_command(command, output)
    event_process = None
    markers = {}
    if args.fixture_images:
        event_command = ["docker", "events", "--since", f"{time.time():.9f}", "--format", "{{json .}}",
                         "--filter", "type=container", "--filter", "type=image"]
        save(output / "fixture-events-command.json", event_command)
        with (output / "fixture-events.jsonl").open("wb") as events, (output / "fixture-events.stderr").open("wb") as errors:
            event_process = subprocess.Popen(event_command, stdout=events, stderr=errors)
    try:
        if event_process is not None:
            markers["ready"] = event_marker(image["Id"], output, "ready")
            wait_event_marker(output / "fixture-events.jsonl", markers["ready"], event_process)
            save(output / "fixture-event-markers.json", markers)
        container = json.loads(perf.read_command(["docker", "inspect", container_id], output))[0]
        if container["Image"] != image["Id"] or container["HostConfig"]["CpusetCpus"] != ",".join(map(str, affinity)):
            raise ValueError("actual campaign container image or CPU bindings differ")
        expected_groups = [str(execution_identity["docker_socket_gid"])] if args.docker_socket else []
        actual_environment = dict(value.split("=", 1) for value in container["Config"]["Env"])
        if (container["Config"]["User"] != f"{execution_identity['uid']}:{execution_identity['gid']}"
                or (container["HostConfig"]["GroupAdd"] or []) != expected_groups
                or actual_environment.get("HOME") != execution_identity["home"]
                or actual_environment.get("CARGO_HOME") != execution_identity["cargo_home"]
                or container["HostConfig"]["Tmpfs"].get(execution_identity["home"]) != home_options):
            raise ValueError("actual campaign user, socket group, private HOME or Cargo cache differs")
        save(output / "container-inspect.json", container)
        save(output / "container.json", {"id": container_id, "image": container["Image"], "cpu_affinity": container["HostConfig"]["CpusetCpus"],
             "command": container["Config"]["Cmd"], "user": container["Config"]["User"], "groups": expected_groups,
             "home": actual_environment["HOME"], "cargo_home": actual_environment["CARGO_HOME"], "tmpfs": container["HostConfig"]["Tmpfs"]})
        result = subprocess.run(["docker", "start", "--attach", container_id], check=False)
        final = json.loads(perf.read_command(["docker", "inspect", container_id], output))[0]
        status = final["State"]["ExitCode"]
        save(output / "container-exit.json", {"status": status, "docker_attach_status": result.returncode})
        if args.fixture_images:
            if event_process.poll() is not None:
                raise ValueError("fixture event observer exited before the suite finished")
            markers["end"] = event_marker(image["Id"], output, "end")
            wait_event_marker(output / "fixture-events.jsonl", markers["end"], event_process)
            save(output / "fixture-event-markers.json", markers)
            event_process.terminate()
            event_process.wait(timeout=10)
            save(output / "fixture-observer-exit.json", {"state": "terminated-after-end", "status": event_process.returncode})
            if event_process.returncode not in (0, -15, 143) or (output / "fixture-events.stderr").read_text().strip():
                raise ValueError("fixture event observer failed or reported diagnostics")
            events = [json.loads(line) for line in (output / "fixture-events.jsonl").read_text().splitlines()]
            report = json.loads((output / "report.json").read_text())
            save(output / "fixture-events-observed.json", check_fixture_events(events, fixture_images, report, markers))
            save(output / "fixture-images-after.json", verify_fixture_images(fixture_images, output))
        if pip33:
            pip33["containers"] = json.loads(perf.read_command(["docker", "inspect", *[row["Id"] for row in pip33["containers"]]], output))
            perf.check_pip33_fixture(pip33)
            save(output / "pip33-after.json", pip33)
        write_artifact_manifest(output)
        verify_artifact_manifest(output / "artifacts.json")
        write_artifact_manifest(output, "campaign-artifacts.json", required=("artifacts.json", "container.json", "environment.json", "container-exit.json"))
        verify_artifact_manifest(output / "campaign-artifacts.json")
        return status if status else result.returncode
    finally:
        if event_process is not None and event_process.poll() is None:
            event_process.terminate()
            event_process.wait(timeout=10)
        for marker in markers.values():
            current_marker = json.loads(perf.read_command(["docker", "inspect", marker["id"]], output))[0]
            if current_marker["Id"] != marker["id"] or current_marker["Image"] != marker["image_id"] or current_marker["Config"]["Labels"].get("magnetar.performance.observer") != marker["label"]:
                raise ValueError("refusing cleanup: observer marker identity differs")
            subprocess.run(["docker", "rm", marker["id"]], check=True)
        current = json.loads(perf.read_command(["docker", "inspect", container_id], output))[0]
        if current["Id"] != container_id or current["Config"]["Labels"].get("magnetar.performance.campaign") != output.name:
            raise ValueError("refusing cleanup: campaign container identity differs")
        subprocess.run(["docker", "rm", container_id], check=True)


def event_marker(image_id, output, phase):
    label = output.name + ":" + phase + ":" + uuid.uuid4().hex
    ident = perf.read_command(["docker", "create", "--label", "magnetar.performance.observer=" + label, image_id, "/bin/true"], output)
    marker = {"id": ident, "label": label, "image_id": image_id}
    save(output / ("fixture-marker-" + phase + ".json"), json.loads(perf.read_command(["docker", "inspect", ident], output))[0])
    return marker


def wait_event_marker(path, marker, process, timeout=10):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if process.poll() is not None:
            raise ValueError("fixture observer exited before a required barrier")
        lines = path.read_text().splitlines(keepends=True)
        for line in lines:
            if not line.endswith("\n"):
                continue
            event = json.loads(line)
            if event.get("Type") == "container" and event.get("Action") == "create" and event.get("Actor", {}).get("ID") == marker["id"]:
                if event["Actor"]["Attributes"].get("magnetar.performance.observer") != marker["label"]:
                    raise ValueError("fixture observer barrier label differs")
                return
        time.sleep(0.02)
    raise ValueError("fixture observer did not capture its required barrier within 10 seconds")


def check_fixture_events(events, images, report=None, markers=None):
    if report is None or markers is None:
        raise ValueError("fixture event collection lacks assigned scope and ready/end proof")
    if set(markers) != {"ready", "end"} or len({marker["id"] for marker in markers.values()}) != 2 or len({marker["label"] for marker in markers.values()}) != 2:
        raise ValueError("fixture observer marker identities are incomplete or duplicated")
    references = {reference: frozen["image_id"] for tag, frozen in images.items()
                  for reference in (tag, frozen["reference"], frozen["image_id"])}
    observed = []
    captured = {}
    for event in events:
        if event.get("Type") not in ("container", "image") or not event.get("Action"):
            raise ValueError("unknown Docker fixture event format")
        if event["Type"] == "image":
            raise ValueError("fixture image changed during suite execution: " + event["Action"])
        if event["Action"] == "create":
            actor = event.get("Actor")
            if not isinstance(actor, dict) or not isinstance(actor.get("Attributes"), dict) or not re.fullmatch(r"[0-9a-f]{64}", actor.get("ID", "")) or not isinstance(event.get("timeNano"), int):
                raise ValueError("fixture create event lacks exact identity/time/attributes")
            if actor["ID"] in {marker["id"] for marker in markers.values()}:
                phase = next(phase for phase, marker in markers.items() if marker["id"] == actor["ID"])
                marker = markers[phase]
                if actor["Attributes"].get("magnetar.performance.observer") != marker["label"] or actor["Attributes"].get("image") != marker["image_id"] or phase in captured:
                    raise ValueError("fixture event barrier identity differs or is duplicated")
                captured[phase] = event["timeNano"]
                continue
            reference = actor["Attributes"].get("image")
            if reference not in references:
                raise ValueError("created fixture image is absent from frozen inventory: " + str(reference))
            if not isinstance(event.get("timeNano"), int) or not re.fullmatch(r"[0-9a-f]{64}", actor["ID"]):
                raise ValueError("fixture event lacks exact identity/time")
            observed.append({"container_id": actor["ID"], "reference": reference, "image_id": references[reference], "created_epoch_ns": event["timeNano"]})
    if set(captured) != {"ready", "end"} or captured["ready"] >= captured["end"] or len({row["container_id"] for row in observed}) != len(observed):
        raise ValueError("fixture event collection is empty, truncated or duplicated")
    proofs = []
    claimed = set()
    for side in ("base", "candidate"):
        entries = {entry["family_id"]: entry for entry in report["manifests"][side]["families"]}
        samples = report["observations"][side] + (report["base_base_calibration"] if side == "base" else [])
        for sample in samples:
            entry = entries[sample["family_id"]]
            policy = entry.get("fixture_policy")
            if not policy or policy.get("scope") not in ("fixture-free", "fixture-required") or not policy.get("basis"):
                raise ValueError("assigned family lacks an explicit fixture declaration")
            runnable = any(not test["ignored"] for test in entry["tests"])
            scope = policy["scope"] if runnable else "fixture-free"
            if sample.get("fixture_scope") != scope:
                raise ValueError("observation fixture scope differs from assigned catalogue")
            if sample["state"] != "valid" and "started_epoch_ns" not in sample:
                continue
            start, end = sample.get("started_epoch_ns"), sample.get("finished_epoch_ns")
            if not isinstance(start, int) or not isinstance(end, int) or not captured["ready"] <= start < end <= captured["end"]:
                raise ValueError("observation lacks a complete ready/end-bounded fixture window")
            children = [row for row in observed if start <= row["created_epoch_ns"] <= end]
            if sample["state"] == "valid" and ((scope == "fixture-required" and not children) or (scope == "fixture-free" and children)):
                raise ValueError("observed fixture creations contradict the assigned observation scope")
            if any(row["container_id"] in claimed for row in children):
                raise ValueError("one fixture creation was credited to multiple observations")
            claimed.update(row["container_id"] for row in children)
            proofs.append({"side": side, "family_id": sample["family_id"], "repetition": sample["repetition"], "scope": scope,
                           "state": sample["state"],
                           "started_epoch_ns": start, "finished_epoch_ns": end, "container_ids": [row["container_id"] for row in children]})
    if claimed != {row["container_id"] for row in observed}:
        raise ValueError("fixture create is outside all attributed valid observations")
    return {"markers": markers, "captured_epoch_ns": captured, "observed": observed, "observations": proofs,
            "scope": "per-observation positive fixture use; instance-count completeness is not claimed"}


def run_functional_contract(command, cwd, stdout, stderr):
    try:
        perf.run(command, cwd, stdout, stderr)
    except perf.CommandFailure as error:
        raise CollectionFailure("functional-failure", str(error), child_exit_code=error.status) from error


def example_contract_completion(text):
    summaries = re.findall(r"test result: (ok|FAILED)\. (\d+) passed; (\d+) failed; (\d+) ignored; (\d+) measured; (\d+) filtered out;", text)
    if len(summaries) != 1:
        raise ValueError("missing or ambiguous example contract result")
    _, passed, _, ignored, _, _ = summaries[0]
    completed = perf.test_completion(text, int(passed) + int(ignored), int(ignored))
    return {"passed": completed, "ignored": int(ignored), "catalogued": int(passed) + int(ignored)}


def ci_checkout(candidate, base, expected_base, expected_candidate, output):
    perf.check_expected_revision(candidate, expected_candidate)
    if not base.exists():
        perf.read_command(["git", "fetch", "--no-tags", "origin", expected_base], candidate)
        perf.read_command(["git", "worktree", "add", "--detach", str(base), expected_base], candidate)
    perf.check_expected_revision(base, expected_base)
    source = base / EXAMPLE_SOURCE
    source.parent.mkdir(parents=True, exist_ok=True)
    source.write_bytes((candidate / EXAMPLE_SOURCE).read_bytes())
    records = subprocess.run(["git", "status", "--porcelain=v1", "--untracked-files=all", "-z"], cwd=base,
                             check=True, text=True, capture_output=True).stdout.split("\0")
    dirty = [{"status": record[:2], "path": record[3:], "sha256": perf.digest(base / record[3:])}
             for record in records if record]
    if any(row["path"] != EXAMPLE_SOURCE for row in dirty):
        raise ValueError("CI baseline overlay contains an unaudited product change")
    overlay = output / "base-overlay.json"
    save(overlay, {"schema_version": 1, "measured_sha": expected_base, "files": dirty})
    perf.source_snapshot(base, overlay)
    perf.source_snapshot(candidate)
    return overlay


def ci_harness(candidate):
    files = ("scripts/performance.py", "scripts/performance_campaign.py", "scripts/performance/Dockerfile",
             "scripts/performance/control.c", EXAMPLE_SOURCE,
             "crates/magnetar/tests/fixtures/docker-compose.replicated-subs.yml")
    return {path: perf.digest(candidate / path) for path in files}


def ci_plan(args):
    """Runs inside the inspected image; planning invokes metadata, never builds."""
    output = perf.output_directory(args.output, [args.base, args.candidate])
    payload = json.loads(args.payload.read_text())
    compiler = perf.read_command(["rustc", "-Vv"], output)
    tools = {package: perf.read_command(["dpkg-query", "-W", "-f=${Version}", package], output) for package in PACKAGES}
    if tools != PACKAGES or not compiler.startswith("rustc 1.98.1 "):
        raise ValueError("planning compiler/tools differ from the qualified image")
    for axis in perf.AXES:
        for side, checkout in (("base", args.base), ("candidate", args.candidate)):
            directory = output / "plans" / axis / side
            directory.mkdir(parents=True)
            environment = {"image_id": payload["image_id"], "dockerfile_sha256": payload["dockerfile_sha256"],
                           "toolchain": compiler, "features": perf.axis_configuration(axis, [])[2], "seed": "17"}
            plan = perf.plan_inventory(checkout, directory, [], payload["revisions"][side],
                                       args.base_overlay if side == "base" else None, args.shards if axis == "workspace-all-features" else 1,
                                       environment, axis)
            if axis == "moonpool-no-buggify":
                for seed in payload["seed_union"]["seeds"]:
                    replay = copy.deepcopy(plan)
                    replay["execution_contract"]["seed"] = str(seed)
                    save(directory / (str(seed) + ".json"), replay)
    save(output / "planning-tools.json", {"compiler": compiler, "packages": tools,
         "execution_identity": {"uid": os.getuid(), "gid": os.getgid(), "home": os.environ["HOME"], "cargo_home": os.environ["CARGO_HOME"]}})


def ci_prepare(args):
    candidate = args.candidate.resolve(); base = args.base.resolve()
    output = perf.output_directory(args.output, [candidate, base])
    if any(output.iterdir()):
        raise ValueError("CI prepare output must be a new empty directory")
    overlay = ci_checkout(candidate, base, args.expected_base_sha, args.expected_candidate_sha, output)
    dockerfile = candidate / "scripts/performance/Dockerfile"
    perf.run(["docker", "build", "--iidfile", str(output / "image.id"), "--build-arg",
              "MAGNETAR_DOCKERFILE_SHA256=" + perf.digest(dockerfile), "-f", str(dockerfile),
              str(dockerfile.parent)], output, output / "image-build.stdout", output / "image-build.stderr")
    image_id = (output / "image.id").read_text().strip()
    image = json.loads(perf.read_command(["docker", "image", "inspect", image_id], output))[0]
    check_image(image, image_id, perf.digest(dockerfile)); save(output / "image-inspect.json", image)
    fixture_images = {}
    for index, tag in enumerate(FIXTURE_TAGS):
        perf.run(["docker", "pull", tag], output, output / f"fixture-pull-{index}.stdout", output / f"fixture-pull-{index}.stderr")
        observed = json.loads(perf.read_command(["docker", "image", "inspect", tag], output))[0]
        repository = tag.rsplit(":", 1)[0]
        digests = [value for value in observed.get("RepoDigests", []) if value.startswith(repository + "@sha256:")]
        if len(digests) != 1:
            raise ValueError("fixture requires one observed registry manifest digest: " + tag)
        fixture_images[tag] = {"image_id": observed["Id"], "reference": digests[0]}
        save(output / f"fixture-inspect-{index}.json", observed)
    payload = {"schema_version": 1, "baseline": "main", "revisions": {"base": args.expected_base_sha, "candidate": args.expected_candidate_sha},
               "image_id": image_id, "dockerfile_sha256": perf.digest(dockerfile), "harness": ci_harness(candidate),
               "fixture_images": fixture_images, "shards": args.shards, "seed_workers": args.seed_workers,
               "seed_union": perf.seed_union({side: checkout / SEED_REGISTRY for side, checkout in (("base", base), ("candidate", candidate))}),
               "scenario_contract": {"messages": 1024, "payload_bytes": 1024, "seed": 17, "batching": False, "control_cost_multiplier": 1, "repetitions": 2}}
    save(output / "ci-input.json", payload)
    save(output / "fixture-images.json", fixture_images)
    execution_identity, execution_options = container_execution_options("/tmp/performance-cargo")
    save(output / "prepare-execution-identity.json", execution_identity)
    common = ["docker", "run", "--rm", "--network", "host", *execution_options]
    mounts = {candidate, base, output, Path(perf.read_command(["git", "rev-parse", "--path-format=absolute", "--git-common-dir"], candidate))}
    for path in sorted(mounts):
        common.extend(["--mount", f"type=bind,src={path},dst={path}" + ("" if path == output else ",readonly")])
    common.extend(["-e", "PYTHONDONTWRITEBYTECODE=1", "-e", "GIT_CONFIG_GLOBAL=/dev/null", "-e", "GIT_CONFIG_SYSTEM=/dev/null",
                   "-e", "GIT_CONFIG_COUNT=1", "-e", "GIT_CONFIG_KEY_0=safe.directory", "-e", "GIT_CONFIG_VALUE_0=*", image_id])
    command = common + ["python3", str(candidate / "scripts/performance_campaign.py"), "ci", "plan", "--base", str(base),
                        "--candidate", str(candidate), "--base-overlay", str(overlay), "--output", str(output),
                        "--payload", str(output / "ci-input.json"), "--shards", str(args.shards)]
    perf.run(command, output, output / "planning.stdout", output / "planning.stderr")
    save(output / "planning-command.json", command)
    planning_tools = json.loads((output / "planning-tools.json").read_text())
    if planning_tools["execution_identity"] != {key: execution_identity[key] for key in ("uid", "gid", "home", "cargo_home")}:
        raise ValueError("planning container user, private HOME or Cargo cache differs")
    payload["compiler"] = planning_tools["compiler"]
    payload["source_snapshots"] = {side: json.loads((output / "plans/workspace-all-features" / side / "plan.json").read_text())["snapshot"] for side in ("base", "candidate")}
    save(output / "ci-input.json", payload)
    contract_command = common + ["cargo", "test", "--manifest-path", str(candidate / "Cargo.toml"), "-p", "magnetar-driver",
                                  "--example", "performance", "--release", "--features", "moonpool,scalable-topics", "--locked"]
    contract_command[2:2] = ["-e", "CARGO_TARGET_DIR=/tmp/performance-example-contracts"]
    run_functional_contract(contract_command, output, output / "example-contracts.stdout", output / "example-contracts.stderr")
    save(output / "example-contracts-command.json", contract_command)
    example_counts = example_contract_completion((output / "example-contracts.stdout").read_text())
    run_functional_contract([sys.executable, "-m", "unittest", "discover", "-s", str(candidate / "scripts"), "-p", "test_performance*.py"],
             output, output / "python-contracts.stdout", output / "python-contracts.stderr")
    save(output / "contracts.json", {"python-performance": "passed", "example-performance": "passed", "example_counts": example_counts,
                                      "resource_metrics": None, "scope": "functional contracts; no performance claim"})
    perf.run(["docker", "save", "-o", str(output / "image.tar"), image_id], output, output / "image-save.stdout", output / "image-save.stderr")
    with (output / "image.tar").open("rb") as source, gzip.open(output / "image.tar.gz", "wb", compresslevel=1) as target:
        shutil.copyfileobj(source, target)
    (output / "image.tar").unlink()
    save(output / "image-transport.json", {"sha256": perf.digest(output / "image.tar.gz"), "image_id": image_id})
    matrix = [{"kind": "workspace", "worker": shard} for shard in range(args.shards)]
    matrix.extend({"kind": "moonpool", "worker": worker} for worker in range(args.seed_workers))
    matrix.append({"kind": "scenarios", "worker": 0})
    save(output / "matrix.json", {"include": matrix})
    write_artifact_manifest(output, "ci-artifacts.json")
    if os.environ.get("GITHUB_OUTPUT"):
        with Path(os.environ["GITHUB_OUTPUT"]).open("a") as handle:
            handle.write("matrix=" + json.dumps({"include": matrix}, separators=(",", ":")) + "\n")
            handle.write("main_sha=" + args.expected_base_sha + "\n")


def private_pip33_config(original, image, prefix, ports):
    """Preserve the repository fixture; bind every shared-network port privately."""
    mapping = dict(zip((2181, 16650, 16651, 18080, 18081, 3181, 3182, 3191, 3192), ports[:9]))
    config = copy.deepcopy(original)
    config["name"] = prefix
    for name, service in config["services"].items():
        service.update(image=image, container_name=prefix + "-" + name, restart="no", network_mode="host",
                       cpus=2, mem_limit="2g", pids_limit=512)
        service["labels"] = {"magnetar.performance.fixture": prefix}
        service.pop("volumes", None)
        service.pop("ports", None)
        environment = service.setdefault("environment", {})
        environment.update(JAVA_TOOL_OPTIONS="-XX:ActiveProcessorCount=2", PULSAR_MEM="-Xms256m -Xmx512m -XX:MaxDirectMemorySize=512m")
        if name.startswith("bookkeeper"):
            environment.update(allowLoopback="true", listeningInterface="lo", prometheusStatsHttpAddress="127.0.0.1")
            service["command"] = [part.replace("bin/apply-config-from-env.py conf/bookkeeper.conf",
                "bin/apply-config-from-env.py conf/bookkeeper.conf\nprintf '\\nlisteningInterface=lo\\nallowLoopback=true\\nprometheusStatsHttpAddress=127.0.0.1\\n' >> conf/bookkeeper.conf") for part in service["command"]]
        if name.startswith("broker"):
            environment.update(bindAddress="127.0.0.1")
            service["command"] = [part.replace("bin/apply-config-from-env.py conf/broker.conf",
                "bin/apply-config-from-env.py conf/broker.conf\nprintf '\\nbindAddress=127.0.0.1\\n' >> conf/broker.conf") for part in service["command"]]
        if name == "zookeeper":
            service["command"] = ["bash", "-c", "set -eu\nbin/apply-config-from-env.py conf/zookeeper.conf\n"
                + f"sed -i 's/^clientPort=.*/clientPort={ports[0]}/;s/^admin.serverPort=.*/admin.serverPort={ports[9]}/;s/^metricsProvider.httpPort=.*/metricsProvider.httpPort={ports[10]}/' conf/zookeeper.conf\n"
                + "printf '\\nclientPortAddress=127.0.0.1\\nadmin.serverAddress=127.0.0.1\\nmetricsProvider.httpHost=127.0.0.1\\n' >> conf/zookeeper.conf\n"
                + "bin/generate-zookeeper-config.sh conf/zookeeper.conf\nexec bin/pulsar zookeeper"]
        encoded = json.dumps(service)
        encoded = re.sub(r"(?<![0-9])(" + "|".join(map(str, mapping)) + r")(?![0-9])", lambda match: str(mapping[int(match[0])]), encoded)
        config["services"][name] = json.loads(encoded)
    return config


def wait_http(url, timeout=180):
    deadline = time.monotonic() + timeout
    last = None
    while time.monotonic() < deadline:
        try:
            with urllib.request.urlopen(url, timeout=3) as response:
                if response.status == 200:
                    return response.read().decode()
        except OSError as error:
            last = error
        time.sleep(1)
    raise ValueError(f"fixture readiness failed: {url}: {last}")


def free_ports(count):
    sockets = []
    try:
        for _ in range(count):
            handle = socket.socket(); handle.bind(("127.0.0.1", 0)); sockets.append(handle)
        return [handle.getsockname()[1] for handle in sockets]
    finally:
        for handle in sockets:
            handle.close()


def start_pip33(candidate, images, output, prefix):
    source = candidate / "crates/magnetar/tests/fixtures/docker-compose.replicated-subs.yml"
    original = json.loads(perf.read_command(["docker", "compose", "-f", str(source), "config", "--format", "json"], output))
    # The exact main tests predate configurable PIP33 endpoints. A dedicated
    # ephemeral Actions runner uses those ports, after proving they are free.
    ports = [2181, 16650, 16651, 18080, 18081, 3181, 3182, 3191, 3192, *free_ports(2)]
    handles = []
    try:
        for port in ports:
            handle = socket.socket(); handle.bind(("127.0.0.1", port)); handles.append(handle)
    finally:
        for handle in handles:
            handle.close()
    image = images["apachepulsar/pulsar:4.2.4"]
    config = private_pip33_config(original, image["reference"], prefix, ports)
    compose = output / "private-pip33.json"; save(compose, config)
    perf.run(["docker", "compose", "-f", str(compose), "up", "-d", "--wait", "--wait-timeout", "180"],
             output, output / "pip33-up.stdout", output / "pip33-up.stderr")
    containers = json.loads(perf.read_command(["docker", "inspect", *[row["container_name"] for row in config["services"].values()]], output))
    for row in containers:
        if row["Image"] != image["image_id"] or row["Config"]["Labels"].get("magnetar.performance.fixture") != prefix:
            raise ValueError("private PIP33 process image/ownership differs")
    admin_a = "http://localhost:" + str(ports[3]); admin_b = "http://localhost:" + str(ports[4])
    ids = {row["Name"].removeprefix("/"): row["Id"] for row in containers}
    for local in ("broker-a", "broker-b"):
        local_id = ids[prefix + "-" + local]
        for cluster, admin, broker_port in (("cluster-a", admin_a, ports[1]), ("cluster-b", admin_b, ports[2])):
            command = ["docker", "exec", local_id, "bin/pulsar-admin", "--admin-url", admin_a if local == "broker-a" else admin_b,
                       "clusters", "update", cluster,
                       "--url", admin, "--broker-url", "pulsar://localhost:" + str(broker_port)]
            perf.run(command, output, output / (local + "-" + cluster + ".stdout"), output / (local + "-" + cluster + ".stderr"))
    for command in (["tenants", "update", "public", "--allowed-clusters", "cluster-a,cluster-b"],
                    ["namespaces", "set-clusters", "public/default", "--clusters", "cluster-a,cluster-b"]):
        perf.run(["docker", "exec", ids[prefix + "-broker-a"], "bin/pulsar-admin", "--admin-url", admin_a, *command],
                 output, output / (command[0] + ".stdout"), output / (command[0] + ".stderr"))
    observed = {"prefix": prefix, "image_id": image["image_id"], "compose_source_sha256": perf.digest(source), "rendered_sha256": perf.digest(compose), "ports": ports,
                "containers": containers, "bindings": {"MAGNETAR_PIP33_CLUSTER_A_URL": "pulsar://localhost:" + str(ports[1]),
                "MAGNETAR_PIP33_CLUSTER_B_URL": "pulsar://localhost:" + str(ports[2]), "MAGNETAR_PIP33_ADMIN_B_URL": admin_b},
                "cluster_a": json.loads(wait_http(admin_a + "/admin/v2/clusters/cluster-b")),
                "cluster_b": json.loads(wait_http(admin_b + "/admin/v2/clusters/cluster-a"))}
    listeners = perf.read_command(["ss", "-H", "-ltn"], output)
    (output / "pip33-listeners.txt").write_text(listeners + "\n")
    for port in ports:
        addresses = [line.split()[3] for line in listeners.splitlines() if line.split()[3].rsplit(":", 1)[-1] == str(port)]
        if not addresses or any(address.rsplit(":", 1)[0] not in ("127.0.0.1", "[::1]") for address in addresses):
            raise ValueError("private PIP33 listener missing or not bound to loopback: " + str(port))
    save(output / "pip33.json", observed)
    return observed


def deduplicate_execution(output):
    # Link immutable retained copies to each other, never to a mutable Cargo
    # target. The archive preserves hardlinks and every path's digest.
    retained = {}
    for path in sorted(output.rglob("*")):
        if not path.is_file() or "execution" not in path.relative_to(output).parts:
            continue
        sha = perf.digest(path)
        if sha in retained:
            path.unlink(); os.link(retained[sha], path)
        else:
            retained[sha] = path


def resource_snapshot(output, cache):
    filesystem = os.statvfs(output)
    paths = [path for directory in (output, cache) if directory.exists() for path in directory.rglob("*") if path.is_file()]
    seen = set(); unique_bytes = 0
    for path in paths:
        stat = path.stat()
        if (stat.st_dev, stat.st_ino) not in seen:
            seen.add((stat.st_dev, stat.st_ino)); unique_bytes += stat.st_blocks * 512
    return {"available_disk_bytes": filesystem.f_bavail * filesystem.f_frsize,
            "unique_allocated_file_bytes": unique_bytes, "apparent_file_bytes": sum(path.stat().st_size for path in paths),
            "logical_cpus": os.cpu_count(), "affinity": sorted(os.sched_getaffinity(0)),
            "memory_kib": {line.split(":")[0]: int(line.split()[1]) for line in Path("/proc/meminfo").read_text().splitlines() if line.startswith(("MemTotal:", "MemAvailable:"))}}


def ci_expected_runs(payload, kind, worker):
    if kind == "workspace" and 0 <= worker < payload["shards"]:
        return [("workspace-all-features", 17, worker, payload["shards"], None, None)]
    if kind == "moonpool" and 0 <= worker < payload["seed_workers"]:
        return [("moonpool-no-buggify", seed, 0, 1, None, None) for index, seed in enumerate(payload["seed_union"]["seeds"]) if index % payload["seed_workers"] == worker]
    if kind == "scenarios" and worker == 0:
        return [("workspace-all-features", 17, 0, 1, runtime, scenario) for runtime in ("tokio", "moonpool") for scenario in ("producer", "consumer", "roundtrip", "idle")]
    raise ValueError("unexpected CI worker kind/index")


def check_ci_workers(payload, workers):
    expected = [("workspace", shard) for shard in range(payload["shards"])] + [("moonpool", worker) for worker in range(payload["seed_workers"])] + [("scenarios", 0)]
    perf.reconcile_families(expected, [[(row["kind"], row["worker"]) for row in workers]])
    for row in workers:
        if row["revisions"] != payload["revisions"]:
            raise ValueError("worker revisions differ from frozen main/PR")
        wanted = [(axis, seed, shard, runtime, scenario) for axis, seed, shard, _, runtime, scenario in ci_expected_runs(payload, row["kind"], row["worker"])]
        perf.reconcile_families(wanted, [[(run["axis"], run["seed"], run["shard"], run["runtime"], run["scenario"]) for run in row["runs"]]])


def worker_failure_state(directories):
    for directory in directories:
        for name in ("report.json", "campaign-failure.json"):
            if (directory / name).exists():
                row = json.loads((directory / name).read_text())
                if row.get("state") == "functional-failure" or row.get("functional_failures"):
                    return "functional-failure"
    return "invalid-collection"


def cleanup_owned(ids, prefix, output):
    for container_id in ids:
        row = json.loads(perf.read_command(["docker", "inspect", container_id], output))[0]
        if row["Id"] != container_id or row["Config"]["Labels"].get("magnetar.performance.fixture") != prefix:
            raise ValueError("refusing cleanup of an unowned fixture container")
        perf.run(["docker", "rm", "-f", container_id], output, output / (container_id + "-remove.stdout"), output / (container_id + "-remove.stderr"))


def ci_worker(args):
    candidate = args.candidate.resolve(); base = args.base.resolve()
    output = perf.output_directory(args.output, [candidate, base]); prepared = args.prepared.resolve()
    if any(output.iterdir()):
        raise ValueError("CI worker output must be a new empty directory")
    verify_artifact_manifest(prepared / "ci-artifacts.json")
    payload = json.loads((prepared / "ci-input.json").read_text())
    runs = ci_expected_runs(payload, args.kind, args.worker)
    if ci_harness(candidate) != payload["harness"]:
        raise ValueError("worker harness/source differs from prepared PR inputs")
    overlay = ci_checkout(candidate, base, payload["revisions"]["base"], payload["revisions"]["candidate"], output)
    transport = json.loads((prepared / "image-transport.json").read_text())
    if perf.digest(prepared / "image.tar.gz") != transport["sha256"]:
        raise ValueError("transported image bytes differ")
    archive = output / "image.tar"
    with gzip.open(prepared / "image.tar.gz", "rb") as source, archive.open("wb") as target:
        shutil.copyfileobj(source, target)
    perf.run(["docker", "load", "-i", str(archive)], output, output / "image-load.stdout", output / "image-load.stderr")
    archive.unlink()
    image = json.loads(perf.read_command(["docker", "image", "inspect", payload["image_id"]], output))[0]
    check_image(image, payload["image_id"], payload["dockerfile_sha256"]); save(output / "loaded-image.json", image)
    images = payload["fixture_images"]
    for index, (tag, frozen) in enumerate(images.items()):
        perf.run(["docker", "pull", frozen["reference"]], output, output / f"fixture-load-{index}.stdout", output / f"fixture-load-{index}.stderr")
        perf.read_command(["docker", "tag", frozen["reference"], tag], output)
    save(output / "fixture-images.json", images)
    save(output / "fixture-images-observed.json", verify_fixture_images(images, output))
    prefix = "magnetar-perf-" + uuid.uuid4().hex[:16]
    owned = []; bindings = {}
    save(output / "resources-before.json", resource_snapshot(output, args.build_cache))
    try:
        if args.kind == "workspace":
            plans = [json.loads((prepared / "plans/workspace-all-features" / side / "plan.json").read_text()) for side in ("base", "candidate")]
            if any(row["source"].endswith("/e2e_replicated_subscriptions.rs") and perf.shard_matches(row["family_id"], args.worker, payload["shards"]) for plan in plans for row in plan["families"]):
                fixture = start_pip33(candidate, images, output, prefix)
                owned = [row["Id"] for row in fixture["containers"]]; bindings = fixture["bindings"]
        if args.kind == "scenarios":
            image = images["apachepulsar/pulsar:4.2.4"]
            container_id = perf.read_command(["docker", "create", "--name", prefix, "--label", "magnetar.performance.fixture=" + prefix,
                "--memory", "2g", "--cpus", "2", "--pids-limit", "512", "-p", "127.0.0.1::6650", "-p", "127.0.0.1::8080",
                "-e", "JAVA_TOOL_OPTIONS=-XX:ActiveProcessorCount=2", "-e", "PULSAR_MEM=-Xms256m -Xmx512m -XX:MaxDirectMemorySize=512m",
                image["image_id"], "bin/pulsar", "standalone"], output)
            owned = [container_id]; perf.read_command(["docker", "start", container_id], output)
            observed = json.loads(perf.read_command(["docker", "inspect", container_id], output))[0]
            port = observed["NetworkSettings"]["Ports"]
            service_url = "pulsar://127.0.0.1:" + port["6650/tcp"][0]["HostPort"]
            wait_http("http://127.0.0.1:" + port["8080/tcp"][0]["HostPort"] + "/admin/v2/brokers/health")
            save(output / "standalone.json", observed)
        statuses = []
        for axis, seed, shard, shards, runtime, scenario in runs:
            directory = output / (f"{runtime}-{scenario}" if runtime else f"{axis}-{seed}-{shard}")
            command = [sys.executable, str(candidate / "scripts/performance.py"), "campaign", "--launch", "--base", str(base), "--candidate", str(candidate),
                       "--expected-base-sha", payload["revisions"]["base"], "--expected-candidate-sha", payload["revisions"]["candidate"],
                       "--base-overlay", str(overlay), "--image-id", payload["image_id"], "--output", str(directory), "--cargo-cache", str(args.cargo_cache),
                       "--build-cache", str(args.build_cache), "--repetitions", "2", "--cpu-count", str(args.cpu_count)]
            if runtime:
                contract = payload["scenario_contract"]
                command.extend(["--broker-container", owned[0], "--broker-image-id", images["apachepulsar/pulsar:4.2.4"]["image_id"],
                                "--fixture-label", "magnetar.performance.fixture=" + prefix, "--service-url", service_url,
                                "--runtime", runtime, "--scenario", scenario, "--messages", str(contract["messages"]), "--payload-bytes", str(contract["payload_bytes"])])
            else:
                command.extend(["--suite", "--axis", axis, "--seed", str(seed), "--shard", str(shard), "--shards", str(shards),
                                "--build-session", args.kind, "--docker-socket", "--fixture-images", str(output / "fixture-images.json")])
                if bindings:
                    command.extend(["--pip33-fixture", str(output / "pip33.json")])
                for name, value in bindings.items():
                    command.extend(["--binding", name + "=" + value])
            save(output / (directory.name + "-command.json"), command)
            with (output / (directory.name + ".stdout")).open("wb") as stdout, (output / (directory.name + ".stderr")).open("wb") as stderr:
                result = subprocess.run(command, cwd=candidate, stdout=stdout, stderr=stderr, check=False)
            deduplicate_execution(output)
            statuses.append({"axis": axis, "seed": seed, "shard": shard, "runtime": runtime, "scenario": scenario,
                             "exit_code": result.returncode, "report": str(directory.relative_to(output))})
        save(output / "resources-after.json", resource_snapshot(output, args.build_cache))
        save(output / "worker.json", {"kind": args.kind, "worker": args.worker, "revisions": payload["revisions"], "runs": statuses,
                                      "scope": "partial metric/dimension coverage; exact functional union reconciled separately"})
        return 2 if any(row["exit_code"] for row in statuses) else 0
    finally:
        # Include task-owned components left by a failed compose setup. Never
        # enumerate or remove another campaign's fixtures.
        discovered = perf.read_command(["docker", "ps", "-aq", "--no-trunc", "--filter", "label=magnetar.performance.fixture=" + prefix], output).splitlines()
        if discovered:
            cleanup_owned(discovered, prefix, output)
        write_artifact_manifest(output, "worker-artifacts.json")


def validate_scenario_report(report, payload, run, root):
    verify_artifact_manifest(root / "campaign-artifacts.json")
    artifacts = json.loads((root / "campaign-artifacts.json").read_text())
    def raw(relative):
        if relative not in artifacts:
            raise ValueError("scenario raw input is absent from its immutable manifest: " + relative)
        return json.loads((root / relative).read_text())
    if raw("scenario-report.json") != report or report["state"] != "partial":
        raise ValueError("scenario report state/content differs from retained artifact")
    environment = report["environment"]
    for field, wanted in (("image_id", payload["image_id"]), ("dockerfile_sha256", payload["dockerfile_sha256"]),
                          ("compiler", payload["compiler"]), ("harness_sha256", payload["harness"]["scripts/performance_campaign.py"]),
                          ("example_sha256", payload["harness"][EXAMPLE_SOURCE]), ("profile", perf.PROFILE_ENV),
                          ("features", ["moonpool", "scalable-topics"])):
        if not perf.known_identity(wanted) or environment.get(field) != wanted:
            raise ValueError("scenario environment differs from frozen preparation: " + field)
    for side in ("base", "candidate"):
        manifest = report["manifests"][side]
        if manifest["measured_sha"] != payload["revisions"][side] or any(manifest.get(key) != value for key, value in payload["source_snapshots"][side].items()):
            raise ValueError("scenario reference/source differs from frozen preparation")
        if manifest["lockfile_sha256"] != manifest["source_manifest"].get("Cargo.lock") or perf.digest(root / side / "performance") != manifest["binary_sha256"] or side + "/performance" not in artifacts:
            raise ValueError("scenario lockfile or retained ELF identity differs")
    contract = payload["scenario_contract"]
    if contract["seed"] != 17 or contract["batching"] is not False or contract["control_cost_multiplier"] != 1 or contract["repetitions"] != 2:
        raise ValueError("scenario CI contract differs from supported fixed replay")
    work = {key: value for key, value in contract.items() if key != "repetitions"}
    work.update(runtime=run["runtime"], scenario_id=run["scenario"], scenario_revision=payload["harness"][EXAMPLE_SOURCE])
    observed = set()
    names = set()
    metric = {"native": "native_run_ns", "syscalls": "syscall_calls", "allocations": "malloc_family_growth_bytes", "copies": "intercepted_copy_bytes"}
    for side in ("calibration", "base", "candidate"):
        samples = report["observations"][side]
        perf.reconcile_families([(mode, repetition) for mode in MODES for repetition in range(contract["repetitions"])],
                                [[(sample["mode"], int(sample["artifact_prefix"].rsplit("-", 1)[1])) for sample in samples]])
        reference = "base" if side == "calibration" else side
        for sample in samples:
            if sample["state"] != "valid" or sample["workload_identity"] != work or workload_identity(sample["request"]) != work:
                raise ValueError("actual scenario route/work differs from its planned tuple")
            observed.add((sample["request"]["runtime"], sample["request"]["scenario_id"]))
            prefix = sample["artifact_prefix"]
            if not re.fullmatch(side + r"-(native|syscalls|allocations|copies)-[01]", prefix) or (reference, prefix) in names:
                raise ValueError("scenario observation prefix is malformed or duplicated")
            names.add((reference, prefix))
            relative = reference + "/" + prefix
            request = raw(relative + ".request.json")
            if request != sample["request"] or perf.digest(root / (relative + ".request.json")) != sample["request_sha256"]:
                raise ValueError("scenario request differs from retained raw input")
            if raw(relative + ".stdout") != sample["observation"]:
                raise ValueError("scenario result differs from retained raw output")
            check_scenario(sample["observation"], request)
            expected_scope = sample["observation"]["scope"] if sample["mode"] == "native" else "scenario-process-lifecycle-including-harness"
            if sample["scope"] != expected_scope or sample["denominator"] != sample["observation"]["denominator"]:
                raise ValueError("scenario metric scope/denominator differs from executed observation")
            if sample["completed"] != sample["observation"]["completed"] or sample["binary_sha256"] != report["manifests"][reference]["binary_sha256"] or raw(relative + ".identity.json")["binary_sha256"] != sample["binary_sha256"]:
                raise ValueError("scenario completed work or launched ELF differs")
            collection = raw(relative + ".collection.json")
            if collection.get("state") != "valid" or collection.get("collector") != sample["mode"] or collection.get("child_exit_code") != 0 or raw(relative + ".exit.json").get("status") != 0:
                raise ValueError("scenario raw collector/child status is invalid")
            if any(sample["metrics"].get(key) != value for key, value in collection["metrics"].items()) or metric[sample["mode"]] not in sample["metrics"]:
                raise ValueError("scenario metrics differ from retained collection")
    if len(observed) != 1:
        raise ValueError("scenario report mixes actual routes")
    return next(iter(observed))


def ci_reconcile(args):
    prepared = args.prepared.resolve(); verify_artifact_manifest(prepared / "ci-artifacts.json")
    payload = json.loads((prepared / "ci-input.json").read_text())
    output = perf.output_directory(args.output, [Path(__file__).resolve().parent.parent])
    args.workers.mkdir(parents=True, exist_ok=True)
    for archive in args.workers.glob("*.tar.gz"):
        directory = args.workers / archive.name.removesuffix(".tar.gz")
        directory.mkdir(exist_ok=True)
        with tarfile.open(archive, "r:gz") as source:
            source.extractall(directory, filter="data")
    workers = []
    for path in args.workers.glob("*/worker.json"):
        verify_artifact_manifest(path.parent / "worker-artifacts.json")
        row = json.loads(path.read_text())
        if row["revisions"] != payload["revisions"]:
            raise ValueError("worker revisions differ from frozen main/PR")
        workers.append((path.parent, row))
    check_ci_workers(payload, [row for _, row in workers])
    failed = [root / run["report"] for root, row in workers for run in row["runs"] if run["exit_code"]]
    if failed:
        raise perf.ReconciliationFailure(worker_failure_state(failed), "worker exited nonzero; structured raw failures retained")
    unions = []
    for axis, seeds in (("workspace-all-features", [17]), ("moonpool-no-buggify", payload["seed_union"]["seeds"])):
        for seed in seeds:
            filename = "plan.json" if axis == "workspace-all-features" else str(seed) + ".json"
            plans = {side: json.loads((prepared / "plans" / axis / side / filename).read_text()) for side in ("base", "candidate")}
            reports = [json.loads((root / run["report"] / "report.json").read_text()) for root, row in workers for run in row["runs"]
                       if run["runtime"] is None and run["axis"] == axis and run["seed"] == seed]
            result = perf.reconcile_reports(plans, reports); result["seed"] = seed; unions.append(result)
    scenarios = [validate_scenario_report(json.loads((root / run["report"] / "scenario-report.json").read_text()), payload, run, root / run["report"])
                 for root, row in workers for run in row["runs"] if run["runtime"]]
    perf.reconcile_families([(runtime, scenario) for runtime in ("tokio", "moonpool") for scenario in ("producer", "consumer", "roundtrip", "idle")], [scenarios])
    tables = []
    for root, worker in workers:
        for run in worker["runs"]:
            path = root / run["report"]
            verify_artifact_manifest(path / "campaign-artifacts.json")
            filename = "scenario-report.md" if run["runtime"] else "report.md"
            if not (path / filename).is_file():
                raise ValueError("worker lacks its complete comparison table")
            destination = output / "tables" / root.name / run["report"]
            destination.mkdir(parents=True)
            for name in (filename, filename.replace(".md", ".json")):
                shutil.copyfile(path / name, destination / name)
            tables.append({"kind": worker["kind"], "axis": run["axis"], "seed": run["seed"], "table": str((destination / filename).relative_to(output))})
    result = {"state": "partial", "baseline": "main", "revisions": payload["revisions"], "coverage": unions, "tables": tables,
              "contracts": json.loads((prepared / "contracts.json").read_text()), "uncovered": GAPS,
              "independent_pr_gates": {"runtime-test-parity": "separate required PR check; no resource metrics",
                                       "crypto-matrix": "separate required PR check; 16 build-only cells; no resource metrics"}}
    save(output / "ci-report.json", result)
    text = ["# Magnetar performance on this PR", "", f"Main `{payload['revisions']['base']}` / PR `{payload['revisions']['candidate']}`.",
            "State: partial. Valid higher costs are informative; functional failures and invalid collection fail the check.", "",
            "| Axis | Seed | Reference | Expected families | Executed families | Ignored cases | Native / RSS measured families |",
            "| --- | --- | --- | --- | --- | --- | --- |"]
    for union in unions:
        for side, counts in union["coverage"].items():
            text.append(f"| {union['axis']} | {union['seed']} | {'main' if side == 'base' else 'PR'} | {counts['expected']} | {counts['executed']} | {counts['ignored_cases']} | {counts['metrics']['native']} / {counts['metrics']['rss']} |")
    text.extend(["", "Client syscall / glibc malloc-family / intercepted-copy measurements apply only to the eight declared Tokio/Moonpool scenarios.",
                 "Doctests execute functionally; their performance remains unmeasured. CPU observations have 0.01 s granularity; <0.01 does not mean no CPU cost.",
                 "Complete main / PR / absolute delta / relative delta / scope tables and all raw/source manifests:", ""])
    text.extend(f"- [{row['kind']} {row['axis']} seed {row['seed']}]({row['table']})" for row in tables)
    text.extend(["", "Uncovered dimensions:", "", *["- " + gap for gap in GAPS], ""])
    (output / "ci-report.md").write_text("\n".join(text))
    write_artifact_manifest(output)
    if os.environ.get("GITHUB_STEP_SUMMARY"):
        with Path(os.environ["GITHUB_STEP_SUMMARY"]).open("a") as handle:
            handle.write(f"Main `{payload['revisions']['base']}` / PR `{payload['revisions']['candidate']}`: family unions verified; {len(payload['seed_union']['seeds'])} no-buggify seeds; 8 client scenarios.\n\n")
            handle.write("Performance state: partial; valid higher costs are informative. Doctest performance and advanced dimensions remain unmeasured.\n\n")
            handle.write("Download the performance-report and performance-worker-* artifacts for main / PR / absolute / relative delta tables, coverage counts and immutable sources/raw.\n")
    return 0


def ci_main():
    parser = argparse.ArgumentParser(description="PR performance delivery using the same audited campaign driver")
    commands = parser.add_subparsers(dest="command", required=True)
    prepare = commands.add_parser("prepare")
    plan = commands.add_parser("plan")
    for command in (prepare, plan):
        command.add_argument("--base", type=Path, required=True); command.add_argument("--candidate", type=Path, required=True)
        command.add_argument("--output", type=Path, required=True); command.add_argument("--shards", type=int, default=4)
    prepare.add_argument("--expected-base-sha", required=True); prepare.add_argument("--expected-candidate-sha", required=True)
    prepare.add_argument("--seed-workers", type=int, default=4)
    plan.add_argument("--payload", type=Path, required=True); plan.add_argument("--base-overlay", type=Path, required=True)
    worker = commands.add_parser("worker")
    worker.add_argument("--base", type=Path, required=True); worker.add_argument("--candidate", type=Path, required=True)
    worker.add_argument("--output", type=Path, required=True); worker.add_argument("--prepared", type=Path, required=True)
    worker.add_argument("--kind", choices=("workspace", "moonpool", "scenarios"), required=True)
    worker.add_argument("--worker", type=int, required=True); worker.add_argument("--cpu-count", type=int, default=2)
    worker.add_argument("--cargo-cache", type=Path, required=True); worker.add_argument("--build-cache", type=Path, required=True)
    reconcile = commands.add_parser("reconcile")
    reconcile.add_argument("--prepared", type=Path, required=True); reconcile.add_argument("--workers", type=Path, required=True)
    reconcile.add_argument("--output", type=Path, required=True)
    args = parser.parse_args(sys.argv[2:])
    try:
        if args.command == "prepare":
            if not 1 <= args.shards <= 64 or not 1 <= args.seed_workers <= 64:
                raise ValueError("PR shards and seed workers must be bounded to 1..64")
            return ci_prepare(args)
        if args.command == "plan":
            return ci_plan(args)
        if args.command == "worker":
            return ci_worker(args)
        return ci_reconcile(args)
    except (ValueError, RuntimeError, OSError, KeyError, subprocess.SubprocessError) as error:
        output = perf.output_directory(args.output, [Path(__file__).resolve().parent.parent])
        save(output / "ci-failure.json", {"stage": args.command, "state": getattr(error, "state", "invalid-collection"),
             "report_state": "partial", "reason": str(error), "metrics": None,
             "expected_axes": list(perf.AXES), "workspace_union_verified": False})
        if args.command == "worker":
            write_artifact_manifest(output, "worker-artifacts.json")
        if os.environ.get("GITHUB_STEP_SUMMARY"):
            with Path(os.environ["GITHUB_STEP_SUMMARY"]).open("a") as handle:
                handle.write("Performance collection failed: " + str(error) + ". Raw structured failures are retained.\n")
        raise


def main():
    if len(sys.argv) > 1 and sys.argv[1] == "ci":
        return ci_main()
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--base", type=Path, required=True)
    parser.add_argument("--candidate", type=Path, required=True)
    parser.add_argument("--expected-base-sha", required=True)
    parser.add_argument("--expected-candidate-sha", required=True)
    parser.add_argument("--base-overlay", type=Path)
    parser.add_argument("--candidate-overlay", type=Path)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--environment", type=Path)
    parser.add_argument("--launch", action="store_true", help="inspect and launch the local frozen Docker image")
    parser.add_argument("--image-id")
    parser.add_argument("--broker-container")
    parser.add_argument("--broker-image-id")
    parser.add_argument("--fixture-label")
    parser.add_argument("--cpu-count", type=int, default=2)
    parser.add_argument("--cargo-cache", type=Path)
    parser.add_argument("--build-cache", type=Path)
    parser.add_argument("--build-session", help="reuse checked per-reference targets across sequential replay seeds")
    parser.add_argument("--axis", choices=perf.AXES, default="workspace-all-features")
    parser.add_argument("--seed", type=int, default=17)
    parser.add_argument("--docker-socket", action="store_true", help="allow testcontainers on this private suite runner")
    parser.add_argument("--binding", action="append", default=[], help="private PIP33 NAME=URL binding")
    parser.add_argument("--fixture-images", type=Path, help="inspected image identities of suite fixtures")
    parser.add_argument("--pip33-fixture", type=Path, help="owned default-port fixture inspection for the exact main PIP-33 tests")
    parser.add_argument("--service-url", default="not-applicable")
    parser.add_argument("--repetitions", type=int, default=2)
    parser.add_argument("--scenario", choices=("producer", "consumer", "roundtrip", "idle"), default="roundtrip")
    parser.add_argument("--runtime", choices=("tokio", "moonpool"), default="tokio")
    parser.add_argument("--messages", type=int, default=16)
    parser.add_argument("--payload-bytes", type=int, default=256)
    parser.add_argument("--calibration-only", action="store_true")
    parser.add_argument("--suite", action="store_true", help="inventory assigned test targets and collect native/RSS suites")
    parser.add_argument("--package", action="append", default=[])
    parser.add_argument("--shard", type=int, default=0)
    parser.add_argument("--shards", type=int, default=1)
    args = parser.parse_args()
    if not 2 <= args.repetitions <= 20:
        raise ValueError("repetitions must be bounded to 2..20")
    if args.shards < 1 or not 0 <= args.shard < args.shards:
        raise ValueError("shard must be in 0..shards")
    if not 0 <= args.seed < 2**64:
        raise ValueError("seed must be an unsigned u64")
    if args.calibration_only and args.suite:
        raise ValueError("calibration-only applies to isolated scenarios")
    args.base, args.candidate = args.base.resolve(), args.candidate.resolve()
    try:
        if args.launch:
            if not all((args.image_id, args.cargo_cache)) or (not args.suite and not all((args.broker_container, args.broker_image_id, args.fixture_label, args.service_url != "not-applicable"))) or not 1 <= args.cpu_count <= 32:
                raise ValueError("launcher requires exact image, task-owned broker, fixture label and cargo cache; cpu-count 1..32")
            return launch_container(args)
        if args.environment is None:
            raise ValueError("internal collection requires the inspected launch environment")
        return internal_suite(args) if args.suite else internal_scenarios(args)
    except (ValueError, RuntimeError, OSError, subprocess.SubprocessError) as error:
        # Only the already validated outside-checkout directory can receive errors.
        output = args.output.resolve()
        if output.is_dir() and not any(output.is_relative_to(checkout) for checkout in (args.base, args.candidate)):
            save(output / "campaign-failure.json", {"state": getattr(error, "state", "invalid-collection"),
                 "report_state": "partial", "reason": str(error), "metrics": None,
                 "collector_exit_code": getattr(error, "collector_exit_code", None),
                 "child_exit_code": getattr(error, "child_exit_code", None), "uncovered": GAPS})
            write_artifact_manifest(output)
        raise


if __name__ == "__main__":
    try:
        sys.exit(main())
    except (ValueError, RuntimeError, OSError, subprocess.SubprocessError) as error:
        print(f"performance campaign: invalid: {error}", file=sys.stderr)
        sys.exit(2)
