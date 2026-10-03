#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
"""External, dependency-free measurement driver; see docs/performance.md."""

import argparse
from collections import Counter
from decimal import Decimal, InvalidOperation
import hashlib
import json
import math
import os
from pathlib import Path
import platform
import re
import shlex
import shutil
import statistics
import subprocess
import sys
import time
import tomllib

sys.dont_write_bytecode = True
SCHEMA_VERSION = 1
PROFILE_ENV = {"CARGO_PROFILE_RELEASE_DEBUG": "1", "CARGO_PROFILE_RELEASE_STRIP": "none"}
EXECUTION_CONTRACT_FIELDS = ("harness_sha256", "profile", "features", "toolchain", "image_id", "dockerfile_sha256", "seed")
# Cross-package executable resolved by current_exe(), rather than Cargo env-dep.
EXECUTION_COMPANIONS = {"crates/magnetar/tests/e2e_scalable_topic.rs": ("magnetarctl", "magnetarctl")}
METRICS = {"elapsed_ns": "ns/suite-launcher-lifetime", "peak_rss_kib": "KiB/process-and-waited-descendants-peak"}
AXES = ("workspace-all-features", "moonpool-no-buggify")
RUNTIME_ENV_KEYS = {"CARGO", "CARGO_MANIFEST_DIR", "CARGO_MANIFEST_PATH", "CARGO_CRATE_NAME",
                    "CARGO_BIN_NAME", "CARGO_TARGET_TMPDIR", "OUT_DIR", "CARGO_PRIMARY_PACKAGE", "LD_LIBRARY_PATH",
                    "PATH", "HOME", "CARGO_HOME", "CARGO_TARGET_DIR", "RUSTUP_HOME", "RUSTUP_TOOLCHAIN", "RUSTUP_AUTO_INSTALL",
                    "TMPDIR", "XDG_CONFIG_HOME", "NO_COLOR", "TERM", "MOONPOOL_SEED", *PROFILE_ENV,
                    "MAGNETAR_PULSAR_IMAGE_REPO", "MAGNETAR_PULSAR_IMAGE_TAG", "MAGNETAR_PULSAR_SCALABLE_IMAGE_TAG",
                    "MAGNETAR_KDC_IMAGE_REPO", "MAGNETAR_KDC_IMAGE_TAG", "MAGNETAR_KDC_SERVICE_PRINCIPAL",
                    "MAGNETAR_ATHENZ_ZTS_IMAGE_REPO", "MAGNETAR_ATHENZ_ZTS_IMAGE_TAG", "MAGNETAR_E2E_DOCKER_HOST_GATEWAY",
                    "MAGNETAR_PIP33_CLUSTER_A_URL", "MAGNETAR_PIP33_CLUSTER_B_URL", "MAGNETAR_PIP33_ADMIN_B_URL",
                    *["CARGO_PKG_" + name for name in ("NAME", "VERSION", "VERSION_MAJOR", "VERSION_MINOR", "VERSION_PATCH",
                        "VERSION_PRE", "AUTHORS", "DESCRIPTION", "HOMEPAGE", "REPOSITORY", "LICENSE", "LICENSE_FILE", "RUST_VERSION", "README")]}


def runtime_env_key(name):
    return name in RUNTIME_ENV_KEYS or name.startswith("CARGO_BIN_EXE_")


def runtime_record(binary):
    binary = Path(binary).resolve()
    return {"cwd": str(Path.cwd()), "binary": str(binary), "binary_sha256": digest(binary),
            "environment": {key: value for key, value in sorted(os.environ.items()) if runtime_env_key(key)}}


def runtime_runner(arguments, capture):
    if capture:
        destination, binary, *child_args = arguments
        Path(destination).write_text(json.dumps(runtime_record(binary), indent=2) + "\n")
    else:
        contract_path, expected_sha, destination, time_output, binary, *child_args = arguments
        if digest(contract_path) != expected_sha:
            raise ValueError("Cargo runtime contract changed in launcher")
        if runtime_record(binary) != json.loads(Path(contract_path).read_text()):
            raise ValueError("effective Cargo runtime context differs in launcher")
        record = runtime_record(binary)
        record["started_monotonic_ns"] = time.perf_counter_ns()
        Path(destination).write_text(json.dumps(record, indent=2) + "\n")
        os.execv("/usr/bin/time", ["/usr/bin/time", "-f", "%e %M %x", "-o", time_output, binary, *child_args])
    os.execve(binary, [binary, *child_args], runtime_record(binary)["environment"])


def cargo_runtime_configs(checkout, env):
    # Cargo searches cwd ancestors and CARGO_HOME. Configured [env] values
    # are unsupported; never serialize arbitrary configuration/credentials.
    directories = [parent / ".cargo" for parent in (checkout, *checkout.parents)]
    directories.append(Path(env.get("CARGO_HOME", str(Path.home() / ".cargo"))))
    inputs = {}
    for directory in dict.fromkeys(directories):
        path = next((directory / name for name in ("config", "config.toml") if (directory / name).is_file()), None)
        if path:
            data = tomllib.loads(path.read_text())
            if data.get("env"):
                raise ValueError("Cargo [env] is outside the supported runtime contract: " + str(path))
            inputs[str(path)] = digest(path)
    return inputs


def capture_cargo_catalogue(checkout, output, target_directory, env, host, package, target, selector, binary, feature_flags):
    family = Path(target["src_path"]).resolve().relative_to(checkout).as_posix() + "::" + target["name"]
    prefix = output / (hashlib.sha256(family.encode()).hexdigest()[:20] + "-cargo-runtime")
    config_inputs = cargo_runtime_configs(checkout, env)
    command = ["cargo", "test", "-p", package["name"], "--release", *feature_flags, "--locked", selector]
    if selector != "--lib":
        command.append(target["name"])
    contexts = []
    listings = []
    commands = []
    for ignored in (False, True):
        context_path = Path(str(prefix) + (".ignored.json" if ignored else ".json"))
        runner = [sys.executable, str(Path(__file__).resolve()), "runtime-capture", str(context_path)]
        invocation = command + ["--config", f"target.{host}.runner=" + json.dumps(runner), "--", "--list", "--format", "terse"] + (["--ignored"] if ignored else [])
        run(invocation, checkout, str(context_path) + ".stdout", str(context_path) + ".stderr", env)
        contexts.append(json.loads(context_path.read_text()))
        listings.append(Path(str(context_path) + ".stdout").read_text())
        commands.append(invocation)
    context = contexts[0]
    package_root = Path(package["manifest_path"]).parent.resolve()
    if contexts[1] != context or context["binary"] != str(binary) or context["binary_sha256"] != digest(binary):
        raise ValueError("Cargo catalogue launched a different executable or runtime context")
    if context["cwd"] != str(package_root) or not package_root.is_relative_to(checkout) or context["environment"].get("CARGO_MANIFEST_DIR") != str(package_root):
        raise ValueError("Cargo catalogue runtime package root differs from metadata")
    if context["environment"].get("CARGO_PKG_NAME") != package["name"] or not context["environment"].get("LD_LIBRARY_PATH"):
        raise ValueError("Cargo catalogue lacks package or loader context")
    semantic = json.dumps({"cwd": context["cwd"], "environment": context["environment"], "config_inputs": config_inputs}, sort_keys=True)
    semantic = semantic.replace(str(target_directory), "<TARGET>").replace(str(checkout), "<CHECKOUT>")
    return listings, {"runtime_context": context, "runtime_context_path": str(prefix) + ".json",
                      "runtime_context_sha256": digest(str(prefix) + ".json"), "runtime_config_inputs": config_inputs,
                      "runtime_identity": hashlib.sha256(semantic.encode()).hexdigest(), "runtime_capture_commands": commands}


def runtime_launch_context(entry):
    path = entry.get("runtime_context_path")
    if not path or not entry.get("runtime_context_sha256") or not entry.get("runtime_context") or not entry.get("runtime_identity"):
        raise ValueError("missing recorded Cargo runtime contract")
    if digest(path) != entry["runtime_context_sha256"] or json.loads(Path(path).read_text()) != entry["runtime_context"]:
        raise ValueError("Cargo runtime contract changed after inventory")
    for source, expected in entry.get("runtime_config_inputs", {}).items():
        if digest(source) != expected:
            raise ValueError("Cargo runtime configuration changed after inventory")
    context = entry["runtime_context"]
    if context["binary"] != entry["binary"] or context["binary_sha256"] != entry["binary_sha256"]:
        raise ValueError("Cargo runtime executable differs from inventory")
    if any(not runtime_env_key(key) for key in context["environment"]):
        raise ValueError("Cargo runtime contract contains an unsupported environment key")
    env = dict(context["environment"])
    return Path(context["cwd"]), env


def check_actual_runtime(entry, path):
    actual = json.loads(Path(path).read_text())
    started = actual.pop("started_monotonic_ns", None)
    if type(started) is not int or started <= 0 or actual != entry["runtime_context"]:
        raise ValueError("actual Cargo runtime context differs from inventory")
    return started


def axis_configuration(axis, packages):
    if axis == "workspace-all-features":
        return list(packages), ["--all-features"], ["all"], "partial-package-selection" if packages else "workspace"
    if axis == "moonpool-no-buggify":
        if packages and packages != ["magnetar-runtime-moonpool"]:
            raise ValueError("no-buggify axis requires exactly magnetar-runtime-moonpool")
        return ["magnetar-runtime-moonpool"], ["--no-default-features", "--features", "crypto-aws-lc-rs"], ["no-default-features", "crypto-aws-lc-rs"], "runtime-moonpool"
    raise ValueError("unknown measurement axis")


def seed_union(registries):
    """Fixed replay set plus open anchors from both exact reference registries."""
    values = set(range(1, 33))
    sources = {}
    for side, path in registries.items():
        data = tomllib.loads(Path(path).read_text())
        sources[side] = {"sha256": digest(path), "open": []}
        for entry in data.get("seed", []):
            status = entry.get("status", "open")
            if status not in ("open", "closed"):
                raise ValueError("unknown known-failing seed status")
            if status == "closed":
                continue
            raw = entry["value"]
            if isinstance(raw, bool) or not isinstance(raw, (str, int)):
                raise ValueError("seed must be an unsigned u64 integer")
            value = int(raw, 16 if isinstance(raw, str) and raw.lower().startswith("0x") else 10) if isinstance(raw, str) else raw
            if not 0 <= value < 2**64:
                raise ValueError("seed is outside u64")
            values.add(value)
            sources[side]["open"].append(value)
    return {"seeds": sorted(values), "fixed": list(range(1, 33)), "registries": sources}


def check_axis_artifacts(axis, metadata, artifacts):
    if axis != "moonpool-no-buggify":
        return
    package_ids = {package["id"] for package in metadata["packages"] if package["name"] == "magnetar-runtime-moonpool"}
    features = [set(row["features"]) for row in artifacts
                if row.get("reason") == "compiler-artifact" and row.get("package_id") in package_ids]
    if not features or any("buggify" in active or "crypto-aws-lc-rs" not in active for active in features):
        raise ValueError("compiled Moonpool features violate the no-buggify axis")


def digest(path):
    return hashlib.sha256(Path(path).read_bytes()).hexdigest()


def output_directory(path, checkouts):
    path = Path(path).resolve()
    for checkout in checkouts:
        if path.is_relative_to(Path(checkout).resolve()):
            raise ValueError("measurement output must be outside every product checkout")
    path.mkdir(parents=True, exist_ok=True)
    return path


def parse_test_list(text, ignored, allow_empty=False):
    names = [line.removesuffix(": test") for line in text.splitlines() if line.endswith(": test")]
    if (not names and not allow_empty) or len(set(names)) != len(names):
        raise ValueError("empty or duplicated executable test catalogue")
    if not set(ignored).issubset(names):
        raise ValueError("ignored catalogue is not a subset of executable tests")
    return [{"name": name, "ignored": name in ignored} for name in sorted(names)]


def test_completion(text, requested, ignored):
    summaries = re.findall(r"test result: (ok|FAILED)\. (\d+) passed; (\d+) failed; (\d+) ignored; (\d+) measured; (\d+) filtered out;", text)
    if len(summaries) != 1:
        raise ValueError("missing or ambiguous functional test result")
    status, passed, failed, actual_ignored, measured, filtered = summaries[0]
    passed, failed, actual_ignored, measured, filtered = map(int, (passed, failed, actual_ignored, measured, filtered))
    if status != "ok" or failed or filtered or measured or passed == 0:
        raise ValueError("functional work did not complete successfully")
    if passed + actual_ignored != requested or actual_ignored != ignored:
        raise ValueError("completed work does not match executable inventory")
    return passed


def doctest_completion(text, tests):
    # Edition 2024 rustdoc splits merged, compile-fail and standalone groups.
    summaries = re.findall(r"test result: (ok|FAILED)\. (\d+) passed; (\d+) failed; (\d+) ignored; (\d+) measured; (\d+) filtered out;", text)
    actual = []
    expected = {test["name"]: test["ignored"] for test in tests}
    for name, status in re.findall(r"^test (.+) \.\.\. (ok|ignored)$", text, re.MULTILINE):
        if name not in expected and name.endswith(" - compile fail"):
            name = name.removesuffix(" - compile fail")
        actual.append({"name": name, "ignored": status == "ignored"})
    if sorted(actual, key=lambda row: row["name"]) != sorted(tests, key=lambda row: row["name"]):
        raise ValueError("doctest executed catalogue differs from inventory")
    passed = ignored = 0
    if not summaries:
        raise ValueError("missing doctest functional summaries")
    for status, done, failed, skipped, measured, filtered in summaries:
        if status != "ok" or any(int(value) for value in (failed, measured, filtered)):
            raise ValueError("doctest functional work failed or was filtered")
        passed += int(done)
        ignored += int(skipped)
    if passed + ignored != len(tests) or ignored != sum(expected.values()):
        raise ValueError("doctest completed work differs from inventory")
    return passed


def check_expected_revision(checkout, expected):
    if not re.fullmatch(r"[0-9a-f]{40}", expected or ""):
        raise ValueError("expected revision must be an immutable lowercase 40-character SHA")
    if read_command(["git", "rev-parse", "HEAD"], checkout) != expected:
        raise ValueError(f"checkout HEAD differs from expected revision: {expected}")


def parse_time(text):
    try:
        elapsed, rss, status = text.strip().split()
        elapsed = Decimal(elapsed)
        rss, status = int(rss), int(status)
    except (ValueError, InvalidOperation) as error:
        raise ValueError("missing or malformed GNU time observation") from error
    if not elapsed.is_finite() or elapsed < 0 or rss <= 0 or status != 0:
        raise ValueError("invalid GNU time observation or child failure")
    # GNU %e is rounded to centiseconds: it is diagnostic, never native nanoseconds.
    return {"gnu_time_elapsed_seconds": str(elapsed), "peak_rss_kib": rss}


def parse_dhat(data, mode):
    if data.get("dhatFileVersion") != 2 or data.get("mode") != mode or not data.get("pps"):
        raise ValueError("empty, unsupported or wrong-mode DHAT profile")
    fields = ["tb", "tbk"] + (["gb", "gbk", "eb", "ebk"] if mode == "heap" else [])
    totals = dict.fromkeys(fields, 0)
    for point in data["pps"]:
        for field in fields:
            value = point.get(field)
            if type(value) is not int or value < 0:
                raise ValueError(f"DHAT point lacks nonnegative integer {field}")
            totals[field] += value
    if mode == "copy":
        return {"intercepted_copy_bytes": totals["tb"], "intercepted_copy_calls": totals["tbk"]}
    if mode != "heap":
        raise ValueError("unsupported DHAT mode")
    return {"allocated_bytes": totals["tb"], "allocation_blocks": totals["tbk"],
            "peak_live_bytes": totals["gb"], "end_live_bytes": totals["eb"]}


def parse_strace(text):
    calls = {}
    total = None
    for line in text.splitlines():
        fields = line.split()
        if len(fields) not in (5, 6) or not re.fullmatch(r"\d+\.\d+", fields[0]):
            continue
        try:
            count = int(fields[3])
            errors = int(fields[4]) if len(fields) == 6 else 0
        except ValueError as error:
            raise ValueError("malformed strace count") from error
        if count < 0 or not 0 <= errors <= count:
            raise ValueError("invalid strace count")
        if fields[-1] == "total":
            if total is not None:
                raise ValueError("duplicate strace total")
            total = (count, errors)
        else:
            if fields[-1] in calls:
                raise ValueError("duplicate syscall row")
            calls[fields[-1]] = {"calls": count, "errors": errors}
    observed = (sum(row["calls"] for row in calls.values()), sum(row["errors"] for row in calls.values()))
    if not calls or not total or total[0] == 0 or total != observed:
        raise ValueError("empty or inconsistent strace summary")
    return {"syscall_calls": total[0], "syscall_errors": total[1], "syscalls": dict(sorted(calls.items()))}


def summarize(base, candidate):
    for value in base + candidate:
        if type(value) not in (int, float) or not math.isfinite(value) or value < 0:
            raise ValueError("metric samples must be finite nonnegative numbers")
    if not base or not candidate:
        return {"base": None, "candidate": None, "delta": None, "relative_percent": None, "verdict": "unmeasured"}
    left, right = statistics.median(base), statistics.median(candidate)
    delta = right - left
    relative = (100 * delta / left) if left != 0 else None
    if left == 0 and right > 0:
        verdict = "new cost (informative)"
    elif min(candidate) > max(base):
        verdict = "higher cost (informative)"
    elif max(candidate) < min(base):
        verdict = "lower cost (informative)"
    else:
        verdict = "overlapping ranges (informative)"
    return {"base": left, "candidate": right, "delta": delta, "relative_percent": relative,
            "base_range": [min(base), max(base)], "candidate_range": [min(candidate), max(candidate)],
            "base_samples": base, "candidate_samples": candidate, "verdict": verdict}


def check_comparable(base, candidate):
    for field in ("harness_sha256", "profile", "features", "toolchain", "runner", "kernel", "cpu", "broker_digests"):
        if field not in base or field not in candidate or base[field] != candidate[field]:
            raise ValueError(f"incomparable provenance: {field}")
        if isinstance(base[field], str) and (not base[field] or base[field] in ("unknown", "unrecorded") or base[field].startswith("unrecorded-")):
            raise ValueError(f"unknown provenance is not comparability evidence: {field}")
    if not isinstance(base["broker_digests"], list) or (not base["broker_digests"] and base.get("fixture_scope") != "no-external-fixture") or any(not re.fullmatch(r"sha256:[0-9a-f]{64}", image) for image in base["broker_digests"]):
        raise ValueError("known broker image digests are required for comparability")
    if base.get("fixture_scope") != candidate.get("fixture_scope"):
        raise ValueError("fixture scopes differ")
    for field in ("cpu_affinity", "seed"):
        if field in base or field in candidate:
            if field not in base or field not in candidate or base[field] != candidate[field]:
                raise ValueError(f"incomparable local execution provenance: {field}")


def run(command, cwd, stdout, stderr, env=None):
    with Path(stdout).open("wb") as out, Path(stderr).open("wb") as err:
        result = subprocess.run(command, cwd=cwd, env=env, stdout=out, stderr=err, check=False)
    if result.returncode != 0:
        raise CommandFailure(result.returncode, f"child exited {result.returncode}: {command!r}; logs: {stdout}, {stderr}")


class CommandFailure(RuntimeError):
    def __init__(self, status, reason):
        super().__init__(reason)
        self.status = status


def read_command(command, cwd, env=None):
    return subprocess.run(command, cwd=cwd, env=env, check=True, text=True, capture_output=True).stdout.strip()


def validate_source_state(measured_sha, dirty, overlay):
    if not dirty and overlay is None:
        return
    if overlay is None or overlay.get("schema_version") != 1 or overlay.get("measured_sha") != measured_sha:
        raise ValueError("dirty source tree requires an exact revision-bound audited overlay")
    files = overlay.get("files")
    if not isinstance(files, list) or len({item.get("path") for item in files}) != len(files):
        raise ValueError("invalid or duplicate audited overlay files")
    if sorted(files, key=lambda item: item["path"]) != sorted(dirty, key=lambda item: item["path"]):
        raise ValueError("source paths/statuses/hashes differ from audited overlay")


def source_snapshot(checkout, overlay_path=None):
    measured_sha = read_command(["git", "rev-parse", "HEAD"], checkout)
    status = subprocess.run(["git", "status", "--porcelain=v1", "--untracked-files=all", "-z"],
                            cwd=checkout, check=True, text=True, capture_output=True).stdout
    dirty = []
    for record in status.split("\0"):
        if not record:
            continue
        state, relative = record[:2], record[3:]
        path = checkout / relative
        if state not in (" M", "M ", "MM", "??", "A ") or not path.is_file() or path.is_symlink():
            raise ValueError(f"unsupported dirty source path for harness overlay: {relative}")
        dirty.append({"status": state, "path": relative, "sha256": digest(path)})
    paths = subprocess.run(["git", "ls-files", "-z", "--cached", "--others", "--exclude-standard"],
                           cwd=checkout, check=True, text=True, capture_output=True).stdout.split("\0")
    git_paths = set(paths) - {""}
    all_paths = set(git_paths)
    # Git ignore is not a source boundary. Only known generated output directories
    # are excluded; rustc dep-info below independently names compiled inputs.
    for directory, children, files in os.walk(checkout, followlinks=False):
        parent = Path(directory)
        children[:] = [name for name in children if name != "__pycache__"
                       and not (parent == checkout and name in (".git", "target"))]
        for name in children:
            if (parent / name).is_symlink():
                raise ValueError(f"source directory symlink is unsupported: {parent / name}")
        for name in files:
            path = parent / name
            if path in (checkout / ".git", checkout / "target"):
                continue
            all_paths.add(path.relative_to(checkout).as_posix())
    sources = {}
    for relative in sorted(all_paths):
        path = checkout / relative
        if not path.resolve().is_relative_to(checkout) or not path.is_file():
            raise ValueError(f"source manifest cannot resolve a regular in-checkout file: {relative}")
        sources[relative] = digest(path)
        if relative not in git_paths:
            dirty.append({"status": "!!", "path": relative, "sha256": sources[relative]})
    overlay = json.loads(Path(overlay_path).read_text()) if overlay_path else None
    validate_source_state(measured_sha, dirty, overlay)
    encoded = json.dumps(sources, sort_keys=True, separators=(",", ":")).encode()
    return {"measured_sha": measured_sha, "source_state": "audited-overlay" if dirty else "clean",
            "overlay": overlay, "source_manifest": sources, "source_tree_sha256": hashlib.sha256(encoded).hexdigest()}


def scenario_identity(checkout, source, tests, dep_info):
    root = (checkout / source).parent
    while root != checkout and not (root / "Cargo.toml").is_file():
        root = root.parent
    paths = {checkout / source}
    try:
        rules = Path(dep_info).read_text().replace("\\\n", " ").splitlines()
        rule = next(line for line in rules if line and not line.startswith("#"))
        dependencies = shlex.split(rule.split(": ", 1)[1])
    except (OSError, StopIteration, IndexError, ValueError) as error:
        raise ValueError(f"missing or malformed compiler dep-info: {dep_info}") from error
    if not dependencies:
        raise ValueError(f"compiler dep-info has no compiled inputs: {dep_info}")
    compiler_paths = set()
    for dependency in dependencies:
        path = Path(dependency)
        path = Path(os.path.abspath(path if path.is_absolute() else checkout / path))
        paths.add(path)
        compiler_paths.add(path)
    # Conservative closure: every local test/fixture/helper, and all src files for embedded tests.
    directories = [root / "tests"]
    if (checkout / source).is_relative_to(root / "src"):
        directories.append(root / "src")
    for directory in directories:
        if directory.is_dir():
            paths.update(path for path in directory.rglob("*") if path.is_file())
    sources = {}
    for path in sorted(paths):
        if not path.is_relative_to(checkout) or not path.is_file():
            raise ValueError(f"compiled scenario input escapes checkout or is missing: {path}")
        relative = path.relative_to(checkout).as_posix()
        if not path.resolve().is_relative_to(checkout) and not relative.startswith("target/"):
            raise ValueError(f"scenario helper escapes checkout: {path}")
        sources[relative] = digest(path)
    encoded = json.dumps(sources, sort_keys=True, separators=(",", ":")).encode()
    catalogue = json.dumps(tests, sort_keys=True, separators=(",", ":")).encode()
    return {"scenario_sources": sources,
            "compiler_sources": {path.relative_to(checkout).as_posix(): sources[path.relative_to(checkout).as_posix()] for path in sorted(compiler_paths)},
            "scenario_sha256": hashlib.sha256(encoded).hexdigest(),
            "catalogue_sha256": hashlib.sha256(catalogue).hexdigest()}


def provenance(checkout, snapshot):
    cpu = next((line.split(":", 1)[1].strip() for line in Path("/proc/cpuinfo").read_text().splitlines()
                if line.startswith("model name")), platform.machine())
    return {**snapshot,
            "working_diff_sha256": hashlib.sha256(read_command(["git", "diff", "HEAD"], checkout).encode()).hexdigest(),
            "lockfile_sha256": digest(checkout / "Cargo.lock"),
            "harness_sha256": digest(__file__), "profile": "release-symbolized",
            "features": ["all"], "toolchain": read_command(["rustc", "-Vv"], checkout),
            "runner": os.environ.get("ImageOS", "unrecorded-local-image") + ":" + os.environ.get("ImageVersion", "unrecorded"),
            "kernel": platform.platform(), "cpu": cpu,
            "broker_digests": [], "seed": os.environ.get("MOONPOOL_SEED", "runtime-default"),
            "allocator": "Rust System/default per build; no injected Rust allocator",
            "time_version": read_command(["/usr/bin/time", "--version"], checkout).splitlines()[0]}


def reconcile_families(expected, shards):
    wanted = Counter(expected)
    actual = Counter(family for shard in shards for family in shard)
    if wanted != actual or any(count != 1 for count in wanted.values()):
        raise ValueError(f"family union differs: missing={dict(wanted - actual)}, unexpected_or_duplicate={dict(actual - wanted)}")


def fixture_policy(checkout, source):
    required = "GenericImage::new" in (checkout / source).read_text()
    return {"scope": "fixture-required" if required else "fixture-free", "source": source, "source_sha256": digest(checkout / source),
            "basis": "assigned target declares GenericImage::new" if required else "assigned target has no GenericImage::new declaration; any observed child creation contradicts this scope"}


def metadata_families(checkout, metadata, packages):
    members = set(metadata.get("workspace_members", []))
    selected = [package for package in metadata["packages"]
                if (not members or package.get("id") in members) and (not packages or package["name"] in packages)]
    if set(packages) - {package["name"] for package in selected}:
        raise ValueError("requested package is absent from Cargo workspace metadata")
    families = []
    library_kinds = {"lib", "rlib", "dylib", "cdylib", "staticlib", "proc-macro"}
    for package in selected:
        for target in package["targets"]:
            kinds = set(target["kind"])
            source = Path(target["src_path"]).resolve().relative_to(checkout).as_posix()
            common = {"package": package["name"], "name": target["name"], "source": source, "fixture_policy": fixture_policy(checkout, source)}
            is_library = bool(kinds & library_kinds)
            if target.get("test", True) and "custom-build" not in kinds:
                selector = "--lib" if is_library else "--test" if kinds == {"test"} else "--bin" if kinds == {"bin"} else "--example" if kinds == {"example"} else "--bench" if kinds == {"bench"} else None
                if selector is None:
                    raise ValueError(f"unsupported executable Cargo test kind: {target['kind']}")
                families.append({**common, "family_id": source + "::" + target["name"], "kind": "executable", "selector": selector})
            if is_library and target.get("doctest", True):
                families.append({**common, "family_id": source + "::doctest::" + target["name"], "kind": "doctest"})
    reconcile_families([entry["family_id"] for entry in families], [[entry["family_id"] for entry in families]])
    return sorted(families, key=lambda entry: entry["family_id"])


def plan_inventory(checkout, output, packages, expected_sha, overlay_path, shards, environment=None, axis="workspace-all-features"):
    if shards < 1:
        raise ValueError("shards must be positive")
    packages, _, features, scope = axis_configuration(axis, packages)
    check_expected_revision(checkout, expected_sha)
    snapshot = source_snapshot(checkout, overlay_path)
    raw = read_command(["cargo", "metadata", "--no-deps", "--format-version", "1", "--locked"], checkout)
    (output / "metadata.json").write_text(raw + "\n")
    families = metadata_families(checkout, json.loads(raw), packages)
    if source_snapshot(checkout, overlay_path) != snapshot:
        raise ValueError("source changed during metadata planning")
    observed = provenance(checkout, snapshot)
    observed["features"] = features
    if environment:
        observed.update(environment)
    if observed["features"] != features:
        raise ValueError("planning environment differs from requested feature axis")
    contract = {field: observed[field] for field in EXECUTION_CONTRACT_FIELDS if field in observed}
    plan = {"execution_contract": contract, "schema_version": SCHEMA_VERSION, "axis": axis, "checkout": str(checkout),
            "scope": scope,
            "expected_revision": expected_sha, "shards": shards, "packages": packages, "families": families,
            "snapshot": snapshot, "metadata_sha256": digest(output / "metadata.json"), "harness_sha256": digest(__file__)}
    (output / "plan.json").write_text(json.dumps(plan, indent=2) + "\n")
    return plan


class ReconciliationFailure(ValueError):
    def __init__(self, state, reason):
        super().__init__(reason)
        self.state = state


def reconcile_reports(plans, reports):
    shards = plans["base"]["shards"]
    axis = plans["base"]["axis"]
    if plans["candidate"]["shards"] != shards or plans["candidate"]["axis"] != axis:
        raise ValueError("reference plans use different shards or axes")
    reconcile_families(range(shards), [[report["shard"] for report in reports]])
    coverage = {}
    for side, plan in plans.items():
        if not re.fullmatch(r"[0-9a-f]{40}", plan["expected_revision"]):
            raise ValueError("reference plan requires an immutable lowercase SHA")
        expected = [entry["family_id"] for entry in plan["families"]]
        _, _, expected_features, expected_scope = axis_configuration(axis, plan["packages"])
        if plan["scope"] != expected_scope or plan.get("execution_contract", {}).get("features") != expected_features:
            raise ValueError("reference plan scope does not match its package selection")
        actual = []
        counts = {"expected": len(expected), "inventoried": 0, "executed": 0, "catalogued_cases": 0,
                  "ignored_cases": 0, "empty_families": 0, "ignored_only_families": 0,
                  "metrics": {metric: 0 for metric in ("native", "rss", "syscall", "allocation", "copy")}}
        for report in reports:
            manifest = report["manifests"][side]
            if report["axis"] != axis or report["shards"] != shards or manifest["provenance"]["measured_sha"] != plan["expected_revision"]:
                raise ValueError("report reference/axis/shard contract differs from plan")
            for field, wanted in (("scope", plan["scope"]), ("packages", plan["packages"]),
                                  ("axis", axis), ("shard", report["shard"]), ("shards", shards)):
                if field not in manifest or manifest[field] != wanted:
                    raise ValueError(f"report manifest differs from plan: {field}")
            contract = plan.get("execution_contract", {})
            for field in EXECUTION_CONTRACT_FIELDS:
                wanted = contract.get(field)
                if wanted is None or (isinstance(wanted, str) and wanted in ("", "unknown", "unrecorded")):
                    raise ValueError(f"plan lacks a known execution contract: {field}")
                if field in ("image_id", "dockerfile_sha256", "harness_sha256") and not re.fullmatch(r"sha256:[0-9a-f]{64}" if field == "image_id" else r"[0-9a-f]{64}", wanted):
                    raise ValueError(f"plan execution digest is malformed: {field}")
                if manifest["provenance"].get(field) != wanted:
                    raise ValueError(f"report execution contract differs from plan: {field}")
            if contract["harness_sha256"] != plan.get("harness_sha256"):
                raise ValueError("plan execution harness differs from its audited planning harness")
            if not plan.get("snapshot", {}).get("source_tree_sha256") or manifest["provenance"].get("source_tree_sha256") != plan["snapshot"]["source_tree_sha256"]:
                raise ValueError("report source snapshot differs from reference plan")
            # CPU identity is local to each base/candidate pair, not equal across shards.
            check_comparable(report["manifests"]["base"]["provenance"], report["manifests"]["candidate"]["provenance"])
            reconcile_families(expected, [manifest["expected_families"]])
            assigned = [family for family in expected if shard_matches(family, report["shard"], shards)]
            reconcile_families(assigned, [manifest["assigned_families"]])
            entries = manifest["families"] + manifest["doctests"]
            reconcile_families(assigned, [[entry["family_id"] for entry in entries]])
            for kind, key in (("executable", "families"), ("doctest", "doctests")):
                reconcile_families([entry["family_id"] for entry in plan["families"] if entry["kind"] == kind and entry["family_id"] in assigned],
                                   [[entry["family_id"] for entry in manifest[key]]])
            repetitions = report["repetitions"]
            if not 2 <= repetitions <= 20:
                raise ValueError("report repetitions must be bounded to 2..20")
            reconcile_families([(entry["family_id"], repetition) for entry in manifest["families"] for repetition in range(repetitions)],
                               [[(sample["family_id"], sample["repetition"]) for sample in report["observations"][side]]])
            reconcile_families([entry["family_id"] for entry in manifest["doctests"]],
                               [[sample["family_id"] for sample in report["doctest_observations"][side]]])
            if side == "base":
                reconcile_families([(entry["family_id"], f"calibration-{repetition}") for entry in manifest["families"] for repetition in range(repetitions)],
                                   [[(sample["family_id"], sample["repetition"]) for sample in report["base_base_calibration"]]])
            samples = report["observations"][side] + report["doctest_observations"][side]
            if any(sample["state"] == "functional-failure" for sample in samples + report["base_base_calibration"]):
                raise ReconciliationFailure("functional-failure", "one or more functional executions failed")
            if any(sample["state"] not in ("valid", "unmeasured") for sample in samples + report["base_base_calibration"]):
                raise ValueError("one or more collections are invalid")
            if any(sample["family_id"] not in assigned for sample in samples):
                raise ValueError("execution contains an unexpected family")
            for entry in entries:
                planned = next(family for family in plan["families"] if family["family_id"] == entry["family_id"])
                if "fixture_policy" not in planned or entry.get("fixture_policy") != planned["fixture_policy"]:
                    raise ValueError("report fixture declaration differs from assigned target plan")
                actual.append(entry["family_id"])
                counts["catalogued_cases"] += len(entry["tests"])
                ignored = sum(test["ignored"] for test in entry["tests"])
                counts["ignored_cases"] += ignored
                work = len(entry["tests"]) - ignored
                if not work:
                    counts["ignored_only_families" if entry["tests"] else "empty_families"] += 1
                    continue
                observations = [sample for sample in samples + (report["base_base_calibration"] if side == "base" else []) if sample["family_id"] == entry["family_id"]]
                if any(sample["state"] == "functional-failure" for sample in observations):
                    raise ReconciliationFailure("functional-failure", f"{side} functional family failed: {entry['family_id']}")
                if not observations or any(sample["state"] != "valid" or sample["requested"] != len(entry["tests"]) or sample["completed"] != work for sample in observations):
                    raise ValueError(f"{side} functional work incomplete or invalid: {entry['family_id']}")
                if entry in manifest["families"] and (not entry.get("runtime_identity") or any(sample.get("runtime_identity") != entry["runtime_identity"] or not sample.get("runtime_record_sha256") for sample in observations)):
                    raise ValueError("executed Cargo runtime context is missing or differs from its manifest")
                counts["executed"] += 1
                for metric, state in entry.get("metric_coverage", {}).items():
                    if metric in counts["metrics"] and state == "direct-passed-cases":
                        counts["metrics"][metric] += 1
        reconcile_families(expected, [actual])
        counts["inventoried"] = len(actual)
        coverage[side] = counts
    return {"schema_version": SCHEMA_VERSION, "state": "partial", "axis": axis, "shards": shards,
            "scopes": {side: {"scope": plan["scope"], "packages": plan["packages"]} for side, plan in plans.items()},
            "workspace_union_verified": all(plan["scope"] == "workspace" for plan in plans.values()),
            "expected_revisions": {side: plan["expected_revision"] for side, plan in plans.items()}, "coverage": coverage,
            "reason": "union and functional work reconciled; unmeasured metric/dimension gaps remain"}


def execution_dep_info(binary, checkout, target_directory):
    compiler_binary = binary
    dep_info = binary.with_suffix(".d")
    mode = "adjacent"
    if not dep_info.is_file():
        # `cargo test` installs CARGO_BIN_EXE binaries at the profile root,
        # but retains rustc's executable and dep-info under deps/<crate>-<hash>.
        matches = [path.resolve() for path in (binary.parent / "deps").glob(binary.name.replace("-", "_") + "-*")
                   if path.is_file() and os.access(path, os.X_OK) and digest(path) == digest(binary)]
        if len(matches) != 1:
            raise ValueError("compiler dep-info requires exactly one byte-identical Cargo executable: " + str(binary))
        compiler_binary = matches[0]
        dep_info = compiler_binary.with_suffix(".d")
        mode = "matched-deps-executable"
    if not compiler_binary.is_relative_to(target_directory) or not dep_info.resolve().is_relative_to(target_directory) or not dep_info.is_file():
        raise ValueError("compiler dep-info is missing or escapes the reference-specific target directory")
    targets = []
    for line in dep_info.read_text().replace("\\\n", " ").splitlines():
        if line and not line.startswith("#") and ": " in line:
            for name in shlex.split(line.split(": ", 1)[0]):
                path = Path(name)
                targets.append((path if path.is_absolute() else checkout / path).resolve())
    if compiler_binary not in targets:
        raise ValueError("compiler dep-info does not name its associated executable")
    return dep_info, {"mode": mode, "origin": str(dep_info), "compiler_executable": str(compiler_binary),
                      "compiler_executable_sha256": digest(compiler_binary)}


def retain_execution_artifact(binary, source, checkout, target_directory, output, role):
    binary = Path(binary).resolve()
    if not binary.is_relative_to(target_directory):
        raise ValueError("executable escapes reference-specific Cargo target directory")
    relative = binary.relative_to(target_directory)
    retained = output / "execution" / relative
    retained.parent.mkdir(parents=True, exist_ok=True)
    shutil.copyfile(binary, retained)
    retained.chmod(0o755)
    dep_info, resolution = execution_dep_info(binary, checkout, target_directory)
    retained_dep_info = retained.with_suffix(".d")
    retained_dep_info.write_bytes(dep_info.read_bytes())
    notes = read_command(["readelf", "-n", str(binary)], checkout)
    build_id = re.search(r"Build ID: ([0-9a-f]+)", notes)
    if build_id is None:
        raise ValueError("execution artifact lacks an ELF build ID")
    notes_path = retained.with_name(retained.name + ".elf-notes")
    notes_path.write_text(notes + "\n")
    compiler_environment = dict(re.findall(r"^# env-dep:([^=]+)=(.*)$", dep_info.read_text(), re.MULTILINE))
    return {"role": role, "build_id": build_id.group(1), "elf_notes": str(notes_path), "elf_notes_sha256": digest(notes_path),
            "compiler_environment": compiler_environment, "source": source, "binary": str(binary), "binary_sha256": digest(binary),
            "retained_binary": str(retained), "compiler_dep_info": str(retained_dep_info),
            "compiler_dep_info_resolution": resolution,
            "compiler_dep_info_sha256": digest(retained_dep_info),
            **scenario_identity(checkout, source, [], retained_dep_info)}


def execution_identity(closure):
    return hashlib.sha256(json.dumps([(entry["role"], entry["binary_sha256"]) for entry in closure],
                                    separators=(",", ":")).encode()).hexdigest()


def check_execution_closure(entry, checkout):
    for artifact in entry.get("execution_closure", []):
        if digest(artifact["binary"]) != artifact["binary_sha256"]:
            raise ValueError("execution closure executable changed: " + artifact["binary"])
        if artifact.get("retained_binary") and digest(artifact["retained_binary"]) != artifact["binary_sha256"]:
            raise ValueError("execution closure retained executable changed")
        if artifact.get("elf_notes") and digest(artifact["elf_notes"]) != artifact["elf_notes_sha256"]:
            raise ValueError("execution closure ELF identity notes changed")
        if artifact.get("compiler_dep_info") and digest(artifact["compiler_dep_info"]) != artifact["compiler_dep_info_sha256"]:
            raise ValueError("execution closure compiler dep-info changed")
        for source, expected in artifact.get("scenario_sources", {}).items():
            if digest(checkout / source) != expected:
                raise ValueError("execution closure compiled source changed: " + source)


def build_inventory(checkout, output, packages, overlay_path=None, shard=0, shards=1, target_directory=None, axis="workspace-all-features"):
    if shards < 1 or not 0 <= shard < shards:
        raise ValueError("shard must be in 0..shards")
    packages, feature_flags, features, scope = axis_configuration(axis, packages)
    snapshot = source_snapshot(checkout, overlay_path)
    target_directory = output_directory(target_directory or output / "target", [checkout])
    env = dict(os.environ, **PROFILE_ENV, CARGO_TARGET_DIR=str(target_directory))
    if sys.platform == "linux":
        env.update(CC="clang", CXX="clang++", ASM="clang", AR="llvm-ar", RANLIB="llvm-ranlib")
    metadata = json.loads(read_command(["cargo", "metadata", "--no-deps", "--format-version", "1", "--locked"], checkout, env))
    if Path(metadata["target_directory"]).resolve() != target_directory:
        raise ValueError("Cargo metadata target differs from reference-specific target directory")
    expected = metadata_families(checkout, metadata, packages)
    assigned = [entry for entry in expected if shard_matches(entry["family_id"], shard, shards)]
    (output / "metadata.json").write_text(json.dumps(metadata, indent=2) + "\n")
    (output / "assignment.json").write_text(json.dumps({"shard": shard, "shards": shards, "expected": expected,
                                                       "assigned": assigned}, indent=2) + "\n")
    build_logs = []
    build_commands = []
    for package, name in sorted({EXECUTION_COMPANIONS[entry["source"]] for entry in assigned
                                if entry["source"] in EXECUTION_COMPANIONS}):
        command = ["cargo", "build", "-p", package, "--bin", name, "--release", *feature_flags, "--locked", "--message-format=json"]
        prefix = output / ("companion-build-" + name)
        run(command, checkout, str(prefix) + ".jsonl", str(prefix) + ".stderr", env)
        build_logs.extend(Path(str(prefix) + ".jsonl").read_text().splitlines())
        build_commands.append(command)
    for package in sorted({entry["package"] for entry in assigned if entry["kind"] == "executable"}):
        command = ["cargo", "test", "-p", package, "--release", *feature_flags, "--locked", "--no-run", "--message-format=json"]
        for entry in assigned:
            if entry["package"] == package and entry["kind"] == "executable":
                command.append(entry["selector"])
                if entry["selector"] != "--lib":
                    command.append(entry["name"])
        prefix = output / ("build-" + package)
        run(command, checkout, str(prefix) + ".jsonl", str(prefix) + ".stderr", env)
        build_logs.extend(Path(str(prefix) + ".jsonl").read_text().splitlines())
        build_commands.append(command)
    artifacts = [json.loads(line) for line in build_logs]
    if any(entry["kind"] == "executable" for entry in assigned):
        check_axis_artifacts(axis, metadata, artifacts)
    companions = {}
    for artifact in artifacts:
        if artifact.get("reason") == "compiler-artifact" and artifact.get("executable") and not artifact.get("profile", {}).get("test"):
            source = Path(artifact["target"]["src_path"]).resolve().relative_to(checkout).as_posix()
            binary = Path(artifact["executable"]).resolve()
            companions[str(binary)] = retain_execution_artifact(binary, source, checkout, target_directory, output, "companion")
    entries = []
    seen = set()
    host = next(line.removeprefix("host: ") for line in read_command(["rustc", "-Vv"], checkout).splitlines() if line.startswith("host: "))
    for artifact in artifacts:
        if artifact.get("reason") != "compiler-artifact" or not artifact.get("profile", {}).get("test") or not artifact.get("executable"):
            continue
        target = artifact["target"]
        # Cargo also emits test=false artifacts (examples or build helpers).
        if not target.get("test", True):
            continue
        src = Path(target["src_path"]).resolve().relative_to(checkout).as_posix()
        family = src + "::" + target["name"]
        if family in seen:
            raise ValueError(f"duplicate executable family {family}")
        seen.add(family)
        binary = Path(artifact["executable"]).resolve()
        harness = retain_execution_artifact(binary, src, checkout, target_directory, output, "harness")
        dep_info = Path(harness["compiler_dep_info"])
        package = next(package for package in metadata["packages"] if package["id"] == artifact["package_id"])
        selector = next(entry["selector"] for entry in assigned if entry["family_id"] == family)
        (tests_text, ignored_text), runtime = capture_cargo_catalogue(checkout, output, target_directory, env, host, package, target, selector, binary, feature_flags)
        child_paths = set(re.findall(r"^# env-dep:CARGO_BIN_EXE_[^=]+=(.+)$", dep_info.read_text(), re.MULTILINE))
        child_paths.update(value for key, value in runtime["runtime_context"]["environment"].items() if key.startswith("CARGO_BIN_EXE_"))
        if src in EXECUTION_COMPANIONS:
            child_paths.add(str(binary.parent.parent / EXECUTION_COMPANIONS[src][1]))
        closure = [harness]
        for child in sorted(child_paths):
            if child not in companions:
                raise ValueError("Cargo execution companion missing from compiler artifacts: " + child)
            closure.append(companions[child])
        ignored = {line.removesuffix(": test") for line in ignored_text.splitlines() if line.endswith(": test")}
        tests = parse_test_list(tests_text, ignored, allow_empty=True)
        coverage = {"native": "pending", "rss": "pending", "syscall": "unmeasured", "allocation": "unmeasured", "copy": "unmeasured"}
        entries.append({"family_id": family, "source": src, "source_sha256": digest(checkout / src),
                        "fixture_policy": fixture_policy(checkout, src),
                        **scenario_identity(checkout, src, tests, dep_info),
                        "compiler_dep_info": str(dep_info), "compiler_dep_info_sha256": digest(dep_info),
                        "binary": str(binary), "binary_sha256": digest(binary), "execution_closure": closure,
                        **runtime,
                        "execution_sha256": execution_identity(closure), "tests": tests,
                        "scope": "test-process-tree-with-fixture-and-launcher", "denominator": "passed-test-cases",
                        "metric_coverage": coverage, "equivalent_scenarios": [],
                        "state": "ready" if tests else "disabled-by-cfg-or-empty"})
    doctests = []
    for target in assigned:
        if target["kind"] == "doctest":
            src = target["source"]
            family = target["family_id"]
            prefix = output / (hashlib.sha256(family.encode()).hexdigest()[:20] + "-catalogue")
            # Stable rustdoc emits independent cfg(doc)/cfg(doctest) inputs. --test and
            # --emit cannot coexist; the explicit host target fixes Cargo's output path.
            input_command = ["cargo", "rustdoc", "-p", target["package"], "--lib", "--target", host,
                             "--release", *feature_flags, "--locked", "--", "--emit=dep-info", "--cfg", "doctest"]
            run(input_command, checkout, str(prefix) + ".inputs.stdout", str(prefix) + ".inputs.stderr", env)
            dep_info = Path(metadata["target_directory"]) / host / "doc" / (target["name"].replace("-", "_") + ".d")
            inputs = scenario_identity(checkout, src, [], dep_info)
            dep_info_sha256 = digest(dep_info)
            retained_dep_info = Path(str(prefix) + ".dep-info")
            retained_dep_info.write_bytes(dep_info.read_bytes())
            command = ["cargo", "test", "-p", target["package"], "--doc", "--target", host, "--release", *feature_flags, "--locked"]
            run(command + ["--", "--list", "--format", "terse"], checkout, str(prefix) + ".stdout", str(prefix) + ".stderr", env)
            run(command + ["--", "--list", "--ignored", "--format", "terse"], checkout, str(prefix) + ".ignored", str(prefix) + ".ignored-stderr", env)
            ignored = {line.removesuffix(": test") for line in Path(str(prefix) + ".ignored").read_text().splitlines() if line.endswith(": test")}
            tests = parse_test_list(Path(str(prefix) + ".stdout").read_text(), ignored, allow_empty=True)
            identity = scenario_identity(checkout, src, tests, dep_info)
            if inputs["scenario_sources"] != identity["scenario_sources"] or digest(dep_info) != dep_info_sha256:
                raise ValueError("rustdoc inputs changed during doctest inventory")
            doctests.append({"family_id": family, "package": target["package"], "kind": "doctest", "source": src,
                             "fixture_policy": target["fixture_policy"],
                             "tests": tests, "command": command, "cargo_target_directory": str(target_directory), "source_sha256": digest(checkout / src),
                             **identity, "input_command": input_command, "rustdoc_dep_info": str(retained_dep_info),
                             "rustdoc_dep_info_sha256": dep_info_sha256,
                             "state": "ready" if tests else "disabled-by-cfg-or-empty",
                             "scope": "cargo-rustdoc-compilation-and-execution", "denominator": "passed-doctest-cases",
                             "metric_coverage": {metric: "unmeasured" for metric in ("native", "rss", "syscall", "allocation", "copy")},
                             "coverage_reason": "functional execution only; stable rustdoc compiles snippets during execution"})
    if not expected:
        raise ValueError("Cargo metadata contains no executable test or doctest families")
    reconcile_families([entry["family_id"] for entry in assigned], [[entry["family_id"] for entry in entries + doctests]])
    if source_snapshot(checkout, overlay_path) != snapshot:
        raise ValueError("source tree changed during inventory build")
    return {"schema_version": SCHEMA_VERSION, "axis": axis, "provenance": dict(provenance(checkout, snapshot), features=features), "packages": packages,
            "cargo_target_directory": str(target_directory),
            "compiled_input_manifest": {path: sha for entry in entries + doctests for artifact in ([entry] + entry.get("execution_closure", [])) for path, sha in artifact["scenario_sources"].items()},
            "scope": scope, "shard": shard, "shards": shards,
            "expected_families": [entry["family_id"] for entry in expected], "assigned_families": [entry["family_id"] for entry in assigned],
            "build_commands": build_commands, "families": sorted(entries, key=lambda entry: entry["family_id"]), "doctests": doctests}


def shard_matches(family_id, shard, shards):
    return int(hashlib.sha256(family_id.encode()).hexdigest(), 16) % shards == shard


def execute_doctest(entry, checkout, output):
    prefix = output / (hashlib.sha256(entry["family_id"].encode()).hexdigest()[:20] + "-execution")
    observation = {"family_id": entry["family_id"], "requested": len(entry["tests"]), "completed": None,
                   "state": "invalid", "reason": None, "scope": entry["scope"], "artifact_prefix": prefix.name}
    try:
        if digest(entry["rustdoc_dep_info"]) != entry["rustdoc_dep_info_sha256"]:
            raise ValueError("rustdoc input manifest changed before execution")
        for source, expected in entry["scenario_sources"].items():
            if digest(checkout / source) != expected:
                raise ValueError(f"doctest compiled input changed before execution: {source}")
        env = dict(os.environ, **PROFILE_ENV, CARGO_TARGET_DIR=entry["cargo_target_directory"])
        if sys.platform == "linux":
            env.update(CC="clang", CXX="clang++", ASM="clang", AR="llvm-ar", RANLIB="llvm-ranlib")
        run(entry["command"] + ["--", "--test-threads=1"], checkout, str(prefix) + ".stdout", str(prefix) + ".stderr", env)
        completed = doctest_completion(Path(str(prefix) + ".stdout").read_text(), entry["tests"])
        if digest(entry["rustdoc_dep_info"]) != entry["rustdoc_dep_info_sha256"]:
            raise ValueError("rustdoc input manifest changed during execution")
        for source, expected in entry["scenario_sources"].items():
            if digest(checkout / source) != expected:
                raise ValueError(f"doctest compiled input changed during execution: {source}")
        observation.update(state="valid" if completed else "unmeasured", completed=completed,
                           reason=None if completed else "no nonignored executable doctests")
    except (ValueError, RuntimeError, OSError) as error:
        if isinstance(error, CommandFailure):
            observation.update(state="functional-failure", child_exit_code=error.status)
        observation["reason"] = str(error)
    return observation


def check_pip33_fixture(fixture):
    required = {"MAGNETAR_PIP33_CLUSTER_A_URL": "pulsar://localhost:16650",
                "MAGNETAR_PIP33_CLUSTER_B_URL": "pulsar://localhost:16651",
                "MAGNETAR_PIP33_ADMIN_B_URL": "http://localhost:18081"}
    if fixture.get("bindings") != required or len(fixture.get("containers", [])) != 6:
        raise ValueError("PIP-33 fixture does not match the exact main endpoint contract")
    prefix = fixture["prefix"]
    names = {"/" + prefix + "-" + name for name in ("zookeeper", "pulsar-init", "bookkeeper-a", "bookkeeper-b", "broker-a", "broker-b")}
    if {row["Name"] for row in fixture["containers"]} != names or len({row["Id"] for row in fixture["containers"]}) != 6:
        raise ValueError("PIP-33 fixture component identities are incomplete or duplicated")
    for row in fixture["containers"]:
        if not re.fullmatch(r"[0-9a-f]{64}", row["Id"]) or row["Config"]["Labels"].get("magnetar.performance.fixture") != prefix:
            raise ValueError("PIP-33 fixture process ownership differs")
        if row["Image"] != fixture["image_id"]:
            raise ValueError("PIP-33 fixture image differs")
        if not row["State"]["Running"] and not (row["Name"].endswith("-init") and row["State"]["ExitCode"] == 0):
            raise ValueError("PIP-33 fixture process is not ready")


def measure_family(entry, checkout, output, repetition):
    ident = hashlib.sha256(entry["family_id"].encode()).hexdigest()[:20]
    prefix = output / f"{ident}-{repetition}"
    observation = {"family_id": entry["family_id"], "source_sha256": entry["source_sha256"],
                   "binary_sha256": entry["binary_sha256"], "execution_sha256": entry.get("execution_sha256"),
                   "calibration_identity": entry.get("calibration_identity"),
                   "runtime_identity": entry.get("runtime_identity"),
                   "scenario_sha256": entry.get("scenario_sha256"), "catalogue_sha256": entry.get("catalogue_sha256"),
                   "scope": entry["scope"], "denominator": entry["denominator"], "repetition": repetition,
                   "requested": len(entry["tests"]), "completed": None, "metrics": None,
                   "artifact_prefix": prefix.name, "state": "invalid", "reason": None}
    observation["fixture_scope"] = entry.get("fixture_policy", {}).get("scope")
    if not any(not test["ignored"] for test in entry["tests"]):
        observation["fixture_scope"] = "fixture-free"
        observation.update(state="unmeasured", completed=0, reason="no nonignored executable tests" if entry["tests"] else "target has no executable tests after cfg/features")
        return observation
    # This fixture uses hardcoded host ports. Local ambient services are never an implicit target.
    if entry["source"].endswith("e2e_replicated_subscriptions.rs") and not entry.get("pip33_fixture"):
        observation.update(state="unmeasured", reason="PIP-33 requires isolated fixture qualification; shared host fixture is forbidden")
        return observation
    try:
        if entry["source"].endswith("e2e_replicated_subscriptions.rs"):
            check_pip33_fixture(entry["pip33_fixture"])
        check_execution_closure(entry, checkout)
        if digest(entry["binary"]) != entry["binary_sha256"]:
            raise ValueError("compiled binary changed after inventory")
        for source, expected in entry.get("scenario_sources", {}).items():
            if digest(checkout / source) != expected:
                raise ValueError(f"scenario compiled input changed after inventory: {source}")
        cwd, env = runtime_launch_context(entry)
        actual_runtime = Path(str(prefix) + ".runtime.json")
        actual_runtime.unlink(missing_ok=True)
        observation["started_epoch_ns"] = time.time_ns()
        try:
            run([sys.executable, str(Path(__file__).resolve()), "runtime-exec", entry["runtime_context_path"],
                 entry["runtime_context_sha256"], str(actual_runtime), str(prefix) + ".time", entry["binary"], "--test-threads=1"],
                cwd, str(prefix) + ".stdout", str(prefix) + ".stderr", env)
        finally:
            observation["finished_epoch_ns"] = time.time_ns()
        finished = time.perf_counter_ns()
        started = check_actual_runtime(entry, actual_runtime)
        elapsed_ns = finished - started
        if elapsed_ns <= 0:
            raise ValueError("invalid cross-process monotonic launch interval")
        observation["runtime_record_sha256"] = digest(actual_runtime)
        runtime_launch_context(entry)
        check_execution_closure(entry, checkout)
        text = Path(str(prefix) + ".stdout").read_text()
        completed = test_completion(text, len(entry["tests"]), sum(test["ignored"] for test in entry["tests"]))
        metrics = parse_time(Path(str(prefix) + ".time").read_text())
        for source, expected in entry.get("scenario_sources", {}).items():
            if digest(checkout / source) != expected:
                raise ValueError(f"scenario compiled input changed during execution: {source}")
        metrics.update(elapsed_ns=elapsed_ns)
        observation["elapsed_resolution_ns"] = max(1, math.ceil(time.get_clock_info("perf_counter").resolution * 1_000_000_000))
        observation.update(state="valid", completed=completed, metrics=metrics)
    except (ValueError, RuntimeError, OSError) as error:
        if isinstance(error, CommandFailure):
            # A runtime guard failure is invalid collection, not a product failure.
            try:
                check_actual_runtime(entry, actual_runtime)
                observation.update(state="functional-failure", child_exit_code=error.status,
                                   runtime_record_sha256=digest(actual_runtime))
            except (ValueError, OSError) as runtime_error:
                error = ValueError(f"invalid runtime launch: {runtime_error}; {error}")
        observation["reason"] = str(error)
    return observation


def compare_observations(left, right):
    rows = []
    families = sorted({entry["family_id"] for entry in left + right})
    for family in families:
        base = [entry for entry in left if entry["family_id"] == family]
        candidate = [entry for entry in right if entry["family_id"] == family]
        reason = None
        if not base or not candidate:
            reason = "family added or removed; absent side is unmeasured"
        elif any(not entry.get("scenario_sha256") or not entry.get("catalogue_sha256") for entry in base + candidate):
            reason = "missing immutable scenario or catalogue identity"
        elif any(len({entry[field] for entry in base + candidate}) != 1
                 for field in ("source_sha256", "scenario_sha256", "catalogue_sha256", "scope", "denominator")):
            reason = "scenario/helper/catalogue/scope/denominator changed; suites are not comparable"
        elif any(not entry.get("runtime_identity") for entry in base + candidate) or len({entry.get("runtime_identity") for entry in base + candidate}) != 1:
            reason = "Cargo runtime context differs or is missing; suites are not comparable"
        elif {entry["completed"] for entry in base + candidate}.__len__() != 1:
            reason = "completed work differs; normalized comparison refused"
        elif any(entry["state"] != "valid" for entry in base + candidate):
            reason = "invalid or unmeasured observation"
        for metric, unit in METRICS.items():
            values_left = [entry["metrics"][metric] for entry in base] if not reason else []
            values_right = [entry["metrics"][metric] for entry in candidate] if not reason else []
            result = summarize(values_left, values_right)
            same_sources = bool(base and candidate and base[0].get("calibration_identity")) and len({entry.get("calibration_identity") for entry in base + candidate}) == 1
            # An unchanged harness alone cannot establish unchanged execution children.
            identity_field = "execution_sha256" if any("execution_sha256" in entry for entry in base + candidate) else "binary_sha256"
            same_elf = bool(base and candidate and base[0].get(identity_field)) and len({entry.get(identity_field) for entry in base + candidate}) == 1
            result.update(comparison_kind="calibration-identical-sources" if same_sources else "calibration-identical-elf" if same_elf else "base-candidate",
                          product_effect=None if same_sources or same_elf or reason else "informative-cost-comparison")
            if (same_sources or same_elf) and not reason:
                result["verdict"] = "calibration variation; no product effect"
            rows.append({"family_id": family, "metric": metric, "unit": unit, "reason": reason,
                         "scope": "test-process-tree-with-fixture-and-launcher", "completed": base[0]["completed"] if base else None,
                         **result})
    return rows


def suite_comparison_kind(manifests, comparison):
    fields = ("measured_sha", "source_tree_sha256", "source_manifest", "lockfile_sha256", *EXECUTION_CONTRACT_FIELDS)
    left, right = (manifests[side]["provenance"] for side in ("base", "candidate"))
    identical = all(known_identity(left.get(field)) and left[field] == right.get(field) for field in fields)
    kind = "calibration-base-base" if identical else "base-candidate"
    return kind + "-partial/unmeasured" if not comparison or any(row["reason"] for row in comparison) else kind


def known_identity(value):
    if isinstance(value, dict):
        return bool(value) and all(known_identity(item) for item in value.values())
    if isinstance(value, list):
        return bool(value) and all(known_identity(item) for item in value)
    return value is not None and value not in ("", "unknown", "unrecorded")


def write_report(report, output):
    (output / "report.json").write_text(json.dumps(report, indent=2, allow_nan=False) + "\n")
    text = ["# Magnetar performance observations", "", "Performance costs are informative; missing or invalid collections never mean zero.", "",
            f"State: **{report['state']}**. Scope: native suite launcher/process-tree lifetime including harness and fixture orchestration; GNU time RSS includes waited descendants, excludes daemon-owned brokers.", "",
            "Comparison: " + report.get("comparison_kind", "unmeasured") + ". Unavailable effects do not establish calibration.", "",
            "Allocation/syscall/copy coverage is unmeasured unless an independently executed scenario is attached to that family.", "",
            "| Family | Metric/unit / scope | Main / base median [range] | PR / candidate median [range] | Absolute delta | Relative delta | Verdict |",
            "| --- | --- | --- | --- | --- | --- | --- |"]
    if report.get("comparison_kind") == "calibration-base-base":
        text[4:4] = ["Calibration base/base: identical measured sources. Deltas are observed variation, not a demonstrated product effect.", ""]
    for row in report.get("comparison", []):
        def display(side):
            value = row[side]
            return "unmeasured" if value is None else f"{value:g} [{row[side + '_range'][0]:g}, {row[side + '_range'][1]:g}]"
        relative = "—" if row["relative_percent"] is None else f"{row['relative_percent']:+.2f}%"
        delta = "—" if row["delta"] is None else f"{row['delta']:+g}"
        text.append(f"| `{row['family_id']}` | {row['metric']} / {row['unit']} | {display('base')} | {display('candidate')} | {delta} | {relative} | {row['reason'] or row['verdict']} |")
    calibration = report.get("base_base_calibration", [])
    if calibration:
        text.extend(["", "## Base/base variation before the comparison", "",
                     "Identical base binary and scenario repeated before candidate observations; these ranges are variation, not product effects.", "",
                     "| Family | Metric | Samples | Median | Range |", "| --- | --- | --- | --- | --- |"])
        for family in sorted({entry["family_id"] for entry in calibration}):
            samples = [entry for entry in calibration if entry["family_id"] == family]
            for metric in METRICS:
                values = [entry["metrics"][metric] for entry in samples if entry["state"] == "valid"]
                if values:
                    text.append(f"| `{family}` | {metric} | {len(values)} | {statistics.median(values):g} | [{min(values):g}, {max(values):g}] |")
    doctests = report.get("doctest_observations", {})
    if doctests:
        text.extend(["", "## Doctest functional execution", "",
                     "Cargo/rustdoc compiles these snippets during execution; client performance metrics are unmeasured.", ""])
        for side, entries in doctests.items():
            for entry in entries:
                text.append(f"- {side} `{entry['family_id']}`: {entry['state']}, {entry['completed']}/{entry['requested']} passed (ignored cases stay outside coverage).")
    if report.get("uncovered"):
        text.extend(["", "## Uncovered dimensions", "", *["- " + gap for gap in report["uncovered"]]])
    text.extend(["", "Raw observations, exact provenance, completed work and metric coverage: [report.json](report.json).", ""])
    (output / "report.md").write_text("\n".join(text))


def session_target(args, output, side, checkout, overlay, environment):
    root = getattr(args, "build_cache", None)
    if root is None:
        return output / side / "target"
    session = getattr(args, "build_session", None)
    if session is None:
        session = output.name + "-" + hashlib.sha256(str(output).encode()).hexdigest()[:12]
    elif not re.fullmatch(r"[a-zA-Z0-9_-]{1,80}", session):
        raise ValueError("build session must be a bounded directory name")
    target = output_directory(Path(root) / session / side, [checkout])
    contract = {"snapshot": source_snapshot(checkout, overlay), "lockfile": digest(checkout / "Cargo.lock"),
                "axis": getattr(args, "axis", "workspace-all-features"), "profile": PROFILE_ENV,
                "harness_sha256": digest(__file__),
                "environment": {key: (environment or {}).get(key) for key in ("image_id", "dockerfile_sha256", "toolchain", "features")}}
    marker = target.parent / (side + "-session.json")
    if marker.exists() and json.loads(marker.read_text()) != contract:
        raise ValueError("build session source/configuration differs from its frozen contract")
    marker.write_text(json.dumps(contract, indent=2) + "\n")
    return target


def compare_checkouts(args, environment=None):
    base, candidate = args.base.resolve(), args.candidate.resolve()
    check_expected_revision(base, args.expected_base_sha)
    check_expected_revision(candidate, args.expected_candidate_sha)
    output = output_directory(args.output, [base, candidate])
    manifests = {}
    for side, checkout in [("base", base), ("candidate", candidate)]:
        directory = output / side
        directory.mkdir(exist_ok=True)
        overlay = args.base_overlay if side == "base" else args.candidate_overlay
        started = time.perf_counter()
        target = session_target(args, output, side, checkout, overlay, environment) if getattr(args, "build_cache", None) else directory / "target"
        manifests[side] = build_inventory(checkout, directory, args.package, overlay, args.shard, args.shards, target, getattr(args, "axis", "workspace-all-features"))
        manifests[side]["build_and_inventory_seconds"] = time.perf_counter() - started
        for entry in manifests[side]["families"]:
            if (environment or {}).get("pip33_fixture") and entry["source"].endswith("e2e_replicated_subscriptions.rs"):
                entry["pip33_fixture"] = environment["pip33_fixture"]
        if environment:
            manifests[side]["provenance"].update(environment)
        manifest = manifests[side]
        identity = {"revision": manifest["provenance"]["measured_sha"],
                    "compiled_sources": manifest.get("compiled_input_manifest", {}),
                    "lockfile": manifest["provenance"].get("lockfile_sha256"),
                    "runtime_contexts": {entry["family_id"]: entry.get("runtime_identity") for entry in manifest["families"]},
                    "configuration": {key: manifest["provenance"].get(key) for key in EXECUTION_CONTRACT_FIELDS}}
        calibration_identity = hashlib.sha256(json.dumps(identity, sort_keys=True, separators=(",", ":")).encode()).hexdigest()
        for entry in manifest["families"]:
            entry["calibration_identity"] = calibration_identity
    check_comparable(manifests["base"]["provenance"], manifests["candidate"]["provenance"])
    doctest_observations = {"base": [], "candidate": []}
    # Cargo/rustdoc compilation and both reference builds finish before native windows.
    for side, checkout in [("base", base), ("candidate", candidate)]:
        for entry in manifests[side].get("doctests", []):
            if shard_matches(entry["family_id"], args.shard, args.shards):
                result = execute_doctest(entry, checkout, output / side)
                doctest_observations[side].append(result)
                entry["functional_state"] = result["state"]
    calibration = []
    for repetition in range(args.repetitions):
        for entry in manifests["base"]["families"]:
            if shard_matches(entry["family_id"], args.shard, args.shards):
                calibration.append(measure_family(entry, base, output / "base", f"calibration-{repetition}"))
    observations = {"base": [], "candidate": []}
    for repetition in range(args.repetitions):
        for side in (["base", "candidate"] if repetition % 2 == 0 else ["candidate", "base"]):
            checkout = base if side == "base" else candidate
            for entry in manifests[side]["families"]:
                if shard_matches(entry["family_id"], args.shard, args.shards):
                    result = measure_family(entry, checkout, output / side, repetition)
                    observations[side].append(result)
                    print(f"{side}/{repetition} {entry['family_id']}: {result['state']}", flush=True)
    invalid = sum(entry["state"] == "invalid" for entries in [*observations.values(), *doctest_observations.values(), calibration] for entry in entries)
    functional_failures = sum(entry["state"] == "functional-failure" for entries in [*observations.values(), *doctest_observations.values(), calibration] for entry in entries)
    unmeasured = sum(entry["state"] == "unmeasured" for entries in [*observations.values(), *doctest_observations.values(), calibration] for entry in entries)
    for side, manifest in manifests.items():
        checkout = base if side == "base" else candidate
        overlay = args.base_overlay if side == "base" else args.candidate_overlay
        if source_snapshot(checkout, overlay) != {key: manifest["provenance"][key] for key in ("measured_sha", "source_state", "overlay", "source_manifest", "source_tree_sha256")}:
            raise ValueError("source tree changed during execution")
        for entry in manifest["families"]:
            samples = [sample for sample in observations[side] if sample["family_id"] == entry["family_id"]]
            coverage = "direct-passed-cases" if samples and all(sample["state"] == "valid" for sample in samples) else "invalid" if any(sample["state"] == "invalid" for sample in samples) else "unmeasured"
            entry["metric_coverage"].update(native=coverage, rss=coverage)
            entry["coverage_reason"] = None if coverage == "direct-passed-cases" else "not selected in this shard or target/fixture unavailable" if not samples or coverage == "unmeasured" else "one or more collections invalid"
    comparison = compare_observations(observations["base"], observations["candidate"])
    report = {"schema_version": SCHEMA_VERSION, "state": "functional-failure" if functional_failures else "invalid" if invalid else "partial",
              "comparison_kind": suite_comparison_kind(manifests, comparison),
              "reason": "client scenarios not integrated; doctests functional-only; suite allocation/syscall/copy coverage unmeasured",
              "expected_revisions": {"base": args.expected_base_sha, "candidate": args.expected_candidate_sha},
              "axis": getattr(args, "axis", "workspace-all-features"), "shard": args.shard, "shards": args.shards, "repetitions": args.repetitions,
              "functional_failures": functional_failures, "invalid_observations": invalid, "unmeasured_observations": unmeasured,
              "manifests": manifests, "observations": observations, "doctest_observations": doctest_observations,
              "base_base_calibration": calibration,
              "comparison": comparison}
    write_report(report, output)
    print(f"report: {output / 'report.md'}; state={report['state']}, invalid={invalid}, unmeasured={unmeasured}")
    return 2 if invalid or functional_failures else 0



def main():
    if len(sys.argv) > 1 and sys.argv[1] in ("runtime-capture", "runtime-exec"):
        return runtime_runner(sys.argv[2:], sys.argv[1] == "runtime-capture")
    if len(sys.argv) > 1 and sys.argv[1] == "campaign":
        import performance_campaign
        sys.argv = [sys.argv[0], *sys.argv[2:]]
        return performance_campaign.main()
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="command", required=True)
    inventory = commands.add_parser("inventory")
    inventory.add_argument("--checkout", type=Path, required=True)
    inventory.add_argument("--output", type=Path, required=True)
    inventory.add_argument("--package", action="append", default=[])
    inventory.add_argument("--overlay-manifest", type=Path)
    inventory.add_argument("--shard", type=int, default=0)
    inventory.add_argument("--shards", type=int, default=1)
    plan = commands.add_parser("plan", help="freeze expected families from Cargo metadata without building tests")
    plan.add_argument("--checkout", type=Path, required=True)
    plan.add_argument("--expected-sha", required=True)
    plan.add_argument("--output", type=Path, required=True)
    plan.add_argument("--package", action="append", default=[])
    plan.add_argument("--overlay-manifest", type=Path)
    plan.add_argument("--shards", type=int, default=1)
    plan.add_argument("--environment", type=Path, help="verified same-image launcher environment, required for global reconciliation")
    reconcile = commands.add_parser("reconcile", help="verify the exact family union and functional work for both references")
    reconcile.add_argument("--base-plan", type=Path, required=True)
    reconcile.add_argument("--candidate-plan", type=Path, required=True)
    reconcile.add_argument("--report", type=Path, action="append", required=True)
    reconcile.add_argument("--output", type=Path, required=True)
    compare = commands.add_parser("compare")
    compare.add_argument("--base", type=Path, required=True)
    compare.add_argument("--candidate", type=Path, required=True)
    compare.add_argument("--output", type=Path, required=True)
    compare.add_argument("--package", action="append", default=[])
    compare.add_argument("--repetitions", type=int, default=3)
    compare.add_argument("--shard", type=int, default=0)
    compare.add_argument("--shards", type=int, default=1)
    compare.add_argument("--base-overlay", type=Path)
    compare.add_argument("--candidate-overlay", type=Path)
    compare.add_argument("--expected-base-sha", required=True)
    compare.add_argument("--expected-candidate-sha", required=True)
    for command in (inventory, plan, compare):
        command.add_argument("--axis", choices=AXES, default="workspace-all-features")
    args = parser.parse_args()
    if args.command == "plan":
        checkout = args.checkout.resolve()
        output = output_directory(args.output, [checkout])
        environment = None
        if args.environment:
            import performance_campaign
            environment = performance_campaign.suite_environment(json.loads(args.environment.read_text()), output)
        manifest = plan_inventory(checkout, output, args.package, args.expected_sha, args.overlay_manifest, args.shards, environment, args.axis)
        print(f"plan: {len(manifest['families'])} expected families; no test targets built or executed")
        return 0
    if args.command == "reconcile":
        import performance_campaign
        plans = {side: json.loads(path.read_text()) for side, path in (("base", args.base_plan), ("candidate", args.candidate_plan))}
        output = output_directory(args.output, [plan["checkout"] for plan in plans.values()])
        try:
            reports = []
            for path in args.report:
                performance_campaign.verify_artifact_manifest(path.parent / "campaign-artifacts.json")
                reports.append(json.loads(path.read_text()))
            result = reconcile_reports(plans, reports)
            result["inputs"] = {str(path.resolve()): digest(path) for path in [args.base_plan, args.candidate_plan, *args.report]}
            (output / "reconciliation.json").write_text(json.dumps(result, indent=2) + "\n")
            text = ["# Magnetar family reconciliation", "", "State: partial; exact selected family union and functional work verified.",
                    "Workspace union verified: " + str(result["workspace_union_verified"]).lower() + ". Axis: " + result["axis"] + ".", "",
                    "| Reference | Expected | Inventoried | Executed families | Ignored cases | Empty families |",
                    "| --- | --- | --- | --- | --- | --- |"]
            for side, counts in result["coverage"].items():
                text.insert(3, f"{side} plan scope: {result['scopes'][side]['scope']}; packages: {result['scopes'][side]['packages']}.")
                text.append(f"| {side} | {counts['expected']} | {counts['inventoried']} | {counts['executed']} | {counts['ignored_cases']} | {counts['empty_families']} |")
            text.extend(["", "Doctest/instrumented/dimension coverage remains separately scoped; no complete performance baseline is claimed.", "",
                         "Exact counts, metric coverage and input digests: [reconciliation.json](reconciliation.json).", ""])
            (output / "reconciliation.md").write_text("\n".join(text))
            print(f"reconciled: {output / 'reconciliation.json'}; state=partial")
            return 0
        except (ValueError, KeyError, OSError) as error:
            (output / "reconciliation-failure.json").write_text(json.dumps({"state": getattr(error, "state", "invalid-collection"),
                "report_state": "partial", "reason": str(error), "metrics": None,
                "plans": {side: {"axis": plan["axis"], "scope": plan["scope"], "packages": plan["packages"],
                                  "expected_revision": plan["expected_revision"], "expected_families": len(plan["families"])}
                          for side, plan in plans.items()}}, indent=2) + "\n")
            raise ValueError(str(error)) from error
    if args.command == "inventory":
        checkout = args.checkout.resolve()
        output = output_directory(args.output, [checkout])
        manifest = build_inventory(checkout, output, args.package, args.overlay_manifest, args.shard, args.shards, axis=args.axis)
        (output / "inventory.json").write_text(json.dumps(manifest, indent=2) + "\n")
        print(f"inventory: {len(manifest['families'])} executable targets, {sum(len(entry['tests']) for entry in manifest['families'])} test cases; {sum(len(entry['tests']) for entry in manifest['doctests'])} doctest cases (listed, not yet executed)")
        return 0
    if args.repetitions < 2 or args.repetitions > 20 or args.shards < 1 or not 0 <= args.shard < args.shards:
        raise ValueError("bounded repetitions must be 2..20 and shard must be in 0..shards")
    return compare_checkouts(args)


if __name__ == "__main__":
    try:
        sys.exit(main())
    except (ValueError, RuntimeError, OSError, subprocess.SubprocessError) as error:
        print(f"performance: {error}", file=sys.stderr)
        sys.exit(2)
