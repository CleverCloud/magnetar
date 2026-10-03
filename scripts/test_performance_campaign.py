#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
"""Collection validity contracts, independent of profiler availability."""

import importlib.util
from pathlib import Path
import sys
import unittest
import json
import os
import stat
import tempfile
from unittest import mock

sys.dont_write_bytecode = True
sys.path.insert(0, str(Path(__file__).parent))
SPEC = importlib.util.spec_from_file_location("campaign", Path(__file__).with_name("performance_campaign.py"))
CAMPAIGN = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(CAMPAIGN)


class CampaignContracts(unittest.TestCase):
    def test_suite_compilation_failure_keeps_original_machine_diagnostic(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            output = root / "output"; output.mkdir()
            environment = root / "environment.json"
            CAMPAIGN.save(environment, {"axis": "workspace-all-features", "seed": 17})
            arguments = ["performance_campaign.py", "--base", str(root / "base"), "--candidate", str(root / "candidate"),
                         "--expected-base-sha", "a" * 40, "--expected-candidate-sha", "b" * 40,
                         "--output", str(output), "--environment", str(environment), "--suite"]
            failure = CAMPAIGN.perf.CommandFailure(101, "controlled compiler command failed; raw stderr retained")
            with mock.patch.object(sys, "argv", arguments), mock.patch.object(CAMPAIGN, "suite_environment", return_value={}), \
                    mock.patch.object(CAMPAIGN.perf, "compare_checkouts", side_effect=failure):
                with self.assertRaises((ValueError, RuntimeError)):
                    CAMPAIGN.main()
            result = json.loads((output / "campaign-failure.json").read_text())
            self.assertEqual(result["state"], "functional-failure")
            self.assertEqual(result["child_exit_code"], 101)
            self.assertEqual(result["stage"], "build/inventory")
            self.assertEqual(result["reason"], str(failure))
            self.assertIsNone(result["metrics"])
            self.assertEqual(CAMPAIGN.suite_observer_report(output, 2), result)
            with self.assertRaises(ValueError):
                CAMPAIGN.suite_observer_report(output, 0)

    def test_failed_build_observer_proves_barriers_without_crediting_fixture_use(self):
        markers = {phase: {"id": char * 64, "image_id": "sha256:" + "c" * 64, "label": phase}
                   for phase, char in (("ready", "a"), ("end", "b"))}
        events = [{"Type": "container", "Action": "create", "timeNano": epoch,
                   "Actor": {"ID": markers[phase]["id"], "Attributes": {"magnetar.performance.observer": phase,
                             "image": markers[phase]["image_id"]}}}
                  for phase, epoch in (("ready", 10), ("end", 20))]
        failure = {"state": "functional-failure", "stage": "build/inventory", "metrics": None, "child_exit_code": 101}
        images = {"broker:fixed": {"image_id": "sha256:" + "d" * 64, "reference": "broker@sha256:" + "e" * 64}}
        proof = CAMPAIGN.check_fixture_events(events, images, failure, markers)
        self.assertEqual(proof["observed"], [])
        self.assertEqual(proof["observations"], [])
        self.assertEqual(proof["state"], "functional-failure")
        for changed in ([], events[:1], events + [{"Type": "container", "Action": "create", "timeNano": 15,
                "Actor": {"ID": "f" * 64, "Attributes": {"image": "broker:fixed"}}}]):
            with self.subTest(events=changed), self.assertRaises(ValueError):
                CAMPAIGN.check_fixture_events(changed, images, failure, markers)

    def test_container_identity_keeps_host_ownership_and_only_socket_group(self):
        socket_stat = os.stat_result((stat.S_IFSOCK | 0o660, 0, 0, 1, 0, 964, 0, 0, 0, 0))
        with mock.patch.object(os, "getuid", return_value=1001), mock.patch.object(os, "getgid", return_value=1002), mock.patch.object(Path, "stat", return_value=socket_stat):
            identity, options = CAMPAIGN.container_execution_options("/cargo", True)
            self.assertEqual(identity["uid"], 1001)
            self.assertEqual(identity["gid"], 1002)
            self.assertEqual(identity["docker_socket_gid"], 964)
            self.assertEqual(options[options.index("--user") + 1], "1001:1002")
            self.assertEqual(options[options.index("--group-add") + 1], "964")
            self.assertIn("/performance-home:rw,size=16m,mode=0700,uid=1001,gid=1002", options)
            self.assertIn("HOME=/performance-home", options)
            self.assertIn("CARGO_HOME=/cargo", options)
            _, prepare_options = CAMPAIGN.container_execution_options("/tmp/performance-cargo")
            self.assertNotIn("--group-add", prepare_options)
            self.assertIn("CARGO_HOME=/tmp/performance-cargo", prepare_options)
        ordinary_file = os.stat_result((stat.S_IFREG | 0o660, 0, 0, 1, 0, 964, 0, 0, 0, 0))
        with mock.patch.object(Path, "stat", return_value=ordinary_file), self.assertRaisesRegex(ValueError, "not a Unix socket"):
            CAMPAIGN.container_execution_options("/cargo", True)

    def test_suite_does_not_require_an_unused_live_broker(self):
        launch = {"image_id": "sha256:" + "a" * 64, "runner_identity": "b" * 64, "cpu_affinity": [2, 3],
                  "profile": CAMPAIGN.perf.PROFILE_ENV, "features": ["all"], "axis": "workspace-all-features",
                  "fixture_scope": "test-suite-owned-fixtures"}
        CAMPAIGN.check_environment(launch, CAMPAIGN.PACKAGES, "rustc 1.98.1 (pinned)", {2, 3}, suite=True)
        launch.update(seed=17, dockerfile_sha256="c" * 64, pip33_fixture={"prefix": "owned"})
        def command(argv, _):
            return "rustc 1.98.1 (pinned)" if argv[0] == "rustc" else CAMPAIGN.PACKAGES[argv[-1]]
        with mock.patch.object(CAMPAIGN.perf, "read_command", side_effect=command), mock.patch.object(os, "sched_getaffinity", return_value={2, 3}):
            environment = CAMPAIGN.suite_environment(launch, Path("."))
        self.assertEqual(environment["pip33_fixture"], launch["pip33_fixture"])

        with self.assertRaises(ValueError):
            CAMPAIGN.check_environment(launch, CAMPAIGN.PACKAGES, "rustc 1.98.1 (pinned)", {2, 3})

    def test_ci_worker_and_seed_union_rejects_missing_duplicate_and_unexpected_runs(self):
        payload = {"shards": 4, "seed_workers": 4, "seed_union": {"seeds": list(range(1, 33)) + [255]},
                   "revisions": {"base": "a" * 40, "candidate": "b" * 40}}
        workers = []
        for kind, index in [("workspace", index) for index in range(4)] + [("moonpool", index) for index in range(4)] + [("scenarios", 0)]:
            runs = [{"axis": axis, "seed": seed, "shard": shard, "runtime": runtime, "scenario": scenario}
                    for axis, seed, shard, _, runtime, scenario in CAMPAIGN.ci_expected_runs(payload, kind, index)]
            workers.append({"kind": kind, "worker": index, "revisions": payload["revisions"], "runs": runs})
        CAMPAIGN.check_ci_workers(payload, workers)
        seeds = [run["seed"] for row in workers if row["kind"] == "moonpool" for run in row["runs"]]
        self.assertCountEqual(seeds, payload["seed_union"]["seeds"])
        for mutation in ("missing-worker", "duplicate-worker", "unexpected-worker", "missing-seed", "duplicate-seed", "unexpected-seed", "wrong-ref"):
            changed = json.loads(json.dumps(workers))
            if mutation == "missing-worker": changed.pop()
            elif mutation == "duplicate-worker": changed.append(changed[0])
            elif mutation == "unexpected-worker": changed[0]["worker"] = 4
            elif mutation == "missing-seed": changed[4]["runs"].pop()
            elif mutation == "duplicate-seed": changed[4]["runs"].append(changed[4]["runs"][0])
            elif mutation == "unexpected-seed": changed[4]["runs"][0]["seed"] = 999
            else: changed[0]["revisions"]["base"] = "c" * 40
            with self.subTest(mutation=mutation), self.assertRaises(ValueError):
                CAMPAIGN.check_ci_workers(payload, changed)

    def test_scenario_union_is_derived_from_actual_reports_and_raw_inputs(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            payload = {"revisions": {"base": "a" * 40, "candidate": "b" * 40},
                       "image_id": "sha256:" + "c" * 64, "dockerfile_sha256": "d" * 64, "compiler": "rustc fixed",
                       "harness": {"scripts/performance_campaign.py": "e" * 64, CAMPAIGN.EXAMPLE_SOURCE: "f" * 64},
                       "scenario_contract": {"messages": 2, "payload_bytes": 16, "seed": 17, "batching": False, "control_cost_multiplier": 1, "repetitions": 2}}
            payload["source_snapshots"] = {side: {"source_tree_sha256": side + " sources", "source_manifest": {"Cargo.lock": side + " lock"}} for side in ("base", "candidate")}
            report = {"state": "partial", "environment": {"image_id": payload["image_id"], "dockerfile_sha256": payload["dockerfile_sha256"],
                      "compiler": payload["compiler"], "harness_sha256": payload["harness"]["scripts/performance_campaign.py"],
                      "example_sha256": payload["harness"][CAMPAIGN.EXAMPLE_SOURCE], "profile": CAMPAIGN.perf.PROFILE_ENV, "features": ["moonpool", "scalable-topics"]}, "manifests": {},
                      "observations": {"base": [], "candidate": [], "calibration": []}}
            run = {"runtime": "tokio", "scenario": "roundtrip"}
            for side in ("base", "candidate"):
                directory = root / side; directory.mkdir(); (directory / "performance").write_text(side + " ELF")
                report["manifests"][side] = {"measured_sha": payload["revisions"][side], **payload["source_snapshots"][side],
                                             "lockfile_sha256": side + " lock", "binary_sha256": CAMPAIGN.perf.digest(directory / "performance")}
            for side in report["observations"]:
                for mode in CAMPAIGN.MODES:
                    for repetition in range(2):
                        prefix = f"{side}-{mode}-{repetition}"
                        reference = "base" if side == "calibration" else side
                        request = {**{key: value for key, value in payload["scenario_contract"].items() if key != "repetitions"},
                                   "scenario_id": "roundtrip", "runtime": "tokio", "scenario_revision": "f" * 64,
                                   "topic": prefix, "service_url": "pulsar://owned"}
                        observation = {"schema_version": 1, **{key: request[key] for key in ("scenario_id", "runtime", "scenario_revision", "seed", "payload_bytes", "batching")},
                                       "requested": 2, "completed": 2, "native_run_ns": 1, "scope": "client", "denominator": "messages",
                                       "send_latency_ns": [1, 1], "receive_ack_latency_ns": [1, 1],
                                       "phases": [[name, index + 1] for index, name in enumerate(("setup", "warmup", "run", "drain", "finished"))],
                                       "terminal_marker_acknowledged": True, "broker_message_after_marker": False,
                                       "consumer_queue_after_drain": 0, "producer_pending_after_drain": 0, "topic_partition_count": 0,
                                       "drain_verified_messages": 2, "confirmed_payload_bytes": 32}
                        metrics = {"native_run_ns": 1, "syscall_calls": 1, "malloc_family_growth_bytes": 1, "intercepted_copy_bytes": 1}
                        request_path = root / reference / (prefix + ".request.json")
                        CAMPAIGN.save(request_path, request); CAMPAIGN.save(root / reference / (prefix + ".stdout"), observation)
                        CAMPAIGN.save(root / reference / (prefix + ".collection.json"), {"state": "valid", "metrics": metrics, "collector": mode, "child_exit_code": 0})
                        CAMPAIGN.save(root / reference / (prefix + ".identity.json"), {"binary_sha256": report["manifests"][reference]["binary_sha256"]})
                        CAMPAIGN.save(root / reference / (prefix + ".exit.json"), {"status": 0})
                        report["observations"][side].append({"mode": mode, "state": "valid", "request": request,
                            "request_sha256": CAMPAIGN.perf.digest(request_path), "workload_identity": CAMPAIGN.workload_identity(request),
                            "binary_sha256": report["manifests"][reference]["binary_sha256"], "completed": 2, "observation": observation,
                            "scope": "client" if mode == "native" else "scenario-process-lifecycle-including-harness", "denominator": "messages",
                            "metrics": metrics, "artifact_prefix": prefix})
            CAMPAIGN.save(root / "scenario-report.json", report)
            CAMPAIGN.write_artifact_manifest(root, "campaign-artifacts.json")
            self.assertEqual(CAMPAIGN.validate_scenario_report(report, payload, run, root), ("tokio", "roundtrip"))
            changed = json.loads(json.dumps(report))
            for sample in changed["observations"]["candidate"]:
                sample["request"]["runtime"] = "moonpool"; sample["workload_identity"]["runtime"] = "moonpool"; sample["observation"]["runtime"] = "moonpool"
            for sample in changed["observations"]["candidate"]:
                prefix = sample["artifact_prefix"]
                CAMPAIGN.save(root / "candidate" / (prefix + ".request.json"), sample["request"])
                CAMPAIGN.save(root / "candidate" / (prefix + ".stdout"), sample["observation"])
                sample["request_sha256"] = CAMPAIGN.perf.digest(root / "candidate" / (prefix + ".request.json"))
            CAMPAIGN.save(root / "scenario-report.json", changed)
            CAMPAIGN.write_artifact_manifest(root, "campaign-artifacts.json")
            with self.assertRaisesRegex(ValueError, "actual scenario route/work"):
                CAMPAIGN.validate_scenario_report(changed, payload, run, root)
            for sample in report["observations"]["candidate"]:
                prefix = sample["artifact_prefix"]
                CAMPAIGN.save(root / "candidate" / (prefix + ".request.json"), sample["request"])
                CAMPAIGN.save(root / "candidate" / (prefix + ".stdout"), sample["observation"])
            for mutation in ("missing-mode", "duplicate-mode", "wrong-sha", "wrong-completed", "changed-work", "changed-raw"):
                changed = json.loads(json.dumps(report))
                if mutation == "missing-mode": changed["observations"]["base"].pop()
                elif mutation == "duplicate-mode": changed["observations"]["base"].append(changed["observations"]["base"][0])
                elif mutation == "wrong-sha": changed["manifests"]["base"]["measured_sha"] = "wrong ref"
                elif mutation == "wrong-completed": changed["observations"]["base"][0]["completed"] = 1
                elif mutation == "changed-work": changed["observations"]["base"][0]["request"]["messages"] = 3
                CAMPAIGN.save(root / "scenario-report.json", changed)
                CAMPAIGN.write_artifact_manifest(root, "campaign-artifacts.json")
                if mutation == "changed-raw": (root / "base/base-native-0.stdout").write_text("mutated raw")
                with self.subTest(mutation=mutation), self.assertRaises(ValueError):
                    CAMPAIGN.validate_scenario_report(changed, payload, run, root)

    def test_fixture_event_closure_is_positive_per_observation(self):
        images = {"broker:fixed": {"image_id": "sha256:" + "a" * 64, "reference": "broker@sha256:" + "b" * 64}}
        tools = "sha256:" + "c" * 64
        markers = {phase: {"id": char * 64, "label": "owned-" + phase, "image_id": tools}
                   for phase, char in (("ready", "d"), ("end", "e"))}
        def event(ident, image, when, label=None):
            attributes = {"image": image}
            if label: attributes["magnetar.performance.observer"] = label
            return {"Type": "container", "Action": "create", "timeNano": when,
                    "Actor": {"ID": ident, "Attributes": attributes}}
        ready = event(markers["ready"]["id"], tools, 10, markers["ready"]["label"])
        end = event(markers["end"]["id"], tools, 100, markers["end"]["label"])
        entry = {"family_id": "e2e", "tests": [{"name": "witness", "ignored": False}],
                 "fixture_policy": {"scope": "fixture-required", "basis": "assigned constructor"}}
        sample = {"family_id": "e2e", "repetition": 0, "state": "valid", "fixture_scope": "fixture-required",
                  "started_epoch_ns": 20, "finished_epoch_ns": 40}
        report = {"manifests": {side: {"families": [entry]} for side in ("base", "candidate")},
                  "observations": {"base": [sample], "candidate": [dict(sample, started_epoch_ns=50, finished_epoch_ns=80)]},
                  "base_base_calibration": []}
        valid = [ready, event("f" * 64, "broker:fixed", 30), event("1" * 64, "broker:fixed", 60), end]
        observed = CAMPAIGN.check_fixture_events(valid, images, report, markers)
        self.assertEqual(len(observed["observations"]), 2)
        self.assertEqual(observed["observed"][0]["image_id"], images["broker:fixed"]["image_id"])
        for bad in ([], valid[1:], valid[:-1], [ready, valid[1], end],
                    [ready, event("f" * 64, "unfrozen:latest", 30), valid[2], end],
                    [*valid, {"Type": "image", "Action": "tag", "Actor": {"ID": tools}}]):
            with self.subTest(bad=bad), self.assertRaises(ValueError):
                CAMPAIGN.check_fixture_events(bad, images, report, markers)
        free = json.loads(json.dumps(report))
        for side in ("base", "candidate"):
            free["manifests"][side]["families"][0]["fixture_policy"]["scope"] = "fixture-free"
            free["observations"][side][0]["fixture_scope"] = "fixture-free"
        self.assertEqual(CAMPAIGN.check_fixture_events([ready, end], images, free, markers)["observed"], [])
        with self.assertRaises(ValueError):
            CAMPAIGN.check_fixture_events(valid, images, free, markers)
        overlapping = json.loads(json.dumps(report))
        overlapping["observations"]["candidate"][0].update(started_epoch_ns=20, finished_epoch_ns=40)
        with self.assertRaises(ValueError):
            CAMPAIGN.check_fixture_events(valid, images, overlapping, markers)
        for side in ("base", "candidate"):
            free["manifests"][side]["families"][0]["fixture_policy"]["scope"] = "fixture-required"
            free["manifests"][side]["families"][0]["tests"][0]["ignored"] = True
            free["observations"][side][0].update(state="unmeasured")
        CAMPAIGN.check_fixture_events([ready, end], images, free, markers)

    def test_observer_barrier_requires_capture_and_live_process(self):
        with tempfile.TemporaryDirectory() as temporary:
            path = Path(temporary) / "events.jsonl"; path.write_text("")
            marker = {"id": "a" * 64, "label": "owned-ready"}
            process = mock.Mock(); process.poll.return_value = None
            with self.assertRaises(ValueError):
                CAMPAIGN.wait_event_marker(path, marker, process, timeout=0)
            CAMPAIGN.save(path, {"Type": "container", "Action": "create", "Actor": {"ID": marker["id"], "Attributes": {"magnetar.performance.observer": marker["label"]}}})
            # Docker events is one compact JSON object per line.
            path.write_text(json.dumps(json.loads(path.read_text())) + "\n")
            CAMPAIGN.wait_event_marker(path, marker, process)
            process.poll.return_value = 0
            with self.assertRaises(ValueError):
                CAMPAIGN.wait_event_marker(path, marker, process)

    def test_empty_fixture_events_are_not_positive_collection_proof(self):
        images = {"broker:fixed": {"image_id": "sha256:" + "a" * 64, "reference": "broker@sha256:" + "b" * 64}}
        with self.assertRaises(ValueError):
            CAMPAIGN.check_fixture_events([], images)

    def test_functional_contract_exit_and_dynamic_example_catalogue(self):
        with mock.patch.object(CAMPAIGN.perf, "run", side_effect=CAMPAIGN.perf.CommandFailure(101, "test failed")):
            with self.assertRaises(CAMPAIGN.CollectionFailure) as caught:
                CAMPAIGN.run_functional_contract(["cargo", "test"], ".", "out", "err")
        self.assertEqual(caught.exception.state, "functional-failure")
        self.assertEqual(caught.exception.child_exit_code, 101)
        complete = "test result: ok. 5 passed; 0 failed; 1 ignored; 0 measured; 0 filtered out;"
        self.assertEqual(CAMPAIGN.example_contract_completion(complete), {"passed": 5, "ignored": 1, "catalogued": 6})
        for invalid in ("", complete.replace("0 failed", "1 failed"), complete.replace("0 filtered", "1 filtered"), complete + complete):
            with self.subTest(invalid=invalid), self.assertRaises(ValueError):
                CAMPAIGN.example_contract_completion(invalid)

    def test_memusage_preserves_net_growth_and_failures(self):
        raw = """Memory usage summary: heap total: 8192, heap peak: 3072, stack peak: 0
         total calls   total memory   failed calls
 malloc|          2           2048              0
realloc|          2           4096              0  (nomove:0, dec:0, free:0)
 calloc|          2           2048              0
   free|          4           8192
"""
        result = CAMPAIGN.parse_memusage(raw)
        self.assertEqual(result["malloc_family_growth_bytes"], 8192)
        self.assertEqual(result["realloc_net_growth_bytes"], 4096)
        self.assertEqual(result["malloc_family_calls"], 6)
        self.assertEqual(result["heap_peak_bytes"], 3072)
        for invalid in ("", raw.replace("3072", "0"), raw.replace("2048              0", "2048              1", 1), raw.replace("8192, heap peak", "9999, heap peak")):
            with self.subTest(invalid=invalid), self.assertRaises(ValueError):
                CAMPAIGN.parse_memusage(invalid)

    def test_collector_failures_do_not_become_zero(self):
        for raw in ("thread panicked at fixture", "strace: ptrace: Operation not permitted", "EPERM", "heaptrack: error: truncated"):
            with self.subTest(raw=raw), self.assertRaises(ValueError):
                CAMPAIGN.check_diagnostics(raw)
        CAMPAIGN.check_diagnostics("PHASE run 1000\n")

    def test_phase_and_completed_work_oracle(self):
        request = {"scenario_id": "control-empty", "scenario_revision": "rev", "runtime": "none", "seed": 17,
                   "messages": 3, "payload_bytes": 256, "batching": False}
        observation = {"schema_version": 1, "scenario_id": "control-empty", "scenario_revision": "rev", "runtime": "none", "seed": 17,
                       "requested": 3, "completed": 3, "payload_bytes": 256, "batching": False, "native_run_ns": 100,
                       "phases": [[name, i + 1] for i, name in enumerate(("setup", "warmup", "run", "drain", "finished"))],
                       "scope": "calibration-control-loop", "denominator": "control-cycle", "send_latency_ns": [], "receive_ack_latency_ns": []}
        CAMPAIGN.check_scenario(observation, request)
        for field, value in (("completed", 2), ("requested", 4), ("native_run_ns", 0), ("phases", []), ("scenario_revision", "other")):
            bad = dict(observation, **{field: value})
            with self.subTest(field=field), self.assertRaises(ValueError):
                CAMPAIGN.check_scenario(bad, request)

    def test_suite_shard_union_detects_omission_and_duplicates(self):
        expected = {"a", "b", "c"}
        CAMPAIGN.check_union(expected, [{"a"}, {"b", "c"}])
        for actual in ([{"a"}, {"b"}], [{"a", "b"}, {"b", "c"}]):
            with self.subTest(actual=actual), self.assertRaises(ValueError):
                CAMPAIGN.check_union(expected, actual)

    def test_semantic_work_and_unique_fixture_bindings_are_distinct(self):
        request = {"scenario_id": "roundtrip", "scenario_revision": "revision", "runtime": "tokio", "messages": 16,
                   "payload_bytes": 256, "seed": 17, "batching": False, "control_cost_multiplier": 1,
                   "topic": "persistent://public/default/one", "service_url": "pulsar://127.0.0.1:20001"}
        fresh = dict(request, topic="persistent://public/default/two", service_url="pulsar://127.0.0.1:20002")
        self.assertEqual(CAMPAIGN.workload_identity(request), CAMPAIGN.workload_identity(fresh))
        for key in ("messages", "payload_bytes", "seed", "batching", "control_cost_multiplier"):
            changed = dict(request, **{key: "different"})
            with self.subTest(key=key):
                self.assertNotEqual(CAMPAIGN.workload_identity(request), CAMPAIGN.workload_identity(changed))

    def test_environment_refuses_missing_identity_and_actual_tool_drift(self):
        launch = {"image_id": "sha256:" + "a" * 64, "runner_identity": "b" * 64, "cpu_affinity": [2, 3],
                  "broker": {"id": "c" * 64, "image": "sha256:" + "d" * 64},
                  "profile": CAMPAIGN.perf.PROFILE_ENV, "features": ["moonpool", "scalable-topics"]}
        CAMPAIGN.check_environment(launch, CAMPAIGN.PACKAGES, "rustc 1.98.1 (pinned)", {2, 3})
        for field in launch:
            with self.subTest(field=field), self.assertRaises(ValueError):
                CAMPAIGN.check_environment(dict(launch, **{field: None}), CAMPAIGN.PACKAGES, "rustc 1.98.1 (pinned)", {2, 3})
        for tools, compiler, affinity in (({}, "rustc 1.98.1 (pinned)", {2, 3}),
                                           (CAMPAIGN.PACKAGES, "rustc 1.99.0 (drift)", {2, 3}),
                                           (CAMPAIGN.PACKAGES, "rustc 1.98.1 (pinned)", {4, 5})):
            with self.subTest(compiler=compiler), self.assertRaises(ValueError):
                CAMPAIGN.check_environment(launch, tools, compiler, affinity)

    def test_rust_allocation_control_checks_bytes_and_rejects_element_size_drift(self):
        # The former inferred i32 Vec produced these real 1000/2000-cycle totals.
        oversized = [{"malloc_family_rows": {"malloc": {"calls": 1100}, "realloc": {"calls": 1006}},
                      "realloc_net_growth_bytes": 1025080},
                     {"malloc_family_rows": {"malloc": {"calls": 2100}, "realloc": {"calls": 2006}},
                      "realloc_net_growth_bytes": 2049080}]
        with self.assertRaises(ValueError):
            CAMPAIGN.check_example_allocations(oversized, 1000, 256)
        byte_samples = [oversized[0], dict(oversized[1], realloc_net_growth_bytes=1281080)]
        CAMPAIGN.check_example_allocations(byte_samples, 1000, 256)
        invalid = [byte_samples[0], dict(byte_samples[1], realloc_net_growth_bytes=1281079)]
        with self.assertRaises(ValueError):
            CAMPAIGN.check_example_allocations(invalid, 1000, 256)

    def test_collect_persists_distinct_functional_and_profiler_failures(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            with self.assertRaises(ValueError) as caught:
                CAMPAIGN.collect([sys.executable, "-c", "raise SystemExit(7)"], root, root / "functional", "native")
            self.assertEqual(getattr(caught.exception, "state", None), "functional-failure")
            status = json.loads((root / "functional.collection.json").read_text())
            self.assertEqual(status["child_exit_code"], 7)
            self.assertIsNone(status["metrics"])
            fake = root / "strace"
            fake.write_text("#!/usr/bin/env python3\nimport sys\nfrom pathlib import Path\nPath(sys.argv[sys.argv.index('-o')+1]).write_text('truncated profile')\n")
            fake.chmod(0o755)
            with mock.patch.dict(os.environ, PATH=str(root) + os.pathsep + os.environ["PATH"]):
                with self.assertRaises(ValueError) as caught:
                    CAMPAIGN.collect([sys.executable, "-c", "print('ok')"], root, root / "profiler", "syscalls")
            self.assertEqual(getattr(caught.exception, "state", None), "invalid-collection")
            status = json.loads((root / "profiler.collection.json").read_text())
            self.assertEqual(status["collector_exit_code"], 0)
            self.assertEqual(status["state"], "invalid-collection")

    def test_cpu_zero_is_reported_below_centisecond_resolution(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            result = CAMPAIGN.collect([sys.executable, "-c", "pass"], root, root / "cpu", "native")
            self.assertEqual(result["cpu_resolution_seconds"], 0.01)
        row = CAMPAIGN.comparison_row("native", "user_cpu_seconds", "s", [0.0, 0.0], [0.0, 0.0], False)
        self.assertEqual(row["base_display"], "<0.01")
        self.assertIsNone(row["relative_percent"])
        self.assertIsNone(row["delta"])
        self.assertIn("quantized", row["verdict"])

    def test_identical_elf_is_calibration_even_with_disjoint_ranges(self):
        row = CAMPAIGN.comparison_row("native", "native_run_ns", "ns", [100, 110], [200, 210], True)
        self.assertEqual(row["comparison_kind"], "calibration-identical-elf")
        self.assertIsNone(row["product_effect"])
        self.assertIn("no product effect", row["verdict"])
        self.assertNotIn("increase", row["verdict"])
        manifests = {side: {"binary_sha256": "a" * 64} for side in ("base", "candidate")}
        observations = {"calibration": [], "base": [], "candidate": []}
        for side, samples in (("calibration", [100, 120]), ("base", [100, 110]), ("candidate", [200, 210])):
            observations[side] = [{"mode": "native", "metrics": {"native_run_ns": value, "user_cpu_seconds": 0.0},
                                   "workload_identity": {"messages": 16}} for value in samples]
        report = {"state": "partial", **CAMPAIGN.summarize_scenarios(manifests, observations)}
        self.assertEqual(report["comparison_kind"], "calibration-identical-elf")
        self.assertEqual(len(report["calibration_summary"]), 2)
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            CAMPAIGN.write_scenario_report(report, root)
            markdown = (root / "scenario-report.md").read_text()
            self.assertIn("identical ELF calibration; no product effect", markdown)
            self.assertIn("<0.01", markdown)
            self.assertIn("Base/base calibration", markdown)


    def test_identical_semantic_inputs_with_different_elf_are_calibration(self):
        identity = {"checkout_inputs": {"client.rs": "d" * 64}, "lockfile_sha256": "c" * 64,
                    "noncompiler_guards": {}, "compiler_inputs": {"client.rs": "d" * 64}, "configuration": {"compiler": "rustc fixed", "profile": "release"},
                    "workload": {"messages": 16}}
        manifests = {"base": {"binary_sha256": "base ELF", "semantic_identity": identity},
                     "candidate": {"binary_sha256": "path-sensitive ELF", "semantic_identity": json.loads(json.dumps(identity))}}
        observations = {"calibration": [], "base": [{"mode": "native", "metrics": {"native_run_ns": 100}, "workload_identity": {"messages": 16}}],
                        "candidate": [{"mode": "native", "metrics": {"native_run_ns": 10}, "workload_identity": {"messages": 16}}]}
        report = CAMPAIGN.summarize_scenarios(manifests, observations)
        self.assertEqual(report["comparison_kind"], "calibration-identical-inputs")
        self.assertIsNone(report["product_effect"])
        self.assertEqual(report["elf_reproducibility"], "different")
        self.assertIn("no product effect", report["comparison"][0]["verdict"])
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            CAMPAIGN.write_scenario_report(report, root)
            self.assertIn("identical source/configuration calibration", (root / "scenario-report.md").read_text())
        for field in identity:
            changed = json.loads(json.dumps(manifests))
            changed["candidate"]["semantic_identity"][field] = "different"
            with self.subTest(field=field):
                self.assertEqual(CAMPAIGN.summarize_scenarios(changed, observations)["comparison_kind"], "base-candidate")
        missing = {side: {"binary_sha256": side + " ELF"} for side in manifests}
        self.assertEqual(CAMPAIGN.summarize_scenarios(missing, observations)["comparison_kind"], "base-candidate")

    def test_documentation_only_reference_change_preserves_product_identity(self):
        base = {"measured_sha": "a" * 40, "source_tree_sha256": "b" * 64, "lockfile_sha256": "c" * 64,
                "source_manifest": {"crates/client.rs": "rust", "docs/performance.md": "old docs", "schema.proto": "schema"},
                "input_manifest": {"scenario_sources": {"crates/client.rs": "rust", "generated.rs": "generated"}, "compiler_sources": {"crates/client.rs": "rust", "generated.rs": "generated"}}}
        candidate = json.loads(json.dumps(base))
        candidate.update(measured_sha="d" * 40, source_tree_sha256="e" * 64)
        candidate["source_manifest"]["docs/performance.md"] = "new docs"
        configuration = {"profile": "release", "toolchain": "rustc fixed"}
        workload = {"messages": 16}
        left = CAMPAIGN.scenario_semantic_identity(base, configuration, workload)
        right = CAMPAIGN.scenario_semantic_identity(candidate, configuration, workload)
        manifests = {"base": {"binary_sha256": "base ELF", "semantic_identity": left},
                     "candidate": {"binary_sha256": "candidate ELF", "semantic_identity": right}}
        observations = {"base": [{"mode": "native", "metrics": {"native_run_ns": 100}, "workload_identity": workload}],
                        "candidate": [{"mode": "native", "metrics": {"native_run_ns": 10}, "workload_identity": workload}], "calibration": []}
        report = CAMPAIGN.summarize_scenarios(manifests, observations)
        self.assertEqual(report["comparison_state"], "unmeasured")
        self.assertIsNone(report["product_effect"])
        self.assertIsNone(report["comparison"][0]["delta"])
        same_bytes = json.loads(json.dumps(base)); same_bytes["measured_sha"] = "different ref"
        self.assertEqual(left, CAMPAIGN.scenario_semantic_identity(same_bytes, configuration, workload))
        for source in ("crates/client.rs", "schema.proto"):
            altered = json.loads(json.dumps(candidate)); altered["source_manifest"][source] = "changed product"
            self.assertNotEqual(left, CAMPAIGN.scenario_semantic_identity(altered, configuration, workload))
        for manifest in (base, candidate):
            manifest["input_manifest"]["scenario_sources"]["docs/performance.md"] = manifest["source_manifest"]["docs/performance.md"]
        self.assertNotEqual(CAMPAIGN.scenario_semantic_identity(base, configuration, workload),
                            CAMPAIGN.scenario_semantic_identity(candidate, configuration, workload))

    def test_noncompiled_snapshot_changes_never_establish_product_effect(self):
        identity = {"checkout_inputs": {"main.rs": "old", ".github/workflows/lint.yml": "old", "data.arbitrary": "old", "dependency.rs": "old"},
                    "noncompiler_guards": {}, "compiler_inputs": {"main.rs": "old"}, "lockfile_sha256": "c" * 64,
                    "configuration": {"compiler": "fixed", "profile": "release"}, "workload": {"messages": 16}}
        observations = {"base": [{"mode": "native", "metrics": {"native_run_ns": 100}, "workload_identity": {"messages": 16}}],
                        "candidate": [{"mode": "native", "metrics": {"native_run_ns": 10}, "workload_identity": {"messages": 16}}], "calibration": []}
        for path in (".github/workflows/lint.yml", "data.arbitrary", "dependency.rs"):
            manifests = {"base": {"binary_sha256": "base ELF", "semantic_identity": json.loads(json.dumps(identity))},
                         "candidate": {"binary_sha256": "candidate ELF", "semantic_identity": json.loads(json.dumps(identity))}}
            manifests["candidate"]["semantic_identity"]["checkout_inputs"][path] = "changed outside proven compiler inputs"
            report = CAMPAIGN.summarize_scenarios(manifests, observations)
            with self.subTest(path=path):
                self.assertEqual(report["comparison_state"], "unmeasured")
                self.assertIsNone(report["product_effect"])
                self.assertIsNone(report["comparison"][0]["delta"])
                self.assertIsNone(report["comparison"][0]["relative_percent"])
        manifests = {"base": {"binary_sha256": "base ELF", "semantic_identity": json.loads(json.dumps(identity))},
                     "candidate": {"binary_sha256": "candidate ELF", "semantic_identity": json.loads(json.dumps(identity))}}
        for key in ("checkout_inputs", "compiler_inputs"):
            manifests["candidate"]["semantic_identity"][key]["main.rs"] = "new compiled implementation"
        report = CAMPAIGN.summarize_scenarios(manifests, observations)
        self.assertEqual(report["comparison_state"], "comparable")
        self.assertEqual(report["product_effect"], "informative-cost-comparison")
        for field in ("configuration", "workload"):
            altered = json.loads(json.dumps(manifests)); altered["candidate"]["semantic_identity"][field]["different"] = True
            with self.subTest(field=field):
                self.assertEqual(CAMPAIGN.summarize_scenarios(altered, observations)["comparison_state"], "unmeasured")

    def test_conservative_test_helper_is_not_proven_product_input(self):
        helper = "crates/magnetar/tests/helper.rs"
        base = {"source_manifest": {"main.rs": "compiled", helper: "helper old"}, "lockfile_sha256": "c" * 64,
                "input_manifest": {"scenario_sources": {"main.rs": "compiled", helper: "helper old"}, "compiler_sources": {"main.rs": "compiled"}}}
        candidate = json.loads(json.dumps(base))
        candidate["source_manifest"][helper] = "helper new"
        candidate["input_manifest"]["scenario_sources"][helper] = "helper new"
        configuration = {"compiler": "rustc fixed", "profile": "release"}; workload = {"messages": 16}
        manifests = {"base": {"binary_sha256": "base ELF", "semantic_identity": CAMPAIGN.scenario_semantic_identity(base, configuration, workload)},
                     "candidate": {"binary_sha256": "path ELF", "semantic_identity": CAMPAIGN.scenario_semantic_identity(candidate, configuration, workload)}}
        observations = {"base": [{"mode": "native", "metrics": {"native_run_ns": 100}, "workload_identity": workload}],
                        "candidate": [{"mode": "native", "metrics": {"native_run_ns": 10}, "workload_identity": workload}], "calibration": []}
        report = CAMPAIGN.summarize_scenarios(manifests, observations)
        self.assertEqual(report["comparison_state"], "unmeasured")
        self.assertIsNone(report["product_effect"])
        self.assertIsNone(report["comparison"][0]["delta"])

    def test_image_requires_matching_build_provenance_labels(self):
        digest = "a" * 64
        image = {"Id": "sha256:" + "b" * 64, "Config": {"Labels": {
            "magnetar.performance.dockerfile-sha256": digest,
            "org.opencontainers.image.base.digest": CAMPAIGN.IMAGE_BASE}}}
        CAMPAIGN.check_image(image, image["Id"], digest)
        for field in image["Config"]["Labels"]:
            changed = dict(image, Config={"Labels": dict(image["Config"]["Labels"], **{field: "stale"})})
            with self.subTest(field=field), self.assertRaises(ValueError):
                CAMPAIGN.check_image(changed, image["Id"], digest)
        with self.assertRaises(ValueError):
            CAMPAIGN.check_image(dict(image, Config={"Labels": {}}), image["Id"], digest)

    def test_final_manifest_binds_every_outer_and_inner_launcher_file(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            names = ("artifacts.json", "container.json", "environment.json", "container-exit.json")
            for name in names:
                (root / name).write_text('{}\n')
            events = root / "fixture-events.jsonl"
            events.write_text("ready\n")
            CAMPAIGN.write_artifact_manifest(root)
            CAMPAIGN.write_artifact_manifest(root, "campaign-artifacts.json", required=names)
            events.write_text("ready\nend\n")
            with self.assertRaisesRegex(ValueError, "fixture-events"):
                CAMPAIGN.verify_artifact_manifest(root / "artifacts.json")
            CAMPAIGN.write_artifact_manifest(root)
            self.assertNotIn("campaign-artifacts.json", json.loads((root / "artifacts.json").read_text()))
            CAMPAIGN.verify_artifact_manifest(root / "artifacts.json")
            CAMPAIGN.write_artifact_manifest(root, "campaign-artifacts.json", required=names)
            CAMPAIGN.verify_artifact_manifest(root / "artifacts.json")
            CAMPAIGN.verify_artifact_manifest(root / "campaign-artifacts.json")
            CAMPAIGN.write_artifact_manifest(root, "campaign-artifacts.json", required=names)
            CAMPAIGN.verify_artifact_manifest(root / "artifacts.json")
            CAMPAIGN.verify_artifact_manifest(root / "campaign-artifacts.json")
            for name in names:
                (root / name).write_text('{"changed":true}\n')
                with self.subTest(name=name), self.assertRaises(ValueError):
                    CAMPAIGN.verify_artifact_manifest(root / "campaign-artifacts.json")
                (root / name).write_text('{}\n')


    def test_copy_calibration_checks_exact_symbolized_work_not_runtime_noise(self):
        raw = {"dhatFileVersion": 2, "mode": "copy", "pps": [{"tb": 256000, "tbk": 1000, "fs": [1]},
               {"tb": 285, "tbk": 23, "fs": [2]}],
               "ftbl": ["[root]", "0x1: forced_copy (/source/control.c:15)", "0x2: open_path (dl-load.c:1850)"]}
        result = CAMPAIGN.copy_witness(raw, 1000)
        self.assertEqual(result["workload_copy_bytes"], 256000)
        self.assertEqual(result["runtime_copy_bytes"], 285)
        for count in (999, 2000):
            with self.subTest(count=count), self.assertRaises(ValueError):
                CAMPAIGN.copy_witness(raw, count)
        unknown = dict(raw, ftbl=["[root]", "0x1: ???", "0x2: open_path (dl-load.c:1850)"])
        with self.assertRaises(ValueError):
            CAMPAIGN.copy_witness(unknown, 1000)


if __name__ == "__main__":
    unittest.main()
