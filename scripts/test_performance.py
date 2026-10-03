#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
"""Behavioral contracts for the external measurement driver (stdlib only)."""

import importlib.util
import argparse
import contextlib
import io
import json
import os
from pathlib import Path
import tempfile
import unittest
import sys
import subprocess
from unittest import mock

sys.dont_write_bytecode = True


SPEC = importlib.util.spec_from_file_location(
    "performance", Path(__file__).with_name("performance.py")
)
PERF = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(PERF)


def runtime_fixture(entry, root):
    context = {"cwd": str(root), "binary": entry["binary"], "binary_sha256": entry["binary_sha256"],
               "environment": {"CARGO_PKG_NAME": "witness", "CARGO_MANIFEST_DIR": str(root), "LD_LIBRARY_PATH": str(root), "PATH": os.environ["PATH"]}}
    path = root / "cargo-runtime.json"
    path.write_text(json.dumps(context) + "\n")
    entry.update(runtime_context=context, runtime_context_path=str(path), runtime_context_sha256=PERF.digest(path), runtime_identity="witness-context")


def record_mock_runtime(command, cwd, env):
    index = command.index("runtime-exec")
    expected = json.loads(Path(command[index + 1]).read_text())
    actual = {"cwd": str(cwd), "binary": command[index + 5], "binary_sha256": PERF.digest(command[index + 5]),
              "environment": {key: value for key, value in env.items() if PERF.runtime_env_key(key)}}
    if actual != expected:
        raise AssertionError("mock child received a different runtime context")
    actual["started_monotonic_ns"] = PERF.time.perf_counter_ns()
    Path(command[index + 3]).write_text(json.dumps(actual) + "\n")


class MeasurementContracts(unittest.TestCase):
    def test_compiler_directory_snapshot_rejects_membership_race(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (root / "Cargo.toml").write_text('[package]\nname="witness"\nversion="0.1.0"\n')
            (root / "main.rs").write_text("fn main() {}\n")
            inputs = root / "inputs"; inputs.mkdir()
            dep_info = root / "binary.d"
            dep_info.write_text("binary: main.rs inputs\n")
            scan = PERF.directory_inputs
            calls = []

            def add_after_snapshot(path):
                snapshot = scan(path)
                calls.append(path)
                if len(calls) == 1:
                    (inputs / "late").write_text("added after membership snapshot\n")
                return snapshot

            with mock.patch.object(PERF, "directory_inputs", side_effect=add_after_snapshot):
                with self.assertRaisesRegex(ValueError, "changed|conflict"):
                    PERF.scenario_identity(root, "main.rs", [], dep_info)
            leaf = inputs / "late"
            dep_info.write_text("binary: main.rs inputs inputs/late\n")

            def change_after_snapshot(path):
                snapshot = scan(path)
                leaf.write_text("different overlapping input\n")
                return snapshot

            with mock.patch.object(PERF, "directory_inputs", side_effect=change_after_snapshot):
                with self.assertRaisesRegex(ValueError, "conflicting compiled input snapshots"):
                    PERF.scenario_identity(root, "main.rs", [], dep_info)

    def test_compiler_directory_inputs_retain_empty_membership_and_guard_mutations(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (root / "Cargo.toml").write_text('[package]\nname="witness"\nversion="0.1.0"\n')
            (root / "src").mkdir()
            (root / "src/main.rs").write_text("fn main() {}\n")
            refs = root / ".git/refs/heads"; refs.mkdir(parents=True)
            dep_info = root / "binary.d"
            dep_info.write_text("binary: src/main.rs .git/refs/heads\n")
            binary = root / "binary"; binary.write_bytes(b"compiled ELF")
            empty = PERF.scenario_identity(root, "src/main.rs", [], dep_info)
            self.assertIn(".git/refs/heads", empty["compiler_sources"])
            self.assertNotEqual(empty["compiler_sources"][".git/refs/heads"], PERF.hashlib.sha256(b"").hexdigest())
            empty_entry = {"execution_closure": [dict(empty, binary=str(binary), binary_sha256=PERF.digest(binary))]}
            PERF.check_execution_closure(empty_entry, root)
            refs.rmdir()
            # A file containing the directory's exact canonical bytes must still differ in type.
            refs.write_text(json.dumps({".": {"type": "directory"}}, sort_keys=True, separators=(",", ":")))
            with self.assertRaises(ValueError): PERF.check_execution_closure(empty_entry, root)
            refs.unlink(); refs.mkdir()
            (refs / "nested").mkdir()
            branch = refs / "nested/branch"; branch.write_text("reference bytes\n")
            # An overlapping file dependency must be retained exactly once.
            dep_info.write_text("binary: src/main.rs .git/refs/heads .git/refs/heads/nested/branch\n")
            identity = PERF.scenario_identity(root, "src/main.rs", [], dep_info)
            self.assertEqual(set(identity["compiler_sources"]), {"src/main.rs", ".git/refs/heads", ".git/refs/heads/nested", ".git/refs/heads/nested/branch"})
            self.assertEqual(identity["scenario_sources"], identity["compiler_sources"])
            self.assertTrue(all(isinstance(value, str) for value in identity["compiler_sources"].values()))
            artifact = dict(identity, binary=str(binary), binary_sha256=PERF.digest(binary))
            entry = {"execution_closure": [artifact]}
            PERF.check_execution_closure(entry, root)
            for case in ("content", "addition", "deletion", "rename", "type", "empty-directory", "symlink", "fifo"):
                with self.subTest(case=case):
                    if case == "content": branch.write_text("changed reference\n")
                    if case == "addition": (refs / "another").write_text("new reference\n")
                    if case == "deletion": branch.unlink()
                    if case == "rename": branch.rename(refs / "nested/renamed")
                    if case == "type": branch.unlink(); branch.mkdir()
                    if case == "empty-directory": (refs / "empty").mkdir()
                    if case == "symlink": (refs / "link").symlink_to(root / "src/main.rs")
                    if case == "fifo": os.mkfifo(refs / "pipe")
                    with self.assertRaises(ValueError): PERF.check_execution_closure(entry, root)
                    for child in refs.iterdir():
                        if child.name == "nested": continue
                        if child.is_dir() and not child.is_symlink(): child.rmdir()
                        else: child.unlink()
                    for child in (refs / "nested").iterdir():
                        if child.is_dir(): child.rmdir()
                        else: child.unlink()
                    branch.write_text("reference bytes\n")
                    PERF.check_execution_closure(entry, root)

    def test_compiler_directory_inputs_reject_escape_and_symlink_roots(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory); checkout = root / "checkout"; checkout.mkdir()
            (checkout / "Cargo.toml").write_text('[package]\nname="witness"\nversion="0.1.0"\n')
            (checkout / "main.rs").write_text("fn main() {}\n")
            outside = root / "outside"; outside.mkdir()
            (checkout / "link").symlink_to(outside, target_is_directory=True)
            dep_info = root / "binary.d"
            for path in (str(outside), "link", "missing-directory"):
                with self.subTest(path=path):
                    dep_info.write_text("binary: main.rs " + path + "\n")
                    with self.assertRaises(ValueError): PERF.scenario_identity(checkout, "main.rs", [], dep_info)
            parent = checkout / "inputs"; (parent / "nested").mkdir(parents=True)
            (parent / "nested/input").write_text("unchanged input\n")
            dep_info.write_text("binary: main.rs inputs/nested\n")
            identity = PERF.scenario_identity(checkout, "main.rs", [], dep_info)
            binary = checkout / "binary"; binary.write_bytes(b"compiled ELF")
            entry = {"execution_closure": [dict(identity, binary=str(binary), binary_sha256=PERF.digest(binary))]}
            PERF.check_execution_closure(entry, checkout)
            parent.rename(outside / "moved")
            parent.symlink_to(outside / "moved", target_is_directory=True)
            with self.assertRaises(ValueError): PERF.check_execution_closure(entry, checkout)

    def test_companion_dep_info_requires_one_identical_compiler_executable(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            checkout = root / "checkout"; checkout.mkdir()
            (checkout / "src").mkdir()
            (checkout / "Cargo.toml").write_text('[package]\nname="witness"\nversion="0.1.0"\n')
            (checkout / "src/main.rs").write_text("fn main() {}\n")
            target = root / "target"; deps = target / "release/deps"; deps.mkdir(parents=True)
            binary = target / "release/probe-child"; binary.write_bytes(b"child ELF"); binary.chmod(0o755)
            matched = deps / "probe_child-one"; matched.write_bytes(binary.read_bytes()); matched.chmod(0o755)
            dep_info = matched.with_suffix(".d")
            dep_info.write_text(str(matched) + ": src/main.rs\n")
            output = root / "output"; output.mkdir()
            with mock.patch.object(PERF, "read_command", return_value="Build ID: 012345"):
                artifact = PERF.retain_execution_artifact(binary, "src/main.rs", checkout, target, output, "companion")
            self.assertEqual(artifact["compiler_dep_info_resolution"]["mode"], "matched-deps-executable")
            self.assertEqual(artifact["compiler_dep_info_resolution"]["compiler_executable"], str(matched))
            self.assertEqual(Path(artifact["compiler_dep_info"]).read_bytes(), dep_info.read_bytes())
            self.assertEqual(artifact["compiler_sources"], {"src/main.rs": PERF.digest(checkout / "src/main.rs")})
            PERF.check_execution_closure({"execution_closure": [artifact]}, checkout)
            for case in ("different-bytes", "ambiguous", "missing-dep-info", "wrong-target", "adjacent-wrong-target"):
                with self.subTest(case=case):
                    matched.write_bytes(binary.read_bytes())
                    dep_info.write_text(str(matched) + ": src/main.rs\n")
                    other = deps / "probe_child-two"
                    if case == "different-bytes": matched.write_bytes(b"foreign ELF")
                    if case == "ambiguous":
                        other.write_bytes(binary.read_bytes()); other.chmod(0o755)
                        other.with_suffix(".d").write_text(str(other) + ": src/main.rs\n")
                    if case == "missing-dep-info": dep_info.unlink()
                    if case == "wrong-target": dep_info.write_text(str(deps / "foreign") + ": src/main.rs\n")
                    if case == "adjacent-wrong-target": binary.with_suffix(".d").write_text(str(deps / "foreign") + ": src/main.rs\n")
                    with self.assertRaises(ValueError):
                        PERF.retain_execution_artifact(binary, "src/main.rs", checkout, target, output, "companion")
                    for path in (other, other.with_suffix(".d"), binary.with_suffix(".d")):
                        path.unlink(missing_ok=True)

    def test_runtime_context_is_applied_and_mutation_is_invalid(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            binary = root / "child"
            binary.write_text('#!/usr/bin/env python3\nimport os\nfrom pathlib import Path\n'
                              'assert os.environ["CARGO_PKG_NAME"] == "witness"\n'
                              'assert Path.cwd() == Path(__file__).parent\n'
                              'assert os.environ["LD_LIBRARY_PATH"] == str(Path.cwd())\n'
                              'assert "EXAMPLE_TOKEN" not in os.environ and "UNKNOWN_AMBIENT_SECRET" not in os.environ\n'
                              'print("test result: ok. 1 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out;")\n')
            binary.chmod(0o755)
            entry = {"family_id": "runtime", "source": "child.rs", "source_sha256": "input", "binary": str(binary),
                     "binary_sha256": PERF.digest(binary), "scope": "suite", "denominator": "cases",
                     "tests": [{"name": "context", "ignored": False}]}
            runtime_fixture(entry, root)
            with mock.patch.dict(os.environ, {"CARGO_PKG_NAME": "wrong ambient package", "CARGO_BIN_EXE_foreign": "foreign child", "EXAMPLE_TOKEN": "private sentinel", "UNKNOWN_AMBIENT_SECRET": "private sentinel"}):
                result = PERF.measure_family(entry, root, root, 0)
            self.assertEqual(result["state"], "valid")
            self.assertEqual(result["completed"], 1)
            self.assertEqual(result["runtime_identity"], entry["runtime_identity"])
            actual = root / (result["artifact_prefix"] + ".runtime.json")
            self.assertEqual(PERF.digest(actual), result["runtime_record_sha256"])
            record = json.loads(actual.read_text())
            self.assertGreater(record.pop("started_monotonic_ns"), 0)
            self.assertEqual(record, entry["runtime_context"])
            Path(entry["runtime_context_path"]).write_text("changed context")
            with mock.patch.object(PERF, "run") as forbidden:
                result = PERF.measure_family(entry, root, root, 1)
            self.assertEqual(result["state"], "invalid")
            self.assertIn("runtime contract changed", result["reason"])
            forbidden.assert_not_called()

    def test_runtime_guard_refuses_wrong_cwd_environment_or_contract(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            binary = root / "child"; binary.write_text('#!/bin/sh\nexit 0\n'); binary.chmod(0o755)
            entry = {"binary": str(binary), "binary_sha256": PERF.digest(binary)}
            runtime_fixture(entry, root)
            cwd, env = PERF.runtime_launch_context(entry)
            for field in ("cwd", "package", "loader", "digest"):
                record = root / (field + ".json")
                command = [sys.executable, str(Path(PERF.__file__)), "runtime-exec", entry["runtime_context_path"],
                           "0" * 64 if field == "digest" else entry["runtime_context_sha256"], str(record), str(root / (field + ".time")), str(binary)]
                applied = dict(env)
                if field == "package": applied["CARGO_PKG_NAME"] = "foreign"
                if field == "loader": applied["LD_LIBRARY_PATH"] = "foreign"
                with self.subTest(field=field):
                    child = subprocess.run(command, cwd=root.parent if field == "cwd" else cwd, env=applied, capture_output=True, text=True)
                    self.assertEqual(child.returncode, 2)
                    self.assertIn("runtime", child.stderr)
                    self.assertFalse(record.exists())

    def test_cargo_config_environment_outside_capture_scope_is_refused(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory); (root / ".cargo").mkdir()
            config = root / ".cargo/config.toml"
            config.write_text('[env]\nCUSTOM_RUNTIME_VALUE="required"\n')
            with self.assertRaisesRegex(ValueError, "outside the supported runtime contract"):
                PERF.cargo_runtime_configs(root, {"CARGO_HOME": str(root / "cargo-home")})
            config.write_text('[env]\nCARGO_PKG_NAME="configured"\n')
            with self.assertRaisesRegex(ValueError, "outside the supported runtime contract"):
                PERF.cargo_runtime_configs(root, {"CARGO_HOME": str(root / "cargo-home")})
            config.write_text('[alias]\nverify="test"\n')
            self.assertEqual(PERF.cargo_runtime_configs(root, {"CARGO_HOME": str(root / "cargo-home")})[str(config)], PERF.digest(config))

    def test_runtime_difference_refuses_comparison_even_with_identical_elf(self):
        base = {"family_id": "runtime", "source_sha256": "source", "scenario_sha256": "source closure", "catalogue_sha256": "tests",
                "scope": "suite", "denominator": "cases", "completed": 1, "state": "valid", "binary_sha256": "same elf",
                "runtime_identity": "base context", "metrics": {"elapsed_ns": 10, "peak_rss_kib": 20}}
        for candidate in (dict(base, runtime_identity="candidate context"), {key: value for key, value in base.items() if key != "runtime_identity"}):
            rows = PERF.compare_observations([base], [candidate])
            self.assertTrue(all(row["product_effect"] is None and row["verdict"] == "unmeasured" for row in rows))
            self.assertTrue(all("runtime context" in row["reason"] for row in rows))

    def test_significant_environment_changes_calculated_identity_and_refuses_comparison(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            common = {"PATH": "/usr/bin", "CARGO_HOME": str(root / "cargo-home"), "MOONPOOL_SEED": "17",
                      "MAGNETAR_PIP33_CLUSTER_A_URL": "pulsar://localhost:6650"}

            def capture(name, checkout, changes=None):
                package_root = checkout / "crate"; package_root.mkdir(parents=True, exist_ok=True)
                source = package_root / "lib.rs"; source.write_text("// catalogue witness\n")
                target_directory = checkout / "target"; target_directory.mkdir(exist_ok=True)
                binary = target_directory / "witness"; binary.write_text("retained executable")
                output = root / name; output.mkdir()
                package = {"name": "witness", "manifest_path": str(package_root / "Cargo.toml")}
                target = {"name": "witness", "src_path": str(source)}
                environment = {**common, "CARGO_PKG_NAME": "witness", "CARGO_MANIFEST_DIR": str(package_root),
                               "LD_LIBRARY_PATH": str(target_directory), "CARGO_BIN_EXE_companion": str(target_directory / "companion"),
                               **(changes or {})}

                def cargo_catalogue(command, cwd, stdout, stderr, env):
                    runner = json.loads(command[command.index("--config") + 1].split("=", 1)[1])
                    with mock.patch.dict(os.environ, env, clear=True), mock.patch.object(PERF.Path, "cwd", return_value=package_root):
                        record = PERF.runtime_record(binary)
                    Path(runner[-1]).write_text(json.dumps(record) + "\n")
                    Path(stdout).write_text("" if "--ignored" in command else "context: test\n")
                    Path(stderr).write_text("")

                with mock.patch.object(PERF, "run", side_effect=cargo_catalogue):
                    return PERF.capture_cargo_catalogue(checkout, output, target_directory, environment, "host", package, target, "--lib", binary, ["--all-features"])[1]

            baseline = capture("base-context", root / "base-checkout")
            equivalent = capture("head-context", root / "head-checkout")
            self.assertNotEqual(baseline["runtime_context"]["cwd"], equivalent["runtime_context"]["cwd"])
            self.assertNotEqual(baseline["runtime_context"]["environment"]["LD_LIBRARY_PATH"], equivalent["runtime_context"]["environment"]["LD_LIBRARY_PATH"])
            self.assertEqual(baseline["runtime_identity"], equivalent["runtime_identity"])
            observation = {"family_id": "runtime", "source_sha256": "same source", "scenario_sha256": "same helpers",
                           "catalogue_sha256": "same cases", "scope": "suite", "denominator": "cases", "completed": 1,
                           "state": "valid", "binary_sha256": baseline["runtime_context"]["binary_sha256"],
                           "runtime_identity": baseline["runtime_identity"], "metrics": {"elapsed_ns": 10, "peak_rss_kib": 20}}
            for key, changed in (("MOONPOOL_SEED", "18"), ("PATH", "/bin"), ("MAGNETAR_PIP33_CLUSTER_A_URL", "pulsar://localhost:6651")):
                with self.subTest(environment_key=key):
                    candidate = capture(key, root / "head-checkout", {key: changed})
                    self.assertEqual(candidate["runtime_context"]["environment"][key], changed)
                    self.assertNotEqual(candidate["runtime_identity"], baseline["runtime_identity"])
                    rows = PERF.compare_observations([observation], [dict(observation, runtime_identity=candidate["runtime_identity"])])
                    self.assertTrue(all(row["verdict"] == "unmeasured" and row["product_effect"] is None and "runtime context" in row["reason"] for row in rows))

    def test_seed_replay_union_keeps_fixed_and_open_anchors_from_both_refs(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            base = root / "base.toml"; candidate = root / "candidate.toml"
            base.write_text('[[seed]]\nvalue="0x1"\n[[seed]]\nvalue="0xff"\n[[seed]]\nvalue="0xaa"\nstatus="closed"\n')
            candidate.write_text('[[seed]]\nvalue="255"\n[[seed]]\nvalue="0x100"\n')
            result = PERF.seed_union({"base": base, "candidate": candidate})
            self.assertEqual(result["seeds"], [*range(1, 33), 255, 256])
            self.assertEqual(result["registries"]["base"]["sha256"], PERF.digest(base))
            for raw in ('-1', str(2**64), 'not-a-seed'):
                candidate.write_text(f'[[seed]]\nvalue="{raw}"\n')
                with self.subTest(raw=raw), self.assertRaises(ValueError):
                    PERF.seed_union({"base": base, "candidate": candidate})

    def test_no_buggify_axis_requires_actual_compiler_features(self):
        packages, flags, features, scope = PERF.axis_configuration("moonpool-no-buggify", [])
        self.assertEqual(packages, ["magnetar-runtime-moonpool"])
        self.assertEqual(flags, ["--no-default-features", "--features", "crypto-aws-lc-rs"])
        self.assertEqual(features, ["no-default-features", "crypto-aws-lc-rs"])
        self.assertEqual(scope, "runtime-moonpool")
        metadata = {"packages": [{"id": "moonpool-id", "name": "magnetar-runtime-moonpool"}]}
        artifact = {"reason": "compiler-artifact", "package_id": "moonpool-id", "features": ["crypto-aws-lc-rs"]}
        PERF.check_axis_artifacts("moonpool-no-buggify", metadata, [artifact])
        for active in (["crypto-aws-lc-rs", "buggify"], [], ["buggify"]):
            with self.subTest(active=active), self.assertRaises(ValueError):
                PERF.check_axis_artifacts("moonpool-no-buggify", metadata, [dict(artifact, features=active)])
        with self.assertRaises(ValueError):
            PERF.check_axis_artifacts("moonpool-no-buggify", metadata, [])
        with self.assertRaises(ValueError):
            PERF.axis_configuration("moonpool-no-buggify", ["magnetar"])

    def test_replay_cache_reuse_binds_source_and_configuration_not_seed(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory); checkout = root / "checkout"; checkout.mkdir()
            (checkout / "Cargo.lock").write_text("own lock")
            args = argparse.Namespace(build_cache=root / "cache", build_session="replay", axis="moonpool-no-buggify")
            environment = {"image_id": "image", "toolchain": "compiler", "features": ["crypto-aws-lc-rs"], "seed": 1}
            with mock.patch.object(PERF, "source_snapshot", return_value={"source_tree_sha256": "first"}):
                first = PERF.session_target(args, root / "first", "base", checkout, None, environment)
                self.assertEqual(first, PERF.session_target(args, root / "second", "base", checkout, None, dict(environment, seed=32)))
                with self.assertRaisesRegex(ValueError, "frozen contract"):
                    PERF.session_target(args, root / "third", "base", checkout, None, dict(environment, image_id="other"))
            with mock.patch.object(PERF, "source_snapshot", return_value={"source_tree_sha256": "changed"}), self.assertRaisesRegex(ValueError, "frozen contract"):
                PERF.session_target(args, root / "third", "base", checkout, None, environment)

    def test_pip33_fixture_requires_owned_images_and_exact_main_bindings(self):
        fixture = {"prefix": "private", "image_id": "sha256:" + "a" * 64,
                   "bindings": {"MAGNETAR_PIP33_CLUSTER_A_URL": "pulsar://localhost:16650",
                                "MAGNETAR_PIP33_CLUSTER_B_URL": "pulsar://localhost:16651",
                                "MAGNETAR_PIP33_ADMIN_B_URL": "http://localhost:18081"},
                   "containers": [{"Id": str(index) * 64, "Image": "sha256:" + "a" * 64, "Name": "/private-" + ("zookeeper", "pulsar-init", "bookkeeper-a", "bookkeeper-b", "broker-a", "broker-b")[index],
                                   "Config": {"Labels": {"magnetar.performance.fixture": "private"}}, "State": {"Running": True}} for index in range(6)]}
        PERF.check_pip33_fixture(fixture)
        for mutation in ("binding", "missing-process", "duplicate-process", "foreign-label", "wrong-image", "stopped"):
            bad = json.loads(json.dumps(fixture))
            if mutation == "binding": bad["bindings"]["MAGNETAR_PIP33_CLUSTER_A_URL"] = "pulsar://localhost:27737"
            elif mutation == "missing-process": bad["containers"].pop()
            elif mutation == "duplicate-process": bad["containers"][0] = bad["containers"][1]
            elif mutation == "foreign-label": bad["containers"][0]["Config"]["Labels"].clear()
            elif mutation == "wrong-image": bad["containers"][0]["Image"] = "sha256:" + "b" * 64
            else: bad["containers"][0]["State"]["Running"] = False
            with self.subTest(mutation=mutation), self.assertRaises(ValueError):
                PERF.check_pip33_fixture(bad)

    def test_catalogue_preserves_names_and_ignored_exceptions(self):
        tests = PERF.parse_test_list("alpha::happy: test\nbeta::refusal: test\n", {"beta::refusal"})
        self.assertEqual(tests, [
            {"name": "alpha::happy", "ignored": False},
            {"name": "beta::refusal", "ignored": True},
        ])
        with self.assertRaises(ValueError):
            PERF.parse_test_list("", set())
        with self.assertRaises(ValueError):
            PERF.parse_test_list("alpha: test\nalpha: test\n", set())

    def test_success_requires_executed_cases_and_no_hidden_failures(self):
        text = "test result: ok. 2 passed; 0 failed; 1 ignored; 0 measured; 0 filtered out; finished in 0.01s"
        self.assertEqual(PERF.test_completion(text, 3, 1), 2)
        for invalid in ["", text.replace("2 passed", "0 passed"), text.replace("0 failed", "1 failed")]:
            with self.subTest(invalid=invalid), self.assertRaises(ValueError):
                PERF.test_completion(invalid, 3, 1)

    def test_time_is_not_missing_zero_and_rss_keeps_its_unit(self):
        self.assertEqual(PERF.parse_time("0.120000 9812 0"), {"gnu_time_elapsed_seconds": "0.120000", "peak_rss_kib": 9812})
        for invalid in ["", "0.0 0 0", "nan 123 0", "0.1 100 1"]:
            with self.subTest(invalid=invalid), self.assertRaises(ValueError):
                PERF.parse_time(invalid)

    def test_dhat_empty_and_wrong_modes_are_rejected(self):
        heap = {"dhatFileVersion": 2, "mode": "heap", "pps": [
            {"tb": 100, "tbk": 3, "gb": 70, "gbk": 2, "eb": 20, "ebk": 1},
            {"tb": 25, "tbk": 1, "gb": 25, "gbk": 1, "eb": 0, "ebk": 0},
        ]}
        result = PERF.parse_dhat(heap, "heap")
        self.assertEqual(result["allocated_bytes"], 125)
        self.assertEqual(result["allocation_blocks"], 4)
        self.assertEqual(result["peak_live_bytes"], 95)
        self.assertEqual(result["end_live_bytes"], 20)
        self.assertEqual(PERF.parse_dhat({"dhatFileVersion": 2, "mode": "copy", "pps": [{"tb": 40, "tbk": 2}]}, "copy"), {"intercepted_copy_bytes": 40, "intercepted_copy_calls": 2})
        for invalid in [{}, {"dhatFileVersion": 2, "mode": "heap", "pps": []}, heap]:
            with self.subTest(invalid=invalid), self.assertRaises(ValueError):
                PERF.parse_dhat(invalid, "copy")

    def test_strace_accepts_calls_without_errors_column(self):
        trace = " % time seconds usecs/call calls errors syscall\n 10.00 0.000001 1 2 write\n 90.00 0.000009 1 8 2 openat\n 100.00 0.000010 1 10 2 total\n"
        self.assertEqual(PERF.parse_strace(trace), {"syscall_calls": 10, "syscall_errors": 2, "syscalls": {"openat": {"calls": 8, "errors": 2}, "write": {"calls": 2, "errors": 0}}})
        for invalid in ["", trace.replace("10 2 total", "11 2 total")]:
            with self.subTest(invalid=invalid), self.assertRaises(ValueError):
                PERF.parse_strace(invalid)

    def test_report_distinguishes_regression_noise_zero_and_absence(self):
        result = PERF.summarize([100, 110, 90], [200, 210, 190])
        self.assertEqual(result["delta"], 100)
        self.assertEqual(result["relative_percent"], 100)
        self.assertEqual(result["verdict"], "higher cost (informative)")
        self.assertEqual(PERF.summarize([0, 0], [1, 2])["verdict"], "new cost (informative)")
        self.assertIsNone(PERF.summarize([0, 0], [1, 2])["relative_percent"])
        self.assertEqual(PERF.summarize([], [1])["verdict"], "unmeasured")
        self.assertEqual(PERF.summarize([9, 10, 12], [10, 11, 12])["verdict"], "overlapping ranges (informative)")

    def test_comparison_refuses_mixed_contracts(self):
        base = {"harness_sha256": "x", "profile": "release-symbolized", "features": ["all"], "toolchain": "rustc x", "runner": "image x", "kernel": "kernel x", "cpu": "cpu x", "broker_digests": ["sha256:" + "f" * 64]}
        PERF.check_comparable(base, dict(base))
        for field in base:
            changed = dict(base)
            changed[field] = "different"
            with self.subTest(field=field), self.assertRaises(ValueError):
                PERF.check_comparable(base, changed)

    def test_outputs_cannot_live_in_product_worktree(self):
        with tempfile.TemporaryDirectory() as root:
            checkout = Path(root) / "checkout"
            checkout.mkdir()
            with self.assertRaises(ValueError):
                PERF.output_directory(checkout / "logs", [checkout])
            output = PERF.output_directory(Path(root) / "artifacts", [checkout])
            self.assertTrue(output.is_dir())

    def test_completed_work_and_new_families_remain_visible(self):
        row = {"family_id": "refusal", "source_sha256": "same", "scenario_sha256": "helpers", "catalogue_sha256": "cases", "runtime_identity": "same runtime", "scope": "test-process-tree-with-fixture-and-launcher", "denominator": "passed-test-cases", "state": "valid", "completed": 2, "metrics": {"elapsed_ns": 20, "peak_rss_kib": 300}}
        changed = dict(row, completed=3)
        comparison = PERF.compare_observations([row], [changed])
        self.assertTrue(all(entry["verdict"] == "unmeasured" for entry in comparison))
        self.assertIn("completed work differs", comparison[0]["reason"])
        new_family = dict(row, family_id="added-schema-tests")
        comparison = PERF.compare_observations([row], [row, new_family])
        self.assertEqual({entry["family_id"] for entry in comparison}, {"refusal", "added-schema-tests"})
        self.assertEqual(next(entry for entry in comparison if entry["family_id"] == "added-schema-tests")["verdict"], "unmeasured")

    def test_child_failure_is_not_hidden_by_logs_or_time_output(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory)
            with self.assertRaisesRegex(RuntimeError, "child exited 7"):
                PERF.run([sys.executable, "-c", "raise SystemExit(7)"], path, path / "stdout", path / "stderr")

    def test_report_keeps_raw_samples_and_does_not_fail_regression(self):
        with tempfile.TemporaryDirectory() as directory:
            output = Path(directory)
            row = dict(PERF.summarize([1, 2], [9, 10]), family_id="schema", metric="elapsed_ns", unit="ns/test-suite", reason=None)
            PERF.write_report({"state": "partial", "comparison": [row]}, output)
            data = json.loads((output / "report.json").read_text())
            self.assertEqual(data["comparison"][0]["candidate_samples"], [9, 10])
            self.assertIn("higher cost (informative)", (output / "report.md").read_text())

    def test_changed_catalogue_and_helper_cannot_compare_equal_counts(self):
        row = {"family_id": "suite", "source_sha256": "same", "scenario_sha256": "helpers-alpha", "catalogue_sha256": "alpha", "runtime_identity": "same runtime", "scope": "test-process-tree-with-fixture-and-launcher", "denominator": "passed-test-cases", "state": "valid", "completed": 1, "metrics": {"elapsed_ns": 100, "peak_rss_kib": 200}}
        for field in ["scenario_sha256", "catalogue_sha256", "scope", "denominator"]:
            candidate = dict(row, **{field: "different"})
            comparison = PERF.compare_observations([row], [candidate])
            self.assertTrue(all(entry["verdict"] == "unmeasured" for entry in comparison), field)

    def test_identical_suite_elf_is_calibration_even_with_disjoint_cost_ranges(self):
        base = {"family_id": "same-elf", "source_sha256": "source", "scenario_sha256": "inputs",
                "catalogue_sha256": "catalogue", "runtime_identity": "same runtime", "scope": "suite", "denominator": "cases",
                "binary_sha256": "a" * 64, "state": "valid", "completed": 1,
                "metrics": {"elapsed_ns": 1, "peak_rss_kib": 200}}
        candidate = dict(base, metrics={"elapsed_ns": 1000, "peak_rss_kib": 2000})
        rows = PERF.compare_observations([base], [candidate])
        for row in rows:
            self.assertEqual(row["comparison_kind"], "calibration-identical-elf")
            self.assertIsNone(row["product_effect"])
            self.assertEqual(row["verdict"], "calibration variation; no product effect")

    def test_untracked_source_requires_exact_audited_overlay(self):
        dirty = [{"status": "??", "path": "cases.rs", "sha256": "first"}]
        with self.assertRaises(ValueError):
            PERF.validate_source_state("a" * 40, dirty, None)
        overlay = {"schema_version": 1, "measured_sha": "a" * 40, "files": dirty}
        PERF.validate_source_state("a" * 40, dirty, overlay)
        for changed in [dict(overlay, measured_sha="b" * 40), dict(overlay, files=[]),
                        dict(overlay, files=[dict(dirty[0], sha256="changed")])]:
            with self.subTest(changed=changed), self.assertRaises(ValueError):
                PERF.validate_source_state("a" * 40, dirty, changed)

    def test_sub_centisecond_process_uses_monotonic_ns(self):
        with tempfile.TemporaryDirectory() as directory:
            output = Path(directory)
            binary = output / "binary"
            binary.write_text("witness")
            entry = {"family_id": "clock", "source": "clock.rs", "source_sha256": "x", "scenario_sha256": "s", "catalogue_sha256": "c", "runtime_identity": "same runtime", "scope": "test-process-tree-with-fixture-and-launcher", "denominator": "passed-test-cases", "binary": str(binary), "binary_sha256": PERF.digest(binary), "tests": [{"name": "clock", "ignored": False}]}
            runtime_fixture(entry, output)
            def witness(command, cwd, stdout, stderr, env=None):
                record_mock_runtime(command, cwd, env)
                Path(str(stdout).replace(".stdout", ".time")).write_text("0.00 2000 0")
                Path(stdout).write_text("test result: ok. 1 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out;")
                Path(stderr).write_text("")
            with mock.patch.object(PERF, "run", side_effect=witness), mock.patch.object(PERF.time, "perf_counter_ns", side_effect=[100, 9100]):
                result = PERF.measure_family(entry, output, output, 0)
            self.assertEqual(result["metrics"]["elapsed_ns"], 9000)
            self.assertEqual(result["state"], "valid")

    def test_companion_mutation_invalidates_execution_and_is_not_identical_elf_calibration(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            harness = root / "harness"; harness.write_text("fixed harness")
            child = root / "companion"; child.write_text("base child")
            dep_info = root / "companion.d"; dep_info.write_text("cli: cli.rs\n")
            source = root / "cli.rs"; source.write_text("base production source")
            closure = [{"role": "harness", "binary": str(harness), "binary_sha256": PERF.digest(harness)},
                       {"role": "companion", "binary": str(child), "binary_sha256": PERF.digest(child),
                        "compiler_dep_info": str(dep_info), "compiler_dep_info_sha256": PERF.digest(dep_info),
                        "scenario_sources": {"cli.rs": PERF.digest(source)}}]
            entry = {"family_id": "children", "source": "suite.rs", "source_sha256": "test-source", "scope": "suite",
                     "denominator": "passed-test-cases", "binary": str(harness), "binary_sha256": PERF.digest(harness),
                     "execution_closure": closure, "execution_sha256": "base closure", "tests": [{"name": "witness", "ignored": False}]}
            runtime_fixture(entry, root)
            def witness(command, cwd, stdout, stderr, env=None):
                record_mock_runtime(command, cwd, env)
                Path(stdout).write_text("test result: ok. 1 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out;")
                Path(stderr).write_text("")
                Path(str(stdout).replace(".stdout", ".time")).write_text("0.01 2000 0")
            with mock.patch.object(PERF, "run", side_effect=witness) as launched:
                self.assertEqual(PERF.measure_family(entry, root, root, 0)["state"], "valid")
                child.write_text("different child with unchanged harness")
                result = PERF.measure_family(entry, root, root, 1)
                self.assertEqual(result["state"], "invalid")
                self.assertIn("execution closure", result["reason"])
                self.assertEqual(launched.call_count, 1)
            base = {"family_id": "children", "source_sha256": "test-source", "scenario_sha256": "same tests",
                    "catalogue_sha256": "catalogue", "runtime_identity": "same runtime", "scope": "suite", "denominator": "cases", "binary_sha256": PERF.digest(harness),
                    "execution_sha256": "base closure", "calibration_identity": "base sources/config", "state": "valid", "completed": 1,
                    "metrics": {"elapsed_ns": 1, "peak_rss_kib": 200}}
            candidate = dict(base, execution_sha256="candidate closure", calibration_identity="candidate sources/config",
                             metrics={"elapsed_ns": 1000, "peak_rss_kib": 2000})
            self.assertTrue(all(row["comparison_kind"] == "base-candidate" for row in PERF.compare_observations([base], [candidate])))
            relocated = dict(base, binary_sha256="different embedded path", execution_sha256="different path ELF",
                             metrics=candidate["metrics"])
            self.assertTrue(all(row["product_effect"] is None for row in PERF.compare_observations([base], [relocated])))
            child.write_text("base child")
            for artifact, field, new_bytes in ((child, "executable", "mutated binary"), (dep_info, "dep-info", "mutated dep-info"),
                                                (source, "source", "mutated production source")):
                before = artifact.read_bytes(); artifact.write_text(new_bytes)
                with self.subTest(field=field), mock.patch.object(PERF, "run") as forbidden:
                    result = PERF.measure_family(entry, root, root, field)
                    self.assertEqual(result["state"], "invalid")
                    self.assertIn("execution closure", result["reason"])
                    forbidden.assert_not_called()
                artifact.write_bytes(before)

    def test_global_calibration_requires_exact_sources_and_configuration(self):
        provenance = {"measured_sha": "a" * 40, "source_tree_sha256": "b" * 64, "source_manifest": {"test.rs": "b" * 64}, "lockfile_sha256": "c" * 64,
                      **{field: "known-" + field for field in PERF.EXECUTION_CONTRACT_FIELDS}}
        manifests = {side: {"provenance": dict(provenance)} for side in ("base", "candidate")}
        base = {"family_id": "embedded-unit", "source_sha256": "test", "scenario_sha256": "base compiled inputs",
                "catalogue_sha256": "cases", "runtime_identity": "same runtime", "scope": "suite", "denominator": "cases", "completed": 1,
                "state": "valid", "binary_sha256": "base ELF", "metrics": {"elapsed_ns": 10, "peak_rss_kib": 20}}
        candidate = dict(base, scenario_sha256="candidate compiled inputs", binary_sha256="candidate ELF")
        comparison = PERF.compare_observations([base], [candidate])
        self.assertTrue(all(row["reason"] and row["product_effect"] is None for row in comparison))
        manifests["candidate"]["provenance"]["measured_sha"] = "d" * 40
        self.assertEqual(PERF.suite_comparison_kind(manifests, comparison), "base-candidate-partial/unmeasured")
        unchanged = dict(base, family_id="unchanged")
        mixed = PERF.compare_observations([base, unchanged], [candidate, unchanged])
        self.assertEqual(PERF.suite_comparison_kind(manifests, mixed), "base-candidate-partial/unmeasured")
        manifests["candidate"]["provenance"] = dict(provenance)
        comparable = PERF.compare_observations([base], [base])
        self.assertEqual(PERF.suite_comparison_kind(manifests, comparable), "calibration-base-base")
        for field in provenance:
            for value in (None, "unknown", "different"):
                manifests["candidate"]["provenance"] = dict(provenance, **{field: value})
                with self.subTest(field=field, value=value):
                    self.assertEqual(PERF.suite_comparison_kind(manifests, comparable), "base-candidate")
        self.assertEqual(PERF.suite_comparison_kind(manifests, []), "base-candidate-partial/unmeasured")

    def test_ignored_only_catalogue_is_unmeasured_without_launching(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            binary = root / "harness"
            binary.write_text("compiled ignored witness")
            entry = {"binary": str(binary), "family_id": "ignored-only", "source": "ignored.rs", "source_sha256": "source",
                     "binary_sha256": PERF.digest(binary), "scope": "suite", "denominator": "passed-test-cases",
                     "tests": [{"name": "exception", "ignored": True}]}
            def witness(command, cwd, stdout, stderr, env=None):
                Path(stdout).write_text("test result: ok. 0 passed; 0 failed; 1 ignored; 0 measured; 0 filtered out;")
                Path(stderr).write_text("")
                Path(str(stdout).replace(".stdout", ".time")).write_text("0.00 2000 0")
            with mock.patch.object(PERF, "run", side_effect=witness) as child:
                result = PERF.measure_family(entry, root, root, 0)
            self.assertEqual(result["state"], "unmeasured")
            self.assertEqual(result["completed"], 0)
            self.assertEqual(result["requested"], 1)
            self.assertIsNone(result["metrics"])
            child.assert_not_called()

    def test_suite_child_failure_is_distinct_from_missing_collection(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            binary = root / "child"
            entry = {"family_id": "failure", "source": "child.rs", "source_sha256": "input",
                     "scope": "test-process-tree-with-fixture-and-launcher", "denominator": "passed-test-cases",
                     "binary": str(binary), "tests": [{"name": "witness", "ignored": False}]}
            for status, state in ((7, "functional-failure"), (0, "invalid")):
                binary.write_text('#!/bin/sh\nexit ' + str(status) + '\n')
                binary.chmod(0o755)
                entry["binary_sha256"] = PERF.digest(binary)
                runtime_fixture(entry, root)
                result = PERF.measure_family(entry, root, root, status)
                self.assertEqual(result["state"], state)
                self.assertIsNone(result["metrics"])
                if status:
                    self.assertEqual(result["child_exit_code"], status)

    def test_campaign_reconciles_metric_coverage_with_valid_observations(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            for side in ("base", "candidate"):
                (root / side).mkdir()
            snapshot = {"measured_sha": "a" * 40, "source_state": "clean", "overlay": None,
                        "source_manifest": {}, "source_tree_sha256": "sources"}
            entry = {"family_id": "suite", "source_sha256": "source", "scenario_sha256": "helpers",
                     "catalogue_sha256": "cases", "runtime_identity": "same runtime", "scope": "test-process-tree-with-fixture-and-launcher",
                     "denominator": "passed-test-cases", "tests": [{"name": "case", "ignored": False}],
                     "metric_coverage": {"native": "pending", "rss": "pending", "syscall": "unmeasured"}}
            def inventory(*args):
                return {"provenance": dict(snapshot), "families": [dict(entry, metric_coverage=dict(entry["metric_coverage"]))]}
            sample = dict(entry, state="valid", completed=1, metrics={"elapsed_ns": 100, "peak_rss_kib": 200})
            argv = ["performance", "compare", "--base", str(root / "base"), "--candidate", str(root / "candidate"),
                    "--output", str(root / "output"), "--repetitions", "2",
                    "--expected-base-sha", "a" * 40, "--expected-candidate-sha", "a" * 40]
            with mock.patch.object(sys, "argv", argv), mock.patch.object(PERF, "build_inventory", side_effect=inventory), \
                    mock.patch.object(PERF, "source_snapshot", return_value=snapshot), \
                    mock.patch.object(PERF, "check_expected_revision"), \
                    mock.patch.object(PERF, "check_comparable"), mock.patch.object(PERF, "measure_family", return_value=sample), \
                    contextlib.redirect_stdout(io.StringIO()):
                self.assertEqual(PERF.main(), 0)
            report = json.loads((root / "output/report.json").read_text())
            self.assertEqual(report["expected_revisions"], {"base": "a" * 40, "candidate": "a" * 40})
            self.assertEqual(len(report["base_base_calibration"]), 2)
            for manifest in report["manifests"].values():
                self.assertEqual(manifest["families"][0]["metric_coverage"]["native"], "direct-passed-cases")
                self.assertEqual(manifest["families"][0]["metric_coverage"]["rss"], "direct-passed-cases")
                self.assertEqual(manifest["families"][0]["metric_coverage"]["syscall"], "unmeasured")

    def test_ignored_fixture_requires_audit_and_changes_source_snapshot(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (root / '.gitignore').write_text('/fixtures/\n/target/\n')
            subprocess.run(['git', 'init', '-q', str(root)], check=True)
            subprocess.run(['git', '-C', str(root), 'add', '.gitignore'], check=True)
            subprocess.run(['git', '-C', str(root), '-c', 'commit.gpgsign=false', '-c', 'user.name=Fixture',
                            '-c', 'user.email=fixture@example.invalid', 'commit', '-qm', 'fixture'], check=True)
            (root / 'fixtures').mkdir()
            helper = root / 'fixtures/workload.rs'
            helper.write_text('first workload')
            with self.assertRaises(ValueError):
                PERF.source_snapshot(root)
            revision = PERF.read_command(['git', 'rev-parse', 'HEAD'], root)
            overlay = Path(directory).parent / (root.name + '-overlay.json')
            def audit():
                overlay.write_text(json.dumps({'schema_version': 1, 'measured_sha': revision,
                    'files': [{'path': 'fixtures/workload.rs', 'status': '!!', 'sha256': PERF.digest(helper)}]}))
            try:
                audit()
                before = PERF.source_snapshot(root, overlay)
                self.assertIn('fixtures/workload.rs', before['source_manifest'])
                helper.write_text('different workload')
                with self.assertRaises(ValueError):
                    PERF.source_snapshot(root, overlay)
                audit()
                after = PERF.source_snapshot(root, overlay)
                self.assertNotEqual(before['source_tree_sha256'], after['source_tree_sha256'])
            finally:
                overlay.unlink(missing_ok=True)

    def test_generated_root_target_symlink_is_not_a_source_even_when_unmounted(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (root / '.gitignore').write_text('/target\n')
            subprocess.run(['git', 'init', '-q', str(root)], check=True)
            subprocess.run(['git', '-C', str(root), 'add', '.gitignore'], check=True)
            subprocess.run(['git', '-C', str(root), '-c', 'commit.gpgsign=false', '-c', 'user.name=Fixture',
                            '-c', 'user.email=fixture@example.invalid', 'commit', '-qm', 'fixture'], check=True)
            before = PERF.source_snapshot(root)
            (root / 'target').symlink_to(root / 'unmounted-build-cache', target_is_directory=True)
            self.assertEqual(PERF.source_snapshot(root), before)
            (root / 'other-source').symlink_to(root / 'unmounted-source')
            with self.assertRaises(ValueError):
                PERF.source_snapshot(root)

    def test_dep_info_closes_external_fixture_and_escaped_paths(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (root / 'Cargo.toml').write_text('[package]\nname="fixture"\nversion="0.1.0"\n')
            (root / 'tests').mkdir()
            (root / 'fixtures').mkdir()
            suite = root / 'tests/suite.rs'
            suite.write_text('#[path="../fixtures/work load.rs"] mod workload;')
            unused = root / 'tests/unused_helper.rs'
            unused.write_text('conservative helper outside compiler rule')
            helper = root / 'fixtures/work load.rs'
            helper.write_text('first workload')
            dep_info = root / 'suite.d'
            dep_info.write_text('suite: tests/suite.rs fixtures/work\\ load.rs\n\ntests/suite.rs:\n')
            tests = [{'name': 'same_case', 'ignored': False}]
            before = PERF.scenario_identity(root, 'tests/suite.rs', tests, dep_info)
            self.assertIn('fixtures/work load.rs', before['scenario_sources'])
            self.assertEqual(set(before['compiler_sources']), {'tests/suite.rs', 'fixtures/work load.rs'})
            self.assertIn('tests/unused_helper.rs', before['scenario_sources'])
            self.assertNotIn('tests/unused_helper.rs', before['compiler_sources'])
            helper.write_text('different workload')
            after = PERF.scenario_identity(root, 'tests/suite.rs', tests, dep_info)
            self.assertNotEqual(before['scenario_sha256'], after['scenario_sha256'])
            with self.assertRaises(ValueError):
                PERF.scenario_identity(root, 'tests/suite.rs', tests, root / 'missing.d')

    def test_doctest_execution_matches_names_across_merged_groups(self):
        tests = [{"name": "src/lib.rs - (line 1)", "ignored": False},
                 {"name": "src/lib.rs - (line 7)", "ignored": True},
                 {"name": "src/lib.rs - (line 4)", "ignored": False}]
        text = ("test src/lib.rs - (line 1) ... ok\n"
                "test src/lib.rs - (line 7) ... ignored\n"
                "test result: ok. 1 passed; 0 failed; 1 ignored; 0 measured; 0 filtered out;\n"
                "test src/lib.rs - (line 4) - compile fail ... ok\n"
                "test result: ok. 1 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out;\n")
        self.assertEqual(PERF.doctest_completion(text, tests), 2)
        for invalid in [text.replace("line 1", "line 2"), text + text,
                        text.replace("0 failed", "1 failed"), text.replace("... ignored", "... ok")]:
            with self.subTest(invalid=invalid), self.assertRaises(ValueError):
                PERF.doctest_completion(invalid, tests)

    def test_expected_revision_refuses_swaps_and_nonimmutable_names(self):
        with mock.patch.object(PERF, "read_command", return_value="a" * 40):
            PERF.check_expected_revision(Path("/checkout"), "a" * 40)
            for expected in ["b" * 40, "main", "a" * 39, "A" * 40]:
                with self.subTest(expected=expected), self.assertRaises(ValueError):
                    PERF.check_expected_revision(Path("/checkout"), expected)

    def test_shards_form_exact_union_including_new_and_doctest_families(self):
        families = ["proto::unit", "tokio::roundtrip", "moonpool::sim",
                    "facade::doctest::magnetar", "new-family::feature"]
        selected = [family for shard in range(4) for family in families
                    if PERF.shard_matches(family, shard, 4)]
        self.assertEqual(sorted(selected), sorted(families))

    def test_assigned_inventory_builds_only_selected_metadata_target(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory) / "checkout"
            root.mkdir()
            (root / "tests").mkdir()
            (root / "Cargo.toml").write_text('[package]\nname="selected"\nversion="0.0.0"\n')
            targets = []
            for name in ("alpha", "beta", "gamma", "new_target"):
                source = root / "tests" / (name + ".rs")
                source.write_text('#[test] fn witness() {}')
                targets.append({"name": name, "kind": ["test"], "test": True, "doctest": False, "src_path": str(source)})
            metadata = {"target_directory": str(root / "target"), "packages": [{"id": "selected-id", "name": "selected", "manifest_path": str(root / "Cargo.toml"), "targets": targets}]}
            family = lambda target: "tests/" + target["name"] + ".rs::" + target["name"]
            shard = next(index for index in range(4) if 0 < sum(PERF.shard_matches(family(t), index, 4) for t in targets) < len(targets))
            selected = [t for t in targets if PERF.shard_matches(family(t), shard, 4)]
            calls = []
            def read(command, cwd, env=None):
                if command[1] == "metadata":
                    metadata["target_directory"] = env["CARGO_TARGET_DIR"]
                    return json.dumps(metadata)
                if command[0] == "readelf":
                    return "Build ID: abcdef0123456789"
                if command[0] == "rustc":
                    return "host: test-host"
                return "" if "--ignored" in command else "witness: test\n"
            def run(command, cwd, stdout, stderr, env=None):
                calls.append(command)
                self.assertNotIn("--workspace", command)
                names = [command[index + 1] for index, argument in enumerate(command) if argument == "--test"]
                self.assertEqual(sorted(names), sorted(t["name"] for t in selected))
                artifacts = []
                for target in selected:
                    binary = Path(env["CARGO_TARGET_DIR"]) / target["name"]
                    binary.parent.mkdir(exist_ok=True)
                    binary.write_text("ELF witness")
                    binary.with_suffix('.d').write_text(str(binary) + ': ' + target['src_path'] + '\n')
                    artifacts.append({"reason": "compiler-artifact", "package_id": "selected-id", "profile": {"test": True}, "target": target, "executable": str(binary)})
                Path(stdout).write_text('\n'.join(json.dumps(a) for a in artifacts))
                Path(stderr).write_text("")
            output = Path(directory) / "output"; output.mkdir()
            with mock.patch.object(PERF, "source_snapshot", return_value={}), mock.patch.object(PERF, "provenance", return_value={}), \
                    mock.patch.object(PERF, "read_command", side_effect=read), mock.patch.object(PERF, "run", side_effect=run), \
                    mock.patch.object(PERF, "capture_cargo_catalogue", return_value=(["witness: test\n", ""], {"runtime_context": {"environment": {}}})):
                inventory = PERF.build_inventory(root, output, [], shard=shard, shards=4)
            self.assertEqual(len(calls), 1)
            self.assertEqual(sorted(e["family_id"] for e in inventory["families"]), sorted(family(t) for t in selected))
            self.assertEqual(sorted(inventory["expected_families"]), sorted(family(t) for t in targets))
            self.assertEqual(sorted(inventory["assigned_families"]), sorted(family(t) for t in selected))
            for entry in inventory["families"]:
                self.assertTrue(Path(entry["binary"]).is_relative_to(output))
                retained = Path(entry["execution_closure"][0]["retained_binary"])
                (root / "target").mkdir(exist_ok=True)
                (root / "target" / entry["family_id"].split("::")[-1]).write_text("other target ELF")
                self.assertEqual(PERF.digest(entry["binary"]), entry["binary_sha256"])
                self.assertEqual(PERF.digest(entry["compiler_dep_info"]), entry["compiler_dep_info_sha256"])

    def test_counter_union_refuses_omissions_duplicates_and_unexpected_targets(self):
        expected = ["unit", "integration", "new_target", "doctest"]
        PERF.reconcile_families(expected, [["unit", "integration"], ["new_target", "doctest"]])
        for actual in ([["unit"], ["new_target", "doctest"]],
                       [["unit", "integration"], ["new_target", "doctest", "unit"]],
                       [["unit", "integration"], ["new_target", "doctest", "unexpected"]]):
            with self.subTest(actual=actual), self.assertRaises(ValueError):
                PERF.reconcile_families(expected, actual)

    def test_global_reconciliation_requires_each_assigned_family_and_functional_work(self):
        families = ["unit", "new_target", "doctest"]
        plans = {side: {"axis": "workspace-all-features", "expected_revision": ("a" if side == "base" else "b") * 40, "shards": 2,
                        "scope": "partial-package-selection", "packages": ["synthetic-fixture"],
                        "families": [{"family_id": family, "kind": "doctest" if family == "doctest" else "executable", "fixture_policy": {"scope": "fixture-free", "basis": "synthetic target"}} for family in families]} for side in ("base", "candidate")}
        contract = {"harness_sha256": "a" * 64, "profile": "release-symbolized", "features": ["all"],
                    "image_id": "sha256:" + "e" * 64, "dockerfile_sha256": "d" * 64, "toolchain": "rustc frozen",
                    "seed": "17", "runner": "frozen image", "kernel": "test kernel", "cpu": "same local CPU", "broker_digests": ["sha256:" + "f" * 64]}
        for plan in plans.values():
            plan["execution_contract"] = dict(contract)
            plan["harness_sha256"] = contract["harness_sha256"]
            plan["snapshot"] = {"source_tree_sha256": "frozen sources"}
        reports = []
        for shard in range(2):
            assigned = [family for family in families if PERF.shard_matches(family, shard, 2)]
            manifests = {side: {"scope": plans[side]["scope"], "packages": plans[side]["packages"], "axis": plans[side]["axis"], "shard": shard, "shards": 2,
                               "provenance": dict(contract, measured_sha=plans[side]["expected_revision"], source_tree_sha256="frozen sources"), "expected_families": families,
                               "assigned_families": assigned, "families": [], "doctests": []} for side in plans}
            observations = {side: [] for side in plans}
            doctests = {side: [] for side in plans}
            for side in plans:
                for family in assigned:
                    is_doc = family == "doctest"
                    manifests[side]["doctests" if is_doc else "families"].append({"family_id": family, "runtime_identity": "same runtime", "tests": [{"name": "witness", "ignored": False}],
                                                         "metric_coverage": {} if is_doc else {"native": "direct-passed-cases", "rss": "direct-passed-cases"}, "fixture_policy": {"scope": "fixture-free", "basis": "synthetic target"}})
                    if is_doc:
                        doctests[side].append({"family_id": family, "state": "valid", "requested": 1, "completed": 1})
                    else:
                        observations[side].extend({"family_id": family, "runtime_identity": "same runtime", "runtime_record_sha256": "a" * 64, "state": "valid", "requested": 1, "completed": 1, "repetition": rep} for rep in range(2))
            reports.append({"axis": "workspace-all-features", "shard": shard, "shards": 2, "manifests": manifests, "repetitions": 2,
                            "observations": observations, "doctest_observations": doctests,
                            "base_base_calibration": [dict(sample, repetition=f"calibration-{sample['repetition']}") for sample in observations["base"]]})
        result = PERF.reconcile_reports(plans, reports)
        self.assertEqual(result["state"], "partial")
        self.assertEqual(result["coverage"]["base"]["expected"], 3)
        self.assertFalse(result["workspace_union_verified"])
        self.assertEqual(result["coverage"]["base"]["metrics"]["native"], 2)
        for field, value in (("scope", "workspace"), ("packages", ["different-package"]), ("shard", 999), ("shards", 999), ("axis", "wrong-axis")):
            broken = json.loads(json.dumps(reports))
            broken[0]["manifests"]["base"][field] = value
            with self.subTest(manifest_field=field), self.assertRaises(ValueError):
                PERF.reconcile_reports(plans, broken)
        for field in (*contract, "source_tree_sha256"):
            broken = json.loads(json.dumps(reports))
            broken[0]["manifests"]["base"]["provenance"][field] = "different"
            with self.subTest(provenance_field=field), self.assertRaises(ValueError):
                PERF.reconcile_reports(plans, broken)
        # Physical runners differ across shards; each pair must be internally equal.
        varied_cpu = json.loads(json.dumps(reports))
        for report in varied_cpu:
            for side in plans:
                report["manifests"][side]["provenance"]["cpu"] = "CPU for shard " + str(report["shard"])
        PERF.reconcile_reports(plans, varied_cpu)
        unknown_plan = json.loads(json.dumps(plans))
        unknown_plan["base"]["execution_contract"]["image_id"] = "unknown"
        with self.assertRaises(ValueError):
            PERF.reconcile_reports(unknown_plan, reports)
        for mutation in ("missing-shard", "duplicate-shard", "missing-family", "duplicate-family", "unexpected-family", "functional-failure", "wrong-ref", "wrong-axis", "missing-observation", "duplicate-observation", "missing-calibration", "missing-runtime", "different-runtime"):
            broken = json.loads(json.dumps(reports))
            populated = next(report for report in broken if report["manifests"]["base"]["families"])
            if mutation == "missing-shard":
                broken.pop()
            elif mutation == "duplicate-shard":
                broken.append(broken[0])
            elif mutation == "missing-family":
                populated["manifests"]["base"]["families"].pop()
            elif mutation == "duplicate-family":
                populated["manifests"]["base"]["families"].append(populated["manifests"]["base"]["families"][0])
            elif mutation == "unexpected-family":
                populated["manifests"]["base"]["families"][0]["family_id"] = "unexpected"
            elif mutation == "functional-failure":
                populated["observations"]["base"][0]["state"] = "functional-failure"
            elif mutation == "wrong-ref":
                populated["manifests"]["base"]["provenance"]["measured_sha"] = "other-ref"
            elif mutation == "missing-observation":
                populated["observations"]["base"].pop()
            elif mutation == "duplicate-observation":
                populated["observations"]["base"].append(populated["observations"]["base"][0])
            elif mutation == "missing-calibration":
                populated["base_base_calibration"].pop()
            elif mutation == "missing-runtime":
                populated["observations"]["base"][0].pop("runtime_record_sha256")
            elif mutation == "different-runtime":
                populated["observations"]["base"][0]["runtime_identity"] = "different context"
            else:
                populated["axis"] = "different-features"
            with self.subTest(mutation=mutation), self.assertRaises(ValueError):
                PERF.reconcile_reports(plans, broken)


    def test_doctest_only_library_freezes_independent_doc_inputs(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory) / "checkout"
            root.mkdir()
            (root / "src").mkdir()
            (root / "Cargo.toml").write_text('[package]\nname="doc-package"\nversion="0.0.0"\n')
            (root / "src/lib.rs").write_text('#[cfg(doc)] #[doc=include_str!("../target/doc.md")] pub struct Example;')
            (root / "target/doc-host/doc").mkdir(parents=True)
            (root / "target/doc.md").write_text('```\nassert_eq!(6/3, 2);\n```')
            metadata = {"target_directory": str(root / "target"), "packages": [{"name": "doc-package", "targets": [
                {"name": "doc_custom_name", "kind": ["lib"], "test": False, "doctest": True,
                 "src_path": str(root / "src/lib.rs")}]}]}
            def read(command, cwd, env=None):
                if command[1] == "metadata":
                    metadata["target_directory"] = env["CARGO_TARGET_DIR"]
                    return json.dumps(metadata)
                return "host: doc-host"
            def run(command, cwd, stdout, stderr, env=None):
                Path(stderr).write_text("")
                if "--emit=dep-info" in command:
                    (Path(env["CARGO_TARGET_DIR"]) / "doc-host/doc").mkdir(parents=True, exist_ok=True)
                    (Path(env["CARGO_TARGET_DIR"]) / "doc-host/doc/doc_custom_name.d").write_text('doc: src/lib.rs target/doc.md\n')
                listing = "src/../target/doc.md - Example (line 1): test\n"
                Path(stdout).write_text(listing if "--list" in command and "--ignored" not in command else "")
            output = Path(directory) / "output"
            output.mkdir()
            with mock.patch.object(PERF, "run", side_effect=run), mock.patch.object(PERF, "read_command", side_effect=read), \
                    mock.patch.object(PERF, "source_snapshot", return_value={}), mock.patch.object(PERF, "provenance", return_value={}):
                inventory = PERF.build_inventory(root, output, [])
            self.assertEqual(inventory["families"], [])
            entry = inventory["doctests"][0]
            self.assertIn("target/doc.md", entry["scenario_sources"])
            self.assertIn("target/doc.md", inventory["compiled_input_manifest"])
            mutable = Path(entry["cargo_target_directory"]) / "doc-host/doc/doc_custom_name.d"
            mutable.write_text("later reference dep-info")
            self.assertNotEqual(Path(entry["rustdoc_dep_info"]), mutable)
            self.assertEqual(PERF.digest(entry["rustdoc_dep_info"]), entry["rustdoc_dep_info_sha256"])
            (root / "target/doc.md").write_text('```\nassert_eq!(8/4, 2);\n```')
            with mock.patch.object(PERF, "run") as child:
                result = PERF.execute_doctest(entry, root, output)
            self.assertEqual(result["state"], "invalid")
            self.assertIn("target/doc.md", result["reason"])
            child.assert_not_called()


if __name__ == "__main__":
    unittest.main()
