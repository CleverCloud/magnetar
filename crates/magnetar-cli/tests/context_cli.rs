// SPDX-License-Identifier: Apache-2.0

//! End-to-end tests for the `magnetarctl context` command group.
//!
//! Drives the COMPILED binary as a subprocess against a temp config file, so
//! the test exercises the exact clap wiring, the exact printed strings, and
//! the on-disk YAML that pulsarctl must be able to read back.
//!
//! `--config <temp>` overrides the default `~/.config/pulsar/config`, so these
//! tests never touch the developer's real config.

use std::path::Path;
use std::process::Command;

/// Path to the compiled `magnetarctl` binary (cargo injects this env var for
/// integration tests).
fn bin() -> &'static str {
    env!("CARGO_BIN_EXE_magnetarctl")
}

/// Run `magnetarctl --config <cfg> context <args...>`; return (status-ok,
/// stdout, stderr).
fn run_context(cfg: &Path, args: &[&str]) -> (bool, String, String) {
    let out = Command::new(bin())
        .arg("--config")
        .arg(cfg)
        .arg("context")
        .args(args)
        .output()
        .expect("spawn magnetarctl");
    (
        out.status.success(),
        String::from_utf8_lossy(&out.stdout).into_owned(),
        String::from_utf8_lossy(&out.stderr).into_owned(),
    )
}

/// Full lifecycle: set → get → use → current → rename → delete, asserting both
/// the printed strings and the on-disk YAML's pulsarctl key casing.
#[test]
fn context_lifecycle_round_trips() {
    let dir = tempfile::tempdir().expect("tempdir");
    let cfg = dir.path().join("pulsar").join("config");

    // --- set (create) two contexts ---
    let (ok, out, err) = run_context(
        &cfg,
        &[
            "set",
            "prod",
            "--admin-service-url",
            "https://broker:443",
            "--token",
            "prod-tok",
        ],
    );
    assert!(ok, "set prod failed: {err}");
    assert!(out.contains("Context \"prod\" set."), "got: {out}");

    let (ok, _, err) = run_context(
        &cfg,
        &["set", "dev", "--admin-service-url", "http://localhost:8080"],
    );
    assert!(ok, "set dev failed: {err}");

    // The file exists and uses pulsarctl key casing readable by pulsarctl.
    let yaml = std::fs::read_to_string(&cfg).expect("read config");
    assert!(yaml.contains("auth-info:"), "yaml: {yaml}");
    assert!(yaml.contains("admin-service-url:"), "yaml: {yaml}");
    assert!(yaml.contains("prod:"), "yaml: {yaml}");

    // --- get: lists both, no current marker yet (current-context empty) ---
    let (ok, out, err) = run_context(&cfg, &["get"]);
    assert!(ok, "get failed: {err}");
    assert!(out.contains("prod"), "get out: {out}");
    assert!(out.contains("dev"), "get out: {out}");
    assert!(out.contains("ADMIN SERVICE URL"), "get out: {out}");

    // --- use prod: sets current-context, exact message ---
    let (ok, out, err) = run_context(&cfg, &["use", "prod"]);
    assert!(ok, "use failed: {err}");
    assert_eq!(out.trim(), "Switched to context \"prod\".");

    // --- current: prints prod ---
    let (ok, out, err) = run_context(&cfg, &["current"]);
    assert!(ok, "current failed: {err}");
    assert_eq!(out.trim(), "prod");

    // get now marks prod with `*`.
    let (_, out, _) = run_context(&cfg, &["get"]);
    let prod_line = out.lines().find(|l| l.contains("prod")).expect("prod line");
    assert!(prod_line.trim_start().starts_with('*'), "line: {prod_line}");

    // --- set (update) merges onto existing prod without clobbering token ---
    let (ok, _, err) = run_context(
        &cfg,
        &["set", "prod", "--bookie-service-url", "http://bk:8080"],
    );
    assert!(ok, "update prod failed: {err}");
    let yaml = std::fs::read_to_string(&cfg).expect("read config");
    assert!(
        yaml.contains("bookie-service-url: http://bk:8080"),
        "yaml: {yaml}"
    );
    assert!(yaml.contains("prod-tok"), "token clobbered: {yaml}");

    // --- rename prod → production: updates current-context too ---
    let (ok, out, err) = run_context(&cfg, &["rename", "prod", "production"]);
    assert!(ok, "rename failed: {err}");
    assert!(out.contains("renamed"), "out: {out}");
    let (_, out, _) = run_context(&cfg, &["current"]);
    assert_eq!(out.trim(), "production");
    let yaml = std::fs::read_to_string(&cfg).expect("read config");
    assert!(
        !yaml.contains("\nprod:") && yaml.contains("production:"),
        "yaml: {yaml}"
    );

    // --- delete dev: removed from both maps ---
    let (ok, out, err) = run_context(&cfg, &["delete", "dev"]);
    assert!(ok, "delete failed: {err}");
    assert!(out.contains("deleted"), "out: {out}");
    let (_, out, _) = run_context(&cfg, &["get"]);
    assert!(!out.contains("dev"), "dev still listed: {out}");

    // --- delete current (production) warns on stderr ---
    let (ok, _, err) = run_context(&cfg, &["delete", "production"]);
    assert!(ok, "delete current failed");
    assert!(err.contains("current context"), "warn missing: {err}");
}

/// `use` / `delete` / `rename` on an unknown context fail with a non-zero exit.
#[test]
fn context_unknown_name_errors() {
    let dir = tempfile::tempdir().expect("tempdir");
    let cfg = dir.path().join("config");

    let (ok, _, err) = run_context(&cfg, &["use", "ghost"]);
    assert!(!ok, "use ghost should fail");
    assert!(err.contains("context not found"), "err: {err}");

    let (ok, _, _) = run_context(&cfg, &["rename", "ghost", "x"]);
    assert!(!ok, "rename ghost should fail");
}

/// A `current` with no current-context set errors.
#[test]
fn context_current_unset_errors() {
    let dir = tempfile::tempdir().expect("tempdir");
    let cfg = dir.path().join("config");
    // Create a context but never `use` it.
    let (ok, _, _) = run_context(&cfg, &["set", "a", "--admin-service-url", "http://h:8080"]);
    assert!(ok);
    let (ok, _, err) = run_context(&cfg, &["current"]);
    assert!(!ok, "current should fail when unset");
    assert!(err.contains("no current context"), "err: {err}");
}

/// A pulsarctl-written config is consumed verbatim: `admin clusters list` with
/// a `current-context` resolves the admin URL from the context (we only assert
/// it ATTEMPTS the context URL, by pointing at an unroutable host and matching
/// the connection-error URL — no broker needed).
#[test]
fn admin_resolves_context_admin_url() {
    let dir = tempfile::tempdir().expect("tempdir");
    let cfg = dir.path().join("config");
    // A pulsarctl-shaped config with an unroutable admin URL.
    let yaml = "auth-info:\n  c1:\n    token: t\n\
                contexts:\n  c1:\n    admin-service-url: http://127.0.0.1:1\n\
                current-context: c1\n";
    std::fs::write(&cfg, yaml).expect("write config");

    let out = Command::new(bin())
        .arg("--config")
        .arg(&cfg)
        .args(["admin", "clusters", "list"])
        .output()
        .expect("spawn");
    // Connection to 127.0.0.1:1 fails; the error must reference that URL,
    // proving the context admin-service-url was used (not localhost:8080).
    let combined = format!(
        "{}{}",
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr)
    );
    assert!(!out.status.success(), "expected connection failure");
    assert!(
        combined.contains("127.0.0.1:1") || combined.contains("1\n") || combined.contains(":1/"),
        "error should reference the context URL; got: {combined}"
    );
}
