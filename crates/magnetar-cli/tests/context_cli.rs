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
    // serde_norway indents nested keys two spaces, so the old context appears as
    // `  prod:` — assert that indented key is gone (the literal `prod:` is not a
    // substring of `production:`, and `prod-tok`/`https://broker:443` contain no
    // `prod:`), and the renamed key is present.
    assert!(
        !yaml.contains("prod:") && yaml.contains("production:"),
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

/// Run `magnetarctl --config <cfg> <args...>` with a controlled environment
/// (ambient `MAGNETAR_TOKEN` is always cleared first, then `envs` applied), so
/// env-vs-flag provenance tests are deterministic regardless of the runner's
/// environment. Returns (status-ok, stdout, stderr).
fn run_cli_env(cfg: &Path, envs: &[(&str, &str)], args: &[&str]) -> (bool, String, String) {
    let mut cmd = Command::new(bin());
    cmd.env_remove("MAGNETAR_TOKEN");
    cmd.arg("--config").arg(cfg);
    for (k, v) in envs {
        cmd.env(k, v);
    }
    cmd.args(args);
    let out = cmd.output().expect("spawn magnetarctl");
    (
        out.status.success(),
        String::from_utf8_lossy(&out.stdout).into_owned(),
        String::from_utf8_lossy(&out.stderr).into_owned(),
    )
}

/// `rename` onto an EXISTING destination is rejected and leaves both contexts
/// (and the destination's credentials) intact — it must not silently clobber.
#[test]
fn context_rename_onto_existing_is_rejected() {
    let dir = tempfile::tempdir().expect("tempdir");
    let cfg = dir.path().join("config");

    let (ok, _, err) = run_context(
        &cfg,
        &["set", "dev", "--admin-service-url", "http://dev:8080"],
    );
    assert!(ok, "set dev failed: {err}");
    let (ok, _, err) = run_context(
        &cfg,
        &[
            "set",
            "prod",
            "--admin-service-url",
            "https://prod:443",
            "--token",
            "prod-secret",
        ],
    );
    assert!(ok, "set prod failed: {err}");

    // rename dev → prod must fail (prod already exists).
    let (ok, _, err) = run_context(&cfg, &["rename", "dev", "prod"]);
    assert!(!ok, "rename onto existing should fail");
    assert!(err.contains("already exists"), "err: {err}");

    // prod's endpoint AND token survive untouched; dev is still present.
    let yaml = std::fs::read_to_string(&cfg).expect("read config");
    assert!(yaml.contains("https://prod:443"), "prod url lost: {yaml}");
    assert!(yaml.contains("prod-secret"), "prod token lost: {yaml}");
    let (_, out, _) = run_context(&cfg, &["get"]);
    assert!(out.contains("dev"), "dev lost: {out}");
}

/// An inherited `MAGNETAR_TOKEN` is for the live connection only — `context set`
/// must NOT persist it to disk. An explicit `--token` flag IS persisted.
#[test]
fn context_set_token_env_not_persisted_flag_is() {
    let dir = tempfile::tempdir().expect("tempdir");
    let cfg = dir.path().join("config");

    // Env-sourced token: not written.
    let (ok, _, err) = run_cli_env(
        &cfg,
        &[("MAGNETAR_TOKEN", "env-secret")],
        &[
            "context",
            "set",
            "fromenv",
            "--admin-service-url",
            "http://h:8080",
        ],
    );
    assert!(ok, "set fromenv failed: {err}");
    let yaml = std::fs::read_to_string(&cfg).expect("read config");
    assert!(
        !yaml.contains("env-secret"),
        "env token must not be persisted: {yaml}"
    );

    // Flag-sourced token: written.
    let (ok, _, err) = run_cli_env(
        &cfg,
        &[],
        &[
            "context",
            "set",
            "fromflag",
            "--admin-service-url",
            "http://h:8080",
            "--token",
            "flag-secret",
        ],
    );
    assert!(ok, "set fromflag failed: {err}");
    let yaml = std::fs::read_to_string(&cfg).expect("read config");
    assert!(
        yaml.contains("flag-secret"),
        "flag token must be persisted: {yaml}"
    );
}

/// Switching auth mode on a later `set` clears the mutually-exclusive fields, so
/// a stale higher-precedence token cannot keep shadowing a freshly-set OAuth2.
#[test]
fn context_set_switching_auth_mode_clears_stale() {
    let dir = tempfile::tempdir().expect("tempdir");
    let cfg = dir.path().join("config");

    let (ok, _, err) = run_context(
        &cfg,
        &[
            "set",
            "c",
            "--admin-service-url",
            "https://h:443",
            "--token",
            "stale-tok",
        ],
    );
    assert!(ok, "set token failed: {err}");

    // Now switch the same context to OAuth2.
    let (ok, _, err) = run_context(
        &cfg,
        &[
            "set",
            "c",
            "--issuer-endpoint",
            "https://idp.example/token",
            "--key-file",
            "/run/kf.json",
        ],
    );
    assert!(ok, "switch to oauth2 failed: {err}");

    let yaml = std::fs::read_to_string(&cfg).expect("read config");
    assert!(
        !yaml.contains("stale-tok"),
        "stale token not cleared: {yaml}"
    );
    assert!(
        yaml.contains("issuer_endpoint: https://idp.example/token"),
        "issuer not set: {yaml}"
    );
}

/// An OAuth2 context with a plaintext `http://` issuer is rejected before any
/// secret is sent — the client_credentials secret must not leak in cleartext.
#[test]
fn oauth2_http_issuer_is_rejected() {
    let dir = tempfile::tempdir().expect("tempdir");
    let cfg = dir.path().join("config");

    let (ok, _, err) = run_context(
        &cfg,
        &[
            "set",
            "oauth",
            "--admin-service-url",
            "http://h:8080",
            "--issuer-endpoint",
            "http://idp.example/token",
            "--key-file",
            "/run/kf.json",
        ],
    );
    assert!(ok, "set oauth failed: {err}");

    let (ok, _, err) = run_cli_env(
        &cfg,
        &[],
        &["--context", "oauth", "admin", "clusters", "list"],
    );
    assert!(!ok, "http issuer should be rejected");
    assert!(err.contains("https"), "err should mention https: {err}");
}

/// An empty token file is rejected locally rather than producing a malformed
/// `Authorization: Bearer ` header sent to the broker.
#[test]
fn empty_token_file_is_rejected() {
    let dir = tempfile::tempdir().expect("tempdir");
    let cfg = dir.path().join("config");
    // An explicit `--config` that does not exist is itself an error; create an
    // empty config (parses as defaults) so the failure under test is the empty
    // token file, not a missing config.
    std::fs::write(&cfg, "").expect("touch config");
    let tok = dir.path().join("empty.token");
    std::fs::write(&tok, "   \n").expect("write empty token");

    let (ok, _, err) = run_cli_env(
        &cfg,
        &[],
        &[
            "--token-file",
            tok.to_str().expect("utf8 path"),
            "admin",
            "clusters",
            "list",
        ],
    );
    assert!(!ok, "empty token file should be rejected");
    assert!(err.contains("is empty"), "err should mention empty: {err}");
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
        combined.contains("127.0.0.1:1") || combined.contains(":1/"),
        "error should reference the context URL; got: {combined}"
    );
}
