// SPDX-License-Identifier: Apache-2.0
//
//! Smoke-tests for the `magnetar --version` / `-V` output.
//!
//! These spawn the actual binary (via `CARGO_BIN_EXE_magnetar`, which
//! Cargo sets for `[[bin]]` targets in the same package) rather than
//! call into `version::{short,long}` directly, so the assertions cover
//! the clap wiring as well as the renderer.
//!
//! Environmental data (git SHA, build timestamp, rustc version) is
//! masked at the assertion level — we check shape, not exact bytes.

use std::path::PathBuf;
use std::process::Command;

fn binary() -> PathBuf {
    env!("CARGO_BIN_EXE_magnetar").into()
}

/// `-V` short form: one line, no ANSI, starts with the binary name and
/// the package version.
#[test]
fn short_form_is_single_line_without_ansi() {
    let out = Command::new(binary())
        .arg("-V")
        .env("NO_COLOR", "1")
        .output()
        .expect("spawn magnetar -V");
    assert!(out.status.success(), "exit code: {:?}", out.status);

    let stdout = String::from_utf8(out.stdout).expect("utf8 stdout");
    assert_eq!(
        stdout.trim_end().lines().count(),
        1,
        "short version must be one line, got: {stdout:?}",
    );
    let expected_prefix = format!("magnetar {} (", env!("CARGO_PKG_VERSION"));
    assert!(
        stdout.starts_with(&expected_prefix),
        "expected prefix {expected_prefix:?}, got: {stdout:?}",
    );
    assert!(
        !stdout.contains('\x1b'),
        "short form must never carry ANSI escapes",
    );
}

/// `--version` long form under `NO_COLOR=1`: multi-line, no ANSI,
/// carries every documented field.
#[test]
fn long_form_no_color_has_all_fields() {
    let out = Command::new(binary())
        .arg("--version")
        .env("NO_COLOR", "1")
        .output()
        .expect("spawn magnetar --version with NO_COLOR=1");
    assert!(out.status.success(), "exit code: {:?}", out.status);

    let stdout = String::from_utf8(out.stdout).expect("utf8 stdout");
    assert!(
        !stdout.contains('\x1b'),
        "NO_COLOR=1 must suppress all ANSI escapes, got: {stdout:?}",
    );
    let expected_prefix = format!("magnetar {} (", env!("CARGO_PKG_VERSION"));
    assert!(stdout.starts_with(&expected_prefix));
    for needle in [
        "built ",
        "profile=",
        "rustc=",
        "target=",
        "features:",
        "pulsar wire protocol: v21",
        "os: ",
        "report bugs at ",
    ] {
        assert!(
            stdout.contains(needle),
            "long form missing field {needle:?}; full output:\n{stdout}",
        );
    }
}

/// `--version` piped (no env override): stdout is not a TTY, so no
/// ANSI escapes should leak out, even without `NO_COLOR`.
#[test]
fn long_form_piped_has_no_ansi() {
    // Spawning via `Command::output()` already wires `stdout` to a pipe,
    // so `IsTerminal::is_terminal()` inside the child returns `false`.
    let out = Command::new(binary())
        .arg("--version")
        .env_remove("NO_COLOR")
        .output()
        .expect("spawn magnetar --version (piped)");
    assert!(out.status.success(), "exit code: {:?}", out.status);

    assert!(
        !out.stdout.contains(&b'\x1b'),
        "piped --version must not emit ANSI; raw bytes: {:?}",
        out.stdout,
    );
}
