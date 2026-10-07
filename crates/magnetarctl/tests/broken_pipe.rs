// SPDX-License-Identifier: Apache-2.0

//! `magnetarctl … | head` must not panic once the reader closes the pipe.
//!
//! Spawns the real binary with its stdout attached to a pipe whose read end
//! is already closed, so the very first write fails with `EPIPE`
//! deterministically (no race with a reader that exits early).

use std::io::Write as _;
use std::process::{Command, Stdio};

#[test]
fn closed_stdout_ends_quietly_without_a_panic() {
    let mut config = tempfile::NamedTempFile::new().expect("temp config");
    writeln!(
        config,
        "contexts:\n  c1:\n    admin-service-url: http://localhost:8080\n    bookie-service-url: http://localhost:8080\ncurrent-context: c1\n"
    )
    .expect("write config");
    let (reader, writer) = std::io::pipe().expect("pipe");
    drop(reader);

    let out = Command::new(env!("CARGO_BIN_EXE_magnetarctl"))
        .args([
            "--config",
            config.path().to_str().expect("utf8 path"),
            "context",
            "current",
        ])
        .env("NO_COLOR", "1")
        .stdout(Stdio::from(writer))
        .stderr(Stdio::piped())
        .output()
        .expect("spawn magnetarctl");

    let stderr = String::from_utf8_lossy(&out.stderr);
    assert!(
        stderr.is_empty(),
        "a closed pipe must stay silent and never panic, stderr was: {stderr}"
    );
    assert_eq!(out.status.code(), Some(141), "status: {:?}", out.status);
}
