// SPDX-License-Identifier: Apache-2.0

//! `xtask` — build helpers for magnetar.
//!
//! Subcommands:
//! - `codegen` / `codegen --check`: regenerate / verify `magnetar-proto/src/pb/`.
//! - `check-no-channels`: grep the workspace for banned channel paths.
//! - `check-no-io-deps`: assert `magnetar-proto` has zero I/O dependencies.
//! - `vendor-proto --rev <sha>`: refresh vendored `PulsarApi.proto`.
//!
//! Real subcommand bodies arrive in M1+. M0 stubs return success so CI is green.

use std::path::PathBuf;
use std::process::ExitCode;

use anyhow::{Result, bail};
use clap::{Parser, Subcommand};

#[derive(Debug, Parser)]
#[command(name = "xtask", version, about = "magnetar build helpers", long_about = None)]
struct Cli {
    #[command(subcommand)]
    cmd: Cmd,
}

#[derive(Debug, Subcommand)]
enum Cmd {
    /// Regenerate `magnetar-proto/src/pb/` from the vendored proto files.
    Codegen {
        /// Verify-only: fail if generated code differs from what is committed.
        #[arg(long)]
        check: bool,
    },
    /// Grep the workspace for banned channel paths.
    CheckNoChannels,
    /// Assert that `magnetar-proto` has no I/O dependencies in its dep graph.
    CheckNoIoDeps,
    /// Refresh the vendored Pulsar proto from a given upstream commit.
    VendorProto {
        /// Apache Pulsar commit SHA to vendor from.
        #[arg(long)]
        rev: String,
        /// Local clone of apache/pulsar (defaults to the workspace neighbour).
        #[arg(long)]
        source: Option<PathBuf>,
    },
}

fn main() -> ExitCode {
    match dispatch() {
        Ok(()) => ExitCode::SUCCESS,
        Err(err) => {
            eprintln!("xtask: {err:#}");
            ExitCode::from(1)
        }
    }
}

fn dispatch() -> Result<()> {
    let cli = Cli::parse();
    match cli.cmd {
        Cmd::Codegen { check } => {
            if check {
                eprintln!("xtask codegen --check: M0 stub — drift check arrives in M1.");
            } else {
                eprintln!("xtask codegen: M0 stub — regen arrives in M1.");
            }
            Ok(())
        }
        Cmd::CheckNoChannels => check_no_channels(),
        Cmd::CheckNoIoDeps => {
            // M1 expands this to a real cargo-tree based check.
            eprintln!("xtask check-no-io-deps: M0 stub — assertion arrives in M1.");
            Ok(())
        }
        Cmd::VendorProto { rev, source: _ } => {
            bail!("xtask vendor-proto: not implemented yet (lands in M1). Requested rev: {rev}");
        }
    }
}

fn check_no_channels() -> Result<()> {
    // Minimal M0 implementation: grep the workspace for the banned channel
    // module paths in non-test Rust files. The clippy `disallowed-types`
    // config + cargo-deny `bans deny` provide deeper coverage; this is a
    // belt-and-braces lint for paths clippy doesn't catch (e.g. plain string
    // matches that look like channel use in macros or comments).
    let workspace_root = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .ok_or_else(|| anyhow::anyhow!("xtask should have a workspace parent"))?
        .to_path_buf();

    let banned: &[&str] = &[
        "tokio::sync::mpsc::",
        "tokio::sync::broadcast::",
        "tokio::sync::watch::",
        "tokio::sync::oneshot::",
        "std::sync::mpsc::",
        "crossbeam_channel::",
        "::flume::",
        "::async_channel::",
        "::kanal::",
        "::postage::",
        "::tachyonix::",
    ];

    let mut offenders: Vec<String> = Vec::new();
    visit(&workspace_root, &mut |path, contents| {
        if path.extension().is_none_or(|ext| ext != "rs") {
            return;
        }
        // Allow xtask itself (this very file) to mention banned strings literally.
        if path.starts_with(workspace_root.join("xtask")) {
            return;
        }
        for needle in banned {
            if contents.contains(needle) {
                offenders.push(format!("{}: contains {needle}", path.display()));
            }
        }
    })?;

    if !offenders.is_empty() {
        for line in &offenders {
            eprintln!("forbidden channel reference — {line}");
        }
        bail!(
            "no-channels check failed: {} offender(s). See GUIDELINES.md#no-channels for the replacement pattern.",
            offenders.len()
        );
    }
    Ok(())
}

fn visit(root: &std::path::Path, callback: &mut dyn FnMut(&std::path::Path, &str)) -> Result<()> {
    use std::fs;

    let skip = |name: &str| {
        matches!(
            name,
            "target" | ".git" | ".github" | "tasks" | ".direnv" | ".vscode" | ".idea"
        )
    };
    for entry in fs::read_dir(root)? {
        let entry = entry?;
        let path = entry.path();
        let name = path.file_name().and_then(|n| n.to_str()).unwrap_or("");
        if entry.file_type()?.is_dir() {
            if skip(name) {
                continue;
            }
            visit(&path, callback)?;
        } else if entry.file_type()?.is_file() {
            if let Ok(contents) = fs::read_to_string(&path) {
                callback(&path, &contents);
            }
        }
    }
    Ok(())
}
