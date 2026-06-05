// SPDX-License-Identifier: Apache-2.0

//! `xtask` — build helpers for magnetar.
//!
//! Subcommands:
//! - `codegen` / `codegen --check`: regenerate / verify `magnetar-proto/src/pb/`.
//! - `check-no-channels`: grep the workspace for banned channel paths.
//! - `check-no-io-deps`: assert `magnetar-proto` has zero I/O dependencies.
//! - `check-no-internal-clock`: assert `magnetar-proto/src/**` never reads the host clock
//!   (`Instant::now()` / `SystemTime::now()`) outside the two documented leak files. Mirrors
//!   ADR-0011.
//! - `check-log-fields`: assert every `error!` / `warn!` / `info!` tracing event in non-test
//!   workspace code carries at least one structured field (`debug!` / `trace!` exempt). Mirrors
//!   ADR-0054.
//! - `check-sim-coverage`: assert that every line added relative to `git merge-base origin/main
//!   HEAD` is executed by at least one moonpool test (`cargo-llvm-cov` patch-coverage style).
//!   Mirrors ADR-0024.
//! - `check-runtime-test-parity`: assert `magnetar-runtime-tokio` and `magnetar-runtime-moonpool`
//!   carry the same number of `#[test]` / `#[tokio::test]` / `#[moonpool::test]` items. Mirrors
//!   ADR-0024.
//! - `check-crypto-matrix`: build the four `crypto-*` provider features in isolation (issue #9,
//!   ADR-0035). Complements `cargo build --workspace --all-features` (which exercises the cfg
//!   cascade) by proving each single-provider cell compiles cleanly.
//! - `vendor-proto --rev <sha>`: refresh vendored `PulsarApi.proto`.
//!
//! Codegen drives `prost-build` against `crates/magnetar-proto/proto/`, writes
//! the generated Rust into `crates/magnetar-proto/src/pb/`, and (with `--check`)
//! diffs the generated output against what is committed so CI catches drift.

use std::path::{Path, PathBuf};
use std::process::{Command as StdCommand, ExitCode};
use std::{env, fs};

use anyhow::{Context, Result, anyhow, bail};
use clap::{Parser, Subcommand};

/// Crates that, if present in `magnetar-proto`'s feature-resolved dep graph,
/// indicate a leaked I/O dependency. The list mirrors GUIDELINES.md
/// ("I/O isolation") and the M1 plan.
const FORBIDDEN_IO_DEPS: &[&str] = &[
    "tokio",
    "mio",
    "socket2",
    "async-std",
    "smol",
    "async-io",
    "polling",
    "reqwest",
    "hyper",
    "surf",
];

/// Proto files we compile. Order matches the natural import graph; prost-build
/// does not care, but stable order keeps the generated module deterministic.
const PROTO_FILES: &[&str] = &["PulsarApi.proto", "PulsarMarkers.proto"];

/// Protobuf prefixes whose `bytes` fields should be generated as
/// `bytes::Bytes` instead of `Vec<u8>`. `["."]` opts every `bytes` field
/// in the descriptor set into refcounted `Bytes` so payload, metadata,
/// auth, and schema-version fields all decode zero-copy out of the
/// inbound `BytesMut` slice.
const BYTES_MESSAGES: &[&str] = &["."];

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
    /// Assert that `magnetar-proto/src/**` does not read the host clock.
    ///
    /// Greps for direct calls to [`std::time::Instant::now`] and
    /// [`std::time::SystemTime::now`] outside `#[cfg(test)]` blocks and
    /// outside the two documented leak files. See ADR-0011.
    CheckNoInternalClock,
    /// Assert every `error!` / `warn!` / `info!` tracing event carries at
    /// least one structured field.
    ///
    /// Parses macro invocations parenthesis-balanced (multi-line invocations
    /// are the house style) in non-`#[cfg(test)]` workspace library/binary
    /// code; brace/bracket delimiter forms (`warn!{…}` / `warn![…]`) are
    /// hard violations since the field grammar only parses parenthesized
    /// invocations. A bare `target:`-only or literal-message-only event is a
    /// violation. Known limitation: a bare named constant in message
    /// position (`info!(SOME_CONST)`) is indistinguishable from `tracing`'s
    /// ident-capture shorthand and passes as a field. `debug!` / `trace!`
    /// are exempt. See ADR-0054.
    CheckLogFields,
    /// Assert that every line added relative to the merge base is covered
    /// by at least one `magnetar-runtime-moonpool` test.
    ///
    /// Runs `cargo llvm-cov --json -p magnetar-runtime-moonpool` and
    /// intersects the LCOV-equivalent JSON with `git diff
    /// merge-base...HEAD` line ranges. Any added line not executed under
    /// the moonpool runner fails the check. See ADR-0024.
    CheckSimCoverage {
        /// Base ref to diff against. Defaults to `origin/main`.
        #[arg(long, default_value = "origin/main")]
        base: String,
    },
    /// Assert tokio ↔ moonpool runtime crates carry the same number of
    /// test items.
    ///
    /// Counts `#[test]`, `#[tokio::test]`, and `#[moonpool::test]`
    /// attributes under `crates/magnetar-runtime-tokio/{src,tests}` and
    /// `crates/magnetar-runtime-moonpool/{src,tests}`. Strict equality
    /// required. See ADR-0024.
    CheckRuntimeTestParity,
    /// Build the per-provider crypto matrix (issue #9, ADR-0035).
    ///
    /// Iterates the four mutually-pluggable `crypto-*` features in
    /// isolation (under `tokio` and `tokio,moonpool`) so each cell is
    /// independently buildable. Complements the `--all-features`
    /// baseline (which goes through the cfg cascade in
    /// `magnetar-runtime-{tokio,moonpool}/src/tls_crypto.rs`).
    CheckCryptoMatrix,
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
        Cmd::Codegen { check } => codegen(check),
        Cmd::CheckNoChannels => check_no_channels(),
        Cmd::CheckNoIoDeps => check_no_io_deps(),
        Cmd::CheckNoInternalClock => check_no_internal_clock(),
        Cmd::CheckLogFields => check_log_fields(),
        Cmd::CheckSimCoverage { base } => check_sim_coverage(&base),
        Cmd::CheckRuntimeTestParity => check_runtime_test_parity(),
        Cmd::CheckCryptoMatrix => check_crypto_matrix(),
        Cmd::VendorProto { rev, source } => vendor_proto(&rev, source.as_deref()),
    }
}

/// Returns the absolute path to the workspace root, derived from this crate's
/// manifest dir at compile time.
fn workspace_root() -> Result<PathBuf> {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .map(Path::to_path_buf)
        .ok_or_else(|| anyhow!("xtask should have a workspace parent"))
}

fn proto_dir() -> Result<PathBuf> {
    Ok(workspace_root()?.join("crates/magnetar-proto/proto"))
}

fn pb_out_dir() -> Result<PathBuf> {
    Ok(workspace_root()?.join("crates/magnetar-proto/src/pb"))
}

/// Build the configured `prost_build::Config` shared by both real codegen and
/// the `--check` variant.
fn build_config(out_dir: &Path) -> prost_build::Config {
    let mut config = prost_build::Config::new();
    config.out_dir(out_dir);
    config.bytes(BYTES_MESSAGES);
    // Pulsar's proto comments are doxygen-style and don't survive rustdoc's
    // markdown linter cleanly; disable to keep `cargo doc -D warnings` quiet.
    config.disable_comments(["."]);
    config
}

/// Compile the proto files into `out_dir`. `out_dir` must exist.
fn run_prost(out_dir: &Path) -> Result<()> {
    let proto_dir = proto_dir()?;
    let inputs: Vec<PathBuf> = PROTO_FILES
        .iter()
        .map(|name| proto_dir.join(name))
        .collect();
    for input in &inputs {
        if !input.exists() {
            bail!("missing vendored proto file: {}", input.display());
        }
    }

    // `prost_build::Config::compile_protos` shells out to `protoc` (or the
    // bundled `protoc` if the `vendored` feature is on). We respect the
    // `PROTOC` env var if the operator has pointed us at a specific binary.
    let mut config = build_config(out_dir);
    let include_paths = std::slice::from_ref(&proto_dir);
    config
        .compile_protos(&inputs, include_paths)
        .context("prost-build failed to compile Pulsar proto definitions")?;
    Ok(())
}

fn codegen(check: bool) -> Result<()> {
    let committed = pb_out_dir()?;

    if check {
        let scratch = tempdir(&workspace_root()?.join("target/xtask-codegen-check"))?;
        run_prost(&scratch)?;
        let diff = diff_dirs(&scratch, &committed)?;
        if diff.is_empty() {
            eprintln!("xtask codegen --check: pb/ is up to date.");
            return Ok(());
        }
        for entry in &diff {
            eprintln!("drift: {entry}");
        }
        bail!(
            "xtask codegen --check: generated pb/ differs from committed pb/ ({} entry/entries). \
             Run `cargo run -p xtask -- codegen` and commit the result.",
            diff.len()
        );
    }

    if committed.exists() {
        // Clear stale files before regenerating so deletions in the proto
        // surface as missing modules.
        for entry in
            fs::read_dir(&committed).with_context(|| format!("reading {}", committed.display()))?
        {
            let entry = entry?;
            if entry.file_type()?.is_file() {
                fs::remove_file(entry.path())?;
            }
        }
    } else {
        fs::create_dir_all(&committed)
            .with_context(|| format!("creating {}", committed.display()))?;
    }
    run_prost(&committed)?;
    eprintln!("xtask codegen: wrote pb/ at {}.", committed.display());
    Ok(())
}

/// Create a fresh empty directory at `base`, removing any prior contents.
fn tempdir(base: &Path) -> Result<PathBuf> {
    if base.exists() {
        fs::remove_dir_all(base)
            .with_context(|| format!("clearing scratch dir {}", base.display()))?;
    }
    fs::create_dir_all(base).with_context(|| format!("creating scratch dir {}", base.display()))?;
    Ok(base.to_path_buf())
}

/// Hand-maintained files that live in `pb/` but are NOT produced by codegen,
/// so the drift check must ignore them. PIP-460's `scalable_topics.rs` is
/// hand-encoded behind `feature = "scalable-topics"` until upstream cuts a
/// Pulsar 5.0 RC including PIP-460 — at which point a dedicated
/// `vendor-proto` commit (ADR-0026 §D4) replaces it and this carve-out is
/// removed. See `crates/magnetar-proto/src/pb/scalable_topics.rs` and
/// ADR-0031.
const PB_HAND_MAINTAINED_FILES: &[&str] = &["scalable_topics.rs"];

/// Compare files in `lhs` against `rhs`. Returns a list of human-readable
/// difference descriptions. An empty Vec means the two trees are identical.
/// Files in [`PB_HAND_MAINTAINED_FILES`] are skipped on both sides.
fn diff_dirs(lhs: &Path, rhs: &Path) -> Result<Vec<String>> {
    use std::collections::BTreeMap;

    fn collect(dir: &Path, into: &mut BTreeMap<String, Vec<u8>>) -> Result<()> {
        if !dir.exists() {
            return Ok(());
        }
        for entry in fs::read_dir(dir).with_context(|| format!("reading {}", dir.display()))? {
            let entry = entry?;
            if !entry.file_type()?.is_file() {
                continue;
            }
            let name = entry
                .file_name()
                .to_str()
                .ok_or_else(|| anyhow!("non-utf8 filename in {}", dir.display()))?
                .to_owned();
            if PB_HAND_MAINTAINED_FILES.contains(&name.as_str()) {
                continue;
            }
            let bytes = fs::read(entry.path())?;
            into.insert(name, bytes);
        }
        Ok(())
    }

    let mut lhs_files = BTreeMap::new();
    let mut rhs_files = BTreeMap::new();
    collect(lhs, &mut lhs_files)?;
    collect(rhs, &mut rhs_files)?;

    let mut diffs = Vec::new();
    for (name, lhs_bytes) in &lhs_files {
        match rhs_files.get(name) {
            None => diffs.push(format!(
                "{name}: present in generated, missing in committed"
            )),
            Some(rhs_bytes) if rhs_bytes != lhs_bytes => {
                diffs.push(format!(
                    "{name}: contents differ ({} -> {} bytes)",
                    rhs_bytes.len(),
                    lhs_bytes.len()
                ));
            }
            Some(_) => {}
        }
    }
    for name in rhs_files.keys() {
        if !lhs_files.contains_key(name) {
            diffs.push(format!(
                "{name}: present in committed, missing in generated"
            ));
        }
    }
    Ok(diffs)
}

fn check_no_io_deps() -> Result<()> {
    // Run `cargo tree -p magnetar-proto -e features --prefix none --no-dedupe`
    // and scan the rendered output for forbidden crate names. We deliberately
    // do not use `--format` because older cargo versions on stable have
    // different placeholder support; the default human-readable format is
    // stable across MSRV.
    // Note: without a dependency-kind edge filter the tree INCLUDES proto's
    // dev-dependency edges (e.g. the ADR-0054 `tracing-subscriber` capture
    // dev-dep), so a dev-dep pulling a forbidden I/O crate trips this gate
    // too — intentionally stricter than a production-graph-only scan
    // (ADR-0054 §5).
    let workspace_root = workspace_root()?;
    let cargo = env::var_os("CARGO").unwrap_or_else(|| "cargo".into());
    let output = StdCommand::new(cargo)
        .current_dir(&workspace_root)
        .args([
            "tree",
            "-p",
            "magnetar-proto",
            "-e",
            "features",
            "--prefix",
            "none",
            "--no-dedupe",
        ])
        .output()
        .context("failed to invoke `cargo tree`")?;
    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        bail!("cargo tree failed (status {}):\n{stderr}", output.status);
    }
    let stdout = String::from_utf8(output.stdout).context("cargo tree produced non-utf8 output")?;

    let mut offenders: Vec<&str> = Vec::new();
    for line in stdout.lines() {
        // Each line looks like `crate vX.Y.Z` or `crate vX.Y.Z (proc-macro)`
        // possibly with feature suffixes. Extract the leading crate name.
        let crate_name = line.split_whitespace().next().unwrap_or("");
        if crate_name.is_empty() {
            continue;
        }
        if FORBIDDEN_IO_DEPS.contains(&crate_name) {
            offenders.push(crate_name);
        }
    }
    offenders.sort_unstable();
    offenders.dedup();

    if !offenders.is_empty() {
        for crate_name in &offenders {
            eprintln!("forbidden I/O dependency in magnetar-proto: {crate_name}");
        }
        bail!(
            "magnetar-proto pulled in {} forbidden I/O crate(s). See GUIDELINES.md#i-o-isolation.",
            offenders.len()
        );
    }
    Ok(())
}

/// File paths inside `magnetar-proto/src/` that are *explicitly* allowed to
/// touch the host clock, mirroring the "Known non-determinism leaks" list in
/// the workspace `ARCHITECTURE.md` + ADR-0011. Every other file under
/// `crates/magnetar-proto/src/` must drive time through the injected
/// `now: Instant` / `wall_clock` parameters.
///
/// Paths are workspace-relative and matched with [`Path::ends_with`] so the
/// check is robust to symlinks and absolute prefixes. Keep this list in lockstep
/// with the leak inventory in `ARCHITECTURE.md` (search for
/// "Known non-determinism leaks").
const CLOCK_LEAK_ALLOWLIST: &[&str] = &[
    // PIP-37 chunked emit currently uses uuid::Uuid::new_v4() — no clock
    // reads, but listed here so the file is visited by the inventory checker
    // when leak categories are expanded.
    "crates/magnetar-proto/src/producer.rs",
    // TokenAuth bootstrap calls std::env::var() once at construction — no
    // clock reads either, but same rationale: keeps the leak list in one
    // place.
    "crates/magnetar-proto/src/auth/token.rs",
];

fn check_no_internal_clock() -> Result<()> {
    // We want to flag direct host-clock reads in `magnetar-proto/src/**`. The
    // patterns we treat as "host clock reads" are
    //   - `Instant::now()`        (matches both `std::time::Instant::now()` and unqualified
    //     `Instant::now()`)
    //   - `SystemTime::now()`     (same logic)
    //
    // We must NOT flag occurrences inside `#[cfg(test)]` blocks (tests
    // legitimately materialise instants for their fixtures) nor inside
    // doc-comments / regular comments (those are documentation, not calls).
    //
    // The cheap implementation: a small line-level scanner that maintains an
    // "inside cfg(test) block" depth counter. It's not a Rust parser, but the
    // workspace style is consistent enough — `#[cfg(test)]` attributes sit on
    // their own line, immediately followed by a `mod` or `fn` and a brace
    // that opens on the same/next line. We follow the brace count from the
    // first `{` after the attribute until we return to the surrounding depth.
    //
    // See ADR-0011 for the rationale; see ARCHITECTURE.md
    // "Known non-determinism leaks (documented)" for the allowlist.
    let workspace_root = workspace_root()?;
    let proto_src = workspace_root.join("crates/magnetar-proto/src");

    let needles: &[&str] = &["Instant::now()", "SystemTime::now()"];

    let mut offenders: Vec<String> = Vec::new();
    visit(&proto_src, &mut |path, contents| {
        if path.extension().is_none_or(|ext| ext != "rs") {
            return;
        }
        // Allow the documented leak sites.
        if CLOCK_LEAK_ALLOWLIST
            .iter()
            .any(|allowed| path.ends_with(allowed) || path.to_string_lossy().ends_with(allowed))
        {
            return;
        }

        // Walk lines, tracking #[cfg(test)] brace depth so we can skip them.
        let mut in_cfg_test = false;
        let mut depth: i32 = 0;
        let mut pending_cfg_test = false;

        for (lineno_zero, line) in contents.lines().enumerate() {
            let lineno = lineno_zero + 1;
            let trimmed = line.trim_start();

            // Detect a fresh `#[cfg(test)]` attribute. We mark it pending so
            // the *next* `{` opens a test scope. We tolerate composite
            // attributes like `#[cfg(all(test, feature = "x"))]`.
            if trimmed.starts_with("#[cfg(") && trimmed.contains("test") {
                pending_cfg_test = true;
            }

            // Count braces on this line so we can enter/leave the cfg(test)
            // span as the source nests.
            let opens = line.matches('{').count() as i32;
            let closes = line.matches('}').count() as i32;

            if pending_cfg_test && opens > 0 {
                in_cfg_test = true;
                pending_cfg_test = false;
                depth = opens - closes;
                continue;
            }

            if in_cfg_test {
                depth += opens - closes;
                if depth <= 0 {
                    in_cfg_test = false;
                    depth = 0;
                }
                continue;
            }

            // Strip the trailing "//" comment (if any) so we don't flag prose
            // that *mentions* `Instant::now()` in a doc-string or comment.
            let code = match line.find("//") {
                Some(idx) => &line[..idx],
                None => line,
            };

            for needle in needles {
                if code.contains(needle) {
                    offenders.push(format!(
                        "{}:{}: contains {needle} outside #[cfg(test)] — see ADR-0011",
                        path.display(),
                        lineno
                    ));
                }
            }
        }
    })?;

    if !offenders.is_empty() {
        for line in &offenders {
            eprintln!("forbidden host-clock read — {line}");
        }
        bail!(
            "no-internal-clock check failed: {} offender(s). \
             magnetar-proto must take `now: Instant` / `wall_clock` providers \
             through its API — see specs/adr/0011-clock-injection-sans-io.md.",
            offenders.len()
        );
    }
    Ok(())
}

fn check_no_channels() -> Result<()> {
    // Minimal lint: grep the workspace for the banned channel module paths in
    // non-test Rust files. The clippy `disallowed-types` config + cargo-deny
    // `bans deny` provide deeper coverage; this is a belt-and-braces lint for
    // paths clippy doesn't catch (e.g. plain string matches that look like
    // channel use in macros or comments).
    let workspace_root = workspace_root()?;

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
        "::thingbuf::",
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

/// Workspace-relative file paths exempt from `check-log-fields`, matched
/// with [`str::ends_with`] against forward-slash relative paths (mirrors
/// [`CLOCK_LEAK_ALLOWLIST`]). The list starts — and should stay — empty:
/// every `error!` / `warn!` / `info!` event must carry at least one
/// structured field per ADR-0054. Add an entry only with a rationale
/// documented in the same changeset.
const LOG_FIELDS_ALLOWLIST: &[&str] = &[];

/// Path fragments excluded from `check-log-fields`: test, bench, example,
/// and fuzz code is not the operator-facing logging surface ADR-0054
/// governs. `#[cfg(test)]` modules inside `src/**` are excluded separately
/// by [`cfg_test_line_flags`]. Matched against `/`-prefixed
/// workspace-relative paths.
const LOG_FIELDS_EXCLUDE_FRAGMENTS: &[&str] = &["/tests/", "/benches/", "/examples/", "/fuzz/"];

/// The tracing event macros `check-log-fields` enforces fields on, with the
/// level name used in violation reports. `debug!` / `trace!` are exempt per
/// ADR-0054 (per-operation internals; not operator-load-bearing).
const LOG_LEVEL_MACROS: &[(&str, &str)] =
    &[("error!", "error"), ("warn!", "warn"), ("info!", "info")];

/// A single `error!` / `warn!` / `info!` invocation found in a source file.
struct LogInvocation {
    /// 1-indexed line of the macro name.
    line: usize,
    /// Level name (`"error"` / `"warn"` / `"info"`), for reporting.
    level: &'static str,
    /// The raw macro-argument text between the balanced outer parentheses,
    /// or `None` for an unsupported `{…}` / `[…]` delimiter form — a hard
    /// violation, since the field grammar only parses parenthesized
    /// invocations.
    args: Option<String>,
}

/// Violation reason: a parenthesized invocation without a structured field.
const LOG_FIELDS_NO_FIELD: &str = "carries no structured field";

/// Violation reason: a brace/bracket macro form the field grammar cannot
/// parse — using it would silently bypass the gate, so it is rejected
/// outright.
const LOG_FIELDS_NON_PAREN: &str =
    "uses brace/bracket macro delimiters; use parentheses so the field grammar can parse it";

/// If `bytes[i]` opens a lexical region the scanner must not look inside —
/// a line or (nested) block comment, a string / raw-string / byte-string
/// literal, or a char literal — return the index just past that region.
/// Returns `None` when `bytes[i]` is plain code (including lifetimes, which
/// consume only their `'` here and leave the identifier as plain code).
fn skip_inert_region(bytes: &[u8], i: usize) -> Option<usize> {
    match bytes[i] {
        b'/' if bytes.get(i + 1) == Some(&b'/') => {
            let mut j = i + 2;
            while j < bytes.len() && bytes[j] != b'\n' {
                j += 1;
            }
            Some(j)
        }
        b'/' if bytes.get(i + 1) == Some(&b'*') => {
            // Block comments nest, per the Rust lexer.
            let mut depth = 1usize;
            let mut j = i + 2;
            while j < bytes.len() && depth > 0 {
                if bytes[j] == b'/' && bytes.get(j + 1) == Some(&b'*') {
                    depth += 1;
                    j += 2;
                } else if bytes[j] == b'*' && bytes.get(j + 1) == Some(&b'/') {
                    depth -= 1;
                    j += 2;
                } else {
                    j += 1;
                }
            }
            Some(j)
        }
        b'"' => Some(skip_string_literal(bytes, i)),
        b'r' | b'b' => skip_raw_or_byte_literal(bytes, i),
        b'\'' => skip_char_literal(bytes, i),
        _ => None,
    }
}

/// Skip a regular `"…"` string literal starting at `bytes[i]` (the opening
/// quote). Handles `\` escapes, including escaped quotes and the
/// line-continuation `\<newline>`.
fn skip_string_literal(bytes: &[u8], i: usize) -> usize {
    let mut j = i + 1;
    while j < bytes.len() {
        match bytes[j] {
            b'\\' => j += 2,
            b'"' => return j + 1,
            _ => j += 1,
        }
    }
    j
}

/// Skip a raw string (`r"…"`, `r#"…"#`, `br"…"`), byte string (`b"…"`), or
/// byte char (`b'…'`) literal starting at `bytes[i]`. Returns `None` when
/// the `r` / `b` is just the start of an identifier (including raw
/// identifiers like `r#match`).
fn skip_raw_or_byte_literal(bytes: &[u8], i: usize) -> Option<usize> {
    let mut j = i;
    if bytes[j] == b'b' {
        j += 1;
    }
    if bytes.get(j) == Some(&b'r') {
        j += 1;
        let mut hashes = 0usize;
        while bytes.get(j) == Some(&b'#') {
            hashes += 1;
            j += 1;
        }
        if bytes.get(j) != Some(&b'"') {
            return None; // identifier (possibly raw) — plain code
        }
        j += 1;
        while j < bytes.len() {
            if bytes[j] == b'"'
                && bytes.len() - (j + 1) >= hashes
                && bytes[j + 1..j + 1 + hashes].iter().all(|b| *b == b'#')
            {
                return Some(j + 1 + hashes);
            }
            j += 1;
        }
        Some(j)
    } else if bytes[i] == b'b' && bytes.get(j) == Some(&b'"') {
        Some(skip_string_literal(bytes, j))
    } else if bytes[i] == b'b' && bytes.get(j) == Some(&b'\'') {
        skip_char_literal(bytes, j)
    } else {
        None
    }
}

/// Skip a char literal starting at the `'` at `bytes[i]`. Returns `None`
/// for lifetimes (`'a`), which have no closing quote — the caller then
/// treats the `'` as plain code and advances one byte.
fn skip_char_literal(bytes: &[u8], i: usize) -> Option<usize> {
    let j = i + 1;
    if j >= bytes.len() {
        return None;
    }
    if bytes[j] == b'\\' {
        // Escaped char literal (`'\n'`, `'\''`, `'\u{7FFF}'`): scan to the
        // closing quote.
        let mut k = j + 2;
        while k < bytes.len() && bytes[k] != b'\'' {
            k += 1;
        }
        return Some((k + 1).min(bytes.len()));
    }
    // Unescaped single-byte char literal: `'x'`.
    if bytes.get(j + 1) == Some(&b'\'') {
        return Some(j + 2);
    }
    None
}

/// Extract the argument text between balanced parentheses, with
/// `bytes[open]` being the opening `(`. Comments and string/char literals
/// inside the arguments do not perturb the balance. Returns the inner text
/// plus the index just past the closing `)`.
fn extract_balanced_parens(bytes: &[u8], open: usize) -> Option<(String, usize)> {
    let mut depth = 0usize;
    let mut j = open;
    let start = open + 1;
    while j < bytes.len() {
        if let Some(next) = skip_inert_region(bytes, j) {
            j = next.max(j + 1);
            continue;
        }
        match bytes[j] {
            b'(' => depth += 1,
            b')' => {
                depth -= 1;
                if depth == 0 {
                    let inner = String::from_utf8_lossy(&bytes[start..j]).into_owned();
                    return Some((inner, j + 1));
                }
            }
            _ => {}
        }
        j += 1;
    }
    None
}

/// Find every `error!(…)` / `warn!(…)` / `info!(…)` invocation in
/// `contents`, parenthesis-balanced so multi-line invocations parse whole.
/// Path-qualified forms (`tracing::warn!`) match too; identifiers merely
/// ending in a level name (`my_error!`) do not. Occurrences inside
/// comments and string literals are ignored. Brace/bracket delimiter forms
/// (`warn!{…}` / `warn![…]`) are returned with `args: None` — hard
/// violations, never silently skipped.
fn find_log_invocations(contents: &str) -> Vec<LogInvocation> {
    let bytes = contents.as_bytes();
    let mut out = Vec::new();
    let mut i = 0usize;
    while i < bytes.len() {
        if let Some(next) = skip_inert_region(bytes, i) {
            i = next.max(i + 1);
            continue;
        }
        let mut matched = false;
        for (needle, level) in LOG_LEVEL_MACROS {
            if !bytes[i..].starts_with(needle.as_bytes()) {
                continue;
            }
            // Reject matches inside larger identifiers (`my_error!`); a
            // preceding `:` (`tracing::error!`) is a path separator and fine.
            if i > 0 && (bytes[i - 1].is_ascii_alphanumeric() || bytes[i - 1] == b'_') {
                continue;
            }
            let mut j = i + needle.len();
            while j < bytes.len() && bytes[j].is_ascii_whitespace() {
                j += 1;
            }
            // Brace/bracket delimiter forms would silently bypass the
            // parenthesis-only field grammar — record them as hard
            // violations instead of skipping.
            if matches!(bytes.get(j), Some(&b'{' | &b'[')) {
                let line = contents[..i].bytes().filter(|b| *b == b'\n').count() + 1;
                out.push(LogInvocation {
                    line,
                    level,
                    args: None,
                });
                i = j;
                matched = true;
                break;
            }
            if bytes.get(j) != Some(&b'(') {
                continue;
            }
            if let Some((args, end)) = extract_balanced_parens(bytes, j) {
                let line = contents[..i].bytes().filter(|b| *b == b'\n').count() + 1;
                out.push(LogInvocation {
                    line,
                    level,
                    args: Some(args),
                });
                i = end;
                matched = true;
                break;
            }
        }
        if !matched {
            i += 1;
        }
    }
    out
}

/// Split macro-argument text on top-level commas. Commas nested inside
/// `()` / `[]` / `{}`, strings, or comments do not split.
fn split_top_level_args(args: &str) -> Vec<&str> {
    let bytes = args.as_bytes();
    let mut parts = Vec::new();
    let mut depth = 0usize;
    let mut start = 0usize;
    let mut i = 0usize;
    while i < bytes.len() {
        if let Some(next) = skip_inert_region(bytes, i) {
            i = next.max(i + 1);
            continue;
        }
        match bytes[i] {
            b'(' | b'[' | b'{' => depth += 1,
            b')' | b']' | b'}' => depth = depth.saturating_sub(1),
            b',' if depth == 0 => {
                parts.push(&args[start..i]);
                start = i + 1;
            }
            _ => {}
        }
        i += 1;
    }
    if start < bytes.len() {
        parts.push(&args[start..]);
    }
    parts
}

/// If `part` starts with a `name:` / `target:` / `parent:` macro spec
/// keyword (single colon — `target::x` is a path, not a spec), return the
/// remainder after the colon.
fn strip_spec_keyword(part: &str) -> Option<&str> {
    for keyword in ["target", "parent", "name"] {
        if let Some(rest) = part.strip_prefix(keyword) {
            let rest = rest.trim_start();
            if rest.starts_with(':') && !rest.starts_with("::") {
                return Some(&rest[1..]);
            }
        }
    }
    None
}

/// If `part` begins with a tracing field path (`ident` or
/// `ident.nested.path`), return the remainder after the path. Returns
/// `None` when the first token is not an identifier.
fn strip_ident_path(part: &str) -> Option<&str> {
    let bytes = part.as_bytes();
    if bytes.is_empty() || !(bytes[0].is_ascii_alphabetic() || bytes[0] == b'_') {
        return None;
    }
    let mut i = 0usize;
    loop {
        let segment_start = i;
        while i < bytes.len() && (bytes[i].is_ascii_alphanumeric() || bytes[i] == b'_') {
            i += 1;
        }
        if i == segment_start {
            return None; // `.` not followed by an identifier
        }
        if i < bytes.len() && bytes[i] == b'.' {
            i += 1;
        } else {
            break;
        }
    }
    Some(&part[i..])
}

/// True when `rest` opens a field assignment: a single `=` (not `==` /
/// `=>`).
fn is_field_assignment(rest: &str) -> bool {
    rest.starts_with('=') && !rest.starts_with("==") && !rest.starts_with("=>")
}

/// If `part` starts with a string / raw-string literal, return the
/// remainder after it; `None` otherwise.
fn strip_leading_string_literal(part: &str) -> Option<&str> {
    let bytes = part.as_bytes();
    if bytes.is_empty() {
        return None;
    }
    let end = match bytes[0] {
        b'"' => skip_string_literal(bytes, 0),
        b'r' => skip_raw_or_byte_literal(bytes, 0)?,
        _ => return None,
    };
    Some(&part[end.min(part.len())..])
}

/// Decide whether one `error!` / `warn!` / `info!` argument list carries at
/// least one structured field.
///
/// Mirrors the tracing shortcut-macro grammar: optional `name:` / `target:`
/// / `parent:` spec args, then zero or more fields (`ident = value`,
/// `field.path = value`, `"quoted.name" = value`, `%shorthand`,
/// `?shorthand`, bare `ident` capture), then the message format string and
/// its format args. The first non-spec, non-field argument is the message —
/// everything after it (inline format args included) is NOT a structured
/// field, so `error!("failed: {}", err)` is a violation while
/// `error!(error = %err, "failed")` is not.
fn has_structured_field(args: &str) -> bool {
    for raw in split_top_level_args(args) {
        let part = raw.trim();
        if part.is_empty() {
            continue; // trailing comma
        }
        if strip_spec_keyword(part).is_some() {
            continue;
        }
        // `%value` / `?value` sigil shorthand.
        if part.starts_with('%') || part.starts_with('?') {
            return true;
        }
        // `{ field = value, … }` brace-delimited field block.
        if let Some(inner) = part.strip_prefix('{') {
            let inner = inner.strip_suffix('}').unwrap_or(inner).trim();
            if !inner.is_empty() {
                return true;
            }
            continue;
        }
        // A leading string literal is either a `"quoted.name" = value`
        // field or the message itself.
        if let Some(rest) = strip_leading_string_literal(part) {
            return is_field_assignment(rest.trim_start());
        }
        // `ident.path` alone (capture shorthand) or `ident.path = value`.
        if let Some(rest) = strip_ident_path(part) {
            let rest = rest.trim_start();
            return rest.is_empty() || is_field_assignment(rest);
        }
        // Some other expression sits in message position — no fields seen.
        return false;
    }
    false
}

/// Per-line `#[cfg(test)]`-membership flags for `contents` (1 entry per
/// line, 1-indexed lines map to `flags[line - 1]`).
///
/// Same line-based brace-tracking heuristic as [`check_no_internal_clock`]:
/// a `#[cfg(…test…)]` attribute arms a pending state; the next `{` opens
/// the excluded span, which closes when the brace depth returns to zero. A
/// braceless `;` declaration (`#[cfg(test)] mod tests;`) excludes only its
/// own lines.
fn cfg_test_line_flags(contents: &str) -> Vec<bool> {
    let mut flags = Vec::new();
    let mut in_cfg_test = false;
    let mut pending = false;
    let mut depth: i32 = 0;
    for line in contents.lines() {
        let trimmed = line.trim_start();
        let opens = line.matches('{').count() as i32;
        let closes = line.matches('}').count() as i32;

        if !in_cfg_test && !pending && trimmed.starts_with("#[cfg(") && trimmed.contains("test") {
            flags.push(true);
            if opens > 0 {
                // Single-line gated item: `#[cfg(test)] mod tests { … }`.
                in_cfg_test = true;
                depth = opens - closes;
                if depth <= 0 {
                    in_cfg_test = false;
                    depth = 0;
                }
            } else if !trimmed.ends_with(';') {
                pending = true;
            }
            continue;
        }

        if pending {
            flags.push(true);
            if opens > 0 {
                pending = false;
                in_cfg_test = true;
                depth = opens - closes;
                if depth <= 0 {
                    in_cfg_test = false;
                    depth = 0;
                }
            } else if trimmed.ends_with(';') {
                // `#[cfg(test)]` + `mod tests;` — gated declaration, no block.
                pending = false;
            }
            continue;
        }

        if in_cfg_test {
            flags.push(true);
            depth += opens - closes;
            if depth <= 0 {
                in_cfg_test = false;
                depth = 0;
            }
            continue;
        }

        flags.push(false);
    }
    flags
}

/// Scan one file's contents for `error!` / `warn!` / `info!` invocations
/// without a structured field (or with an unparseable brace/bracket
/// delimiter form), excluding `#[cfg(test)]` regions. Returns
/// `(line, level, reason)` per violation.
fn scan_log_field_violations(contents: &str) -> Vec<(usize, &'static str, &'static str)> {
    let in_test = cfg_test_line_flags(contents);
    find_log_invocations(contents)
        .into_iter()
        .filter(|inv| !in_test.get(inv.line - 1).copied().unwrap_or(false))
        .filter_map(|inv| match inv.args {
            None => Some((inv.line, inv.level, LOG_FIELDS_NON_PAREN)),
            Some(args) if !has_structured_field(&args) => {
                Some((inv.line, inv.level, LOG_FIELDS_NO_FIELD))
            }
            Some(_) => None,
        })
        .collect()
}

fn check_log_fields() -> Result<()> {
    let workspace_root = workspace_root()?;

    let mut offenders: Vec<String> = Vec::new();
    visit(&workspace_root, &mut |path, contents| {
        if path.extension().is_none_or(|ext| ext != "rs") {
            return;
        }
        // xtask itself mentions the macro names literally (this very check).
        if path.starts_with(workspace_root.join("xtask")) {
            return;
        }
        let rel = path.strip_prefix(&workspace_root).unwrap_or(path);
        let rel = rel.to_string_lossy().replace('\\', "/");
        let probe = format!("/{rel}");
        if LOG_FIELDS_EXCLUDE_FRAGMENTS
            .iter()
            .any(|frag| probe.contains(frag))
        {
            return;
        }
        if LOG_FIELDS_ALLOWLIST
            .iter()
            .any(|allowed| rel == *allowed || rel.ends_with(allowed))
        {
            return;
        }
        for (line, level, reason) in scan_log_field_violations(contents) {
            offenders.push(format!("{rel}:{line}: {level}! {reason}"));
        }
    })?;

    if !offenders.is_empty() {
        offenders.sort();
        for line in &offenders {
            eprintln!("unstructured log event — {line}");
        }
        bail!(
            "log-fields check failed: {} offender(s). Every `error!` / `warn!` / `info!` \
             event must carry at least one structured field (`debug!` / `trace!` are \
             exempt) — see specs/adr/0054-logging-policy.md.",
            offenders.len()
        );
    }
    eprintln!("xtask check-log-fields: every error!/warn!/info! event carries structured fields.");
    Ok(())
}

fn visit(root: &Path, callback: &mut dyn FnMut(&Path, &str)) -> Result<()> {
    let skip = |name: &str| {
        matches!(
            name,
            "target" | ".git" | ".github" | "tasks" | ".direnv" | ".vscode" | ".idea" | ".claude"
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
        } else if entry.file_type()?.is_file()
            && let Ok(contents) = fs::read_to_string(&path)
        {
            callback(&path, &contents);
        }
    }
    Ok(())
}

/// Production-source paths excluded from sim-coverage requirements. Generated
/// proto, test scaffolds, and tooling don't carry the load-bearing semantics
/// ADR-0024 is asserting equivalence over; demanding 100% on them would only
/// chase noise.
///
/// Matched by `Path::starts_with` against workspace-relative paths.
const SIM_COVERAGE_EXCLUDE_PREFIXES: &[&str] = &[
    "crates/magnetar-proto/src/pb/",
    "xtask/",
    "docs/",
    "specs/",
    "tasks/",
    ".claude/",
    ".github/",
];

/// File-name fragments excluded from sim-coverage (test files and benches).
const SIM_COVERAGE_EXCLUDE_FRAGMENTS: &[&str] = &["/tests/", "/benches/", "/examples/"];

/// Returns true if `relpath` (workspace-relative, forward slashes) is excluded
/// from sim-coverage enforcement.
fn is_sim_coverage_excluded(relpath: &str) -> bool {
    if SIM_COVERAGE_EXCLUDE_PREFIXES
        .iter()
        .any(|prefix| relpath.starts_with(prefix))
    {
        return true;
    }
    if SIM_COVERAGE_EXCLUDE_FRAGMENTS
        .iter()
        .any(|frag| relpath.contains(frag))
    {
        return true;
    }
    !Path::new(relpath)
        .extension()
        .is_some_and(|ext| ext.eq_ignore_ascii_case("rs"))
}

/// Run `git` with the given arguments at `cwd`. Returns stdout on success;
/// bails with stderr on failure.
fn run_git(args: &[&str], cwd: &Path) -> Result<String> {
    let output = StdCommand::new("git")
        .args(args)
        .current_dir(cwd)
        .output()
        .with_context(|| format!("failed to invoke `git {}`", args.join(" ")))?;
    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        bail!(
            "`git {}` failed (status {}):\n{stderr}",
            args.join(" "),
            output.status
        );
    }
    String::from_utf8(output.stdout)
        .with_context(|| format!("`git {}` produced non-utf8 output", args.join(" ")))
}

/// Resolve `git merge-base <base> HEAD`. Returns the commit SHA as a String.
fn git_merge_base(base: &str, cwd: &Path) -> Result<String> {
    let raw = run_git(&["merge-base", base, "HEAD"], cwd).with_context(|| {
        format!(
            "could not resolve merge-base against `{base}` — \
             does the ref exist? Try `git fetch origin` first."
        )
    })?;
    Ok(raw.trim().to_owned())
}

/// Parse a unified-diff blob produced by `git diff --unified=0` and return
/// the set of added new-side line numbers per workspace-relative file path.
///
/// Only `+` lines (excluding `+++` file headers) are considered additions.
/// Hunk headers `@@ -... +start,count @@` reset the new-side cursor.
/// Return the 1-indexed line number of the first `#[cfg(test)]` attribute in
/// `path`, or `None` if the file has no inline test module. Used to drop
/// inline-test lines from the sim-coverage diff scan: every `src/**/*.rs` in
/// magnetar puts its unit tests inside `#[cfg(test)] mod tests { … }` at the
/// bottom of the file, so the first occurrence is a reliable upper bound on
/// the production region.
fn first_cfg_test_line(path: &Path) -> Option<u32> {
    let contents = fs::read_to_string(path).ok()?;
    for (idx, line) in contents.lines().enumerate() {
        if line.trim_start().starts_with("#[cfg(test)]") {
            // 1-indexed to match git diff line numbering.
            return Some((idx as u32).saturating_add(1));
        }
    }
    None
}

/// Return the 1-indexed set of source lines that contain a
/// by-design-never-executed marker (`unreachable!`, `unimplemented!`,
/// `todo!`). Coverage tools instrument these as executable but no live
/// test can hit them. Demanding 100% coverage on them is meaningless and
/// would force authors to add fake tests or `#[coverage(off)]` (nightly-
/// only) — neither helps. We drop them at the diff stage instead.
fn unreachable_lines(path: &Path) -> std::collections::BTreeSet<u32> {
    let mut out = std::collections::BTreeSet::new();
    let Ok(contents) = fs::read_to_string(path) else {
        return out;
    };
    for (idx, line) in contents.lines().enumerate() {
        let trimmed = line.trim_start();
        if trimmed.contains("unreachable!(")
            || trimmed.contains("unimplemented!(")
            || trimmed.contains("todo!(")
        {
            out.insert((idx as u32).saturating_add(1));
        }
    }
    out
}

fn parse_diff_added_lines(
    diff: &str,
) -> std::collections::HashMap<String, std::collections::BTreeSet<u32>> {
    use std::collections::{BTreeSet, HashMap};

    let mut by_file: HashMap<String, BTreeSet<u32>> = HashMap::new();
    let mut current_file: Option<String> = None;
    let mut cursor: u32 = 0;

    for line in diff.lines() {
        if let Some(rest) = line.strip_prefix("+++ ") {
            // "+++ b/path/to/file" or "+++ /dev/null" (file deleted — ignored).
            current_file = rest
                .strip_prefix("b/")
                .filter(|p| !p.is_empty() && *p != "/dev/null")
                .map(str::to_owned);
            cursor = 0;
            continue;
        }
        if let Some(rest) = line.strip_prefix("@@ ") {
            // Header: "@@ -<old> +<new_start>[,<new_count>] @@ context"
            // Extract the new-side start. Format is "+<n>" or "+<n>,<m>".
            if let Some(plus_idx) = rest.find('+') {
                let after = &rest[plus_idx + 1..];
                let end = after.find([' ', ',']).unwrap_or(after.len());
                if let Ok(start) = after[..end].parse::<u32>() {
                    cursor = start;
                }
            }
            continue;
        }
        if line.starts_with("---") {
            continue; // old-side file header
        }
        if let Some(file) = current_file.as_deref() {
            if let Some(_added) = line.strip_prefix('+') {
                by_file.entry(file.to_owned()).or_default().insert(cursor);
                cursor = cursor.saturating_add(1);
            } else if line.starts_with('-') {
                // removed line — does not advance the new-side cursor
            } else {
                // context line (rare with unified=0) or empty — advance cursor
                cursor = cursor.saturating_add(1);
            }
        }
    }
    by_file
}

/// Parse an LCOV report and return `(executable, covered)` line sets per
/// absolute source path. LCOV format key lines:
///
/// - `SF:<source file path>` — opens a record.
/// - `DA:<line>,<count>[,<checksum>]` — line-execution datum. The presence of the entry means the
///   line is executable; `count > 0` means it was hit.
/// - `end_of_record` — closes a record.
///
/// Returning both sets lets the coverage check filter out non-executable
/// additions (use statements, doc comments, blank lines, closing braces),
/// which are always absent from the LCOV and would otherwise be flagged as
/// "uncovered" forever.
fn parse_lcov_coverage(
    lcov: &str,
) -> std::collections::HashMap<
    String,
    (
        std::collections::BTreeSet<u32>,
        std::collections::BTreeSet<u32>,
    ),
> {
    use std::collections::{BTreeSet, HashMap};

    let mut by_file: HashMap<String, (BTreeSet<u32>, BTreeSet<u32>)> = HashMap::new();
    let mut current_file: Option<String> = None;

    for line in lcov.lines() {
        if let Some(path) = line.strip_prefix("SF:") {
            current_file = Some(path.to_owned());
            continue;
        }
        if line == "end_of_record" {
            current_file = None;
            continue;
        }
        if let Some(rest) = line.strip_prefix("DA:")
            && let Some(file) = current_file.as_deref()
        {
            let mut parts = rest.split(',');
            let Some(line_no) = parts.next().and_then(|s| s.parse::<u32>().ok()) else {
                continue;
            };
            let Some(count) = parts.next().and_then(|s| s.parse::<u64>().ok()) else {
                continue;
            };
            let entry = by_file.entry(file.to_owned()).or_default();
            entry.0.insert(line_no);
            if count > 0 {
                entry.1.insert(line_no);
            }
        }
    }
    by_file
}

/// Run `cargo llvm-cov` against the moonpool runtime + differential test
/// crates and return the emitted LCOV report as a string.
///
/// The whole workspace is instrumented (so coverage attributes to the
/// originating crate, e.g. `magnetar-proto`), but only the moonpool /
/// differential test binaries execute — that's the surface ADR-0024 demands
/// patch coverage on.
fn run_moonpool_lcov(workspace_root: &Path) -> Result<String> {
    let lcov_path = workspace_root.join("target/sim-coverage.lcov");
    if let Some(parent) = lcov_path.parent() {
        fs::create_dir_all(parent).with_context(|| format!("creating {}", parent.display()))?;
    }
    let cargo = env::var_os("CARGO").unwrap_or_else(|| "cargo".into());
    // `-p` is a cargo flag, not a test-runner flag. Putting it after `--`
    // routes it to libtest, which rejects it ("Unrecognized option: 'p'") and
    // aborts the whole coverage run. Filtering the packages with `-p` (and
    // dropping `--workspace`, which is mutually exclusive) restricts both
    // instrumentation and test execution to the moonpool + differential
    // crates — the surface ADR-0024 demands patch coverage on.
    let status = StdCommand::new(&cargo)
        .current_dir(workspace_root)
        .args(["llvm-cov", "--lcov", "--output-path"])
        .arg(&lcov_path)
        .args([
            "-p",
            "magnetar-runtime-moonpool",
            "-p",
            "magnetar-differential",
            "--all-features",
            "--locked",
            "--quiet",
        ])
        .status()
        .context("failed to invoke `cargo llvm-cov`")?;
    if !status.success() {
        bail!("`cargo llvm-cov` exited with status {status}");
    }
    fs::read_to_string(&lcov_path).with_context(|| format!("reading {}", lcov_path.display()))
}

/// Intersect the per-file added-line sets from the diff with the executable
/// + executed line sets from LCOV.
///
/// An added line is reported as uncovered only when LCOV considers it
/// executable (an `DA:` entry exists for it) AND the moonpool runner did not
/// hit it. Non-executable additions (use statements, doc comments, blank
/// lines, closing braces, attribute-only lines) are silently skipped — they
/// have no LCOV entry and demanding "coverage" on them is meaningless.
fn intersect_diff_with_coverage(
    workspace_root: &Path,
    tracked: &[(String, std::collections::BTreeSet<u32>)],
    covered: &std::collections::HashMap<
        String,
        (
            std::collections::BTreeSet<u32>,
            std::collections::BTreeSet<u32>,
        ),
    >,
) -> Vec<(String, u32)> {
    let mut uncovered = Vec::new();
    for (relpath, added_lines) in tracked {
        let abs = workspace_root.join(relpath);
        let abs_key = abs.to_string_lossy().into_owned();
        let entry = covered.get(&abs_key);
        for &line in added_lines {
            let is_executable = entry.is_some_and(|(exec, _)| exec.contains(&line));
            let is_hit = entry.is_some_and(|(_, hit)| hit.contains(&line));
            if is_executable && !is_hit {
                uncovered.push((relpath.clone(), line));
            }
        }
    }
    uncovered
}

/// Print per-file uncovered ranges and bail with a summary. Always returns
/// `Err` — the caller relies on `?` to surface the failure.
fn report_uncovered(workspace_root: &Path, uncovered: &[(String, u32)]) -> Result<()> {
    let mut by_file: std::collections::BTreeMap<&str, Vec<u32>> = std::collections::BTreeMap::new();
    for (path, line) in uncovered {
        by_file.entry(path.as_str()).or_default().push(*line);
    }
    for (path, lines) in &by_file {
        eprintln!(
            "uncovered (moonpool runner): {}: {} line(s) — {}",
            path,
            lines.len(),
            lines
                .iter()
                .map(ToString::to_string)
                .collect::<Vec<_>>()
                .join(", ")
        );
    }
    bail!(
        "xtask check-sim-coverage: {} added line(s) across {} file(s) not \
         executed by `magnetar-runtime-moonpool` / `magnetar-differential` \
         tests (workspace root: {}). Patch coverage must be 100% — see ADR-0024.",
        uncovered.len(),
        by_file.len(),
        workspace_root.display(),
    );
}

/// Verify `cargo-llvm-cov` is installed. Returns the resolved cargo invocation
/// command on success; bails with install instructions otherwise.
fn ensure_cargo_llvm_cov() -> Result<()> {
    let cargo = env::var_os("CARGO").unwrap_or_else(|| "cargo".into());
    let output = StdCommand::new(&cargo)
        .args(["llvm-cov", "--version"])
        .output();
    match output {
        Ok(o) if o.status.success() => Ok(()),
        _ => bail!(
            "cargo-llvm-cov not found — required by `xtask check-sim-coverage`. \
             Install with: cargo install cargo-llvm-cov"
        ),
    }
}

fn check_sim_coverage(base: &str) -> Result<()> {
    ensure_cargo_llvm_cov()?;

    let workspace_root = workspace_root()?;
    let merge_base = git_merge_base(base, &workspace_root)?;

    // 1. Collect added new-side line ranges relative to merge-base, scoped to `.rs` files.
    //    `--unified=0` keeps the hunk headers strict so the cursor advance in
    //    `parse_diff_added_lines` stays correct.
    let diff = run_git(
        &[
            "diff",
            "--unified=0",
            "--no-color",
            &format!("{merge_base}..HEAD"),
            "--",
            "*.rs",
        ],
        &workspace_root,
    )?;
    let added = parse_diff_added_lines(&diff);

    // 2. Drop excluded paths (generated proto, tests, tooling, docs).
    let mut tracked: Vec<(String, std::collections::BTreeSet<u32>)> = added
        .into_iter()
        .filter(|(path, _)| !is_sim_coverage_excluded(path))
        .collect();
    tracked.sort_by(|a, b| a.0.cmp(&b.0));

    // 2b. Inside `src/**/*.rs`, strip lines that live below the file's first
    //     `#[cfg(test)]` attribute — those are unit tests, not production
    //     code. The path-level excludes already drop `tests/`, `benches/`,
    //     `examples/`; this handles the same intent for inline test modules
    //     (the project convention is `#[cfg(test)] mod tests { … }` at the
    //     bottom of every src file). Also drop lines marked
    //     `unreachable!()` / `unimplemented!()` / `todo!()` — coverage
    //     tools count them as executable but they are by-design dead arms.
    for (relpath, lines) in &mut tracked {
        let abs = workspace_root.join(relpath);
        if let Some(cfg_test_start) = first_cfg_test_line(&abs) {
            lines.retain(|&line| line < cfg_test_start);
        }
        let unreachable = unreachable_lines(&abs);
        lines.retain(|line| !unreachable.contains(line));
    }
    tracked.retain(|(_, lines)| !lines.is_empty());

    if tracked.is_empty() {
        eprintln!(
            "xtask check-sim-coverage: every added production line lives \
             outside the moonpool sim surface (or inside a `#[cfg(test)]` \
             block) — nothing to verify."
        );
        return Ok(());
    }

    if tracked.is_empty() {
        eprintln!(
            "xtask check-sim-coverage: no production-surface .rs additions \
             relative to {base} — nothing to verify."
        );
        return Ok(());
    }

    // 3. Run moonpool-side coverage and emit LCOV. We run the moonpool runtime crate's tests + the
    //    differential harness, both gated on `--all-features` so chaos-pack scenarios participate.
    //    The whole workspace is instrumented so coverage attributes to the originating crate (e.g.
    //    magnetar-proto), not just the runner.
    let lcov = run_moonpool_lcov(&workspace_root)?;
    let covered = parse_lcov_coverage(&lcov);

    // 4. Intersect: for every added line in a tracked file, check that the moonpool runner reached
    //    it. LCOV emits absolute paths; the diff surfaces workspace-relative paths, so we resolve
    //    both to absolutes.
    let uncovered = intersect_diff_with_coverage(&workspace_root, &tracked, &covered);

    if !uncovered.is_empty() {
        report_uncovered(&workspace_root, &uncovered)?;
    }

    eprintln!(
        "xtask check-sim-coverage: all added lines across {} file(s) are \
         covered by the moonpool runner.",
        tracked.len()
    );
    Ok(())
}

/// Files whose tests are intentionally one-sided per an ADR exception
/// and so MUST be excluded from the runtime-test-parity count.
///
/// Each entry is `<crate>/<rel-path-from-crate-root>`. Add to this list
/// only when the carve-out is justified in an ADR — e.g.
/// `magnetar-runtime-moonpool/tests/sim_chaos.rs` is exempt per
/// ADR-0026 §D2 (pure-sim chaos suite is engine-specific by design;
/// the tokio engine has equivalent coverage via the differential
/// broker tests in `magnetar-differential`).
///
/// The `magnetar-runtime-moonpool/{src/pool.rs, tests/proxy_multi_conn.rs}`
/// entries are exempt per the 2026-06-01 ADR-0039 amendment ("Moonpool
/// engine parity"). Both files were added by F8 in the lookup-hardening
/// push to bring the moonpool engine UP to the proxy-pool coverage tokio
/// already had on `main` (tokio's `tests/proxy_multi_conn.rs` and inline
/// pool unit tests pre-dated the lookup-hardening branch). Counting these
/// "catch-up" tests as new moonpool-only tests would penalise the parity
/// gate for what is in fact a parity *improvement*. The carve-out lifts
/// once the symmetrical multi-broker DIRECT routing port lands on
/// moonpool (tracked in `docs/follow-ups.md §3`) — by then the parity
/// landscape rebalances naturally.
const PARITY_EXEMPT_FILES: &[&str] = &[
    "magnetar-runtime-moonpool/tests/sim_chaos.rs",
    "magnetar-runtime-moonpool/src/pool.rs",
    "magnetar-runtime-moonpool/tests/proxy_multi_conn.rs",
];

/// Count test attributes (`#[test]`, `#[tokio::test]`, `#[moonpool::test]`)
/// inside a crate's `src` and `tests` directories.
///
/// Attributes are recognised by trimmed-line prefix. Composite attributes
/// like `#[tokio::test(flavor = "multi_thread")]` are matched on the
/// `#[tokio::test` prefix so they count once. Files in
/// [`PARITY_EXEMPT_FILES`] are skipped (see that constant for the rules
/// around when a carve-out is justified).
fn count_test_attributes(crate_root: &Path) -> Result<usize> {
    let mut total = 0usize;
    let crate_name = crate_root
        .file_name()
        .and_then(|s| s.to_str())
        .unwrap_or("");
    for subdir in ["src", "tests"] {
        let dir = crate_root.join(subdir);
        if !dir.exists() {
            continue;
        }
        visit(&dir, &mut |path, contents| {
            if path.extension().is_none_or(|ext| ext != "rs") {
                return;
            }
            if let Ok(rel) = path.strip_prefix(crate_root) {
                let key = format!("{crate_name}/{}", rel.display());
                if PARITY_EXEMPT_FILES.iter().any(|exempt| *exempt == key) {
                    return;
                }
            }
            for line in contents.lines() {
                let trimmed = line.trim_start();
                let is_plain = trimmed == "#[test]" || trimmed.starts_with("#[test(");
                let is_tokio =
                    trimmed.starts_with("#[tokio::test]") || trimmed.starts_with("#[tokio::test(");
                let is_moonpool = trimmed.starts_with("#[moonpool::test]")
                    || trimmed.starts_with("#[moonpool::test(");
                if is_plain || is_tokio || is_moonpool {
                    total += 1;
                }
            }
        })?;
    }
    Ok(total)
}

fn check_runtime_test_parity() -> Result<()> {
    let workspace_root = workspace_root()?;
    let tokio_crate = workspace_root.join("crates/magnetar-runtime-tokio");
    let moonpool_crate = workspace_root.join("crates/magnetar-runtime-moonpool");

    if !tokio_crate.exists() {
        bail!(
            "magnetar-runtime-tokio not found at {} — workspace layout drift?",
            tokio_crate.display()
        );
    }
    if !moonpool_crate.exists() {
        bail!(
            "magnetar-runtime-moonpool not found at {} — workspace layout drift?",
            moonpool_crate.display()
        );
    }

    let tokio_count = count_test_attributes(&tokio_crate)?;
    let moonpool_count = count_test_attributes(&moonpool_crate)?;

    if tokio_count != moonpool_count {
        let (leader, leader_count, lagger, lagger_count) = if tokio_count > moonpool_count {
            (
                "magnetar-runtime-tokio",
                tokio_count,
                "magnetar-runtime-moonpool",
                moonpool_count,
            )
        } else {
            (
                "magnetar-runtime-moonpool",
                moonpool_count,
                "magnetar-runtime-tokio",
                tokio_count,
            )
        };
        let gap = leader_count - lagger_count;
        bail!(
            "xtask check-runtime-test-parity: tokio={tokio_count} moonpool={moonpool_count} \
             — {leader} is ahead by {gap} test(s). Add equivalent tests to {lagger} \
             before merging. See ADR-0024."
        );
    }

    eprintln!(
        "xtask check-runtime-test-parity: tokio={tokio_count} moonpool={moonpool_count} (parity ok)."
    );
    Ok(())
}

/// Files to copy from upstream into `crates/magnetar-proto/proto/`.
/// Upstream path is `pulsar-common/src/main/proto/{name}`; local
/// path is `crates/magnetar-proto/proto/{name}`. Update this list
/// only when upstream adds or removes a load-bearing `.proto` file.
const VENDORED_PROTOS: &[&str] = &["PulsarApi.proto", "PulsarMarkers.proto"];

/// Refresh `crates/magnetar-proto/proto/{PulsarApi,PulsarMarkers}.proto`
/// from `apache/pulsar` at the given commit SHA, then rerun codegen.
///
/// `source` is an optional local clone of `apache/pulsar`. When `None`,
/// the helper shells out to `git clone --filter=blob:none --depth 1
/// --branch <rev>` into a tempdir. When `Some`, the helper runs
/// `git -C <source> fetch && git -C <source> checkout <rev>` and copies
/// from there — useful when the operator already has a clone and wants
/// to avoid the round-trip.
///
/// The function:
///
/// 1. Fetches the upstream tree at `rev`.
/// 2. Copies each file in [`VENDORED_PROTOS`] into the local `crates/magnetar-proto/proto/`
///    directory.
/// 3. Rewrites `crates/magnetar-proto/proto/SOURCE` with the new commit SHA + date pulled from `git
///    show -s --format=%ci`.
/// 4. Re-runs `codegen` (without `--check`) so the generated `pb/` directory reflects the new
///    proto.
///
/// The caller is expected to `git add` the resulting changes, review
/// them, and commit. The function does NOT commit on its own.
///
/// # Errors
/// Bubbles up any `git` / `fs::copy` / codegen failure with context.
fn vendor_proto(rev: &str, source: Option<&Path>) -> Result<()> {
    let workspace_root = workspace_root()?;
    let proto_dir = proto_dir()?;
    if !proto_dir.exists() {
        bail!(
            "proto/ directory missing at {}; nothing to refresh",
            proto_dir.display()
        );
    }

    // 1. Resolve the upstream source — either user-supplied or a fresh shallow clone.
    let (source_root, _scratch) = if let Some(local) = source {
        ensure_git_clean(local)?;
        run_git_in(local, &["fetch", "origin", rev])?;
        run_git_in(local, &["checkout", rev])?;
        (local.to_path_buf(), None)
    } else {
        let scratch = tempfile::tempdir().context("creating tempdir for upstream clone")?;
        let scratch_root = scratch.path().to_path_buf();
        eprintln!(
            "xtask vendor-proto: cloning apache/pulsar @ {rev} into {}",
            scratch_root.display()
        );
        let scratch_str = scratch_root
            .to_str()
            .ok_or_else(|| anyhow!("scratch tempdir path is not valid UTF-8"))?;
        run_git_in(
            Path::new("."),
            &[
                "clone",
                "--filter=blob:none",
                "--no-checkout",
                "https://github.com/apache/pulsar.git",
                scratch_str,
            ],
        )?;
        run_git_in(&scratch_root, &["fetch", "origin", rev])?;
        run_git_in(&scratch_root, &["checkout", rev])?;
        (scratch_root, Some(scratch))
    };

    // 2. Copy each vendored proto file.
    let upstream_proto_dir = source_root.join("pulsar-common/src/main/proto");
    if !upstream_proto_dir.exists() {
        bail!(
            "upstream proto dir missing at {} — wrong commit?",
            upstream_proto_dir.display()
        );
    }
    for name in VENDORED_PROTOS {
        let src = upstream_proto_dir.join(name);
        let dst = proto_dir.join(name);
        if !src.exists() {
            bail!(
                "upstream is missing {} at commit {rev}; refusing to drop the local copy",
                src.display()
            );
        }
        fs::copy(&src, &dst)
            .with_context(|| format!("copying {} → {}", src.display(), dst.display()))?;
        eprintln!(
            "xtask vendor-proto: copied {} ({} bytes)",
            name,
            fs::metadata(&dst).map_or(0, |m| m.len())
        );
    }

    // 3. Refresh proto/SOURCE with the new commit + date. Use `%cs` (committer short ISO date,
    //    YYYY-MM-DD) to match the format of the existing SOURCE file. Avoid `%ci` — that adds a
    //    time and zone.
    let resolved_rev = run_git_in_capture(&source_root, &["rev-parse", rev])?
        .trim()
        .to_owned();
    let date = run_git_in_capture(&source_root, &["show", "-s", "--format=%cs", &resolved_rev])?
        .trim()
        .to_owned();
    let source_path = proto_dir.join("SOURCE");
    let source_contents = format!(
        "Vendored from apache/pulsar:\n\
         \n  \
         Repository: https://github.com/apache/pulsar\n  \
         Commit:     {resolved_rev}\n  \
         Date:       {date}\n  \
         Files:      pulsar-common/src/main/proto/PulsarApi.proto\n              \
         pulsar-common/src/main/proto/PulsarMarkers.proto\n\
         \nRefresh by running:\n\
         \n  \
         cargo run -p xtask -- vendor-proto --rev <sha>\n  \
         cargo run -p xtask -- codegen\n\
         \nDo not hand-edit. Upstream license: Apache-2.0.\n"
    );
    fs::write(&source_path, source_contents)
        .with_context(|| format!("writing {}", source_path.display()))?;

    // 4. Rerun codegen so the generated `pb/` reflects the new proto.
    eprintln!("xtask vendor-proto: regenerating pb/ via codegen");
    codegen(false)?;

    eprintln!(
        "xtask vendor-proto: done. Review `git diff -- crates/magnetar-proto/` and commit \
         with a message naming the upstream commit + the feature it unblocks. \
         Workspace root: {}",
        workspace_root.display()
    );
    Ok(())
}

fn ensure_git_clean(repo: &Path) -> Result<()> {
    let output = StdCommand::new("git")
        .args(["status", "--porcelain"])
        .current_dir(repo)
        .output()
        .with_context(|| format!("`git status` in {}", repo.display()))?;
    if !output.status.success() {
        bail!(
            "`git status` failed in {}: {}",
            repo.display(),
            String::from_utf8_lossy(&output.stderr)
        );
    }
    if !output.stdout.is_empty() {
        bail!(
            "{} has uncommitted changes; refusing to overwrite",
            repo.display()
        );
    }
    Ok(())
}

fn run_git_in(repo: &Path, args: &[&str]) -> Result<()> {
    let status = StdCommand::new("git")
        .args(args)
        .current_dir(repo)
        .status()
        .with_context(|| format!("`git {}` in {}", args.join(" "), repo.display()))?;
    if !status.success() {
        bail!(
            "`git {}` in {} exited with {status}",
            args.join(" "),
            repo.display()
        );
    }
    Ok(())
}

fn run_git_in_capture(repo: &Path, args: &[&str]) -> Result<String> {
    let output = StdCommand::new("git")
        .args(args)
        .current_dir(repo)
        .output()
        .with_context(|| format!("`git {}` in {}", args.join(" "), repo.display()))?;
    if !output.status.success() {
        bail!(
            "`git {}` in {} failed: {}",
            args.join(" "),
            repo.display(),
            String::from_utf8_lossy(&output.stderr)
        );
    }
    Ok(String::from_utf8_lossy(&output.stdout).into_owned())
}

/// Force clang for `crypto-fips` cells on Linux.
///
/// aws-lc's FIPS BCM is post-processed by the `delocate` tool, which
/// rejects any `.data*` section in the module assembly. GCC 16+ emits
/// `.data.rel.ro.local` for some `-fPIC` const-pointer patterns; clang
/// places the equivalent in `.rodata`. `aws-lc-fips-sys` only
/// auto-switches to clang for the `asan` feature, so a plain Linux
/// FIPS build inherits whatever `cmake-rs` picks (`/usr/bin/cc` → gcc
/// on Fedora) and trips the delocate guard intermittently — the trip
/// depends on which aws-lc sources cargo's feature unification pulls
/// into `bcm.c`. Setting the C/asm toolchain explicitly here keeps the
/// matrix green regardless of host gcc version.
fn apply_fips_toolchain(cmd: &mut StdCommand, features: &str) {
    if !cfg!(target_os = "linux") {
        return;
    }
    if !features.split(',').any(|f| f.trim() == "crypto-fips") {
        return;
    }
    cmd.env("CC", "clang")
        .env("CXX", "clang++")
        .env("ASM", "clang")
        .env("AR", "llvm-ar")
        .env("RANLIB", "llvm-ranlib");
}

/// Build the four `crypto-*` provider features in isolation.
///
/// Each cell is exercised with the `tokio` feature on (production
/// surface) and with both `tokio` + `moonpool` on (so the moonpool
/// engine's `tls_crypto` sibling compiles under each provider too).
/// `cargo build --workspace --all-features` already validates the cfg
/// cascade in `magnetar-runtime-{tokio,moonpool}/src/tls_crypto.rs`;
/// this check is the per-cell complement (issue #9, ADR-0035).
///
/// A second pass also builds the `magnetar-auth-athenz` crate in
/// isolation across the cartesian product `{none, crypto-aws-lc-rs,
/// crypto-ring, both}` × `{zts off, zts on}` so the concrete
/// `JwtSigner` backends (ADR-0030 close-out — see
/// `crates/magnetar-auth-athenz/src/jwt_signer/`) compile cleanly in
/// every callable shape. The `none` cell preserves the "ship the
/// trait, downstream picks the signer" stance from before the
/// concrete backends landed.
fn check_crypto_matrix() -> Result<()> {
    const PROVIDERS: &[&str] = &[
        "crypto-aws-lc-rs",
        "crypto-ring",
        "crypto-openssl",
        "crypto-fips",
    ];
    // Athenz signer matrix: `none` exercises the trait-only surface
    // (existing behaviour). `both` validates the cfg cascade —
    // aws-lc-rs wins per ADR-0035 priority.
    const ATHENZ_CELLS: &[&str] = &[
        "",
        "crypto-aws-lc-rs",
        "crypto-ring",
        "crypto-aws-lc-rs,crypto-ring",
    ];

    let workspace_root = workspace_root()?;
    let cargo = env::var_os("CARGO").unwrap_or_else(|| "cargo".into());
    let mut failures: Vec<String> = Vec::new();
    let mut total_cells: usize = 0;

    // ── Façade matrix ─────────────────────────────────────────────
    for crypto in PROVIDERS {
        for base in ["tokio", "tokio,moonpool"] {
            let features = format!("{base},{crypto}");
            eprintln!(
                "xtask check-crypto-matrix: cargo build -p magnetar --no-default-features --features {features}"
            );
            let mut cmd = StdCommand::new(&cargo);
            cmd.current_dir(&workspace_root).args([
                "build",
                "-p",
                "magnetar",
                "--no-default-features",
                "--features",
                &features,
            ]);
            apply_fips_toolchain(&mut cmd, &features);
            let status = cmd.status().with_context(|| {
                format!("failed to invoke `cargo build` for features `{features}`")
            })?;
            total_cells += 1;
            if !status.success() {
                failures.push(format!("magnetar:{features}"));
            }
        }
    }

    // ── Athenz signer matrix ──────────────────────────────────────
    for athenz_features in ATHENZ_CELLS {
        for base in ["", "zts"] {
            let features = match (base.is_empty(), athenz_features.is_empty()) {
                (true, true) => String::new(),
                (true, false) => (*athenz_features).to_owned(),
                (false, true) => (*base).to_owned(),
                (false, false) => format!("{base},{athenz_features}"),
            };
            let mut args: Vec<&str> = vec![
                "build",
                "-p",
                "magnetar-auth-athenz",
                "--no-default-features",
            ];
            if !features.is_empty() {
                args.extend(["--features", features.as_str()]);
            }
            eprintln!(
                "xtask check-crypto-matrix: cargo build -p magnetar-auth-athenz --no-default-features --features '{features}'"
            );
            let status = StdCommand::new(&cargo)
                .current_dir(&workspace_root)
                .args(&args)
                .status()
                .with_context(|| {
                    format!(
                        "failed to invoke `cargo build -p magnetar-auth-athenz` for features `{features}`"
                    )
                })?;
            total_cells += 1;
            if !status.success() {
                failures.push(format!("magnetar-auth-athenz:{features}"));
            }
        }
    }

    if failures.is_empty() {
        eprintln!("xtask check-crypto-matrix: all {total_cells} cells built successfully.");
        Ok(())
    } else {
        for cell in &failures {
            eprintln!("xtask check-crypto-matrix: FAILED cell: {cell}");
        }
        bail!(
            "xtask check-crypto-matrix: {} of {total_cells} cell(s) failed.",
            failures.len()
        );
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // ── check-log-fields parser ─────────────────────────────────────

    #[test]
    fn log_fields_flags_bare_message() {
        let src = r#"
fn run() {
    tracing::error!("supervisor: begin_handshake after reset failed");
}
"#;
        let violations = scan_log_field_violations(src);
        assert_eq!(violations, vec![(3, "error", LOG_FIELDS_NO_FIELD)]);
    }

    #[test]
    fn log_fields_flags_inline_format_args_only() {
        // Inline-formatted values in the message string are NOT structured
        // fields (ADR-0054 §2.2) — and neither are positional format args
        // after the message.
        let src = r#"
fn run() {
    tracing::warn!("reconnect attempt {attempt} failed: {err}; will retry");
    tracing::warn!("gave up after {} attempt(s)", attempts);
}
"#;
        let violations = scan_log_field_violations(src);
        assert_eq!(
            violations,
            vec![
                (3, "warn", LOG_FIELDS_NO_FIELD),
                (4, "warn", LOG_FIELDS_NO_FIELD)
            ]
        );
    }

    #[test]
    fn log_fields_accepts_structured_fields() {
        let src = r#"
fn run() {
    tracing::warn!(attempt, max_attempts = max, "reconnect failed");
    tracing::info!(?handle, code, %message, "transient error; retrying");
    tracing::error!(error = %err, "lookup failed");
    tracing::warn!(target: "magnetar::auth", auth_method = %method, "auth refresh failed");
    tracing::info!("question.answer" = 42, "quoted field name");
    info!(count);
}
"#;
        assert!(scan_log_field_violations(src).is_empty());
    }

    #[test]
    fn log_fields_parses_multi_line_invocations() {
        // Parenthesis-balanced parsing: the structured invocation spans
        // several lines, the bare one wraps its message string. A line-window
        // heuristic would misclassify both.
        let src = r#"
fn run() {
    tracing::warn!(
        source,
        rejected_url = broker_service_url.as_deref(),
        "broker-advertised redirect URL rejected by redirect_url_allow_list; \
         ignoring the hint",
    );
    tracing::warn!(
        "supervisor: service-url provider returned an unparseable URL \
         on this attempt; falling back to the cached URL"
    );
}
"#;
        let violations = scan_log_field_violations(src);
        assert_eq!(violations, vec![(9, "warn", LOG_FIELDS_NO_FIELD)]);
    }

    #[test]
    fn log_fields_skips_cfg_test_modules() {
        let src = r#"
fn run() {
    tracing::info!(topic = %topic, "producer created");
}

#[cfg(test)]
mod tests {
    #[test]
    fn capture() {
        tracing::error!("bare message inside a test module is fine");
    }
}
"#;
        assert!(scan_log_field_violations(src).is_empty());
    }

    #[test]
    fn log_fields_flags_target_only_invocation() {
        // `target:` (and `name:` / `parent:`) are macro spec args, not
        // structured fields.
        let src = r#"
fn run() {
    tracing::info!(target: "magnetar::pattern_consumer", "discovery tick");
    tracing::info!(target: "magnetar::pattern_consumer", added, "discovery delta");
}
"#;
        let violations = scan_log_field_violations(src);
        assert_eq!(violations, vec![(3, "info", LOG_FIELDS_NO_FIELD)]);
    }

    #[test]
    fn log_fields_rejects_brace_and_bracket_delimiter_forms() {
        // The field grammar parses only parenthesized invocations; the
        // other delimiter forms would bypass it silently, so they are hard
        // violations even WITH a structured field inside.
        let src = r#"
fn run() {
    tracing::warn!{ error = %err, "brace form" };
    info!["bracket form"];
}
"#;
        let violations = scan_log_field_violations(src);
        assert_eq!(
            violations,
            vec![
                (3, "warn", LOG_FIELDS_NON_PAREN),
                (4, "info", LOG_FIELDS_NON_PAREN)
            ]
        );
    }

    #[test]
    fn log_fields_ignores_comments_strings_and_lookalike_macros() {
        let src = r#"
fn run() {
    // tracing::error!("commented out");
    /* tracing::warn!("block comment") */
    let doc = "error!(\"inside a string literal\")";
    my_error!("custom macro, not tracing");
    tracing::debug!("debug is exempt");
    tracing::trace!("trace is exempt");
}
"#;
        assert!(scan_log_field_violations(src).is_empty());
    }
}
