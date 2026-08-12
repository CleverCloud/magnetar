// SPDX-License-Identifier: Apache-2.0

//! `xtask` — build helpers for magnetar.
//!
//! Subcommands:
//! - `codegen` / `codegen --check`: regenerate / verify `magnetar-proto/src/pb/`.
//! - `check-no-channels`: grep the workspace for banned channel paths.
//! - `check-no-io-deps`: assert `magnetar-proto` has zero I/O dependencies.
//! - `check-no-internal-clock`: assert `magnetar-proto/src/**` never reads the host clock
//!   (`Instant::now()` / `SystemTime::now()` / `.elapsed()`) outside `#[cfg(test)]`, with no file
//!   allowlist. Mirrors ADR-0011 as amended by ADR-0086.
//! - `check-log-fields`: assert every `error!` / `warn!` / `info!` tracing event in non-test
//!   workspace code carries at least one structured field (`debug!` / `trace!` exempt). Mirrors
//!   ADR-0054.
//! - `check-e2e-container-memory`: assert every `pulsar standalone` container the e2e suite starts
//!   caps its JVM with `.with_env_var("PULSAR_MEM", …)` before `.start()`. Mirrors
//!   `docs/testing.md` § "e2e container memory budget".
//! - `check-sim-coverage`: assert that every executable line added relative to `git merge-base
//!   origin/main HEAD` is executed in its owning isolated evidence domain. Moonpool+differential
//!   tests own shared/proto/sim/façade/fakes/auth source; Tokio unit/integration tests own only the
//!   Tokio adapter. Separate objects, profiles, profdata, and reports prevent cross-discharge
//!   (ADR-0103).
//! - `check-runtime-test-parity`: assert `magnetar-runtime-tokio` and `magnetar-runtime-moonpool`
//!   carry the same number of `#[test]` / `#[tokio::test]` / `#[moonpool::test]` items. Mirrors
//!   ADR-0024.
//! - `check-known-failing-seeds`: replay every `status = "open"` entry of
//!   `crates/magnetar-runtime-moonpool/seeds/known-failing.toml` with the exact per-PR
//!   `seed-replay` invocation from `.github/workflows/ci.yml` and fail on any reproducing seed.
//!   Mirrors ADR-0047 §5 — "if CI's replay job would fail, this xtask fails too" (landed by
//!   ADR-0097).
//! - `check-crypto-matrix`: build the four `crypto-*` provider features in isolation (issue #9,
//!   ADR-0035). Complements `cargo build --workspace --all-features` (which exercises the cfg
//!   cascade) by proving each single-provider cell compiles cleanly.
//! - `vendor-proto --rev <sha>`: refresh vendored `PulsarApi.proto`.
//!
//! Codegen drives `prost-build` against `crates/magnetar-proto/proto/`, writes
//! the generated Rust into `crates/magnetar-proto/src/pb/`, and (with `--check`)
//! diffs the generated output against what is committed so CI catches drift.

use std::ffi::{OsStr, OsString};
use std::path::{Path, PathBuf};
use std::process::{Command as StdCommand, ExitCode};
use std::time::Duration;
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
    /// Greps for direct calls to [`std::time::Instant::now`],
    /// [`std::time::SystemTime::now`], and `.elapsed()` outside
    /// `#[cfg(test)]` blocks. There is no file allowlist. See ADR-0011 and
    /// ADR-0086.
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
    /// Assert every `pulsar standalone` container the e2e suite starts
    /// caps its JVM heap.
    ///
    /// Walks each `GenericImage::new(…)` builder chain under
    /// `crates/magnetar/tests/`, resolves the image repository the first
    /// constructor argument denotes (string literal, `&str` const, or a
    /// zero-argument accessor returning one), and requires every
    /// `apachepulsar/…` chain to carry `.with_env_var("PULSAR_MEM", …)`
    /// before `.start()`. Non-Pulsar containers — the Kerberos KDC and the
    /// Athenz ZTS server — are out of scope; a chain whose image cannot be
    /// resolved, or that never reaches `.start()`, is a violation rather
    /// than a silent skip. Only the call's presence is checked: the budget
    /// value lives in `docs/testing.md` § "e2e container memory budget".
    CheckE2eContainerMemory,
    /// Enforce added executable-line coverage in separate Moonpool/shared and
    /// Tokio-adapter evidence domains. Each owns an invocation-local report;
    /// packages owned by neither remain advisory `not gated` scope.
    CheckSimCoverage {
        /// Base ref to diff against. Defaults to `origin/main`.
        #[arg(long, default_value = "origin/main")]
        base: String,
        /// Reuse both target/{sim,tokio}-coverage.lcov diagnostics instead of running
        /// coverage commands. SIZING / DEBUG ONLY — never in validation or CI.
        /// Every line the run prints is tagged `[REUSED LCOV — NOT A FRESH
        /// MEASUREMENT]`, because a stale report maps old line numbers onto new
        /// code and the success sentence is otherwise identical to a real run.
        #[arg(long)]
        reuse_lcov: bool,
        /// Fail on uncovered added lines instead of only reporting them.
        ///
        /// Redundant since ADR-0092: `SIM_COVERAGE_ENFORCES_UNCOVERED` is
        /// `true`, so an uncovered added line already fails without this flag.
        /// It is retained rather than removed because it ORs into that constant
        /// — existing invocations keep working, the CI job passes it so the
        /// workflow states its own intent, and it stays the one explicit way to
        /// ask for the verdict if the constant is ever flipped back.
        #[arg(long)]
        enforce: bool,
    },
    /// Assert tokio ↔ moonpool runtime crates carry the same number of
    /// test items.
    ///
    /// Counts `#[test]`, `#[tokio::test]`, and `#[moonpool::test]`
    /// attributes under `crates/magnetar-runtime-tokio/{src,tests}` and
    /// `crates/magnetar-runtime-moonpool/{src,tests}`. Strict equality
    /// required. See ADR-0024.
    CheckRuntimeTestParity,
    /// Replay every open registry seed locally (ADR-0047 §5, ADR-0097).
    ///
    /// Parses `crates/magnetar-runtime-moonpool/seeds/known-failing.toml`
    /// and runs `MOONPOOL_SEED=<value> cargo test -p
    /// magnetar-runtime-moonpool --no-default-features --features
    /// crypto-aws-lc-rs --locked` for each `status = "open"` entry —
    /// the exact invocation the per-PR `seed-replay` CI job uses, so
    /// the local invariant is "if CI's replay job would fail, this
    /// xtask fails too".
    CheckKnownFailingSeeds,
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
        Cmd::CheckE2eContainerMemory => check_e2e_container_memory(),
        Cmd::CheckSimCoverage {
            base,
            reuse_lcov,
            enforce,
        } => check_sim_coverage(&base, reuse_lcov, enforce),
        Cmd::CheckRuntimeTestParity => check_runtime_test_parity(),
        Cmd::CheckKnownFailingSeeds => check_known_failing_seeds(),
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

/// Compare files in `lhs` against `rhs`. Returns a list of human-readable
/// difference descriptions. An empty Vec means the two trees are identical.
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

/// Host-clock reads forbidden in `crates/magnetar-proto/src/**` outside
/// `#[cfg(test)]` (ADR-0011, ADR-0086).
///
/// `.elapsed()` carries its leading dot deliberately: a bare `elapsed()`
/// needle would also match method *names* like
/// `ConsumerState::batch_deadline_elapsed(now)` and the test
/// `record_rate_window_safe_under_zero_elapsed`, neither of which reads a
/// clock. Matching is plain substring over the code portion of the file, so
/// each needle catches both its qualified (`std::time::Instant::now()`) and
/// unqualified spelling.
const CLOCK_NEEDLES: &[&str] = &["Instant::now()", "SystemTime::now()", ".elapsed()"];

/// File paths inside `magnetar-proto/src/` explicitly allowed to touch the
/// host clock. The list starts — and should stay — **empty**: as of ADR-0086
/// no file in the sans-io core reads a clock it was not handed.
///
/// It previously carried `producer.rs` and `auth/token.rs`, whose rationale
/// was the `uuid::Uuid::new_v4()` and `std::env::var()` leaks — neither of
/// which this gate has ever scanned for. The entries bought no enforcement
/// and cost real blindness: they whole-file-skipped `producer.rs`, which is
/// where one of the two ADR-0086 `.elapsed()` leaks lived. Those two
/// non-clock leaks remain documented in `ARCHITECTURE.md` under "Known
/// non-determinism leaks", which is the inventory of record.
///
/// Add an entry only with a rationale documented in the same changeset AND a
/// matching entry in that `ARCHITECTURE.md` section. Paths are
/// workspace-relative and matched with [`Path::ends_with`] so the check is
/// robust to symlinks and absolute prefixes.
const CLOCK_LEAK_ALLOWLIST: &[&str] = &[];

/// Scan one file's contents for forbidden host-clock reads, returning
/// `(1-indexed line, needle)` per violation.
///
/// Skips two kinds of region:
///
/// - `#[cfg(test)]` spans, via the shared [`cfg_test_line_flags`] — tests legitimately materialise
///   instants for their fixtures.
/// - Lexically inert regions, via the shared [`skip_inert_region`] — line and (nested) block
///   comments, string / raw-string / byte-string literals, and char literals. Prose that *mentions*
///   `Instant::now()` in a doc comment is documentation, not a call.
///
/// This is the pure, unit-testable seam of [`check_no_internal_clock`],
/// mirroring [`scan_log_field_violations`]. It replaced a hand-rolled
/// line-level scanner that duplicated `cfg_test_line_flags`' brace-tracking
/// heuristic and stripped comments with a naive `line.find("//")` — the
/// latter silently exempted any line containing `//` inside a string literal
/// (e.g. a `"pulsar://host"` URL on the same line as a clock read).
fn scan_clock_violations(contents: &str) -> Vec<(usize, &'static str)> {
    let in_cfg_test = cfg_test_line_flags(contents);
    let bytes = contents.as_bytes();

    // Byte offset of each line start, so a match offset maps to a line number
    // with a binary search rather than a rescan.
    let mut line_starts = vec![0usize];
    line_starts.extend(
        bytes
            .iter()
            .enumerate()
            .filter(|(_, b)| **b == b'\n')
            .map(|(i, _)| i + 1),
    );

    let mut violations = Vec::new();
    let mut i = 0usize;
    while i < bytes.len() {
        if let Some(next) = skip_inert_region(bytes, i) {
            i = next.max(i + 1);
            continue;
        }
        for needle in CLOCK_NEEDLES {
            if bytes[i..].starts_with(needle.as_bytes()) {
                let line = line_starts.partition_point(|&start| start <= i);
                if !in_cfg_test.get(line - 1).copied().unwrap_or(false) {
                    violations.push((line, *needle));
                }
            }
        }
        i += 1;
    }
    violations
}

fn check_no_internal_clock() -> Result<()> {
    // Flag direct host-clock reads in `magnetar-proto/src/**`. See ADR-0011
    // for the clock-injection rule, ADR-0086 for the `.elapsed()` extension
    // and the emptied allowlist, and ARCHITECTURE.md "Known non-determinism
    // leaks (documented)" for the leaks this gate does NOT mechanically
    // enforce (uuid, env::var).
    let workspace_root = workspace_root()?;
    let proto_src = workspace_root.join("crates/magnetar-proto/src");

    let mut offenders: Vec<String> = Vec::new();
    visit(&proto_src, &mut |path, contents| {
        if path.extension().is_none_or(|ext| ext != "rs") {
            return;
        }
        // Allow the documented leak sites (currently none).
        if CLOCK_LEAK_ALLOWLIST
            .iter()
            .any(|allowed| path.ends_with(allowed) || path.to_string_lossy().ends_with(allowed))
        {
            return;
        }

        let relative = path
            .strip_prefix(&workspace_root)
            .unwrap_or(path)
            .to_string_lossy()
            .replace('\\', "/");

        for (line, needle) in scan_clock_violations(contents) {
            offenders.push(format!(
                "{relative}:{line}: contains {needle} outside #[cfg(test)] — see ADR-0011/ADR-0086"
            ));
        }
    })?;

    if !offenders.is_empty() {
        offenders.sort();
        for line in &offenders {
            eprintln!("forbidden host-clock read — {line}");
        }
        bail!(
            "no-internal-clock check failed: {} offender(s). \
             magnetar-proto must take `now: Instant` / `wall_clock` providers \
             through its API — see specs/adr/0011-clock-injection-sans-io.md \
             and specs/adr/0086-inject-now-into-proto-latency-recording.md.",
            offenders.len()
        );
    }
    eprintln!(
        "xtask check-no-internal-clock: no host-clock reads in magnetar-proto/src (needles: {}).",
        CLOCK_NEEDLES.join(", ")
    );
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
    extract_balanced(bytes, open, b'(', b')')
}

/// Extract the text between a balanced `open_ch` / `close_ch` pair, with
/// `bytes[open]` being the opening delimiter. Comments and string/char
/// literals inside do not perturb the balance. Returns the inner text plus
/// the index just past the closing delimiter.
fn extract_balanced(
    bytes: &[u8],
    open: usize,
    open_ch: u8,
    close_ch: u8,
) -> Option<(String, usize)> {
    let mut depth = 0usize;
    let mut j = open;
    let start = open + 1;
    while j < bytes.len() {
        if let Some(next) = skip_inert_region(bytes, j) {
            j = next.max(j + 1);
            continue;
        }
        if bytes[j] == open_ch {
            depth += 1;
        } else if bytes[j] == close_ch {
            depth -= 1;
            if depth == 0 {
                let inner = String::from_utf8_lossy(&bytes[start..j]).into_owned();
                return Some((inner, j + 1));
            }
        }
        j += 1;
    }
    None
}

/// True for bytes that can appear inside a Rust identifier.
fn is_ident_byte(b: u8) -> bool {
    b.is_ascii_alphanumeric() || b == b'_'
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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum CfgTruth {
    AlwaysFalse,
    AlwaysTrue,
    Either,
}

impl CfgTruth {
    const fn and(self, other: Self) -> Self {
        match (self, other) {
            (Self::AlwaysFalse, _) | (_, Self::AlwaysFalse) => Self::AlwaysFalse,
            (Self::AlwaysTrue, Self::AlwaysTrue) => Self::AlwaysTrue,
            _ => Self::Either,
        }
    }

    const fn or(self, other: Self) -> Self {
        match (self, other) {
            (Self::AlwaysTrue, _) | (_, Self::AlwaysTrue) => Self::AlwaysTrue,
            (Self::AlwaysFalse, Self::AlwaysFalse) => Self::AlwaysFalse,
            _ => Self::Either,
        }
    }

    const fn not(self) -> Self {
        match self {
            Self::AlwaysFalse => Self::AlwaysTrue,
            Self::AlwaysTrue => Self::AlwaysFalse,
            Self::Either => Self::Either,
        }
    }
}

/// Minimal parser for Rust's `cfg` predicate grammar.
///
/// Every predicate other than `test` is unknown rather than false. Evaluating
/// with `test = false` therefore proves an item test-only only when no possible
/// feature, target, or custom cfg value can make the expression true.
struct CfgExprParser<'a> {
    bytes: &'a [u8],
    cursor: usize,
}

impl<'a> CfgExprParser<'a> {
    const fn new(input: &'a str) -> Self {
        Self {
            bytes: input.as_bytes(),
            cursor: 0,
        }
    }

    fn parse(mut self) -> Option<CfgTruth> {
        let value = self.parse_expr()?;
        self.cursor = skip_cfg_trivia(self.bytes, self.cursor);
        (self.cursor == self.bytes.len()).then_some(value)
    }

    fn parse_expr(&mut self) -> Option<CfgTruth> {
        self.cursor = skip_cfg_trivia(self.bytes, self.cursor);
        let name_start = self.cursor;
        while self
            .bytes
            .get(self.cursor)
            .copied()
            .is_some_and(is_ident_byte)
        {
            self.cursor += 1;
        }
        if self.cursor == name_start {
            return None;
        }
        let name_end = self.cursor;
        self.cursor = skip_cfg_trivia(self.bytes, self.cursor);

        if self.bytes.get(self.cursor) == Some(&b'=') {
            self.cursor += 1;
            self.cursor = skip_cfg_trivia(self.bytes, self.cursor);
            let value_start = self.cursor;
            if let Some(next) = skip_inert_region(self.bytes, self.cursor) {
                self.cursor = next;
            } else {
                while self
                    .bytes
                    .get(self.cursor)
                    .copied()
                    .is_some_and(|byte| is_ident_byte(byte) || byte == b'-' || byte == b'.')
                {
                    self.cursor += 1;
                }
            }
            return (self.cursor > value_start).then_some(CfgTruth::Either);
        }

        if self.bytes.get(self.cursor) != Some(&b'(') {
            return Some(if &self.bytes[name_start..name_end] == b"test" {
                CfgTruth::AlwaysFalse
            } else {
                CfgTruth::Either
            });
        }

        let name = &self.bytes[name_start..name_end];
        if !matches!(name, b"all" | b"any" | b"not") {
            let (_, after) = extract_balanced_parens(self.bytes, self.cursor)?;
            self.cursor = after;
            return Some(CfgTruth::Either);
        }

        self.cursor += 1;
        let mut values = Vec::new();
        loop {
            self.cursor = skip_cfg_trivia(self.bytes, self.cursor);
            if self.bytes.get(self.cursor) == Some(&b')') {
                self.cursor += 1;
                break;
            }
            values.push(self.parse_expr()?);
            self.cursor = skip_cfg_trivia(self.bytes, self.cursor);
            match self.bytes.get(self.cursor) {
                Some(b',') => self.cursor += 1,
                Some(b')') => {
                    self.cursor += 1;
                    break;
                }
                _ => return None,
            }
        }

        match name {
            b"all" => Some(values.into_iter().fold(CfgTruth::AlwaysTrue, CfgTruth::and)),
            b"any" => Some(values.into_iter().fold(CfgTruth::AlwaysFalse, CfgTruth::or)),
            b"not" => match values.as_slice() {
                [value] => Some(value.not()),
                _ => None,
            },
            _ => None,
        }
    }
}

fn skip_cfg_trivia(bytes: &[u8], mut cursor: usize) -> usize {
    loop {
        while bytes
            .get(cursor)
            .copied()
            .is_some_and(|byte| byte.is_ascii_whitespace())
        {
            cursor += 1;
        }
        if matches!(bytes.get(cursor..cursor + 2), Some(b"//" | b"/*"))
            && let Some(next) = skip_inert_region(bytes, cursor)
        {
            cursor = next;
            continue;
        }
        return cursor;
    }
}

fn cfg_expression_is_test_only(expression: &str) -> bool {
    CfgExprParser::new(expression).parse() == Some(CfgTruth::AlwaysFalse)
}

/// Parse one outer `#[cfg(...)]` attribute at `start`.
fn cfg_attribute(bytes: &[u8], start: usize) -> Option<(bool, usize)> {
    if bytes.get(start) != Some(&b'#') {
        return None;
    }
    let mut cursor = skip_cfg_trivia(bytes, start + 1);
    if bytes.get(cursor) != Some(&b'[') {
        return None;
    }
    cursor = skip_cfg_trivia(bytes, cursor + 1);
    if bytes.get(cursor..cursor + 3) != Some(b"cfg")
        || bytes.get(cursor + 3).copied().is_some_and(is_ident_byte)
    {
        return None;
    }
    cursor = skip_cfg_trivia(bytes, cursor + 3);
    if bytes.get(cursor) != Some(&b'(') {
        return None;
    }
    let (expression, after_expression) = extract_balanced_parens(bytes, cursor)?;
    cursor = skip_cfg_trivia(bytes, after_expression);
    if bytes.get(cursor) != Some(&b']') {
        return None;
    }
    Some((cfg_expression_is_test_only(&expression), cursor + 1))
}

fn item_prefix_uses_semicolon(bytes: &[u8]) -> bool {
    let mut cursor = 0usize;
    let mut bracket_depth = 0usize;
    let mut has_fn = false;
    let mut semicolon_item = false;
    let mut value_or_type_started = false;
    while cursor < bytes.len() {
        if let Some(next) = skip_inert_region(bytes, cursor) {
            cursor = next.max(cursor + 1);
            continue;
        }
        match bytes[cursor] {
            b'[' => {
                bracket_depth += 1;
                cursor += 1;
            }
            b']' => {
                bracket_depth = bracket_depth.saturating_sub(1);
                cursor += 1;
            }
            byte if bracket_depth == 0 && (byte.is_ascii_alphabetic() || byte == b'_') => {
                let start = cursor;
                cursor += 1;
                while bytes.get(cursor).copied().is_some_and(is_ident_byte) {
                    cursor += 1;
                }
                match &bytes[start..cursor] {
                    b"fn" if !value_or_type_started => has_fn = true,
                    b"use" | b"type" | b"const" | b"static" | b"let" => {
                        semicolon_item = true;
                    }
                    _ => {}
                }
            }
            b':' | b'=' if bracket_depth == 0 => {
                value_or_type_started = true;
                cursor += 1;
            }
            _ => cursor += 1,
        }
    }
    semicolon_item && !has_fn
}

/// Find the end of the item or statement governed by an outer cfg attribute.
///
/// The scanner is deliberately conservative: failure to identify an end means
/// no exclusion. Strings, chars, raw strings, and comments are skipped before
/// examining delimiters, so inert braces cannot extend a test-only span over
/// later production code.
fn cfg_attributed_item_end(bytes: &[u8], start: usize) -> Option<usize> {
    let mut cursor = start;
    let mut paren_depth = 0usize;
    let mut bracket_depth = 0usize;
    let mut brace_depth = 0usize;
    let mut semicolon_item = None;
    while cursor < bytes.len() {
        if let Some(next) = skip_inert_region(bytes, cursor) {
            cursor = next.max(cursor + 1);
            continue;
        }
        match bytes[cursor] {
            b'(' => paren_depth += 1,
            b')' => paren_depth = paren_depth.saturating_sub(1),
            b'[' => bracket_depth += 1,
            b']' => bracket_depth = bracket_depth.saturating_sub(1),
            b'{' if paren_depth == 0 && bracket_depth == 0 => {
                let uses_semicolon = *semicolon_item
                    .get_or_insert_with(|| item_prefix_uses_semicolon(&bytes[start..cursor]));
                if !uses_semicolon {
                    let (_, after) = extract_balanced(bytes, cursor, b'{', b'}')?;
                    return Some(after);
                }
                brace_depth += 1;
            }
            b'}' if paren_depth == 0 && bracket_depth == 0 && brace_depth > 0 => {
                brace_depth -= 1;
            }
            b';' if paren_depth == 0 && bracket_depth == 0 && brace_depth == 0 => {
                return Some(cursor + 1);
            }
            _ => {}
        }
        cursor += 1;
    }
    None
}

fn cfg_test_spans(contents: &str) -> Vec<(usize, usize)> {
    let bytes = contents.as_bytes();
    let mut spans = Vec::new();
    let mut cursor = 0usize;
    while cursor < bytes.len() {
        if let Some(next) = skip_inert_region(bytes, cursor) {
            cursor = next.max(cursor + 1);
            continue;
        }
        let Some((test_only, after_attribute)) = cfg_attribute(bytes, cursor) else {
            cursor += 1;
            continue;
        };
        if test_only && let Some(item_end) = cfg_attributed_item_end(bytes, after_attribute) {
            spans.push((cursor, item_end));
            cursor = item_end;
        } else {
            cursor = after_attribute;
        }
    }
    spans
}

fn line_has_code_outside_test_spans(
    bytes: &[u8],
    start: usize,
    end: usize,
    spans: &[(usize, usize)],
) -> bool {
    let mut cursor = start;
    while cursor < end {
        if let Some((_, span_end)) = spans
            .iter()
            .find(|(span_start, span_end)| *span_start <= cursor && cursor < *span_end)
        {
            cursor = (*span_end).min(end);
            continue;
        }
        if bytes[cursor].is_ascii_whitespace() {
            cursor += 1;
            continue;
        }
        if matches!(bytes.get(cursor..cursor + 2), Some(b"//" | b"/*"))
            && let Some(next) = skip_inert_region(bytes, cursor)
        {
            cursor = next.min(end);
            continue;
        }
        return true;
    }
    false
}

/// Per-line `#[cfg(test)]`-membership flags for `contents` (1 entry per
/// line, 1-indexed lines map to `flags[line - 1]`).
///
/// A line is excluded only when an actual cfg predicate cannot be true with
/// `test = false` and no production token shares that source line. This keeps
/// `not(test)`, `any(test, feature = "...")`, and unrelated names containing
/// `test` in the production surface while still recognizing `all(test, ...)`.
fn cfg_test_line_flags(contents: &str) -> Vec<bool> {
    let bytes = contents.as_bytes();
    let spans = cfg_test_spans(contents);
    let mut ranges = Vec::new();
    let mut start = 0usize;
    for (index, byte) in bytes.iter().enumerate() {
        if *byte == b'\n' {
            ranges.push((start, index));
            start = index + 1;
        }
    }
    if start < bytes.len() {
        ranges.push((start, bytes.len()));
    }

    ranges
        .into_iter()
        .map(|(line_start, line_end)| {
            let touches_test_span = spans.iter().any(|(span_start, span_end)| {
                if line_start == line_end {
                    *span_start <= line_start && line_start < *span_end
                } else {
                    *span_start < line_end && line_start < *span_end
                }
            });
            touches_test_span
                && !line_has_code_outside_test_spans(bytes, line_start, line_end, &spans)
        })
        .collect()
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

/// Workspace-relative directory the container-memory gate scans. Only the
/// façade's e2e suite starts `testcontainers` images.
const E2E_TESTS_DIR: &str = "crates/magnetar/tests";

/// The `testcontainers` constructor every e2e container is built from.
const CONTAINER_CTOR: &str = "GenericImage::new";

/// A chain is governed by the container-memory budget iff its resolved
/// image repository starts with this prefix. The suite also starts a
/// Kerberos KDC (`gcavalcante8808/krb5-server`) and an Athenz ZTS server
/// (`athenz/athenz-zts-server`); neither runs the Pulsar JVM, so neither
/// is in scope.
const PULSAR_IMAGE_PREFIX: &str = "apachepulsar/";

/// The env var every Pulsar container must set. The gate asserts the
/// *presence* of a `.with_env_var("PULSAR_MEM", …)` call; the budget value
/// itself is governed by `docs/testing.md` § "e2e container memory
/// budget", not by this check.
const PULSAR_MEM_ENV: &str = "PULSAR_MEM";

/// Violation reason: a Pulsar container reaches `.start()` uncapped.
const CONTAINER_MEM_NO_ENV: &str =
    "starts a Pulsar container without .with_env_var(\"PULSAR_MEM\", …)";

/// Violation reason: the builder is stashed instead of started in the same
/// chain, so the gate cannot see whether it is capped. Rejected outright
/// rather than skipped — mirrors how [`LOG_FIELDS_NON_PAREN`] treats a
/// macro form the field grammar cannot parse.
const CONTAINER_MEM_NOT_STARTED: &str = "GenericImage builder does not reach .start() in the same chain; keep .start() on the chain \
     so the memory cap can be verified";

/// Violation reason: the image repository could not be resolved to a
/// string, so the gate cannot tell whether the budget applies.
const CONTAINER_MEM_UNRESOLVED: &str = "cannot resolve the image repository; pass a string literal, a `const …: &str`, or a \
     zero-argument accessor returning one";

/// One `GenericImage::new(…)` builder chain found in a file.
struct ContainerChain {
    /// 1-indexed line of the `GenericImage::new` token.
    line: usize,
    /// Every image-repository value the first constructor argument can
    /// resolve to. Empty when unresolvable.
    repos: Vec<String>,
    /// True when the chain reaches `.start()`.
    started: bool,
    /// True when a `.with_env_var("PULSAR_MEM", …)` precedes `.start()`.
    caps_memory: bool,
}

/// Inner text of a plain `"…"` string literal at the start of `part`.
/// Returns `None` when `part` does not begin with one. Escapes come back
/// raw — the gate only compares against escape-free literals (image
/// repository names, `PULSAR_MEM`).
fn leading_string_literal_value(part: &str) -> Option<&str> {
    let part = part.trim_start();
    let bytes = part.as_bytes();
    if bytes.first() != Some(&b'"') {
        return None;
    }
    let end = skip_string_literal(bytes, 0);
    part.get(1..end.saturating_sub(1))
}

/// Every `const <NAME>: &str = "<literal>";` declared in plain code, as
/// `(name, value)` pairs. Non-`&str` consts and non-literal initialisers
/// (`&["a", "b"]`, `concat!(…)`) are skipped.
fn const_str_table(contents: &str) -> Vec<(&str, &str)> {
    let bytes = contents.as_bytes();
    let mut out = Vec::new();
    let mut i = 0usize;
    while i < bytes.len() {
        if let Some(next) = skip_inert_region(bytes, i) {
            i = next.max(i + 1);
            continue;
        }
        if bytes[i..].starts_with(b"const")
            && (i == 0 || !is_ident_byte(bytes[i - 1]))
            && !bytes.get(i + 5).copied().is_some_and(is_ident_byte)
        {
            if let Some(decl) = parse_const_str_decl(&contents[i + 5..]) {
                out.push(decl);
            }
            i += 5;
            continue;
        }
        i += 1;
    }
    out
}

/// Parse the tail of a `const` declaration — everything after the `const`
/// keyword — into `(name, literal value)`. Only `&str` consts initialised
/// with a plain string literal are returned.
fn parse_const_str_decl(rest: &str) -> Option<(&str, &str)> {
    let rest = rest.trim_start();
    let name_end = rest.find(|c: char| !(c.is_ascii_alphanumeric() || c == '_'))?;
    let name = &rest[..name_end];
    if name.is_empty() {
        return None;
    }
    let after = rest[name_end..].trim_start().strip_prefix(':')?;
    let (ty, value) = after.split_once('=')?;
    if !ty.contains("str") {
        return None;
    }
    leading_string_literal_value(value).map(|literal| (name, literal))
}

/// Body text of every zero-argument function in `contents`, as
/// `(name, body)` pairs, so a call like `image_repo()` can be resolved to
/// the const it falls back to.
fn zero_arg_fn_bodies(contents: &str) -> Vec<(&str, String)> {
    let bytes = contents.as_bytes();
    let mut out = Vec::new();
    let mut i = 0usize;
    while i < bytes.len() {
        if let Some(next) = skip_inert_region(bytes, i) {
            i = next.max(i + 1);
            continue;
        }
        if !bytes[i..].starts_with(b"fn")
            || (i > 0 && (is_ident_byte(bytes[i - 1]) || !bytes[i - 1].is_ascii()))
            || bytes
                .get(i + 2)
                .copied()
                .is_some_and(|byte| is_ident_byte(byte) || !byte.is_ascii())
        {
            i += 1;
            continue;
        }
        let mut j = i + 2;
        while j < bytes.len() && bytes[j].is_ascii_whitespace() {
            j += 1;
        }
        let name_start = j;
        while j < bytes.len() && is_ident_byte(bytes[j]) {
            j += 1;
        }
        let name = &contents[name_start..j];
        // Zero-arg only: `fn name()`. Anything taking parameters cannot be
        // a bare `image_repo()`-style accessor.
        if !name.is_empty()
            && bytes.get(j) == Some(&b'(')
            && bytes.get(j + 1) == Some(&b')')
            && let Some(brace) = find_plain_brace(bytes, j + 2)
            && let Some((body, end)) = extract_balanced(bytes, brace, b'{', b'}')
        {
            out.push((name, body));
            i = end;
            continue;
        }
        i = j.max(i + 1);
    }
    out
}

/// Index of the `{` opening a function body, scanning from `from` past the
/// return type. Returns `None` if a `;` (bodyless declaration) or another
/// brace-closing token is reached first.
fn find_plain_brace(bytes: &[u8], from: usize) -> Option<usize> {
    let mut i = from;
    while i < bytes.len() {
        if let Some(next) = skip_inert_region(bytes, i) {
            i = next.max(i + 1);
            continue;
        }
        match bytes[i] {
            b'{' => return Some(i),
            b';' | b'}' => return None,
            _ => i += 1,
        }
    }
    None
}

/// Every image-repository value a `GenericImage::new` first argument can
/// resolve to, using the file's `&str` const table and its zero-argument
/// accessor bodies:
///
/// - a string literal resolves to itself;
/// - a bare identifier resolves through the const table;
/// - `accessor()` resolves to every `&str` const its body names — all of them, so a body mentioning
///   more than one const cannot silently drop the chain out of scope.
///
/// An empty result means unresolvable, which the classifier treats as a
/// violation rather than a skip.
fn resolve_image_repos(arg: &str, consts: &[(&str, &str)], fns: &[(&str, String)]) -> Vec<String> {
    let arg = arg.trim();
    if let Some(literal) = leading_string_literal_value(arg) {
        return vec![literal.to_owned()];
    }
    if let Some((_, value)) = consts.iter().find(|(name, _)| *name == arg) {
        return vec![(*value).to_owned()];
    }
    let Some(callee) = arg.strip_suffix("()").map(str::trim) else {
        return Vec::new();
    };
    let Some((_, body)) = fns.iter().find(|(name, _)| *name == callee) else {
        return Vec::new();
    };
    consts
        .iter()
        .filter(|(name, _)| body_names_ident(body, name))
        .map(|(_, value)| (*value).to_owned())
        .collect()
}

/// True when `body` names `ident` in plain code (not inside a comment or
/// string literal) as a whole identifier.
fn body_names_ident(body: &str, ident: &str) -> bool {
    let bytes = body.as_bytes();
    let mut i = 0usize;
    while i < bytes.len() {
        if let Some(next) = skip_inert_region(bytes, i) {
            i = next.max(i + 1);
            continue;
        }
        if bytes[i..].starts_with(ident.as_bytes())
            && (i == 0 || !is_ident_byte(bytes[i - 1]))
            && !bytes
                .get(i + ident.len())
                .copied()
                .is_some_and(is_ident_byte)
        {
            return true;
        }
        i += 1;
    }
    false
}

/// The next `.method(…)` link of a builder chain starting at `from`,
/// as `(method name, argument text, index past the closing paren)`.
/// Whitespace and comments between links — house style in the longer e2e
/// chains — are skipped. Returns `None` at anything that is not a method
/// call, which ends the chain (`.await`, `?`, `;`).
fn next_chain_call(bytes: &[u8], from: usize) -> Option<(String, String, usize)> {
    let mut i = from;
    while i < bytes.len() {
        if bytes[i].is_ascii_whitespace() {
            i += 1;
            continue;
        }
        if bytes[i] == b'/' && matches!(bytes.get(i + 1), Some(&b'/' | &b'*')) {
            i = skip_inert_region(bytes, i)?.max(i + 1);
            continue;
        }
        break;
    }
    if bytes.get(i) != Some(&b'.') {
        return None;
    }
    let name_start = i + 1;
    let mut j = name_start;
    while j < bytes.len() && is_ident_byte(bytes[j]) {
        j += 1;
    }
    if j == name_start || bytes.get(j) != Some(&b'(') {
        return None;
    }
    let name = String::from_utf8_lossy(&bytes[name_start..j]).into_owned();
    let (args, end) = extract_balanced_parens(bytes, j)?;
    Some((name, args, end))
}

/// Find every `GenericImage::new(…)` builder chain in `contents`,
/// parenthesis-balanced so multi-line chains parse whole. Occurrences
/// inside comments and string literals are ignored, which is what keeps
/// prose mentioning `container.start()` from being read as a chain.
fn find_container_chains(contents: &str) -> Vec<ContainerChain> {
    let consts = const_str_table(contents);
    let fns = zero_arg_fn_bodies(contents);
    let bytes = contents.as_bytes();
    let mut out = Vec::new();
    let mut i = 0usize;
    while i < bytes.len() {
        if let Some(next) = skip_inert_region(bytes, i) {
            i = next.max(i + 1);
            continue;
        }
        if !bytes[i..].starts_with(CONTAINER_CTOR.as_bytes())
            || (i > 0 && is_ident_byte(bytes[i - 1]))
        {
            i += 1;
            continue;
        }
        let mut j = i + CONTAINER_CTOR.len();
        while j < bytes.len() && bytes[j].is_ascii_whitespace() {
            j += 1;
        }
        let Some((ctor_args, mut cursor)) = (bytes.get(j) == Some(&b'('))
            .then(|| extract_balanced_parens(bytes, j))
            .flatten()
        else {
            i = j.max(i + 1);
            continue;
        };
        let line = contents[..i].bytes().filter(|b| *b == b'\n').count() + 1;
        let repos = split_top_level_args(&ctor_args)
            .first()
            .map(|arg| resolve_image_repos(arg, &consts, &fns))
            .unwrap_or_default();

        let mut started = false;
        let mut caps_memory = false;
        while let Some((method, args, next)) = next_chain_call(bytes, cursor) {
            cursor = next;
            if method == "with_env_var"
                && split_top_level_args(&args)
                    .first()
                    .and_then(|arg| leading_string_literal_value(arg))
                    == Some(PULSAR_MEM_ENV)
            {
                caps_memory = true;
            }
            if method == "start" {
                started = true;
                break;
            }
        }

        out.push(ContainerChain {
            line,
            repos,
            started,
            caps_memory,
        });
        i = cursor;
    }
    out
}

/// Classify one chain: `Ok(true)` for an in-scope, capped container,
/// `Ok(false)` for a container the budget does not govern, `Err(reason)`
/// for a violation.
fn classify_container_chain(chain: &ContainerChain) -> std::result::Result<bool, &'static str> {
    if chain.repos.is_empty() {
        return Err(CONTAINER_MEM_UNRESOLVED);
    }
    if !chain
        .repos
        .iter()
        .any(|repo| repo.starts_with(PULSAR_IMAGE_PREFIX))
    {
        return Ok(false);
    }
    if !chain.started {
        return Err(CONTAINER_MEM_NOT_STARTED);
    }
    if !chain.caps_memory {
        return Err(CONTAINER_MEM_NO_ENV);
    }
    Ok(true)
}

/// Tally of one file's `GenericImage::new` chains.
#[derive(Debug, Default, PartialEq, Eq)]
struct ContainerMemoryScan {
    /// In-scope Pulsar chains that carry `PULSAR_MEM`.
    capped: usize,
    /// Chains the budget does not govern (non-Pulsar images).
    out_of_scope: usize,
    /// `(line, reason)` per violation.
    violations: Vec<(usize, &'static str)>,
}

/// Scan one file's contents for uncapped Pulsar containers.
fn scan_container_memory(contents: &str) -> ContainerMemoryScan {
    let mut scan = ContainerMemoryScan::default();
    for chain in find_container_chains(contents) {
        match classify_container_chain(&chain) {
            Ok(true) => scan.capped += 1,
            Ok(false) => scan.out_of_scope += 1,
            Err(reason) => scan.violations.push((chain.line, reason)),
        }
    }
    scan
}

fn check_e2e_container_memory() -> Result<()> {
    let workspace_root = workspace_root()?;
    let tests_dir = workspace_root.join(E2E_TESTS_DIR);
    if !tests_dir.is_dir() {
        bail!("e2e test directory not found: {}", tests_dir.display());
    }

    let mut offenders: Vec<String> = Vec::new();
    let mut capped = 0usize;
    let mut out_of_scope = 0usize;
    visit(&tests_dir, &mut |path, contents| {
        if path.extension().is_none_or(|ext| ext != "rs") {
            return;
        }
        let rel = path.strip_prefix(&workspace_root).unwrap_or(path);
        let rel = rel.to_string_lossy().replace('\\', "/");
        let scan = scan_container_memory(contents);
        capped += scan.capped;
        out_of_scope += scan.out_of_scope;
        for (line, reason) in scan.violations {
            offenders.push(format!("{rel}:{line}: {reason}"));
        }
    })?;

    if !offenders.is_empty() {
        offenders.sort();
        for line in &offenders {
            eprintln!("uncapped e2e container — {line}");
        }
        bail!(
            "e2e-container-memory check failed: {} offender(s). Every `pulsar standalone` \
             container the e2e suite starts must chain \
             `.with_env_var(\"PULSAR_MEM\", PULSAR_MEM_LIMIT)` before `.start()` — see \
             docs/testing.md § \"e2e container memory budget\".",
            offenders.len()
        );
    }
    // Report the non-Pulsar chains too: a silent drop in the capped count
    // is the regression this gate exists to make visible.
    eprintln!(
        "xtask check-e2e-container-memory: {capped} Pulsar container chain(s) carry PULSAR_MEM \
         ({out_of_scope} non-Pulsar chain(s) out of scope)."
    );
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

/// One isolated coverage evidence domain.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct SimCoverageDomain {
    name: &'static str,
    execution_packages: &'static [&'static str],
    report_packages: &'static [&'static str],
    gated_prefixes: &'static [&'static str],
    lcov_file: &'static str,
}

#[derive(Debug)]
struct DomainEvidence {
    domain: &'static SimCoverageDomain,
    lcov: Vec<u8>,
    elapsed: Duration,
}

const MOONPOOL_COVERAGE_REPORT_PACKAGES: &[&str] = &[
    "magnetar-proto",
    "magnetar-runtime-moonpool",
    "magnetar-differential",
    "magnetar-auth-athenz",
    "magnetar-auth-sasl",
    "magnetar-driver",
    "magnetar-fakes",
];

const MOONPOOL_COVERAGE_GATED_PREFIXES: &[&str] = &[
    "crates/magnetar-proto/src/",
    "crates/magnetar-runtime-moonpool/src/",
    "crates/magnetar-differential/src/",
    "crates/magnetar-auth-sasl/src/",
    "crates/magnetar-auth-athenz/src/",
    "crates/magnetar/src/",
    "crates/magnetar-fakes/src/",
];

const TOKIO_COVERAGE_REPORT_PACKAGES: &[&str] = &["magnetar-runtime-tokio"];
const TOKIO_COVERAGE_GATED_PREFIXES: &[&str] = &["crates/magnetar-runtime-tokio/src/"];

const SIM_COVERAGE_DOMAINS: &[SimCoverageDomain] = &[
    SimCoverageDomain {
        name: "moonpool",
        execution_packages: &["magnetar-runtime-moonpool", "magnetar-differential"],
        report_packages: MOONPOOL_COVERAGE_REPORT_PACKAGES,
        gated_prefixes: MOONPOOL_COVERAGE_GATED_PREFIXES,
        lcov_file: "sim-coverage.lcov",
    },
    SimCoverageDomain {
        name: "tokio",
        execution_packages: &["magnetar-runtime-tokio", "magnetar-differential"],
        report_packages: TOKIO_COVERAGE_REPORT_PACKAGES,
        gated_prefixes: TOKIO_COVERAGE_GATED_PREFIXES,
        lcov_file: "tokio-coverage.lcov",
    },
];

/// Source trees whose record-less executable files are hard failures rather
/// than advisory `not gated` lines.
///
/// Every owned crate is compiled and linked by its evidence domain, so
/// [`run_sim_lcov`]'s domain report must emit `SF:` records for it. A prefix
/// that contributes *no* record at all means one of two things, and `llvm-cov`
/// reports neither: nothing linked the crate into that domain's binaries, or
/// the report silently produced nothing. Both would make
/// [`intersect_diff_with_coverage`] read every added line in that crate as
/// "not executable" and pass — the exact fail-open hole ADR-0088 documented.
/// Failing loudly is the only honest answer.
///
/// The check distinguishes executable and non-executable files inside those
/// crates. LLVM builds its coverage mapping from per-function `covfun` records,
/// so a source file holding only `pub mod` / `pub use` / `pub const` /
/// attributes or bodyless declarations emits no `SF:` record even when its
/// crate is fully instrumented. `crates/magnetar-proto/src/lib.rs`,
/// `crates/magnetar-proto/src/trackers/mod.rs`,
/// `crates/magnetar-differential/src/lib.rs`,
/// `crates/magnetar-runtime-moonpool/src/crypto.rs` and
/// `crates/magnetar-runtime-tokio/src/crypto.rs` are all in that shape today.
/// They stay advisory because no test could create a mapping for them. A file
/// with a non-test function body must emit its own `SF:` record; sibling records
/// cannot make it measurable, so [`classify_uninstrumented_for`] fails it.
///
/// Domain ownership is declared by [`SIM_COVERAGE_DOMAINS`]: the Moonpool
/// execution reports its seven shared/simulation packages, while the separate
/// Tokio execution reports only `magnetar-runtime-tokio`. Every other workspace
/// member remains advisory `not gated` scope.
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

/// The 1-indexed lines of `contents` that sit inside a `#[cfg(test)]` span,
/// and so are unit-test code rather than production code for sim-coverage
/// purposes.
///
/// Thin adapter over the shared [`cfg_test_line_flags`], existing so that
/// `check_sim_coverage`'s stripping is a named, directly testable step rather
/// than an inline expression — `sim_coverage_cfg_test_import_does_not_exempt_the_rest_of_the_file`
/// asserts on exactly what the gate applies, and `dead_code` under
/// `-D warnings` catches the call site being replaced by another ad-hoc scan.
/// It was an ad-hoc scan until ADR-0092, and it exempted 71% of the gated
/// lines added over ten merged pull requests.
fn sim_coverage_cfg_test_lines(contents: &str) -> std::collections::BTreeSet<u32> {
    cfg_test_line_flags(contents)
        .into_iter()
        .enumerate()
        .filter(|(_, gated)| *gated)
        .map(|(idx, _)| (idx as u32).saturating_add(1))
        .collect()
}

/// Parse a unified-diff blob produced by `git diff --unified=0` and return
/// the set of added new-side line numbers per workspace-relative file path.
///
/// Only `+` lines (excluding `+++` file headers) are considered additions.
/// Hunk headers `@@ -... +start,count @@` reset the new-side cursor.
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

/// Normalize a path into the key both sides of the coverage intersection are
/// compared on.
///
/// LCOV `SF:` paths come from `llvm-cov`, the diff side is
/// `workspace_root.join(relpath)`; nothing guarantees the two spell the same
/// file the same way. A symlinked checkout (`/home/x/work` →
/// `/mnt/nvme/work`) is enough to make every comparison miss, and a miss does
/// not fail loudly — it degrades the file to "no LCOV record", i.e. the whole
/// gate silently passes. Canonicalizing both sides collapses symlinks, `..`
/// segments and `.` segments so the keys agree.
///
/// Falls back to the un-normalized path when canonicalization fails: the unit
/// tests key off a nonexistent `/ws` root, and a real run must not start
/// erroring because one tracked file was deleted after the diff was taken.
fn coverage_key(path: &Path) -> String {
    fs::canonicalize(path)
        .unwrap_or_else(|_| path.to_path_buf())
        .to_string_lossy()
        .into_owned()
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
///
/// Keys go through [`coverage_key`] so they compare equal to the diff side.
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
            // Normalized so a symlinked checkout still matches the diff side.
            current_file = Some(coverage_key(Path::new(path)));
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

/// Optimization level the coverage build runs at, overriding the workspace's
/// `[profile.test] opt-level = 1` for the duration of the measurement.
///
/// At `opt-level >= 1` rustc enables MIR inlining, and an inlined callee's
/// coverage counter never fires: the call site is attributed, the callee reads
/// zero. The gate then calls a line uncovered that a passing test provably
/// executes. Measured 2026-08-04 on `magnetar-proto/src/scalable_consumer.rs`,
/// whose `consumer_type()` is called twice from the synchronous
/// `scalable_consumer_session_and_watch_accessors` test in
/// `magnetar-differential`:
///
/// | `[profile.test] opt-level` | `DA:271` | verdict |
/// | --- | --- | --- |
/// | `1` (workspace default) | `0` | uncovered, gate fails |
/// | `0` (this constant) | `2` — exactly the two call sites | gate passes |
///
/// The failure is not stable either, because whether a given function is
/// inlined depends on codegen-unit partitioning: the same tree measured warm,
/// cold, and on CI produced three different reports (63, 70 and 81 `SF:`
/// records) and three different uncovered sets. CI blamed five signature lines
/// of the `async fn` `magnetar-runtime-tokio::Client::scalable_topic_subscribe`
/// — the outer future constructor, inlined away, while the coroutine body it
/// builds cannot be inlined and reported hits throughout. That is the same
/// mechanism seen from the other side.
///
/// It cuts both ways and the fail-open direction is the dangerous one: a line
/// genuinely never executed can be credited because a neighbour it was folded
/// into was. Coverage must therefore be measured against the source structure
/// it reports on, which means unoptimized.
///
/// This is a `[profile.test]` override rather than a `RUSTFLAGS` entry because
/// `cargo-llvm-cov` owns `RUSTFLAGS` — it appends `-C instrument-coverage`
/// there — and a second writer would clobber it.
const SIM_COVERAGE_OPT_LEVEL: &str = "0";

/// Greppable tag stamped on every line `check-sim-coverage` emits when it was
/// handed `--reuse-lcov`.
///
/// Without it a reused-report run is textually identical to a measured one —
/// same success sentence, same exit code — so pasting the transcript as proof
/// of ADR-0024 patch coverage cannot be distinguished from the real thing. The
/// flag stays out of CI and out of the `CLAUDE.md` validation chain, so the
/// exposure is exactly that: a transcript that overstates what ran.
const SIM_COVERAGE_REUSE_MARKER: &str = "[REUSED LCOV — NOT A FRESH MEASUREMENT]";

/// cargo-llvm-cov appends these values directly to `llvm-cov` or
/// `llvm-profdata`, so any one can select an artifact outside the isolated
/// current-pass root.
const SIM_COVERAGE_ARTIFACT_FLAG_ENV: &[&str] = &[
    "LLVM_COV_FLAGS",
    "LLVM_PROFDATA_FLAGS",
    "CARGO_LLVM_COV_FLAGS",
    "CARGO_LLVM_PROFDATA_FLAGS",
];

fn validate_sim_coverage_flag_environment(
    mut value_of: impl FnMut(&str) -> Option<OsString>,
) -> Result<()> {
    let non_empty: Vec<&str> = SIM_COVERAGE_ARTIFACT_FLAG_ENV
        .iter()
        .copied()
        .filter(|name| value_of(name).is_some_and(|value| !value.is_empty()))
        .collect();
    if non_empty.is_empty() {
        return Ok(());
    }
    bail!(
        "sim-coverage refuses non-empty artifact-injection environment variable(s): {}. These \
         values are appended directly to llvm-cov/llvm-profdata and can select cached objects or \
         profiles outside the invocation-owned target; unset them before retrying.",
        non_empty.join(", ")
    );
}

fn clear_sim_coverage_artifact_flags(command: &mut StdCommand) {
    for name in SIM_COVERAGE_ARTIFACT_FLAG_ENV {
        command.env_remove(name);
    }
}

fn parse_json_hex_quad(bytes: &[u8], start: usize) -> Option<u16> {
    bytes
        .get(start..start.saturating_add(4))?
        .iter()
        .try_fold(0_u16, |value, byte| {
            let digit = match byte {
                b'0'..=b'9' => u16::from(byte - b'0'),
                b'a'..=b'f' => u16::from(byte - b'a') + 10,
                b'A'..=b'F' => u16::from(byte - b'A') + 10,
                _ => return None,
            };
            Some((value << 4) | digit)
        })
}

/// Parse one JSON string and return its decoded value plus the first byte after
/// the closing quote. This keeps Cargo metadata parsing dependency-free while
/// still handling Windows separators and Unicode paths correctly.
fn parse_json_string(json: &str, start: usize) -> Result<(String, usize)> {
    let bytes = json.as_bytes();
    if bytes.get(start) != Some(&b'"') {
        bail!("expected JSON string");
    }

    let mut out = String::new();
    let mut segment = start.saturating_add(1);
    let mut cursor = segment;
    while let Some(&byte) = bytes.get(cursor) {
        match byte {
            b'"' => {
                out.push_str(&json[segment..cursor]);
                return Ok((out, cursor.saturating_add(1)));
            }
            b'\\' => {
                out.push_str(&json[segment..cursor]);
                cursor = cursor.saturating_add(1);
                let escape = *bytes
                    .get(cursor)
                    .ok_or_else(|| anyhow!("unterminated JSON escape"))?;
                match escape {
                    b'"' => out.push('"'),
                    b'\\' => out.push('\\'),
                    b'/' => out.push('/'),
                    b'b' => out.push('\u{0008}'),
                    b'f' => out.push('\u{000c}'),
                    b'n' => out.push('\n'),
                    b'r' => out.push('\r'),
                    b't' => out.push('\t'),
                    b'u' => {
                        let first = parse_json_hex_quad(bytes, cursor.saturating_add(1))
                            .ok_or_else(|| anyhow!("invalid JSON Unicode escape"))?;
                        cursor = cursor.saturating_add(5);
                        let scalar = if (0xd800..=0xdbff).contains(&first) {
                            if bytes.get(cursor..cursor.saturating_add(2)) != Some(b"\\u") {
                                bail!("JSON high surrogate has no low surrogate");
                            }
                            let second = parse_json_hex_quad(bytes, cursor.saturating_add(2))
                                .ok_or_else(|| anyhow!("invalid JSON low surrogate"))?;
                            if !(0xdc00..=0xdfff).contains(&second) {
                                bail!("invalid JSON low surrogate");
                            }
                            cursor = cursor.saturating_add(6);
                            0x1_0000
                                + ((u32::from(first) - 0xd800) << 10)
                                + (u32::from(second) - 0xdc00)
                        } else {
                            if (0xdc00..=0xdfff).contains(&first) {
                                bail!("JSON low surrogate has no high surrogate");
                            }
                            u32::from(first)
                        };
                        out.push(
                            char::from_u32(scalar)
                                .ok_or_else(|| anyhow!("invalid JSON Unicode scalar"))?,
                        );
                        segment = cursor;
                        continue;
                    }
                    _ => bail!("invalid JSON escape"),
                }
                cursor = cursor.saturating_add(1);
                segment = cursor;
            }
            0x00..=0x1f => bail!("unescaped control byte in JSON string"),
            _ => cursor = cursor.saturating_add(1),
        }
    }
    bail!("unterminated JSON string");
}

fn top_level_json_string(json: &str, field: &str) -> Result<Option<String>> {
    let bytes = json.as_bytes();
    let mut object_depth = 0_usize;
    let mut cursor = 0_usize;
    while let Some(&byte) = bytes.get(cursor) {
        match byte {
            b'{' => {
                object_depth = object_depth.saturating_add(1);
                cursor = cursor.saturating_add(1);
            }
            b'}' => {
                object_depth = object_depth.saturating_sub(1);
                cursor = cursor.saturating_add(1);
            }
            b'"' => {
                let (name, after_name) = parse_json_string(json, cursor)?;
                cursor = after_name;
                if object_depth != 1 || name != field {
                    continue;
                }
                while bytes.get(cursor).is_some_and(u8::is_ascii_whitespace) {
                    cursor = cursor.saturating_add(1);
                }
                if bytes.get(cursor) != Some(&b':') {
                    continue;
                }
                cursor = cursor.saturating_add(1);
                while bytes.get(cursor).is_some_and(u8::is_ascii_whitespace) {
                    cursor = cursor.saturating_add(1);
                }
                if bytes.get(cursor) != Some(&b'"') {
                    bail!("Cargo metadata field `{field}` is not a string");
                }
                return parse_json_string(json, cursor).map(|(value, _)| Some(value));
            }
            _ => cursor = cursor.saturating_add(1),
        }
    }
    Ok(None)
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct CargoStorage {
    target_directory: PathBuf,
    build_directory: PathBuf,
    build_directory_supported: bool,
}

fn apply_command_environment(command: &mut StdCommand, values: &[(&str, Option<&OsStr>)]) {
    for (name, value) in values {
        if let Some(value) = value {
            command.env(name, value);
        } else {
            command.env_remove(name);
        }
    }
}

/// Resolve every existing component, including a final-component symlink,
/// without creating a configured target/build path that does not exist yet.
fn resolve_storage_path(path: &Path) -> Result<PathBuf> {
    let mut ancestor = path.to_path_buf();
    let mut missing = Vec::<OsString>::new();
    loop {
        match fs::symlink_metadata(&ancestor) {
            Ok(_) => break,
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => {
                let component = ancestor.file_name().ok_or_else(|| {
                    anyhow!(
                        "Cargo storage path {} has no existing ancestor",
                        path.display()
                    )
                })?;
                missing.push(component.to_os_string());
                if !ancestor.pop() {
                    bail!(
                        "Cargo storage path {} has no existing ancestor",
                        path.display()
                    );
                }
            }
            Err(err) => {
                return Err(err).with_context(|| {
                    format!("inspecting Cargo storage path {}", ancestor.display())
                });
            }
        }
    }

    let mut resolved = fs::canonicalize(&ancestor)
        .with_context(|| format!("resolving Cargo storage ancestor {}", ancestor.display()))?;
    for component in missing.into_iter().rev() {
        resolved.push(component);
    }
    Ok(resolved)
}

fn resolve_cargo_storage(
    cargo: &OsStr,
    workspace_root: &Path,
    command_environment: &[(&str, Option<&OsStr>)],
) -> Result<CargoStorage> {
    let manifest_path = workspace_root.join("Cargo.toml");
    let mut command = StdCommand::new(cargo);
    command
        .current_dir(workspace_root)
        .args([
            "metadata",
            "--format-version=1",
            "--locked",
            "--manifest-path",
        ])
        .arg(&manifest_path);
    apply_command_environment(&mut command, command_environment);
    clear_sim_coverage_artifact_flags(&mut command);
    let output = command
        .output()
        .context("failed to invoke `cargo metadata`")?;
    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        bail!(
            "`cargo metadata` exited with status {}:\n{stderr}",
            output.status
        );
    }
    let metadata = String::from_utf8(output.stdout).context("cargo metadata produced non-UTF-8")?;
    let target_directory = top_level_json_string(&metadata, "target_directory")?
        .map(PathBuf::from)
        .ok_or_else(|| anyhow!("cargo metadata omitted `target_directory`"))?;
    let build_directory = top_level_json_string(&metadata, "build_directory")?.map(PathBuf::from);
    if !target_directory.is_absolute()
        || build_directory
            .as_deref()
            .is_some_and(|path| !path.is_absolute())
    {
        bail!("cargo metadata returned a non-absolute target/build directory");
    }
    let target_directory = resolve_storage_path(&target_directory)?;
    let build_directory_supported = build_directory.is_some();
    let build_directory = build_directory
        .map(|path| resolve_storage_path(&path))
        .transpose()?
        .unwrap_or_else(|| target_directory.clone());
    Ok(CargoStorage {
        target_directory,
        build_directory,
        build_directory_supported,
    })
}

#[cfg(unix)]
fn ensure_scratch_parent_filesystem(anchor: &Path, parent: &Path) -> Result<()> {
    use std::os::unix::fs::MetadataExt as _;

    if anchor.exists() {
        let anchor_device = fs::metadata(anchor)
            .with_context(|| format!("reading Cargo storage metadata for {}", anchor.display()))?
            .dev();
        let parent_device = fs::metadata(parent)
            .with_context(|| format!("reading scratch-parent metadata for {}", parent.display()))?
            .dev();
        if anchor_device != parent_device {
            bail!(
                "Cargo storage {} is a filesystem root; no outside-cache sibling exists on its build volume",
                anchor.display()
            );
        }
    }
    Ok(())
}

#[cfg(not(unix))]
fn ensure_scratch_parent_filesystem(_anchor: &Path, _parent: &Path) -> Result<()> {
    Ok(())
}

fn create_sim_coverage_target(storage: &CargoStorage) -> Result<tempfile::TempDir> {
    // If one cache root contains the other, use the outer root as the anchor so
    // the scratch sibling is outside both trees. Otherwise prefer Cargo's build
    // storage, where the cold all-feature object set belongs.
    let anchor = if storage
        .build_directory
        .starts_with(&storage.target_directory)
    {
        &storage.target_directory
    } else {
        &storage.build_directory
    };
    let parent = anchor.parent().ok_or_else(|| {
        anyhow!(
            "Cargo target/build storage {} has no parent for an isolated sibling",
            anchor.display()
        )
    })?;
    fs::create_dir_all(parent)
        .with_context(|| format!("creating Cargo build-storage parent {}", parent.display()))?;
    ensure_scratch_parent_filesystem(anchor, parent)?;
    let parent = fs::canonicalize(parent)
        .with_context(|| format!("resolving Cargo build-storage parent {}", parent.display()))?;
    let target = tempfile::Builder::new()
        .prefix(".sim-coverage-target-")
        .tempdir_in(&parent)
        .with_context(|| {
            format!(
                "creating isolated sim-coverage target beside Cargo storage {}",
                anchor.display()
            )
        })?;
    let path = target.path().to_path_buf();
    if path.starts_with(&storage.target_directory) || path.starts_with(&storage.build_directory) {
        let primary = Err(anyhow!(
            "isolated sim-coverage target {} landed inside cached Cargo storage",
            path.display()
        ));
        return finish_sim_coverage_run(primary, target.close(), &path);
    }
    Ok(target)
}

fn create_sim_coverage_domain_targets(
    root: &Path,
) -> Result<Vec<(&'static SimCoverageDomain, PathBuf)>> {
    SIM_COVERAGE_DOMAINS
        .iter()
        .map(|domain| {
            let target = root.join(domain.name);
            fs::create_dir(&target).with_context(|| {
                format!(
                    "creating isolated {} coverage target {}",
                    domain.name,
                    target.display()
                )
            })?;
            Ok((domain, target))
        })
        .collect()
}

/// Produce and retain the two isolated coverage reports.
///
/// Each domain independently executes tests and exports only its owned packages:
///
/// 1. **Moonpool/shared** executes `magnetar-runtime-moonpool` and `magnetar-differential`, then
///    reports `magnetar-proto`, `magnetar-runtime-moonpool`, `magnetar-differential`,
///    `magnetar-auth-athenz`, `magnetar-auth-sasl`, `magnetar-driver`, and `magnetar-fakes`.
/// 2. **Tokio adapter** executes `magnetar-runtime-tokio` and `magnetar-differential`, then reports
///    only `magnetar-runtime-tokio`.
///
/// Both domains run at [`SIM_COVERAGE_OPT_LEVEL`], which overrides the
/// workspace's `[profile.test] opt-level = 1`. Above zero the MIR inliner
/// silences an inlined callee's counter, and the gate reports lines a passing
/// test provably executed — non-deterministically, since inlining follows
/// codegen-unit partitioning. See that constant for the measurement.
///
/// `--no-report` implies `--no-clean`, so locked Cargo metadata first resolves the effective
/// target/build storage and an empty scratch sibling is created on that filesystem. The preflight
/// resolves dependencies because Cargo's `--no-deps` metadata mode bypasses lock validation even
/// when paired with `--locked`. One scratch root contains separate `moonpool/`
/// and `tokio/` targets. Each domain's execute and report phases point
/// `CARGO_TARGET_DIR`, `CARGO_LLVM_COV_TARGET_DIR`, `CARGO_LLVM_COV_BUILD_DIR`, and supported
/// `CARGO_BUILD_BUILD_DIR` at that domain's absolute target. Overriding Cargo's own metadata target
/// is required because cargo-llvm-cov 0.8.7 otherwise scans `ui_test` and trybuild objects under
/// the original cached target. Objects, profiles, profdata, and reports never cross domains.
///
/// Non-empty LLVM coverage/profdata flag variables are rejected before metadata or coverage runs:
/// cargo-llvm-cov appends them directly to its tool commands, where they can name arbitrary cached
/// inputs. The children also remove all four variables so empty inherited values cannot drift.
///
/// Each report is read and validated inside scratch, then retained as
/// authoritative in-memory bytes through cleanup. After both domain evidence
/// values are retained, diagnostics are atomically published as
/// `target/sim-coverage.lcov` and `target/tokio-coverage.lcov`; the returned
/// in-memory bytes then drive the aggregate verdict. Diagnostics are outputs,
/// never authoritative inputs, and are not reread by a fresh run.
///
/// An owned crate that emits no record at all is a hard failure
/// ([`report_missing_gated`]). A record-less file in a
/// gated crate also fails when it contains a non-test function body: sibling
/// records prove the crate reached the report, but do not make that executable
/// file measurable. A genuinely non-executable module/export/constant or
/// bodyless-declaration file remains advisory rather than silently passed
/// (ADR-0088, ADR-0102).
///
/// With `reuse_lcov`, both commands are skipped and both diagnostics are loaded
/// once for sizing and debugging only. Reused diagnostics are explicitly
/// non-authoritative and tagged with [`SIM_COVERAGE_REUSE_MARKER`].
fn run_sim_lcov(workspace_root: &Path, reuse_lcov: bool) -> Result<Vec<DomainEvidence>> {
    let cargo = env::var_os("CARGO").unwrap_or_else(|| "cargo".into());
    run_sim_lcov_with_cargo(workspace_root, reuse_lcov, &cargo)
}

/// Report cleanup failure without replacing an earlier coverage failure.
fn finish_sim_coverage_run<T>(
    result: Result<T>,
    cleanup: std::io::Result<()>,
    coverage_target: &Path,
) -> Result<T> {
    match (result, cleanup) {
        (Ok(value), Ok(())) => Ok(value),
        (Ok(_), Err(cleanup)) => Err(cleanup).with_context(|| {
            format!(
                "removing isolated sim-coverage target {}",
                coverage_target.display()
            )
        }),
        (Err(primary), Ok(())) => Err(primary),
        (Err(primary), Err(cleanup)) => Err(primary.context(format!(
            "sim-coverage run failed; additionally failed to remove isolated target {}: \
             {cleanup}",
            coverage_target.display()
        ))),
    }
}

fn run_sim_lcov_with_cargo(
    workspace_root: &Path,
    reuse_lcov: bool,
    cargo: &OsStr,
) -> Result<Vec<DomainEvidence>> {
    run_sim_lcov_with_cargo_environment(workspace_root, reuse_lcov, cargo, &[])
}

#[expect(
    clippy::too_many_lines,
    reason = "artifact provenance phases stay visibly ordered"
)]
fn run_sim_lcov_with_cargo_environment(
    workspace_root: &Path,
    reuse_lcov: bool,
    cargo: &OsStr,
    command_environment: &[(&str, Option<&OsStr>)],
) -> Result<Vec<DomainEvidence>> {
    let workspace_root = fs::canonicalize(workspace_root)
        .with_context(|| format!("resolving workspace root {}", workspace_root.display()))?;
    let diagnostics = workspace_root.join("target");
    if reuse_lcov {
        return SIM_COVERAGE_DOMAINS
            .iter()
            .map(|domain| {
                let path = diagnostics.join(domain.lcov_file);
                let lcov = fs::read(&path).with_context(|| {
                    format!("--reuse-lcov requires readable diagnostic {}", path.display())
                })?;
                validate_domain_lcov(domain, &lcov)?;
                eprintln!(
                    "{SIM_COVERAGE_REUSE_MARKER} {}: NO coverage run happened; stale line numbers are non-authoritative.",
                    path.display()
                );
                Ok(DomainEvidence {
                    domain,
                    lcov,
                    elapsed: Duration::ZERO,
                })
            })
            .collect();
    }

    validate_sim_coverage_flag_environment(|name| env::var_os(name))?;
    let storage = resolve_cargo_storage(cargo, &workspace_root, command_environment)?;

    fs::create_dir_all(&diagnostics)
        .with_context(|| format!("creating {}", diagnostics.display()))?;

    // Keep the cold all-feature build on Cargo's configured storage filesystem,
    // but in a fresh sibling the cache never restores or saves.
    let coverage_target = create_sim_coverage_target(&storage)?;
    let coverage_target_path = coverage_target.path().to_path_buf();

    let result = (|| -> Result<Vec<DomainEvidence>> {
        let domain_targets = create_sim_coverage_domain_targets(&coverage_target_path)?;
        let mut evidence = Vec::with_capacity(domain_targets.len());
        for (domain, domain_target) in domain_targets {
            let domain_started = std::time::Instant::now();
            let domain_lcov_path = domain_target.join("report.lcov");
            let apply_coverage_target = |command: &mut StdCommand| {
                command
                    .env("CARGO_LLVM_COV_TARGET_DIR", &domain_target)
                    .env("CARGO_LLVM_COV_BUILD_DIR", &domain_target)
                    .env("CARGO_TARGET_DIR", &domain_target)
                    .env("CARGO_PROFILE_TEST_OPT_LEVEL", SIM_COVERAGE_OPT_LEVEL);
                if storage.build_directory_supported {
                    command.env("CARGO_BUILD_BUILD_DIR", &domain_target);
                } else {
                    command.env_remove("CARGO_BUILD_BUILD_DIR");
                }
                clear_sim_coverage_artifact_flags(command);
            };

            // Step 1 — execution. `-p` is a cargo flag, not a test-runner flag. Putting
            // it after `--` routes it to libtest, which rejects it ("Unrecognized
            // option: 'p'") and aborts the whole coverage run. Here it picks the test
            // binaries to run (and `--workspace`, mutually exclusive with it, would drag
            // in every workspace test target, including the façade's Docker-bound
            // e2e suite). The differential dev-dependency still compiles the façade
            // library without running those targets. `-p` does NOT restrict
            // instrumentation: cargo-llvm-cov instruments every workspace member
            // regardless, which is exactly why step 2 can widen the report for free.
            let mut exec = StdCommand::new(cargo);
            exec.current_dir(&workspace_root)
                .args(["llvm-cov", "--no-report"]);
            for package in domain.execution_packages {
                exec.args(["-p", package]);
            }
            exec.args(["--all-features", "--locked", "--quiet"]);
            // `--all-features` reaches `crypto-fips`, so this build compiles
            // `aws-lc-fips-sys`. On Linux that needs clang or it dies in `delocate`
            // — see [`force_clang_toolchain`]. Without this the gate is unrunnable on
            // a bare Linux checkout, which is how it shipped until 2026-07-31.
            force_clang_toolchain(&mut exec);
            // Measure at `opt-level = 0`, overriding the workspace's
            // `[profile.test] opt-level = 1`. See [`SIM_COVERAGE_OPT_LEVEL`].
            apply_coverage_target(&mut exec);
            let status = exec.status().context("failed to invoke `cargo llvm-cov`")?;
            if !status.success() {
                bail!(
                    "{} coverage execution exited with status {status}",
                    domain.name
                );
            }
            if domain_lcov_path.exists() {
                bail!(
                    "{} coverage execution left unexpected pre-report output {}",
                    domain.name,
                    domain_lcov_path.display()
                );
            }

            // Step 2 — export this domain's profdata + object files over its owned
            // package set. `cargo llvm-cov report` accepts `-p` only; `--workspace`,
            // `--exclude-from-report` and `--exclude-from-test` are all rejected on it.
            let mut report = StdCommand::new(cargo);
            report
                .current_dir(&workspace_root)
                .args(["llvm-cov", "report", "--lcov", "--output-path"])
                .arg(&domain_lcov_path);
            for package in domain.report_packages {
                report.args(["-p", package]);
            }
            // Generated proto is excluded from the gate diff-side already
            // (`SIM_COVERAGE_EXCLUDE_PREFIXES`); dropping it here too keeps the report
            // from carrying tens of thousands of prost-generated lines.
            report.args(["--ignore-filename-regex", "crates/magnetar-proto/src/pb/"]);
            // The same target and profile override make this report consume only
            // the profiles and objects the execution phase just produced.
            apply_coverage_target(&mut report);
            let status = report
                .status()
                .context("failed to invoke `cargo llvm-cov report`")?;
            if !status.success() {
                bail!(
                    "{} coverage report exited with status {status}",
                    domain.name
                );
            }
            if !fs::symlink_metadata(&domain_lcov_path)
                .is_ok_and(|metadata| metadata.file_type().is_file())
            {
                bail!(
                    "{} coverage report did not replace scratch output {} with a regular file",
                    domain.name,
                    domain_lcov_path.display()
                );
            }
            let lcov = fs::read(&domain_lcov_path)
                .with_context(|| format!("reading {}", domain_lcov_path.display()))?;
            validate_domain_lcov(domain, &lcov)?;
            let elapsed = domain_started.elapsed();
            eprintln!(
                "xtask check-sim-coverage: {} domain fresh execution + report completed in {:.3}s",
                domain.name,
                elapsed.as_secs_f64()
            );
            evidence.push(DomainEvidence {
                domain,
                lcov,
                elapsed,
            });
        }
        Ok(evidence)
    })();

    let cleanup = coverage_target.close();
    let evidence = finish_sim_coverage_run(result, cleanup, &coverage_target_path)?;
    for item in &evidence {
        publish_coverage_diagnostic(&diagnostics.join(item.domain.lcov_file), &item.lcov)?;
    }
    Ok(evidence)
}

fn validate_domain_lcov(domain: &SimCoverageDomain, lcov: &[u8]) -> Result<()> {
    let lcov = std::str::from_utf8(lcov)
        .with_context(|| format!("{} coverage report is not UTF-8", domain.name))?;
    for foreign in SIM_COVERAGE_DOMAINS
        .iter()
        .filter(|other| other.name != domain.name)
        .flat_map(|other| other.gated_prefixes.iter())
    {
        if lcov
            .lines()
            .filter_map(|line| line.strip_prefix("SF:"))
            .any(|path| path.contains(foreign))
        {
            bail!(
                "{} coverage report contains foreign-domain source prefix {foreign}",
                domain.name
            );
        }
    }
    Ok(())
}

fn publish_coverage_diagnostic(path: &Path, contents: &[u8]) -> Result<()> {
    use std::io::Write as _;

    let parent = path
        .parent()
        .ok_or_else(|| anyhow!("coverage diagnostic {} has no parent", path.display()))?;
    fs::create_dir_all(parent).with_context(|| format!("creating {}", parent.display()))?;
    let mut staged = tempfile::NamedTempFile::new_in(parent)
        .with_context(|| format!("staging coverage diagnostic beside {}", path.display()))?;
    staged
        .write_all(contents)
        .with_context(|| format!("writing staged coverage diagnostic for {}", path.display()))?;
    staged
        .as_file()
        .sync_all()
        .with_context(|| format!("syncing staged coverage diagnostic for {}", path.display()))?;
    staged
        .persist(path)
        .map_err(|err| err.error)
        .with_context(|| {
            format!(
                "atomically publishing coverage diagnostic {}",
                path.display()
            )
        })?;
    Ok(())
}

/// Intersect the per-file added-line sets from the diff with the executable
/// + executed line sets from LCOV.
///
/// An added line is reported as uncovered only when LCOV considers it
/// executable (an `DA:` entry exists for it) AND the owning evidence domain did
/// not hit it. Non-executable additions (use statements, doc comments, blank
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
        let abs_key = coverage_key(&workspace_root.join(relpath));
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

fn domain_tracked(
    domain: &SimCoverageDomain,
    tracked: &[(String, std::collections::BTreeSet<u32>)],
) -> Vec<(String, std::collections::BTreeSet<u32>)> {
    tracked
        .iter()
        .filter(|(path, _)| {
            domain
                .gated_prefixes
                .iter()
                .any(|prefix| path.starts_with(prefix))
        })
        .cloned()
        .collect()
}

/// Partition off the tracked files that LCOV never mentions at all.
///
/// [`run_sim_lcov`]'s domains report their owned packages; a file outside them
/// — in `magnetar-admin`, `magnetarctl` or `xtask`, for example —
/// emits no `DA:` records at all, so every added line in it reads as "not
/// executable" to [`intersect_diff_with_coverage`] and passes.
///
/// That is a scope limit on ADR-0024's patch-coverage gate, not a pass.
/// Returning it separately lets the caller say so out loud instead of folding
/// those files into a "100% covered" summary they were never measured against.
///
/// [`classify_uninstrumented_for`] splits this result again: a file whose whole
/// gated crate is missing from the report is not a scope limit but a broken
/// run, and goes to [`report_missing_gated`] to fail the check.
///
/// Returns `(relpath, added_line_count)` per uninstrumented file, sorted by path.
fn uninstrumented_files(
    workspace_root: &Path,
    tracked: &[(String, std::collections::BTreeSet<u32>)],
    covered: &std::collections::HashMap<
        String,
        (
            std::collections::BTreeSet<u32>,
            std::collections::BTreeSet<u32>,
        ),
    >,
) -> Vec<(String, usize)> {
    let mut ungated: Vec<(String, usize)> = tracked
        .iter()
        .filter(|(relpath, _)| !covered.contains_key(&coverage_key(&workspace_root.join(relpath))))
        .map(|(relpath, lines)| (relpath.clone(), lines.len()))
        .collect();
    ungated.sort_by(|a, b| a.0.cmp(&b.0));
    ungated
}

/// The supplied domain prefixes that contributed no `SF:` record whatsoever to
/// `covered`.
///
/// This is the crate-wide "the run is broken" signal. An owned crate is compiled
/// and linked by its evidence domain, so its object files carry a coverage
/// mapping and `llvm-cov export` must emit records for it. Zero records for the
/// entire crate means nothing linked it or the re-export produced nothing, and
/// `llvm-cov` reports neither. [`classify_uninstrumented_for`] separately catches a
/// per-file miss when that file contains a non-test function body.
///
/// `covered` is keyed on canonicalized absolute paths ([`coverage_key`]) while
/// the prefixes are workspace-relative, so the match is a substring test rather
/// than `starts_with`. Canonicalization only rewrites the checkout-root portion
/// of a key; the `crates/<name>/src/` tail survives it, which is exactly the
/// property that makes the substring safe here.
fn silent_gated_prefixes_for(
    gated_prefixes: &'static [&'static str],
    covered: &std::collections::HashMap<
        String,
        (
            std::collections::BTreeSet<u32>,
            std::collections::BTreeSet<u32>,
        ),
    >,
) -> Vec<&'static str> {
    gated_prefixes
        .iter()
        .copied()
        .filter(|prefix| !covered.keys().any(|key| key.contains(prefix)))
        .collect()
}

fn closure_prefix_allows(bytes: &[u8], pipe: usize) -> bool {
    let mut cursor = 0usize;
    let mut allowed = false;
    while cursor < pipe {
        if let Some(next) = skip_inert_region(bytes, cursor) {
            cursor = next.min(pipe).max(cursor + 1);
            continue;
        }
        if bytes[cursor].is_ascii_whitespace() {
            cursor += 1;
            continue;
        }
        if bytes[cursor].is_ascii_alphabetic() || bytes[cursor] == b'_' {
            let start = cursor;
            cursor += 1;
            while cursor < pipe && is_ident_byte(bytes[cursor]) {
                cursor += 1;
            }
            allowed = matches!(&bytes[start..cursor], b"move" | b"async" | b"return");
            continue;
        }
        allowed = matches!(
            bytes[cursor],
            b'=' | b'(' | b'[' | b'{' | b',' | b':' | b';'
        ) || (bytes[cursor] == b'>' && cursor > 0 && bytes[cursor - 1] == b'=');
        cursor += 1;
    }
    allowed
}

fn closure_body_start(bytes: &[u8], pipe: usize) -> Option<usize> {
    let mut cursor = pipe + 1;
    if bytes.get(cursor) == Some(&b'|') {
        cursor += 1;
    } else {
        loop {
            if cursor >= bytes.len() {
                return None;
            }
            if let Some(next) = skip_inert_region(bytes, cursor) {
                cursor = next.max(cursor + 1);
                continue;
            }
            if bytes[cursor] == b'|' {
                cursor += 1;
                break;
            }
            if bytes[cursor] == b';' {
                return None;
            }
            cursor += 1;
        }
    }
    cursor = skip_cfg_trivia(bytes, cursor);
    bytes.get(cursor).map(|_| cursor)
}

fn has_non_test_closure_body(contents: &str, in_cfg_test: &[bool]) -> bool {
    let bytes = contents.as_bytes();
    let mut line_starts = vec![0usize];
    line_starts.extend(
        bytes
            .iter()
            .enumerate()
            .filter(|(_, byte)| **byte == b'\n')
            .map(|(index, _)| index + 1),
    );
    let mut cursor = 0usize;
    while cursor < bytes.len() {
        if let Some(next) = skip_inert_region(bytes, cursor) {
            cursor = next.max(cursor + 1);
            continue;
        }
        if bytes[cursor] != b'|' || !closure_prefix_allows(bytes, cursor) {
            cursor += 1;
            continue;
        }
        let line = line_starts.partition_point(|&start| start <= cursor);
        if !in_cfg_test.get(line - 1).copied().unwrap_or(false)
            && closure_body_start(bytes, cursor).is_some()
        {
            return true;
        }
        cursor += 1;
    }
    false
}

/// Whether a Rust source file contains a named or closure function body outside
/// `#[cfg(test)]`.
///
/// LCOV derives `SF:` entries from function coverage mappings. A record-less
/// gated file containing a production function body is therefore unmeasured,
/// even when a sibling proves that the crate reached the report. This includes
/// closures stored in a `const`, `static`, or lazy initializer; a bare function
/// pointer value remains data-only. Module/export/data-constant-only files and
/// bodyless trait or extern declarations legitimately have no mapping and stay
/// advisory.
///
/// This is deliberately lexical rather than a Rust parser: comments and
/// literals are skipped by [`skip_inert_region`], test spans by
/// [`cfg_test_line_flags`], and a declaration counts only when `fn` is followed
/// by a name and [`find_plain_brace`] reaches `{` before `;` or `}`. Requiring a
/// name also excludes function-pointer types such as `const HANDLER: fn() = …`.
fn has_non_test_function_body(contents: &str) -> bool {
    let in_cfg_test = cfg_test_line_flags(contents);
    let bytes = contents.as_bytes();
    let mut line_starts = vec![0usize];
    line_starts.extend(
        bytes
            .iter()
            .enumerate()
            .filter(|(_, byte)| **byte == b'\n')
            .map(|(index, _)| index + 1),
    );

    let mut i = 0usize;
    while i < bytes.len() {
        if let Some(next) = skip_inert_region(bytes, i) {
            i = next.max(i + 1);
            continue;
        }
        if !bytes[i..].starts_with(b"fn")
            || (i > 0 && is_ident_byte(bytes[i - 1]))
            || bytes.get(i + 2).copied().is_some_and(is_ident_byte)
        {
            i += 1;
            continue;
        }

        let line = line_starts.partition_point(|&start| start <= i);
        let mut j = i + 2;
        loop {
            while j < bytes.len() && bytes[j].is_ascii_whitespace() {
                j += 1;
            }
            if matches!(bytes.get(j..j + 2), Some(b"//" | b"/*"))
                && let Some(next) = skip_inert_region(bytes, j)
            {
                j = next;
                continue;
            }
            break;
        }
        if in_cfg_test.get(line - 1).copied().unwrap_or(false) {
            i = j.max(i + 1);
            continue;
        }

        // Raw identifiers are valid function names (`fn r#type() { … }`).
        if bytes.get(j) == Some(&b'r') && bytes.get(j + 1) == Some(&b'#') {
            j += 2;
        }
        let name_start = j;
        if !bytes
            .get(j)
            .copied()
            .is_some_and(|byte| byte.is_ascii_alphabetic() || byte == b'_' || !byte.is_ascii())
        {
            i = j.max(i + 1);
            continue;
        }
        j += 1;
        while bytes
            .get(j)
            .copied()
            .is_some_and(|byte| is_ident_byte(byte) || !byte.is_ascii())
        {
            j += 1;
        }
        if j > name_start && find_plain_brace(bytes, j).is_some() {
            return true;
        }
        i = j.max(i + 1);
    }
    has_non_test_closure_body(contents, &in_cfg_test)
}

/// `(missing_gated, ungated)` — the two record-less buckets.
type UninstrumentedSplit = (Vec<(String, usize)>, Vec<(String, usize)>);

/// Split [`uninstrumented_files`] into hard failures and advisory files.
///
/// A record-less gated file hard-fails when its crate produced no records at
/// all ([`silent_gated_prefixes`]) or when the file contains a non-test function
/// body ([`has_non_test_function_body`]). Inside a crate that did emit records,
/// a genuinely non-executable file remains advisory because LLVM derives its
/// coverage mapping from functions and nothing a test could cover can create an
/// `SF:` entry for it. Files outside the reported closure remain advisory too.
///
/// `check_sim_coverage` and its unit tests both call this, so the classification
/// has exactly one implementation and cannot drift from what the check does.
fn classify_uninstrumented_for(
    domain: &SimCoverageDomain,
    workspace_root: &Path,
    tracked: &[(String, std::collections::BTreeSet<u32>)],
    covered: &std::collections::HashMap<
        String,
        (
            std::collections::BTreeSet<u32>,
            std::collections::BTreeSet<u32>,
        ),
    >,
) -> UninstrumentedSplit {
    let silent = silent_gated_prefixes_for(domain.gated_prefixes, covered);
    uninstrumented_files(workspace_root, tracked, covered)
        .into_iter()
        .partition(|(relpath, _)| {
            let Some(prefix) = domain
                .gated_prefixes
                .iter()
                .find(|prefix| relpath.starts_with(**prefix))
            else {
                return false;
            };
            silent.contains(prefix)
                || fs::read_to_string(workspace_root.join(relpath))
                    .map_or(true, |contents| has_non_test_function_body(&contents))
        })
}

/// Print record-less files that do not prove an unmeasured executable surface.
///
/// Deliberately does NOT fail the check. Most live outside
/// the domain's reported packages; a gated file can also land here when it has
/// no non-test function body and therefore no executable coverage mapping.
/// Reporting both keeps the scope limit visible instead of silent — see
/// ADR-0088 and ADR-0102.
fn report_ungated_for(domain: &SimCoverageDomain, ungated: &[(String, usize)]) {
    for (path, count) in ungated {
        eprintln!("not gated (no executable LCOV file record): {path}: {count} added line(s)");
    }
    eprintln!(
        "xtask check-sim-coverage: {} record-less file(s) above are advisory. \
         A file outside the reported closure ({}) is not gated; a file inside \
         it reached this path only because it has no non-test function body. \
         ADR-0024 patch coverage was NOT enforced on their added lines \
         (ADR-0088, ADR-0102).",
        ungated.len(),
        domain.report_packages.join(", ")
    );
}

/// Print the added lines that landed in a gated crate the report never
/// mentioned, then bail. Always returns `Err` — the caller relies on `?`.
///
/// Reaching this means [`classify_uninstrumented_for`] found either a whole
/// gated crate absent from this domain's diagnostic or a record-less gated file
/// with a non-test function body. Both make executable additions read as "not
/// executable" and pass. Treating either as advisory would be a fail-open hole
/// exactly where the gate is supposed to bite hardest.
fn report_missing_gated(domain: &SimCoverageDomain, missing: &[(String, usize)]) -> Result<()> {
    for (path, count) in missing {
        eprintln!("no coverage records (gated executable source): {path}: {count} added line(s)");
    }
    bail!(
        "xtask check-sim-coverage: {} record-less file(s) above cannot be \
         treated as covered. Each contains a non-test function body, belongs \
         to a gated crate that emitted no records at all, or could not be read \
         for classification. Inspect `target/{}` \
         (`rg -o '^SF:.*' target/{}`) and the file; executable \
         gated source must emit an `SF:` record, while only genuinely \
         non-executable files may remain advisory.",
        missing.len(),
        domain.lcov_file,
        domain.lcov_file
    );
}

/// Report both record-less buckets produced by [`classify_uninstrumented`],
/// then propagate the hard failure if there was one.
///
/// The ordering is load-bearing and belongs here rather than inlined at the
/// call site: [`report_missing_gated`] bails, so the advisory has to be printed
/// first or a diff carrying both classes would show only the failure and hide
/// the scope limit entirely.
fn report_record_less_for(domain: &SimCoverageDomain, split: &UninstrumentedSplit) -> Result<()> {
    let (missing_gated, ungated) = split;
    if !ungated.is_empty() {
        report_ungated_for(domain, ungated);
    }
    if !missing_gated.is_empty() {
        report_missing_gated(domain, missing_gated)?;
    }
    Ok(())
}

/// Whether an uncovered added line fails the check by default.
///
/// `true` — an added line inside the reported scope that the sim run never
/// executed fails `check-sim-coverage`. That is ADR-0024's 100%-on-the-diff
/// requirement stated as an exit code instead of as a printed finding.
///
/// It was `false` from ADR-0090 until ADR-0092, and the reason was not doubt
/// about the requirement: enforcing it would have changed nothing, because the
/// gate had no per-PR home. `.github/workflows/xtask-gates.yml` ran it on a
/// daily cron against `main`, where `merge-base(origin/main, HEAD) == HEAD`
/// makes the diff empty and the check short-circuits with "nothing to verify"
/// before it builds anything. ADR-0092 closes both halves in one changeset:
/// `.github/workflows/ci.yml` carries a `check-sim-coverage` job on
/// `pull_request`, and this constant is `true`.
///
/// The backlog that argued for the advisory landing is charged to no branch.
/// This is a *patch* gate against `git merge-base origin/main HEAD`, so each
/// PR is measured only on its own added lines. Replaying history on 2026-07-31
/// measured 6 uncovered added lines across 4 files against `HEAD~10` — the
/// realistic per-PR shape — while the 450 against `HEAD~25` is an artifact of
/// a 25-commit-old base no ordinary workflow diffs against.
///
/// `--enforce` still ORs into this (see [`check_sim_coverage`]), so it is now
/// redundant rather than removed: existing invocations keep working, and the
/// flag remains the one explicit way to ask for the verdict if this is ever
/// flipped back. `sim_coverage_enforces_uncovered_by_default` pins this value
/// with a `const` assertion — reverting it stops the `xtask` test build
/// compiling, which `cargo test --workspace` and `clippy --all-targets` both
/// reach in CI — because
/// the CI job passes `--enforce` and would therefore stay green straight
/// through a silent regression here.
const SIM_COVERAGE_ENFORCES_UNCOVERED: bool = true;

/// Resolve whether uncovered added lines are fatal for this invocation.
///
/// `--enforce` can only ever tighten: it turns uncovered lines fatal. Since
/// ADR-0092 the constant is `true`, so the flag adds nothing and this is always
/// `true` — kept as an OR rather than collapsed so that flipping the constant
/// back restores the flag's meaning without touching the call site.
///
/// This is a named function rather than an inline expression so that
/// `sim_coverage_enforces_uncovered_by_default` can pin its semantics, and so
/// the concept has a name to point at. Note where the protection against
/// cutting the CALL SITE actually comes from, because it is not this test:
/// measured 2026-08-01, rewriting `check_sim_coverage` to `let enforcing =
/// enforce;` leaves the test passing (the helper still returns `true`), and
/// what fails is `cargo clippy -p xtask -- -D warnings` with `constant
/// SIM_COVERAGE_ENFORCES_UNCOVERED is never used` / `function
/// sim_coverage_enforcing is never used`. The `dead_code` lint is the
/// tripwire; keep both this function and the constant reachable from exactly
/// one production call site so it stays one.
const fn sim_coverage_enforcing(enforce: bool) -> bool {
    enforce || SIM_COVERAGE_ENFORCES_UNCOVERED
}

/// Print per-file uncovered ranges, then fail unless the caller asked not to.
///
/// Returns `Err` when `enforcing`; otherwise prints an advisory summary and
/// returns `Ok(())`. The per-file lines above are identical either way, so the
/// only difference between the two modes is the exit code and the final
/// sentence — a reader diffing two transcripts sees exactly what changed.
///
/// Since ADR-0092 every production caller passes `enforcing = true`, because
/// `SIM_COVERAGE_ENFORCES_UNCOVERED` is `true` and `--enforce` only ORs into
/// it. The advisory arm is kept — and pinned by
/// `sim_coverage_enforces_uncovered_by_default` — so that flipping the constant
/// back stays a one-line change rather than a rewrite, and so the cost of doing
/// so is spelled out in the message a reader would then see.
fn report_uncovered_domain(
    domain: &str,
    workspace_root: &Path,
    uncovered: &[(String, u32)],
    enforcing: bool,
) -> Result<()> {
    let mut by_file: std::collections::BTreeMap<&str, Vec<u32>> = std::collections::BTreeMap::new();
    for (path, line) in uncovered {
        by_file.entry(path.as_str()).or_default().push(*line);
    }
    for (path, lines) in &by_file {
        eprintln!(
            "uncovered ({domain} domain): {}: {} line(s) — {}",
            path,
            lines.len(),
            lines
                .iter()
                .map(ToString::to_string)
                .collect::<Vec<_>>()
                .join(", ")
        );
    }
    if !enforcing {
        eprintln!(
            "xtask check-sim-coverage: ADVISORY — {} added line(s) across {} \
             file(s) were NOT executed by the {domain} coverage domain \
             (workspace root: {}). ADR-0103 wants \
             100%, and this run does NOT prove it: the check is exiting 0 \
             because SIM_COVERAGE_ENFORCES_UNCOVERED has been set back to false, \
             reversing ADR-0092. Do not cite a green run here as patch-coverage \
             evidence. Re-run with `--enforce` to get the failing exit code.",
            uncovered.len(),
            by_file.len(),
            workspace_root.display(),
        );
        return Ok(());
    }
    bail!(
        "xtask check-sim-coverage: {} added line(s) across {} file(s) not \
         executed by the {domain} coverage domain (workspace root: {}). \
         Patch coverage must be 100% — see ADR-0103.",
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

#[expect(
    clippy::too_many_lines,
    reason = "the gate's fail-closed phases remain visibly ordered"
)]
fn check_sim_coverage(base: &str, reuse_lcov: bool, enforce: bool) -> Result<()> {
    if !reuse_lcov {
        ensure_cargo_llvm_cov()?;
    }
    // Record-less executable gated source is NOT governed by this. It signals
    // a broken or incomplete measurement rather than an uncovered-line
    // verdict, and a gate that cannot measure must never report success.
    let enforcing = sim_coverage_enforcing(enforce);

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

    // 2b. Inside `src/**/*.rs`, strip lines that live INSIDE a `#[cfg(test)]`
    //     span — those are unit tests, not production code. The path-level
    //     excludes already drop `tests/`, `benches/`, `examples/`; this
    //     handles the same intent for inline test modules. Executable
    //     `unreachable!`, `unimplemented!`, and `todo!` lines remain gated.
    //
    //     Span membership comes from the shared [`cfg_test_line_flags`], the
    //     same brace-tracking scanner `check-log-fields` and
    //     `check-no-internal-clock` use. Until ADR-0092 this gate instead cut
    //     at the file's FIRST `#[cfg(test)]` line and dropped everything
    //     below it, on the stated premise that every file puts its tests in
    //     one `mod tests` at the bottom, making that "a reliable upper bound
    //     on the production region". Measured 2026-08-01, that premise is
    //     false and the cost was enormous: the first `#[cfg(test)]` is often
    //     a gated `use` or helper near the top, so the cut exempted 48% of
    //     all gated lines — 2781 of the 2828 lines of
    //     `magnetar-runtime-tokio/src/driver.rs`, whose cut sits at line 48
    //     on a `#[cfg(test)] use std::io::IoSlice;` — and 71% of the gated
    //     lines added over the preceding ten merged pull requests. A gate
    //     silently exempting most of its own surface is the fail-open shape
    //     ADR-0088 exists to prevent, and enforcing it (ADR-0092) without
    //     fixing it would have made the enforcement mostly theatre.
    for (relpath, lines) in &mut tracked {
        let abs = workspace_root.join(relpath);
        if let Ok(contents) = fs::read_to_string(&abs) {
            let cfg_test = sim_coverage_cfg_test_lines(&contents);
            lines.retain(|line| !cfg_test.contains(line));
        }
    }
    tracked.retain(|(_, lines)| !lines.is_empty());

    if tracked.is_empty() {
        eprintln!(
            "xtask check-sim-coverage: no added production Rust lines remain \
             after path and `#[cfg(test)]` exclusions — nothing to verify."
        );
        return Ok(());
    }

    let unowned: Vec<_> = tracked
        .iter()
        .filter(|(path, _)| {
            !SIM_COVERAGE_DOMAINS.iter().any(|domain| {
                domain
                    .gated_prefixes
                    .iter()
                    .any(|prefix| path.starts_with(prefix))
            })
        })
        .map(|(path, lines)| (path.clone(), lines.len()))
        .collect();
    if !unowned.is_empty() {
        for (path, count) in &unowned {
            eprintln!("not gated (outside both coverage domains): {path}: {count} added line(s)");
        }
        eprintln!(
            "xtask check-sim-coverage: {} file(s) above are advisory because neither evidence domain owns their package.",
            unowned.len()
        );
    }

    let evidence = run_sim_lcov(&workspace_root, reuse_lcov)?;
    let mut failures = Vec::new();
    let mut total_gated = 0_usize;
    let mut total_advisory = unowned.len();
    for item in &evidence {
        let domain = item.domain;
        if !reuse_lcov {
            eprintln!(
                "xtask check-sim-coverage: {} domain authoritative evidence retained in memory ({:.3}s).",
                domain.name,
                item.elapsed.as_secs_f64()
            );
        }
        let domain_lines = domain_tracked(domain, &tracked);
        if domain_lines.is_empty() {
            continue;
        }
        let domain_lcov = std::str::from_utf8(&item.lcov)
            .with_context(|| format!("{} retained report is not UTF-8", domain.name))?;
        let covered = parse_lcov_coverage(domain_lcov);
        let uncovered = intersect_diff_with_coverage(&workspace_root, &domain_lines, &covered);
        let record_less =
            classify_uninstrumented_for(domain, &workspace_root, &domain_lines, &covered);
        if let Err(err) = report_record_less_for(domain, &record_less) {
            failures.push(format!("{} domain: {err:#}", domain.name));
        }
        let (missing_gated, ungated) = record_less;
        total_gated += domain_lines
            .len()
            .saturating_sub(ungated.len())
            .saturating_sub(missing_gated.len());
        total_advisory += ungated.len();
        if !uncovered.is_empty()
            && let Err(err) =
                report_uncovered_domain(domain.name, &workspace_root, &uncovered, enforcing)
        {
            failures.push(format!("{} domain: {err:#}", domain.name));
        }
        if uncovered.is_empty() && missing_gated.is_empty() {
            eprintln!(
                "xtask check-sim-coverage: {} domain covered all executable added lines across {} LCOV-recorded file(s).",
                domain.name,
                domain_lines.len().saturating_sub(ungated.len())
            );
        }
    }
    if !failures.is_empty() {
        bail!(
            "xtask check-sim-coverage: {} isolated coverage domain(s) failed:\n{}",
            failures.len(),
            failures.join("\n")
        );
    }

    // Three buckets now: measured-and-clean, advisory record-less, and missing
    // gated executable source. The last is empty here (it bails above), but
    // subtract it anyway so the count stays right if the ordering ever changes.
    let gated = total_gated;
    // A reused report produces this exact sentence off stale line numbers, so
    // the success line has to carry the caveat with it — the warning printed
    // back in `run_sim_lcov` is thousands of lines up a real transcript.
    let reuse_suffix = if reuse_lcov {
        format!(" {SIM_COVERAGE_REUSE_MARKER}")
    } else {
        String::new()
    };
    // Reaching here with a non-empty `uncovered` means advisory mode swallowed
    // a real failure. Saying "all added lines are covered" then would be the
    // exact false summary ADR-0088 was written to stop — it counted files it
    // never measured. Keep the two cases textually distinct.
    eprintln!(
        "xtask check-sim-coverage: both isolated domains covered all executable added lines across {gated} LCOV-recorded file(s) ({total_advisory} advisory record-less file(s)).{reuse_suffix}"
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
/// moonpool — by then the parity landscape rebalances naturally.
const PARITY_EXEMPT_FILES: &[&str] = &[
    "magnetar-runtime-moonpool/tests/sim_chaos.rs",
    "magnetar-runtime-moonpool/src/pool.rs",
    "magnetar-runtime-moonpool/tests/proxy_multi_conn.rs",
    // Moonpool-only deterministic SimProviders harness for the PIP-33 marker
    // lost-wakeup fix. Like sim_chaos.rs it has no tokio
    // twin — the tokio engine's equivalent coverage is the live-path positive
    // test in `marker_lost_wakeup.rs` (which IS a 1:1 twin across engines).
    "magnetar-runtime-moonpool/tests/replicated_subscriptions_sim.rs",
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

/// One `[[seed]]` entry of the known-failing registry, as much of it as
/// the replay needs: the `MOONPOOL_SEED` value and the triage status.
#[derive(Debug, PartialEq, Eq)]
struct RegistrySeed {
    value: String,
    status: String,
}

/// Parse `known-failing.toml`'s `[[seed]]` entries without a TOML
/// dependency. The registry schema (ADR-0047) is deliberately flat —
/// scalar `key = value` lines plus `"""`-delimited multiline `note`
/// strings — so a line scanner that skips multiline-string bodies is
/// exact for this file. Unknown keys are ignored; an entry missing
/// `value` or `status` is an error rather than a silent skip.
fn parse_known_failing_seeds(contents: &str) -> Result<Vec<RegistrySeed>> {
    let mut seeds: Vec<RegistrySeed> = Vec::new();
    let mut current: Option<(Option<String>, Option<String>)> = None;
    let mut in_multiline = false;

    let finish = |entry: Option<(Option<String>, Option<String>)>,
                  seeds: &mut Vec<RegistrySeed>|
     -> Result<()> {
        if let Some((value, status)) = entry {
            let value = value.ok_or_else(|| anyhow!("[[seed]] entry missing `value`"))?;
            let status = status.ok_or_else(|| anyhow!("[[seed]] entry missing `status`"))?;
            seeds.push(RegistrySeed { value, status });
        }
        Ok(())
    };

    for line in contents.lines() {
        if in_multiline {
            // A `"""` on its own (or ending a note) closes the string;
            // registry notes never nest quotes, per the schema comment.
            if line.contains("\"\"\"") {
                in_multiline = false;
            }
            continue;
        }
        let trimmed = line.trim();
        if trimmed.starts_with('#') || trimmed.is_empty() {
            continue;
        }
        if trimmed == "[[seed]]" {
            finish(current.take(), &mut seeds)?;
            current = Some((None, None));
            continue;
        }
        let Some((key, raw)) = trimmed.split_once('=') else {
            continue;
        };
        let key = key.trim();
        let raw = raw.trim();
        // A multiline string opens when the value starts with `"""` and
        // does not close on the same line.
        if raw.starts_with("\"\"\"") && raw[3..].find("\"\"\"").is_none() {
            in_multiline = true;
            continue;
        }
        let unquoted = raw.trim_matches('"').to_owned();
        if let Some((value, status)) = current.as_mut() {
            match key {
                "value" => *value = Some(unquoted),
                "status" => *status = Some(unquoted),
                _ => {}
            }
        }
    }
    finish(current.take(), &mut seeds)?;
    Ok(seeds)
}

/// ADR-0047 §5 (landed by ADR-0097): replay every `status = "open"`
/// registry seed with the exact per-PR `seed-replay` CI invocation, so
/// the local invariant is "if CI's replay job would fail, this xtask
/// fails too". Exit is non-zero on any reproducing seed.
fn check_known_failing_seeds() -> Result<()> {
    let workspace_root = workspace_root()?;
    let registry = workspace_root.join("crates/magnetar-runtime-moonpool/seeds/known-failing.toml");
    let contents = fs::read_to_string(&registry)
        .with_context(|| format!("failed to read {}", registry.display()))?;
    let seeds = parse_known_failing_seeds(&contents)?;
    let open: Vec<&RegistrySeed> = seeds.iter().filter(|s| s.status == "open").collect();
    if open.is_empty() {
        eprintln!("xtask check-known-failing-seeds: no open registry entries — nothing to replay.");
        return Ok(());
    }

    let cargo = env::var_os("CARGO").unwrap_or_else(|| "cargo".into());
    let mut failures: Vec<String> = Vec::new();
    for seed in &open {
        eprintln!(
            "xtask check-known-failing-seeds: MOONPOOL_SEED={} cargo test -p magnetar-runtime-moonpool --no-default-features --features crypto-aws-lc-rs --locked",
            seed.value
        );
        let status = StdCommand::new(&cargo)
            .current_dir(&workspace_root)
            .env("MOONPOOL_SEED", &seed.value)
            .args([
                "test",
                "-p",
                "magnetar-runtime-moonpool",
                "--no-default-features",
                "--features",
                "crypto-aws-lc-rs",
                "--locked",
            ])
            .status()
            .with_context(|| format!("failed to invoke `cargo test` for seed {}", seed.value))?;
        if status.success() {
            eprintln!(
                "xtask check-known-failing-seeds: seed {} passed (anchor holds).",
                seed.value
            );
        } else {
            eprintln!(
                "xtask check-known-failing-seeds: seed {} REPRODUCED a failure.",
                seed.value
            );
            failures.push(seed.value.clone());
        }
    }
    if !failures.is_empty() {
        bail!(
            "xtask check-known-failing-seeds: {}/{} open seed(s) reproduced: {} — \
             fix the regression or triage per ADR-0047 §4.",
            failures.len(),
            open.len(),
            failures.join(", ")
        );
    }
    eprintln!(
        "xtask check-known-failing-seeds: all {} open seed(s) replayed green.",
        open.len()
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
/// rejects any `.data*` section in the module assembly. GCC emits
/// `.data.rel.ro.local` for some `-fPIC` const-pointer patterns; clang
/// places the equivalent in `.rodata`. `aws-lc-fips-sys` only
/// auto-switches to clang for the `asan` feature, so a plain Linux
/// FIPS build inherits whatever `cmake-rs` picks (`/usr/bin/cc` → gcc
/// on Fedora) and trips the delocate guard intermittently — the trip
/// depends on which aws-lc sources cargo's feature unification pulls
/// into `bcm.c`. Setting the C/asm toolchain explicitly here keeps the
/// matrix green regardless of host gcc version.
///
/// This used to say "GCC 16+". That is wrong and was load-bearing
/// enough to mislead: on 2026-07-31 the same `delocate` failure
/// reproduced on **gcc 14.4.0** while running `check-sim-coverage`.
/// The feature-unification sentence above is the real explanation, so
/// no gcc version is safe and none is named.
///
/// Beware a stale `CMake` cache when debugging this. `cmake-rs` reuses
/// `OUT_DIR/build`, and a re-run with `CC=clang` after a failed gcc run
/// re-configures the top level while `try_compile` probes still use the
/// cached `/usr/host/bin/cc`; `CMake` reports "You have changed variables
/// that require your cache to be deleted" and the build fails a second
/// time for what looks like the first reason. Delete the
/// `aws-lc-fips-sys-*` build directory before re-testing.
fn apply_fips_toolchain(cmd: &mut StdCommand, features: &str) {
    if !features.split(',').any(|f| f.trim() == "crypto-fips") {
        return;
    }
    force_clang_toolchain(cmd);
}

/// Pin `cmd`'s C/C++/asm toolchain to clang on Linux.
///
/// Split out of [`apply_fips_toolchain`] because that helper matches a
/// comma-separated feature list, and the callers that need this most pass
/// `--all-features` instead — a shape no feature-name match can recognise.
///
/// Any Linux build that reaches `crypto-fips` needs this. `--all-features`
/// always does: `magnetar-runtime-moonpool` and `magnetar-runtime-tokio` both
/// declare `crypto-fips = ["rustls/fips"]`, which pulls `aws-lc-fips-sys`.
/// Without it the build dies inside aws-lc's `delocate` pass with
/// `".data section found in module"`, because `cmake-rs` falls back to the
/// host `cc` (gcc) and gcc emits `.data.rel.ro.local` for some `-fPIC`
/// const-pointer patterns where clang emits `.rodata`.
///
/// Not gated on a host gcc version on purpose. That failure was long
/// attributed to "gcc 16+", but it reproduced on **gcc 14.4.0** on
/// 2026-07-31 while running `check-sim-coverage`; which sources land in
/// `bcm.c` depends on cargo's feature unification, so the version is the
/// wrong axis. Pin the toolchain unconditionally instead.
fn force_clang_toolchain(cmd: &mut StdCommand) {
    if !cfg!(target_os = "linux") {
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
                "xtask check-crypto-matrix: cargo build -p magnetar-driver --no-default-features --features {features}"
            );
            let mut cmd = StdCommand::new(&cargo);
            cmd.current_dir(&workspace_root).args([
                "build",
                "-p",
                "magnetar-driver",
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
                failures.push(format!("magnetar-driver:{features}"));
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

    fn coverage_domain(name: &str) -> &'static SimCoverageDomain {
        SIM_COVERAGE_DOMAINS
            .iter()
            .find(|domain| domain.name == name)
            .unwrap_or_else(|| panic!("missing coverage domain {name}"))
    }

    fn all_coverage_gated_prefixes() -> Vec<&'static str> {
        SIM_COVERAGE_DOMAINS
            .iter()
            .flat_map(|domain| domain.gated_prefixes.iter().copied())
            .collect()
    }

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

    // ── check-e2e-container-memory parser ───────────────────────────

    /// The house e2e preamble: a Pulsar image const plus the accessor
    /// pair every `e2e_*.rs` copies.
    const PULSAR_PREAMBLE: &str = r#"
const DEFAULT_IMAGE_REPO: &str = "apachepulsar/pulsar";
const DEFAULT_IMAGE_TAG: &str = "4.0.4";
const PULSAR_MEM_LIMIT: &str = "-Xms256m -Xmx1g -XX:MaxDirectMemorySize=1g";

fn image_repo() -> String {
    std::env::var("MAGNETAR_PULSAR_IMAGE_REPO").unwrap_or_else(|_| DEFAULT_IMAGE_REPO.to_owned())
}

fn image_tag() -> String {
    std::env::var("MAGNETAR_PULSAR_IMAGE_TAG").unwrap_or_else(|_| DEFAULT_IMAGE_TAG.to_owned())
}
"#;

    #[test]
    fn container_memory_flags_uncapped_pulsar_chain() {
        let src = format!(
            "{PULSAR_PREAMBLE}
async fn start_pulsar() {{
    let container = GenericImage::new(image_repo(), image_tag())
        .with_exposed_port(ContainerPort::Tcp(BROKER_BINARY_PORT))
        .with_startup_timeout(Duration::from_mins(2))
        .with_cmd(vec![\"bin/pulsar\".to_owned(), \"standalone\".to_owned()])
        .start()
        .await?;
}}
"
        );
        let scan = scan_container_memory(&src);
        assert_eq!(scan.violations, vec![(15, CONTAINER_MEM_NO_ENV)]);
        assert_eq!(scan.capped, 0);
    }

    #[test]
    fn container_memory_accepts_capped_pulsar_chain() {
        // Comments between chain links are house style in the longer
        // suites (`e2e_handshake_error.rs`); they must not end the walk.
        let src = format!(
            "{PULSAR_PREAMBLE}
async fn start_pulsar() {{
    let container = GenericImage::new(image_repo(), image_tag())
        .with_exposed_port(ContainerPort::Tcp(BROKER_BINARY_PORT))
        .with_env_var(\"PULSAR_MEM\", PULSAR_MEM_LIMIT)
        // Token-auth on, applied through `apply-config-from-env`.
        .with_env_var(\"PULSAR_PREFIX_authenticationEnabled\", \"true\")
        .with_cmd(vec![\"bin/pulsar\".to_owned(), \"standalone\".to_owned()])
        .start()
        .await?;
}}
"
        );
        let scan = scan_container_memory(&src);
        assert!(scan.violations.is_empty());
        assert_eq!(scan.capped, 1);
    }

    #[test]
    fn container_memory_checks_every_chain_in_a_file() {
        // `e2e_batch_chunk.rs` / `e2e_pulsar_proxy.rs` each build two.
        let src = format!(
            "{PULSAR_PREAMBLE}
async fn start_standalone() {{
    let a = GenericImage::new(image_repo(), image_tag())
        .with_env_var(\"PULSAR_MEM\", PULSAR_MEM_LIMIT)
        .start()
        .await?;
}}

async fn start_proxy() {{
    let b = GenericImage::new(image_repo(), image_tag())
        .with_env_var(\"PULSAR_PREFIX_zookeeperServers\", &zk_servers)
        .start()
        .await?;
}}
"
        );
        let scan = scan_container_memory(&src);
        assert_eq!(scan.violations, vec![(22, CONTAINER_MEM_NO_ENV)]);
        assert_eq!(scan.capped, 1);
    }

    #[test]
    fn container_memory_ignores_non_pulsar_images() {
        // `e2e_sasl_kerberos.rs` shadows `image_repo()` with a KDC image
        // and `e2e_athenz_zts.rs` builds a ZTS server. Neither runs the
        // Pulsar JVM, so `PULSAR_MEM` does not apply.
        let src = r#"
const DEFAULT_KDC_IMAGE_REPO: &str = "gcavalcante8808/krb5-server";
const DEFAULT_KDC_IMAGE_TAG: &str = "latest";
const DEFAULT_ZTS_IMAGE_REPO: &str = "athenz/athenz-zts-server";

fn image_repo() -> String {
    std::env::var("MAGNETAR_KDC_IMAGE_REPO").unwrap_or_else(|_| DEFAULT_KDC_IMAGE_REPO.to_owned())
}

fn image_tag() -> String {
    std::env::var("MAGNETAR_KDC_IMAGE_TAG").unwrap_or_else(|_| DEFAULT_KDC_IMAGE_TAG.to_owned())
}

async fn start_kdc() {
    let container = GenericImage::new(image_repo(), image_tag())
        .with_env_var("KRB5_REALM", "EXAMPLE.COM")
        .start()
        .await?;
}

async fn start_zts() {
    let container = GenericImage::new(DEFAULT_ZTS_IMAGE_REPO, "1.12.5")
        .with_startup_timeout(Duration::from_secs(30))
        .start()
        .await;
}
"#;
        let scan = scan_container_memory(src);
        assert!(scan.violations.is_empty());
        assert_eq!(scan.capped, 0);
        assert_eq!(scan.out_of_scope, 2);
    }

    #[test]
    fn container_memory_reads_an_inline_image_literal() {
        // A file written from scratch rather than cloned still gets caught.
        let uncapped = r#"
async fn start_pulsar() {
    let container = GenericImage::new("apachepulsar/pulsar", "4.0.4")
        .start()
        .await?;
}
"#;
        assert_eq!(
            scan_container_memory(uncapped).violations,
            vec![(3, CONTAINER_MEM_NO_ENV)]
        );

        let capped = r#"
async fn start_pulsar() {
    let container = GenericImage::new("apachepulsar/pulsar", "4.0.4")
        .with_env_var("PULSAR_MEM", "-Xms256m -Xmx1g")
        .start()
        .await?;
}
"#;
        let scan = scan_container_memory(capped);
        assert!(scan.violations.is_empty());
        assert_eq!(scan.capped, 1);
    }

    #[test]
    fn container_memory_flags_a_builder_that_never_starts() {
        // Stashing the builder would put `.start()` out of the gate's
        // reach — rejected rather than silently passed.
        let src = format!(
            "{PULSAR_PREAMBLE}
async fn start_pulsar() {{
    let builder = GenericImage::new(image_repo(), image_tag())
        .with_exposed_port(ContainerPort::Tcp(BROKER_BINARY_PORT));
    let container = builder.start().await?;
}}
"
        );
        assert_eq!(
            scan_container_memory(&src).violations,
            vec![(15, CONTAINER_MEM_NOT_STARTED)]
        );
    }

    #[test]
    fn container_memory_flags_an_unresolvable_image() {
        let src = r#"
async fn start_pulsar() {
    let repo = format!("{registry}/pulsar");
    let container = GenericImage::new(repo, "4.0.4")
        .with_env_var("PULSAR_MEM", "-Xms256m -Xmx1g")
        .start()
        .await?;
}
"#;
        assert_eq!(
            scan_container_memory(src).violations,
            vec![(4, CONTAINER_MEM_UNRESOLVED)]
        );
    }

    #[test]
    fn container_memory_ignores_comments_and_strings() {
        // `e2e_reconnect.rs` discusses `container.start()` in prose, and
        // the ctor name appears in doc comments.
        let src = r#"
/// `container.start()` only re-runs `docker start`, so
/// `GenericImage::new(image_repo(), image_tag()).start()` is not re-executed.
fn explain() {
    // let c = GenericImage::new(image_repo(), image_tag()).start();
    let doc = "GenericImage::new(image_repo(), image_tag()).start()";
    let _ = MyGenericImage::new(image_repo(), image_tag());
}
"#;
        let scan = scan_container_memory(src);
        assert_eq!(scan, ContainerMemoryScan::default());
    }

    // ── check-no-internal-clock scanner (ADR-0011, ADR-0086) ────────

    #[test]
    fn clock_flags_instant_now_and_system_time_now() {
        let src = r"
fn stamp() {
    let a = Instant::now();
    let b = std::time::SystemTime::now();
}
";
        let hits = scan_clock_violations(src);
        assert_eq!(
            hits,
            vec![(3, "Instant::now()"), (4, "SystemTime::now()")],
            "both qualified and unqualified spellings must be flagged"
        );
    }

    /// The ADR-0086 regression: the two leaks were `.elapsed()` calls, which
    /// the pre-0084 needle list did not contain at all.
    #[test]
    fn clock_flags_dot_elapsed() {
        let src = r"
fn pop(msg: &Msg) -> u64 {
    u64::try_from(msg.arrived_at.elapsed().as_millis()).unwrap_or(u64::MAX)
}
";
        assert_eq!(scan_clock_violations(src), vec![(3, ".elapsed()")]);
    }

    /// The needle carries a leading dot for a reason: `elapsed` appears in
    /// legitimate *method names* in this crate. Flagging those would make the
    /// gate unusable.
    #[test]
    fn clock_ignores_elapsed_without_leading_dot() {
        let src = r"
fn record_rate_window_safe_under_zero_elapsed() {}

fn check(&self, now: Instant) -> bool {
    self.batch_deadline_elapsed(now)
}
";
        assert!(
            scan_clock_violations(src).is_empty(),
            "a bare `elapsed(` must not match — only `.elapsed()` reads a clock"
        );
    }

    #[test]
    fn clock_skips_cfg_test_modules() {
        let src = r"
fn deliver(now: Instant) {}

#[cfg(test)]
mod tests {
    #[test]
    fn fixture() {
        let t0 = Instant::now();
        let d = t0.elapsed();
    }
}
";
        assert!(
            scan_clock_violations(src).is_empty(),
            "tests legitimately materialise instants for their fixtures"
        );
    }

    /// The concrete defect the pre-ADR-0086 `line.find("//")` comment strip
    /// had: it truncated at the first `//` *anywhere* on the line, including
    /// inside a string literal, silently exempting everything after it.
    #[test]
    fn clock_flags_read_after_a_url_string_literal_on_the_same_line() {
        let src = r#"
fn connect() {
    let url = "pulsar://host:6650"; let t = Instant::now();
}
"#;
        assert_eq!(
            scan_clock_violations(src),
            vec![(3, "Instant::now()")],
            "a `//` inside a string literal must not exempt the rest of the line"
        );
    }

    #[test]
    fn clock_ignores_comment_and_doc_comment_mentions() {
        let src = r"
/// Records the latency (`Instant::now() - msg.arrived_at`) — prose only.
fn pop(now: Instant) {
    // Formerly `msg.arrived_at.elapsed()`; see ADR-0086.
    /* Also SystemTime::now() in a block comment. */
}
";
        assert!(
            scan_clock_violations(src).is_empty(),
            "documentation that mentions a clock read is not a clock read"
        );
    }

    // ── check-sim-coverage scope reporting (ADR-0088) ───────────────

    /// Build the `(executable, hit)` map `intersect_diff_with_coverage` and
    /// `uninstrumented_files` consume, keyed the way LCOV emits it: absolute
    /// paths under the workspace root.
    fn coverage_of(
        root: &Path,
        entries: &[(&str, &[u32], &[u32])],
    ) -> std::collections::HashMap<
        String,
        (
            std::collections::BTreeSet<u32>,
            std::collections::BTreeSet<u32>,
        ),
    > {
        entries
            .iter()
            .map(|(relpath, executable, hit)| {
                (
                    root.join(relpath).to_string_lossy().into_owned(),
                    (
                        executable.iter().copied().collect(),
                        hit.iter().copied().collect(),
                    ),
                )
            })
            .collect()
    }

    fn tracked_of(entries: &[(&str, &[u32])]) -> Vec<(String, std::collections::BTreeSet<u32>)> {
        entries
            .iter()
            .map(|(relpath, lines)| ((*relpath).to_owned(), lines.iter().copied().collect()))
            .collect()
    }

    #[cfg(unix)]
    fn fake_coverage_cargo(root: &Path) -> PathBuf {
        use std::os::unix::fs::PermissionsExt;

        let cargo = root.join("fake-cargo");
        fs::write(
            &cargo,
            r#"#!/bin/sh
set -eu

if [ "${1:-}" = "metadata" ]; then
    printf 'metadata\t%s\n' "$*" >> "$PWD/fake-cargo.log"
    workspace_parent="${PWD%/*}"
    if [ -f "$PWD/metadata-symlink-storage" ]; then
        metadata_target="$PWD/configured-target"
        metadata_build="$PWD/configured-build"
    else
        metadata_target="$PWD/target"
        metadata_build="$workspace_parent/build-storage/cached-build"
    fi
    if [ -f "$PWD/metadata-without-build-directory" ]; then
        printf '{"packages":[{"metadata":{"target_directory":"/nested-poison"}}],"target_directory":"%s"}\n' "$metadata_target"
    else
        printf '{"packages":[{"metadata":{"target_directory":"/nested-poison"}}],"target_directory":"%s","build_directory":"%s"}\n' "$metadata_target" "$metadata_build"
    fi
    exit 0
fi

phase="${2:-missing}"
coverage_target="${CARGO_LLVM_COV_TARGET_DIR:-$PWD/target/llvm-cov-target}"
build_target="${CARGO_LLVM_COV_BUILD_DIR:-$coverage_target}"
cargo_target="${CARGO_TARGET_DIR:-unset}"
cargo_build="${CARGO_BUILD_BUILD_DIR:-unset}"
artifact_flags="${LLVM_COV_FLAGS+set}${LLVM_PROFDATA_FLAGS+set}${CARGO_LLVM_COV_FLAGS+set}${CARGO_LLVM_PROFDATA_FLAGS+set}"
argv=
for argument in "$@"; do
    argv="${argv}${argv:+|}${argument}"
done
printf '%s\t%s\t%s\t%s\t%s\t%s\t%s\n' "$phase" "$coverage_target" "$build_target" "$cargo_target" "$cargo_build" "$artifact_flags" "$argv" >> "$PWD/fake-cargo.log"

[ "$coverage_target" = "$build_target" ] || exit 77
[ "$coverage_target" = "$cargo_target" ] || exit 78
if [ -f "$PWD/metadata-without-build-directory" ]; then
    [ "$cargo_build" = "unset" ] || exit 79
else
    [ "$coverage_target" = "$cargo_build" ] || exit 80
fi
[ -z "$artifact_flags" ] || exit 81

case "$phase" in
    --no-report)
        [ -d "$coverage_target" ] || exit 70
        [ -z "$(find "$coverage_target" -mindepth 1 -print -quit)" ] || exit 71
        mkdir -p "$coverage_target/debug/deps"
        : > "$coverage_target/current.profraw"
        : > "$coverage_target/debug/deps/current-object"
        [ ! -f "$PWD/fail-execution" ] || exit 17
        case "$coverage_target" in
            */moonpool)
                [ ! -f "$PWD/fail-moonpool-execution" ] || exit 17
                [ ! -f "$PWD/preexisting-moonpool-report" ] || : > "$coverage_target/report.lcov"
                ;;
            */tokio)
                [ ! -f "$PWD/fail-tokio-execution" ] || exit 17
                [ ! -f "$PWD/preexisting-tokio-report" ] || : > "$coverage_target/report.lcov"
                ;;
        esac
        ;;
    report)
        [ -f "$coverage_target/current.profraw" ] || exit 72
        [ -f "$coverage_target/debug/deps/current-object" ] || exit 73
        [ ! -e "$coverage_target/stale.profraw" ] || exit 74
        [ ! -e "$coverage_target/debug/deps/stale-object" ] || exit 75
        [ ! -f "$PWD/fail-report" ] || exit 18
        case "$coverage_target" in
            */moonpool) [ ! -f "$PWD/fail-moonpool-report" ] || exit 18 ;;
            */tokio) [ ! -f "$PWD/fail-tokio-report" ] || exit 18 ;;
        esac

        output_path=
        previous=
        for argument in "$@"; do
            if [ "$previous" = "--output-path" ]; then
                output_path="$argument"
                break
            fi
            previous="$argument"
        done
        [ -n "$output_path" ] || exit 76
        [ ! -f "$PWD/missing-report-output" ] || exit 0
        if [ -f "$PWD/foreign-report" ] ||
           { [ -f "$PWD/foreign-moonpool-report" ] && [ "${coverage_target%/moonpool}" != "$coverage_target" ]; } ||
           { [ -f "$PWD/foreign-tokio-report" ] && [ "${coverage_target%/tokio}" != "$coverage_target" ]; }; then
            case "$coverage_target" in
                */moonpool) printf 'SF:%s/crates/magnetar-runtime-tokio/src/client.rs\nDA:1,1\nend_of_record\n' "$PWD" > "$output_path" ;;
                */tokio) printf 'SF:%s/crates/magnetar-proto/src/conn.rs\nDA:1,1\nend_of_record\n' "$PWD" > "$output_path" ;;
            esac
        else
            printf 'TN:current-pass\n' > "$output_path"
        fi
        ;;
    *)
        exit 19
        ;;
esac
"#,
        )
        .expect("write fake cargo");
        let mut permissions = fs::metadata(&cargo)
            .expect("read fake cargo metadata")
            .permissions();
        permissions.set_mode(0o700);
        fs::set_permissions(&cargo, permissions).expect("make fake cargo executable");
        cargo
    }

    #[cfg(unix)]
    #[derive(Debug)]
    struct FakeCoverageInvocation {
        phase: String,
        llvm_target: PathBuf,
        llvm_build: PathBuf,
        cargo_target: PathBuf,
        cargo_build: Option<PathBuf>,
        artifact_flags: String,
        argv: String,
    }

    #[cfg(unix)]
    fn fake_coverage_invocations(root: &Path) -> Vec<FakeCoverageInvocation> {
        fs::read_to_string(root.join("fake-cargo.log"))
            .expect("read fake cargo log")
            .lines()
            .filter(|line| !line.starts_with("metadata\t"))
            .map(|line| {
                let mut fields = line.split('\t');
                let phase = fields.next().expect("phase field").to_owned();
                let llvm_target = PathBuf::from(fields.next().expect("coverage target field"));
                let llvm_build = PathBuf::from(fields.next().expect("build target field"));
                let cargo_target = PathBuf::from(fields.next().expect("Cargo target field"));
                let cargo_build = match fields.next().expect("Cargo build field") {
                    "unset" => None,
                    path => Some(PathBuf::from(path)),
                };
                let artifact_flags = fields.next().expect("artifact flags field").to_owned();
                let argv = fields.next().expect("argv field").to_owned();
                assert!(fields.next().is_none(), "unexpected fake cargo log field");
                FakeCoverageInvocation {
                    phase,
                    llvm_target,
                    llvm_build,
                    cargo_target,
                    cargo_build,
                    artifact_flags,
                    argv,
                }
            })
            .collect()
    }

    #[cfg(unix)]
    fn fake_metadata_invocation(root: &Path) -> String {
        fs::read_to_string(root.join("fake-cargo.log"))
            .expect("read fake cargo log")
            .lines()
            .find_map(|line| line.strip_prefix("metadata\t").map(str::to_owned))
            .expect("cargo metadata invocation")
    }

    #[cfg(unix)]
    fn assert_fake_coverage_contract(
        root: &Path,
        expected_scratch_parent: &Path,
        case: &str,
    ) -> PathBuf {
        let invocations = fake_coverage_invocations(root);
        assert_eq!(
            invocations.len(),
            4,
            "{case}: two isolated execute + report pairs"
        );
        assert_eq!(
            invocations[0].phase, "--no-report",
            "{case}: moonpool execute"
        );
        assert_eq!(invocations[1].phase, "report", "{case}: moonpool report");
        assert_eq!(invocations[2].phase, "--no-report", "{case}: tokio execute");
        assert_eq!(invocations[3].phase, "report", "{case}: tokio report");
        assert_eq!(
            invocations[0].argv,
            "llvm-cov|--no-report|-p|magnetar-runtime-moonpool|-p|magnetar-differential|--all-features|--locked|--quiet"
        );
        assert_eq!(
            invocations[1].argv,
            format!(
                "llvm-cov|report|--lcov|--output-path|{}/report.lcov|-p|magnetar-proto|-p|magnetar-runtime-moonpool|-p|magnetar-differential|-p|magnetar-auth-athenz|-p|magnetar-auth-sasl|-p|magnetar-driver|-p|magnetar-fakes|--ignore-filename-regex|crates/magnetar-proto/src/pb/",
                invocations[0].llvm_target.display()
            )
        );
        assert_eq!(
            invocations[2].argv,
            "llvm-cov|--no-report|-p|magnetar-runtime-tokio|-p|magnetar-differential|--all-features|--locked|--quiet"
        );
        assert_eq!(
            invocations[3].argv,
            format!(
                "llvm-cov|report|--lcov|--output-path|{}/report.lcov|-p|magnetar-runtime-tokio|--ignore-filename-regex|crates/magnetar-proto/src/pb/",
                invocations[2].llvm_target.display()
            )
        );
        assert_eq!(
            fake_metadata_invocation(root),
            format!(
                "metadata --format-version=1 --locked --manifest-path {}",
                root.join("Cargo.toml").display()
            ),
            "{case}: resolve effective Cargo storage before isolation"
        );
        let moonpool_target = invocations[0].llvm_target.clone();
        let tokio_target = invocations[2].llvm_target.clone();
        assert_ne!(
            moonpool_target, tokio_target,
            "{case}: domains must not share artifacts"
        );
        assert!(moonpool_target.is_absolute(), "{case}: target is absolute");
        for (index, invocation) in invocations.into_iter().enumerate() {
            let isolated_target = if index < 2 {
                &moonpool_target
            } else {
                &tokio_target
            };
            assert_eq!(
                &invocation.llvm_target, isolated_target,
                "{case}: only phases within one domain share an llvm-cov target"
            );
            assert_eq!(
                &invocation.llvm_build, isolated_target,
                "{case}: llvm-cov objects must share the isolated target"
            );
            assert_eq!(
                &invocation.cargo_target, isolated_target,
                "{case}: Cargo metadata and auxiliary targets must be isolated"
            );
            assert_eq!(
                invocation.cargo_build.as_ref(),
                Some(isolated_target),
                "{case}: supported Cargo build storage must be isolated"
            );
            assert!(
                invocation.artifact_flags.is_empty(),
                "{case}: artifact-injection flags must be cleared"
            );
        }
        assert_eq!(
            moonpool_target.parent().and_then(Path::parent),
            Some(expected_scratch_parent),
            "{case}: cold build must use a scratch sibling on the build filesystem"
        );
        moonpool_target
            .parent()
            .expect("scratch root")
            .to_path_buf()
    }

    #[test]
    fn sim_coverage_metadata_parser_uses_top_level_decoded_paths() {
        let metadata = r#"{
            "packages": [{"metadata": {"target_directory": "/nested-poison"}}],
            "target_directory": "/cache/target\\with-escape",
            "build_directory": "/cache/build-\u03bb"
        }"#;

        assert_eq!(
            top_level_json_string(metadata, "target_directory").expect("parse target directory"),
            Some("/cache/target\\with-escape".to_owned())
        );
        assert_eq!(
            top_level_json_string(metadata, "build_directory").expect("parse build directory"),
            Some("/cache/build-λ".to_owned())
        );
        assert_eq!(
            top_level_json_string(metadata, "missing").expect("parse absent field"),
            None
        );
    }

    #[test]
    fn sim_coverage_metadata_preflight_locked_preserves_missing_and_stale_lock() {
        let tmp = tempfile::tempdir().expect("create lock preflight fixture");
        let root = tmp.path().join("workspace");
        fs::create_dir_all(root.join("src")).expect("create fixture source directory");
        fs::write(root.join("src/lib.rs"), "pub fn fixture() {}\n").expect("write fixture source");
        let initial_manifest = concat!(
            "[package]\nname = \"lock-fixture\"\nversion = \"0.0.0\"\nedition = \"2024\"\n",
            "\n[dependencies]\nlock-dependency = { path = \"dependency\" }\n",
        );
        fs::write(root.join("Cargo.toml"), initial_manifest).expect("write fixture manifest");
        let dependency = root.join("dependency");
        fs::create_dir_all(dependency.join("src")).expect("create path dependency");
        fs::write(
            dependency.join("Cargo.toml"),
            "[package]\nname = \"lock-dependency\"\nversion = \"0.0.0\"\nedition = \"2024\"\n",
        )
        .expect("write dependency manifest");
        fs::write(dependency.join("src/lib.rs"), "pub fn dependency() {}\n")
            .expect("write dependency source");

        let cargo = env::var_os("CARGO").unwrap_or_else(|| "cargo".into());
        let cached_target = tmp.path().join("cached-target");
        let command_environment = [
            ("CARGO_TARGET_DIR", Some(cached_target.as_os_str())),
            ("CARGO_BUILD_BUILD_DIR", None),
        ];
        let missing = resolve_cargo_storage(&cargo, &root, &command_environment)
            .expect_err("missing lockfile must fail the locked metadata preflight");
        assert!(
            format!("{missing:#}").contains("--locked"),
            "Cargo must explain the immutable lock failure: {missing:#}"
        );
        assert!(
            !root.join("Cargo.lock").exists(),
            "locked metadata must not create a missing lockfile"
        );

        let mut generate = StdCommand::new(&cargo);
        generate.current_dir(&root).arg("generate-lockfile");
        apply_command_environment(&mut generate, &command_environment);
        clear_sim_coverage_artifact_flags(&mut generate);
        assert!(generate.status().expect("generate baseline lock").success());
        let lock_before = fs::read(root.join("Cargo.lock")).expect("read baseline lock");

        let dependency_two = root.join("dependency-two");
        fs::create_dir_all(dependency_two.join("src")).expect("create second path dependency");
        fs::write(
            dependency_two.join("Cargo.toml"),
            "[package]\nname = \"lock-dependency-two\"\nversion = \"0.0.0\"\nedition = \"2024\"\n",
        )
        .expect("write second dependency manifest");
        fs::write(
            dependency_two.join("src/lib.rs"),
            "pub fn dependency_two() {}\n",
        )
        .expect("write second dependency source");
        fs::write(
            root.join("Cargo.toml"),
            format!("{initial_manifest}lock-dependency-two = {{ path = \"dependency-two\" }}\n"),
        )
        .expect("make fixture lock stale");

        resolve_cargo_storage(&cargo, &root, &command_environment)
            .expect_err("stale lockfile must fail the locked metadata preflight");
        assert_eq!(
            fs::read(root.join("Cargo.lock")).expect("read lock after failed preflight"),
            lock_before,
            "locked metadata must not rewrite a stale lockfile"
        );
    }

    #[test]
    fn sim_coverage_rejects_every_non_empty_artifact_flag_without_printing_values() {
        for rejected in SIM_COVERAGE_ARTIFACT_FLAG_ENV {
            let err = validate_sim_coverage_flag_environment(|name| {
                (name == *rejected).then(|| OsString::from("--object /secret/cached-object"))
            })
            .expect_err("non-empty artifact flag must be rejected");
            let rendered = format!("{err:#}");
            assert!(
                rendered.contains(rejected),
                "diagnostic must name the rejected variable: {rendered}"
            );
            assert!(
                !rendered.contains("/secret/cached-object"),
                "diagnostic must not print the injected value: {rendered}"
            );
        }

        validate_sim_coverage_flag_environment(|_| Some(OsString::new()))
            .expect("empty artifact flags are harmless and are cleared on children");
    }

    #[cfg(unix)]
    #[test]
    fn sim_coverage_isolated_target_omits_unsupported_cargo_build_directory() {
        let tmp = tempfile::tempdir().expect("create temp dir");
        let root = tmp.path().join("workspace");
        fs::create_dir_all(&root).expect("create fake workspace");
        fs::write(root.join("metadata-without-build-directory"), "old Cargo")
            .expect("select old Cargo metadata fixture");
        let cargo = fake_coverage_cargo(&root);

        run_sim_lcov_with_cargo(&root, false, cargo.as_os_str())
            .expect("old Cargo metadata fixture should pass");
        let invocations = fake_coverage_invocations(&root);
        assert_eq!(invocations.len(), 4);
        let isolated_root = invocations[0]
            .llvm_target
            .parent()
            .expect("scratch root")
            .to_path_buf();
        for invocation in invocations {
            assert_eq!(invocation.llvm_build, invocation.llvm_target);
            assert_eq!(invocation.cargo_target, invocation.llvm_target);
            assert_eq!(invocation.cargo_build, None);
            assert!(invocation.artifact_flags.is_empty());
        }
        assert!(
            !isolated_root.exists(),
            "old-Cargo success must still remove the isolated target"
        );
    }

    #[cfg(unix)]
    #[test]
    fn sim_coverage_storage_resolves_final_component_symlinks() {
        use std::os::unix::fs::symlink;

        let tmp = tempfile::tempdir().expect("create symlink storage fixture");
        let root = tmp.path().join("workspace");
        let real_storage = tmp.path().join("real-storage");
        let real_target = real_storage.join("cached-target");
        let real_build = real_storage.join("cached-build");
        fs::create_dir_all(&root).expect("create fake workspace");
        fs::create_dir_all(&real_target).expect("create real target storage");
        fs::create_dir_all(&real_build).expect("create real build storage");
        let configured_target = root.join("configured-target");
        let configured_build = root.join("configured-build");
        symlink(&real_target, &configured_target).expect("symlink final target component");
        symlink(&real_build, &configured_build).expect("symlink final build component");
        fs::write(root.join("metadata-symlink-storage"), "symlink storage")
            .expect("select symlink metadata fixture");
        let cargo = fake_coverage_cargo(&root);

        run_sim_lcov_with_cargo(&root, false, cargo.as_os_str())
            .expect("final-component symlink fixture should pass");
        let isolated_target = assert_fake_coverage_contract(
            &root,
            &fs::canonicalize(&real_storage).expect("resolve real storage"),
            "final-component symlink",
        );
        assert!(
            !isolated_target.exists(),
            "symlink fixture must clean scratch"
        );
        assert!(
            !isolated_target.starts_with(&real_target) && !isolated_target.starts_with(&real_build),
            "scratch must be outside both resolved cache trees"
        );
        assert_eq!(
            resolve_storage_path(&configured_target.join("future/nested"))
                .expect("resolve nearest existing symlink ancestor"),
            real_target.join("future/nested"),
            "missing suffixes must be replayed after resolving their nearest existing ancestor"
        );
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn sim_coverage_rejects_a_mount_root_without_an_outside_cache_sibling() {
        let err = ensure_scratch_parent_filesystem(Path::new("/proc"), Path::new("/"))
            .expect_err("a mount root and its parent are on different filesystems");
        assert!(
            format!("{err:#}").contains("no outside-cache sibling exists"),
            "diagnostic must explain why hermetic scratch placement is impossible: {err:#}"
        );
    }

    #[cfg(unix)]
    #[test]
    fn sim_coverage_isolated_target_ignores_every_default_artifact_poison() {
        let poison_cases = [
            ("profile-only", true, false),
            ("object-only", false, true),
            ("combined", true, true),
        ];

        for (case, poison_profile, poison_object) in poison_cases {
            let tmp = tempfile::tempdir().expect("create temp dir");
            let root = tmp.path().join("workspace");
            let default_target = root.join("target/llvm-cov-target");
            let ui_test_target = root.join("target/ui");
            let trybuild_target = root.join("target/tests/trybuild/debug");
            let trybuild_fallback_target = root.join("target/tests/target/debug");
            fs::create_dir_all(default_target.join("debug/deps"))
                .expect("create poisoned default coverage target");
            for auxiliary in [&ui_test_target, &trybuild_target, &trybuild_fallback_target] {
                fs::create_dir_all(auxiliary).expect("create poisoned auxiliary target");
                fs::write(auxiliary.join("stale-object"), "stale auxiliary object")
                    .expect("poison metadata-derived auxiliary target");
            }
            if poison_profile {
                fs::write(default_target.join("stale.profraw"), "stale profile")
                    .expect("poison default coverage profile");
            }
            if poison_object {
                fs::write(
                    default_target.join("debug/deps/stale-object"),
                    "stale object",
                )
                .expect("poison default coverage object");
            }
            let cargo = fake_coverage_cargo(&root);

            let evidence = run_sim_lcov_with_cargo(&root, false, cargo.as_os_str())
                .unwrap_or_else(|err| panic!("{case} poison selected: {err:#}"));
            assert_eq!(evidence.len(), 2, "{case}");
            assert!(
                evidence
                    .iter()
                    .all(|item| item.lcov == b"TN:current-pass\n")
            );

            let isolated_target =
                assert_fake_coverage_contract(&root, &tmp.path().join("build-storage"), case);
            assert!(
                !isolated_target.starts_with(root.join("target")),
                "{case}: isolated target must sit outside cached target/"
            );
            assert!(
                !isolated_target.exists(),
                "{case}: successful run must remove its isolated target"
            );
            assert_eq!(
                default_target.join("stale.profraw").exists(),
                poison_profile,
                "{case}: default profile poison must remain untouched"
            );
            assert_eq!(
                default_target.join("debug/deps/stale-object").exists(),
                poison_object,
                "{case}: default object poison must remain untouched"
            );
            for auxiliary in [ui_test_target, trybuild_target, trybuild_fallback_target] {
                assert!(
                    auxiliary.join("stale-object").is_file(),
                    "{case}: metadata-derived poison must remain untouched"
                );
            }
        }
    }

    #[cfg(unix)]
    #[test]
    fn sim_coverage_isolated_target_is_removed_after_each_child_failure() {
        let failure_cases = [
            ("moonpool execution", "fail-moonpool-execution", 1_usize),
            ("moonpool report", "fail-moonpool-report", 2_usize),
            ("tokio execution", "fail-tokio-execution", 3_usize),
            ("tokio report", "fail-tokio-report", 4_usize),
        ];

        for (case, failure_marker, expected_invocations) in failure_cases {
            let tmp = tempfile::tempdir().expect("create temp dir");
            let root = tmp.path().join("workspace");
            fs::create_dir_all(&root).expect("create fake workspace");
            fs::write(root.join(failure_marker), "fail").expect("write failure marker");
            let cargo = fake_coverage_cargo(&root);

            let err = run_sim_lcov_with_cargo(&root, false, cargo.as_os_str())
                .expect_err("fake cargo failure must propagate");
            assert!(
                format!("{err:#}").contains("coverage"),
                "{case}: child failure must remain the primary error: {err:#}"
            );

            let invocations = fake_coverage_invocations(&root);
            assert_eq!(invocations.len(), expected_invocations, "{case}");
            assert!(
                invocations.iter().all(|invocation| {
                    invocation.llvm_target == invocation.llvm_build
                        && invocation.llvm_target == invocation.cargo_target
                        && invocation.cargo_build.as_ref() == Some(&invocation.llvm_target)
                        && invocation.artifact_flags.is_empty()
                }),
                "{case}: every reached child must stay in its domain target"
            );
            let isolated_target = &invocations[0].llvm_target;
            assert!(
                !isolated_target.parent().expect("scratch root").exists(),
                "{case}: failed run must remove its isolated target"
            );
        }
    }

    #[cfg(unix)]
    #[test]
    fn sim_coverage_rejects_missing_or_foreign_scratch_reports() {
        for marker in [
            "missing-report-output",
            "foreign-moonpool-report",
            "foreign-tokio-report",
            "preexisting-moonpool-report",
            "preexisting-tokio-report",
        ] {
            let tmp = tempfile::tempdir().expect("create report-integrity fixture");
            let root = tmp.path().join("workspace");
            fs::create_dir_all(&root).expect("create workspace");
            fs::write(root.join(marker), marker).expect("write marker");
            let cargo = fake_coverage_cargo(&root);
            let err = run_sim_lcov_with_cargo(&root, false, cargo.as_os_str())
                .expect_err("invalid scratch report must fail");
            let rendered = format!("{err:#}");
            assert!(
                rendered.contains("did not replace scratch output")
                    || rendered.contains("unexpected pre-report output")
                    || rendered.contains("foreign-domain source"),
                "{marker}: {rendered}"
            );
            let invocations = fake_coverage_invocations(&root);
            assert!(
                !invocations[0]
                    .llvm_target
                    .parent()
                    .expect("scratch root")
                    .exists()
            );
        }
    }

    #[test]
    fn sim_coverage_atomic_publication_replaces_stale_diagnostics() {
        let tmp = tempfile::tempdir().expect("create publication fixture");
        let path = tmp.path().join("sim-coverage.lcov");
        fs::write(&path, "stale\n").expect("poison diagnostic");
        publish_coverage_diagnostic(&path, b"authoritative\n").expect("publish diagnostic");
        assert_eq!(
            fs::read_to_string(&path).expect("read diagnostic"),
            "authoritative\n"
        );
        let leftovers: Vec<_> = fs::read_dir(tmp.path())
            .expect("read diagnostic directory")
            .filter_map(std::result::Result::ok)
            .filter(|entry| entry.path() != path)
            .collect();
        assert!(
            leftovers.is_empty(),
            "atomic publication leaked staging files"
        );
    }

    #[test]
    fn sim_coverage_atomic_publication_is_never_torn_under_concurrency() {
        let tmp = tempfile::tempdir().expect("create concurrent publication fixture");
        let path = std::sync::Arc::new(tmp.path().join("sim-coverage.lcov"));
        let left = vec![b'A'; 128 * 1024];
        let right = vec![b'B'; 128 * 1024];
        let threads: Vec<_> = [left.clone(), right.clone()]
            .into_iter()
            .map(|payload| {
                let path = std::sync::Arc::clone(&path);
                std::thread::spawn(move || publish_coverage_diagnostic(&path, &payload))
            })
            .collect();
        for thread in threads {
            thread
                .join()
                .expect("publisher thread")
                .expect("publish diagnostic");
        }
        let published = fs::read(path.as_ref()).expect("read published diagnostic");
        assert!(
            published == left || published == right,
            "published bytes were torn or mixed"
        );
    }

    #[test]
    fn sim_coverage_isolated_target_cleanup_error_preserves_the_primary_failure() {
        let primary = anyhow!("execution failed first");
        let cleanup = std::io::Error::new(
            std::io::ErrorKind::PermissionDenied,
            "cleanup failed second",
        );
        let err = finish_sim_coverage_run::<()>(
            Err(primary),
            Err(cleanup),
            Path::new("/tmp/coverage-target"),
        )
        .expect_err("combined failure must remain an error");
        let rendered = format!("{err:#}");

        assert!(
            rendered.contains("execution failed first"),
            "primary child failure must remain in the error chain: {rendered}"
        );
        assert!(
            rendered.contains("cleanup failed second"),
            "cleanup failure must be reported beside it: {rendered}"
        );
    }

    #[cfg(unix)]
    fn write_real_coverage_fixture(root: &Path) {
        let packages = [
            "magnetar-proto",
            "magnetar-runtime-tokio",
            "magnetar-runtime-moonpool",
            "magnetar-differential",
            "magnetar-auth-athenz",
            "magnetar-auth-sasl",
        ];
        let members = packages
            .iter()
            .map(|package| format!("    \"crates/{package}\","))
            .collect::<Vec<_>>()
            .join("\n");
        fs::create_dir_all(root).expect("create real-tool fixture root");
        fs::write(
            root.join("Cargo.toml"),
            format!("[workspace]\nresolver = \"2\"\nmembers = [\n{members}\n]\n"),
        )
        .expect("write real-tool workspace manifest");

        for package in packages {
            let package_root = root.join("crates").join(package);
            fs::create_dir_all(package_root.join("src")).expect("create fixture package");
            let dependencies = match package {
                "magnetar-runtime-tokio" => "magnetar-proto = { path = \"../magnetar-proto\" }\n",
                "magnetar-runtime-moonpool" => concat!(
                    "magnetar-proto = { path = \"../magnetar-proto\" }\n",
                    "magnetar-runtime-tokio = { path = \"../magnetar-runtime-tokio\" }\n",
                    "magnetar-auth-athenz = { path = \"../magnetar-auth-athenz\" }\n",
                    "magnetar-auth-sasl = { path = \"../magnetar-auth-sasl\" }\n",
                ),
                "magnetar-differential" => concat!(
                    "magnetar-proto = { path = \"../magnetar-proto\" }\n",
                    "magnetar-runtime-tokio = { path = \"../magnetar-runtime-tokio\" }\n",
                    "magnetar-runtime-moonpool = { path = \"../magnetar-runtime-moonpool\" }\n",
                    "magnetar-auth-athenz = { path = \"../magnetar-auth-athenz\" }\n",
                    "magnetar-auth-sasl = { path = \"../magnetar-auth-sasl\" }\n",
                ),
                _ => "",
            };
            fs::write(
                package_root.join("Cargo.toml"),
                format!(
                    "[package]\nname = \"{package}\"\nversion = \"0.0.0\"\nedition = \
                     \"2024\"\n\n[dependencies]\n{dependencies}"
                ),
            )
            .expect("write fixture package manifest");
            let source = match package {
                "magnetar-proto" => "pub fn value() -> u32 { 1 }\n",
                "magnetar-runtime-tokio" => {
                    "pub fn value() -> u32 { magnetar_proto::value() + 1 }\n"
                }
                "magnetar-auth-athenz" => "pub fn value() -> u32 { 2 }\n",
                "magnetar-auth-sasl" => "pub fn value() -> u32 { 3 }\n",
                "magnetar-runtime-moonpool" => concat!(
                    "pub fn value() -> u32 {\n",
                    "    magnetar_proto::value() + magnetar_runtime_tokio::value()\n",
                    "        + magnetar_auth_athenz::value() + magnetar_auth_sasl::value()\n",
                    "}\n",
                    "#[cfg(test)]\n",
                    "mod tests {\n",
                    "    #[test]\n",
                    "    fn reaches_the_dependency_closure() { assert_eq!(super::value(), 8); }\n",
                    "}\n",
                ),
                "magnetar-differential" => concat!(
                    "pub fn value() -> u32 {\n",
                    "    magnetar_runtime_moonpool::value() + magnetar_runtime_tokio::value()\n",
                    "        + magnetar_proto::value() + magnetar_auth_athenz::value()\n",
                    "        + magnetar_auth_sasl::value()\n",
                    "}\n",
                    "#[cfg(test)]\n",
                    "mod tests {\n",
                    "    #[test]\n",
                    "    fn reaches_both_runners() { assert_eq!(super::value(), 16); }\n",
                    "}\n",
                ),
                _ => unreachable!(),
            };
            fs::write(package_root.join("src/lib.rs"), source).expect("write fixture source");
        }
    }

    #[cfg(unix)]
    fn write_executable_coverage_poison(path: &Path) {
        use std::os::unix::fs::PermissionsExt;

        fs::create_dir_all(path.parent().expect("poison parent")).expect("create poison parent");
        fs::write(path, "not an LLVM coverage object\n").expect("write poison object");
        let mut permissions = fs::metadata(path)
            .expect("read poison metadata")
            .permissions();
        permissions.set_mode(0o700);
        fs::set_permissions(path, permissions).expect("make poison executable");
    }

    #[cfg(unix)]
    fn clear_coverage_environment(command: &mut StdCommand) {
        clear_sim_coverage_artifact_flags(command);
        command
            .env_remove("CARGO_LLVM_COV_TARGET_DIR")
            .env_remove("CARGO_LLVM_COV_BUILD_DIR")
            .env_remove("CARGO_BUILD_BUILD_DIR");
    }

    #[cfg(unix)]
    fn assert_no_real_fixture_scratch(parent: &Path) {
        let leftovers: Vec<_> = fs::read_dir(parent)
            .expect("read scratch parent")
            .filter_map(std::result::Result::ok)
            .filter(|entry| {
                entry
                    .file_name()
                    .to_string_lossy()
                    .starts_with(".sim-coverage-target-")
            })
            .map(|entry| entry.path())
            .collect();
        assert!(
            leftovers.is_empty(),
            "real-tool invocation leaked scratch targets: {leftovers:?}"
        );
    }

    #[cfg(unix)]
    fn assert_real_cargo_llvm_cov_version(cargo: &OsStr) {
        let mut version = StdCommand::new(cargo);
        version.args(["llvm-cov", "--version"]);
        clear_coverage_environment(&mut version);
        let version = version.output().expect("invoke cargo-llvm-cov --version");
        assert!(version.status.success(), "cargo-llvm-cov must be installed");
        assert_eq!(
            String::from_utf8(version.stdout)
                .expect("cargo-llvm-cov version is UTF-8")
                .trim(),
            "cargo-llvm-cov 0.8.7",
            "the fixture pins the reviewed object-discovery implementation"
        );
    }

    #[cfg(unix)]
    fn generate_real_fixture_lock(
        cargo: &OsStr,
        root: &Path,
        metadata_environment: &[(&str, Option<&OsStr>)],
    ) {
        let mut lock = StdCommand::new(cargo);
        lock.current_dir(root).arg("generate-lockfile");
        apply_command_environment(&mut lock, metadata_environment);
        clear_coverage_environment(&mut lock);
        assert!(lock.status().expect("generate fixture lockfile").success());
    }

    #[cfg(unix)]
    #[test]
    fn sim_coverage_real_tool_excludes_cold_and_warm_metadata_artifact_poison() {
        const ENABLE: &str = "MAGNETAR_RUN_CARGO_LLVM_COV_INTEGRATION";
        if env::var_os(ENABLE).as_deref() != Some(OsStr::new("1")) {
            eprintln!(
                "skipping real cargo-llvm-cov fixture; run with {ENABLE}=1 to exercise 0.8.7"
            );
            return;
        }

        let cargo = env::var_os("CARGO").unwrap_or_else(|| "cargo".into());
        assert_real_cargo_llvm_cov_version(&cargo);

        let tmp = tempfile::tempdir().expect("create real-tool fixture temp dir");
        let root = tmp.path().join("workspace");
        let cached_target = tmp.path().join("cached-target");
        write_real_coverage_fixture(&root);
        let metadata_environment = [
            ("CARGO_TARGET_DIR", Some(cached_target.as_os_str())),
            ("CARGO_BUILD_BUILD_DIR", None),
        ];

        generate_real_fixture_lock(&cargo, &root, &metadata_environment);

        let primary = cached_target.join("llvm-cov-target");
        fs::create_dir_all(primary.join("debug")).expect("create primary poison target");
        fs::write(primary.join("stale.profraw"), "invalid raw profile")
            .expect("poison primary profile");
        write_executable_coverage_poison(&primary.join("debug/magnetar_proto-deadbeef"));
        let ui_poison = cached_target.join("ui/stale-ui-object");
        write_executable_coverage_poison(&ui_poison);
        let trybuild_root = cached_target.join("tests/trybuild");
        let trybuild_package = trybuild_root.join("poison-package");
        fs::create_dir_all(trybuild_package.join("src")).expect("create trybuild poison package");
        fs::write(
            trybuild_package.join("Cargo.toml"),
            "[package]\nname = \"poison-trybuild\"\nversion = \"0.0.0\"\nedition = \"2024\"\n",
        )
        .expect("write trybuild poison manifest");
        fs::write(trybuild_package.join("src/lib.rs"), "pub fn poison() {}\n")
            .expect("write trybuild poison source");
        let trybuild_poison = trybuild_root.join("debug/stale-trybuild-object");
        write_executable_coverage_poison(&trybuild_poison);

        let run = || {
            let evidence = run_sim_lcov_with_cargo_environment(
                &root,
                false,
                cargo.as_os_str(),
                &metadata_environment,
            )
            .expect("isolated real-tool coverage run");
            assert!(
                evidence
                    .iter()
                    .all(|item| String::from_utf8_lossy(&item.lcov).contains("SF:")),
                "fixture must produce a real LCOV report"
            );
            assert!(
                String::from_utf8_lossy(&evidence[0].lcov).contains("magnetar-proto/src/lib.rs"),
                "dependency coverage must survive isolation"
            );
            assert_no_real_fixture_scratch(tmp.path());
        };

        // Cold current-pass build against a default target containing only poison.
        run();

        // Populate the default llvm-cov target with real warm objects/profiles;
        // the invalid primary and metadata-derived artifacts remain beside them.
        let mut warm = StdCommand::new(&cargo);
        warm.current_dir(&root).args([
            "llvm-cov",
            "--no-report",
            "--workspace",
            "--locked",
            "--quiet",
        ]);
        warm.env("CARGO_TARGET_DIR", &cached_target);
        clear_coverage_environment(&mut warm);
        assert!(
            warm.status()
                .expect("warm default coverage target")
                .success()
        );
        assert!(
            fs::read_dir(&primary)
                .expect("read warmed profile target")
                .filter_map(std::result::Result::ok)
                .any(|entry| {
                    entry.path().extension() == Some(OsStr::new("profraw"))
                        && entry.file_name() != OsStr::new("stale.profraw")
                }),
            "warm fixture must contain a real current profile beside the poison"
        );
        assert!(
            primary.join("debug/deps").is_dir(),
            "warm fixture must contain real compiled objects"
        );

        // Two consecutive invocations prove a warm cached tree and a prior
        // isolated pass cannot become inputs to the next current-pass report.
        run();
        run();

        for poison in [
            primary.join("stale.profraw"),
            primary.join("debug/magnetar_proto-deadbeef"),
            ui_poison,
            trybuild_poison,
        ] {
            assert!(
                poison.is_file(),
                "fixture poison was unexpectedly consumed: {poison:?}"
            );
        }
    }

    /// The defect this reporting exists for: `run_sim_lcov`'s domains
    /// deliberately omit
    /// `magnetar-admin`, so its files emit no LCOV records at all. Before
    /// ADR-0088 those lines were indistinguishable from non-executable ones and
    /// passed silently.
    #[test]
    fn sim_coverage_reports_an_admin_file_the_runner_never_instrumented() {
        let root = Path::new("/ws");
        let tracked = tracked_of(&[
            ("crates/magnetar-admin/src/lib.rs", &[10, 11, 12]),
            ("crates/magnetar-proto/src/conn.rs", &[40]),
        ]);
        let covered = coverage_of(root, &[("crates/magnetar-proto/src/conn.rs", &[40], &[40])]);

        assert_eq!(
            uninstrumented_files(root, &tracked, &covered),
            vec![("crates/magnetar-admin/src/lib.rs".to_owned(), 3)],
            "a file with no LCOV entry must be reported as ungated, not counted as covered"
        );
        assert!(
            intersect_diff_with_coverage(root, &tracked, &covered).is_empty(),
            "the instrumented file is fully hit, so nothing may fail"
        );
    }

    /// An instrumented-but-unhit line is a genuine gate failure and must NOT
    /// be reclassified as merely ungated — the two paths stay disjoint.
    #[test]
    fn sim_coverage_keeps_instrumented_misses_failing() {
        let root = Path::new("/ws");
        let tracked = tracked_of(&[("crates/magnetar-proto/src/conn.rs", &[40, 41])]);
        let covered = coverage_of(
            root,
            &[("crates/magnetar-proto/src/conn.rs", &[40, 41], &[40])],
        );

        assert!(
            uninstrumented_files(root, &tracked, &covered).is_empty(),
            "the file WAS instrumented, so it is in scope"
        );
        assert_eq!(
            intersect_diff_with_coverage(root, &tracked, &covered),
            vec![("crates/magnetar-proto/src/conn.rs".to_owned(), 41)]
        );
    }

    #[test]
    fn sim_coverage_gates_panic_and_placeholder_macro_lines() {
        let diff = "+++ b/crates/magnetar-runtime-tokio/src/client.rs\n@@ -0,0 +1,3 @@\n+unreachable!();\n+unimplemented!();\n+todo!();\n";
        let added = parse_diff_added_lines(diff);
        assert_eq!(
            added["crates/magnetar-runtime-tokio/src/client.rs"],
            [1, 2, 3].into_iter().collect(),
            "executable panic and placeholder macros have no lexical coverage exemption"
        );
    }

    #[test]
    fn sim_coverage_domains_cannot_discharge_each_other() {
        let tracked = tracked_of(&[
            ("crates/magnetar-proto/src/conn.rs", &[1]),
            ("crates/magnetar-runtime-tokio/src/client.rs", &[2]),
        ]);
        assert_eq!(
            domain_tracked(coverage_domain("moonpool"), &tracked).len(),
            1
        );
        assert_eq!(domain_tracked(coverage_domain("tokio"), &tracked).len(), 1);
        assert!(
            domain_tracked(coverage_domain("moonpool"), &tracked)[0]
                .0
                .contains("proto")
        );
        assert!(
            domain_tracked(coverage_domain("tokio"), &tracked)[0]
                .0
                .contains("tokio")
        );
    }

    /// Both classes can occur in one diff; each must be routed to its own
    /// report rather than one swallowing the other.
    #[test]
    fn sim_coverage_separates_ungated_files_from_uncovered_lines() {
        let root = Path::new("/ws");
        let tracked = tracked_of(&[
            ("crates/magnetar-admin/src/lib.rs", &[7]),
            ("crates/magnetar-runtime-moonpool/src/driver.rs", &[80, 81]),
        ]);
        let covered = coverage_of(
            root,
            &[(
                "crates/magnetar-runtime-moonpool/src/driver.rs",
                &[80, 81],
                &[80],
            )],
        );

        assert_eq!(
            uninstrumented_files(root, &tracked, &covered),
            vec![("crates/magnetar-admin/src/lib.rs".to_owned(), 1)]
        );
        assert_eq!(
            intersect_diff_with_coverage(root, &tracked, &covered),
            vec![(
                "crates/magnetar-runtime-moonpool/src/driver.rs".to_owned(),
                81
            )]
        );
    }

    /// `magnetar-proto` IS in the reported closure, so a diff touching it while
    /// the report carries not one `magnetar-proto` record does not mean "out of
    /// scope" — it means nothing linked the crate into its evidence-domain
    /// binaries, or the report step produced nothing. `llvm-cov` reports neither, and every added
    /// line would read as non-executable and pass. That must fail, not print an
    /// advisory.
    #[test]
    fn sim_coverage_fails_when_a_gated_crate_reached_the_report_not_at_all() {
        let root = Path::new("/ws");
        let tracked = tracked_of(&[("crates/magnetar-proto/src/producer.rs", &[10, 11])]);
        // Records exist — but none of them are `magnetar-proto`'s.
        let covered = coverage_of(
            root,
            &[("crates/magnetar-runtime-moonpool/src/driver.rs", &[7], &[7])],
        );

        assert!(
            silent_gated_prefixes_for(coverage_domain("moonpool").gated_prefixes, &covered)
                .contains(&"crates/magnetar-proto/src/"),
            "a gated crate with zero records is the broken-run signal"
        );
        let domain = coverage_domain("moonpool");
        let (missing_gated, ungated) =
            classify_uninstrumented_for(domain, root, &tracked, &covered);
        assert_eq!(
            missing_gated,
            vec![("crates/magnetar-proto/src/producer.rs".to_owned(), 2)],
            "a gated crate emitting no records is a broken run, not a scope limit"
        );
        assert!(
            ungated.is_empty(),
            "it must not be routed to the advisory bucket"
        );
        assert!(
            report_missing_gated(domain, &missing_gated).is_err(),
            "the missing-gated report must fail the check"
        );
    }

    /// A sibling `SF:` record proves only that the crate reached the report.
    /// It must not let a function-bearing file pass as advisory: that file has
    /// executable source but no coverage mapping, so the gate cannot measure
    /// its additions.
    #[test]
    fn sim_coverage_fails_a_function_bearing_file_omitted_from_a_reported_crate() {
        let tmp = tempfile::tempdir().expect("create fixture root");
        let root = tmp.path();
        let src = root.join("crates/magnetar-proto/src");
        fs::create_dir_all(&src).expect("create fixture source directory");
        fs::write(
            src.join("omitted.rs"),
            "// fn in_a_comment() {}\n\
             const TEXT: &str = \"fn in_a_string() {}\";\n\
             #[cfg(test)]\n\
             fn test_helper() {}\n\
             pub fn production() -> u8 { 1 }\n",
        )
        .expect("write executable fixture");
        fs::write(src.join("sibling.rs"), "pub fn covered() {}\n").expect("write covered sibling");

        let tracked = tracked_of(&[("crates/magnetar-proto/src/omitted.rs", &[5])]);
        let covered = coverage_of(
            root,
            &[("crates/magnetar-proto/src/sibling.rs", &[1], &[1])],
        );

        assert!(has_non_test_function_body(
            &fs::read_to_string(src.join("omitted.rs")).expect("read executable fixture")
        ));
        let domain = coverage_domain("moonpool");
        let (missing_gated, advisory) =
            classify_uninstrumented_for(domain, root, &tracked, &covered);
        assert_eq!(
            missing_gated,
            vec![("crates/magnetar-proto/src/omitted.rs".to_owned(), 1)],
            "a production function without an SF: record is unmeasured, even when its crate reported"
        );
        assert!(advisory.is_empty());
        assert!(report_missing_gated(domain, &missing_gated).is_err());
    }

    /// Anonymous functions receive coverage mappings too. A record-less file
    /// whose only executable item is a closure-backed static must not be
    /// mistaken for a data-only constants module.
    #[test]
    fn sim_coverage_fails_a_closure_bearing_static_omitted_from_a_reported_crate() {
        let tmp = tempfile::tempdir().expect("create fixture root");
        let root = tmp.path();
        let src = root.join("crates/magnetar-proto/src");
        fs::create_dir_all(&src).expect("create fixture source directory");
        fs::write(
            src.join("callback.rs"),
            "pub const HANDLER: fn() = || {};\n",
        )
        .expect("write closure fixture");
        fs::write(src.join("sibling.rs"), "pub fn covered() {}\n").expect("write covered sibling");

        let tracked = tracked_of(&[("crates/magnetar-proto/src/callback.rs", &[1])]);
        let covered = coverage_of(
            root,
            &[("crates/magnetar-proto/src/sibling.rs", &[1], &[1])],
        );

        assert!(has_non_test_function_body(
            &fs::read_to_string(src.join("callback.rs")).expect("read closure fixture")
        ));
        let (missing_gated, advisory) =
            classify_uninstrumented_for(coverage_domain("moonpool"), root, &tracked, &covered);
        assert_eq!(
            missing_gated,
            vec![("crates/magnetar-proto/src/callback.rs".to_owned(), 1)]
        );
        assert!(advisory.is_empty());
    }

    /// LLVM derives its coverage mapping from per-function records, so a file
    /// containing only modules, exports, constants, and bodyless declarations
    /// emits no `SF:` record even though its crate is fully instrumented. A
    /// per-file rule that ignores executability would hard-fail that diff with
    /// no possible remedy.
    #[test]
    fn sim_coverage_keeps_a_non_executable_file_advisory_when_its_crate_reported() {
        let tmp = tempfile::tempdir().expect("create fixture root");
        let root = tmp.path();
        let src = root.join("crates/magnetar-proto/src");
        fs::create_dir_all(&src).expect("create fixture source directory");
        fs::write(
            src.join("surface.rs"),
            "pub mod child;\n\
             pub use child::Thing;\n\
             pub const LIMIT: usize = 1;\n\
             pub const HANDLER: fn() = callback;\n\
             pub const MASK: u8 = LEFT | RIGHT;\n\
             pub trait Contract { fn required(&self); }\n\
             unsafe extern \"C\" { fn foreign(); }\n\
             #[cfg(test)]\n\
             fn test_helper() {}\n\
             #[cfg(test)]\n\
             pub const TEST_HANDLER: fn() = || {};\n",
        )
        .expect("write non-executable fixture");
        fs::write(src.join("sibling.rs"), "pub fn covered() {}\n").expect("write covered sibling");

        let tracked = tracked_of(&[("crates/magnetar-proto/src/surface.rs", &[1, 2, 3])]);
        // A covered sibling in the SAME crate: the crate did reach the report.
        let covered = coverage_of(
            root,
            &[("crates/magnetar-proto/src/sibling.rs", &[1], &[1])],
        );

        assert!(
            !silent_gated_prefixes_for(coverage_domain("moonpool").gated_prefixes, &covered)
                .contains(&"crates/magnetar-proto/src/"),
            "the crate emitted records, so it is not the broken-run signal"
        );
        assert!(
            !has_non_test_function_body(
                &fs::read_to_string(src.join("surface.rs")).expect("read non-executable fixture")
            ),
            "data-only constants, bodyless declarations, and test-only closures are not executable production functions"
        );
        let (missing_gated, ungated) =
            classify_uninstrumented_for(coverage_domain("moonpool"), root, &tracked, &covered);
        assert!(
            missing_gated.is_empty(),
            "a genuinely non-executable file must not hard-fail"
        );
        assert_eq!(
            ungated,
            vec![("crates/magnetar-proto/src/surface.rs".to_owned(), 3)],
            "it stays visible on the advisory path"
        );
    }

    /// The five function-less files that exist in the gated crates today. Each
    /// is a real path in this workspace with no non-test function body, so each
    /// legitimately emits no `SF:` record — pinned here so a future scanner
    /// regression trips this test instead of a routine commit.
    #[test]
    fn sim_coverage_never_hard_fails_the_known_function_less_files() {
        let root = workspace_root().expect("resolve workspace root");
        let function_less = [
            "crates/magnetar-proto/src/lib.rs",
            "crates/magnetar-proto/src/trackers/mod.rs",
            "crates/magnetar-differential/src/lib.rs",
            "crates/magnetar-runtime-moonpool/src/crypto.rs",
            "crates/magnetar-runtime-tokio/src/crypto.rs",
        ];
        // Every gated crate reported at least one file, which is the state a
        // healthy run is in.
        let covered = coverage_of(
            &root,
            &[
                ("crates/magnetar-proto/src/conn.rs", &[1], &[1]),
                ("crates/magnetar-runtime-tokio/src/client.rs", &[1], &[1]),
                ("crates/magnetar-runtime-moonpool/src/driver.rs", &[1], &[1]),
                ("crates/magnetar-differential/src/trace.rs", &[1], &[1]),
                ("crates/magnetar-auth-sasl/src/lib.rs", &[1], &[1]),
                ("crates/magnetar-auth-athenz/src/lib.rs", &[1], &[1]),
                ("crates/magnetar/src/scalable.rs", &[1], &[1]),
                ("crates/magnetar-fakes/src/m1.rs", &[1], &[1]),
            ],
        );
        assert!(
            SIM_COVERAGE_DOMAINS.iter().all(|domain| {
                silent_gated_prefixes_for(domain.gated_prefixes, &covered).is_empty()
            }),
            "the fixture must model a healthy report, or the assertion below is vacuous"
        );

        for path in function_less {
            let tracked = tracked_of(&[(path, &[1])]);
            let domain = SIM_COVERAGE_DOMAINS
                .iter()
                .find(|domain| {
                    domain
                        .gated_prefixes
                        .iter()
                        .any(|prefix| path.starts_with(prefix))
                })
                .expect("function-less path must have a production domain");
            let (missing_gated, ungated) =
                classify_uninstrumented_for(domain, &root, &tracked, &covered);
            assert!(
                missing_gated.is_empty(),
                "{path} has no non-test function body, so it can never emit an \
                 SF: record — hard-failing it would block a routine diff forever"
            );
            assert_eq!(ungated, vec![(path.to_owned(), 1)]);
        }
    }

    /// `report_record_less` is the single place the two buckets are turned into
    /// output and into an exit code. Pin both halves: a non-empty gated bucket
    /// must produce `Err` (otherwise `check_sim_coverage` runs on to its
    /// success sentence), and a gated bucket alongside a non-empty advisory one
    /// must still fail — the advisory does not absorb it.
    #[test]
    fn sim_coverage_record_less_report_fails_only_on_the_gated_bucket() {
        let advisory = vec![("crates/magnetar-admin/src/lib.rs".to_owned(), 3)];
        let gated = vec![("crates/magnetar-proto/src/producer.rs".to_owned(), 2)];

        assert!(
            report_record_less_for(coverage_domain("moonpool"), &(Vec::new(), Vec::new())).is_ok(),
            "nothing record-less at all is a pass"
        );
        assert!(
            report_record_less_for(coverage_domain("moonpool"), &(Vec::new(), advisory.clone()))
                .is_ok(),
            "an advisory-only diff must still exit 0 — ADR-0088 scope limit"
        );
        assert!(
            report_record_less_for(coverage_domain("moonpool"), &(gated.clone(), Vec::new()))
                .is_err(),
            "a silent gated crate must fail the check"
        );
        assert!(
            report_record_less_for(coverage_domain("moonpool"), &(gated, advisory)).is_err(),
            "the advisory bucket must not swallow the hard failure"
        );
    }

    #[test]
    fn sim_coverage_record_less_failure_names_its_domain_diagnostic() {
        let missing = vec![("crates/owned/src/lib.rs".to_owned(), 1)];
        for (name, diagnostic) in [
            ("moonpool", "target/sim-coverage.lcov"),
            ("tokio", "target/tokio-coverage.lcov"),
        ] {
            let error = report_missing_gated(coverage_domain(name), &missing)
                .expect_err("record-less owned source must fail");
            assert!(
                format!("{error:#}").contains(diagnostic),
                "{name} failure must point to {diagnostic}"
            );
        }
    }

    /// Split one crate manifest's `magnetar-*` dependency entries into
    /// `(normal, dev)`. Non-optional `[dependencies]` and
    /// `[build-dependencies]` are compiled for every node of the closure;
    /// `[dev-dependencies]` only for the packages whose test targets are
    /// actually built. An unselected `optional = true` declaration is not a
    /// closure edge. Feature strings such as
    /// `scalable-topics = ["magnetar-proto/scalable-topics"]` live under
    /// `[features]` and are ignored by the section match.
    fn manifest_magnetar_deps(manifest: &str) -> (Vec<String>, Vec<String>) {
        let mut normal = Vec::new();
        let mut dev = Vec::new();
        let mut section = String::new();
        for line in manifest.lines() {
            let trimmed = line.trim();
            if trimmed.starts_with('[') {
                section = trimmed.to_owned();
                continue;
            }
            let Some((name, value)) = trimmed.split_once('=') else {
                continue;
            };
            let name = name.trim();
            if !name.starts_with("magnetar-") {
                continue;
            }
            match section.as_str() {
                "[dependencies]" | "[build-dependencies]" if !value.contains("optional = true") => {
                    normal.push(name.to_owned());
                }
                "[dev-dependencies]" => dev.push(name.to_owned()),
                _ => {}
            }
        }
        (normal, dev)
    }

    /// `-p` on `cargo llvm-cov report` cannot widen a report: it only drops a
    /// package's manifest directory from the `-ignore-filename-regex`. Object
    /// files are walked out of the target directory, so a package cargo never
    /// compiled has no coverage mapping and naming it here adds nothing — while
    /// `report_ungated` still prints the list as "the reported closure", telling
    /// the operator a file sits outside a closure that names its own crate.
    ///
    /// So the constant must equal the closure step 1 really builds: the two
    /// `-p` roots, their dev-dependencies (their test targets are what runs),
    /// and the normal dependencies reachable from there. Computed from the
    /// manifests rather than restated, so adding `magnetar-admin` as a selected
    /// runner dependency trips here while its unselected optional declaration
    /// on the façade does not.
    #[test]
    fn sim_coverage_domains_assign_packages_to_their_evidence_owner() {
        let root = workspace_root().expect("resolve workspace root");
        let manifest_of = |package: &str| {
            // The published package is `magnetar-driver`, but its workspace
            // directory and library/import name remain `magnetar`.
            let directory = match package {
                "magnetar-driver" => "magnetar",
                package => package,
            };
            let path = root.join("crates").join(directory).join("Cargo.toml");
            fs::read_to_string(&path)
                .unwrap_or_else(|err| panic!("reading {}: {err}", path.display()))
        };

        // Step 1 runs these two packages' test targets, so their
        // dev-dependencies are compiled; nothing deeper contributes its own.
        let roots = ["magnetar-runtime-moonpool", "magnetar-differential"];
        let mut closure: std::collections::BTreeSet<String> =
            roots.iter().map(|p| (*p).to_owned()).collect();
        let mut queue: Vec<(String, bool)> =
            roots.iter().map(|p| ((*p).to_owned(), true)).collect();
        while let Some((package, with_dev)) = queue.pop() {
            let (normal, dev) = manifest_magnetar_deps(&manifest_of(&package));
            let reachable = normal
                .into_iter()
                .chain(if with_dev { dev } else { Vec::new() });
            for dep in reachable {
                if closure.insert(dep.clone()) {
                    queue.push((dep, false));
                }
            }
        }

        let compiled: std::collections::BTreeSet<String> = closure;
        let moonpool: std::collections::BTreeSet<String> = MOONPOOL_COVERAGE_REPORT_PACKAGES
            .iter()
            .map(|p| (*p).to_owned())
            .collect();
        let tokio: std::collections::BTreeSet<String> = TOKIO_COVERAGE_REPORT_PACKAGES
            .iter()
            .map(|p| (*p).to_owned())
            .collect();
        assert!(moonpool.is_subset(&compiled));
        assert!(moonpool.contains("magnetar-driver"));
        assert!(moonpool.contains("magnetar-fakes"));
        assert!(!moonpool.contains("magnetar-runtime-tokio"));
        assert_eq!(
            tokio,
            ["magnetar-runtime-tokio".to_owned()].into_iter().collect()
        );
        assert!(
            moonpool.is_disjoint(&tokio),
            "one package cannot be discharged by two domains"
        );
    }

    /// The hard-gated source prefixes must name exactly the reported package
    /// closure. Omitting a reported package would silently make its record-less
    /// files advisory; adding an uncompiled package would hard-fail a scope
    /// limit. The façade's package/directory alias is resolved explicitly.
    #[test]
    fn sim_coverage_gated_prefixes_are_exactly_the_reported_packages() {
        let gated: std::collections::BTreeSet<&str> = all_coverage_gated_prefixes()
            .into_iter()
            .map(|prefix| {
                let directory = prefix
                    .strip_prefix("crates/")
                    .and_then(|rest| rest.strip_suffix("/src/"))
                    .unwrap_or_else(|| {
                        panic!("gated prefix `{prefix}` is not `crates/<pkg>/src/`")
                    });
                match directory {
                    "magnetar" => "magnetar-driver",
                    package => package,
                }
            })
            .collect();
        let reported: std::collections::BTreeSet<&str> = SIM_COVERAGE_DOMAINS
            .iter()
            .flat_map(|domain| domain.report_packages.iter().copied())
            .collect();

        assert_eq!(gated, reported);
        assert!(gated.contains("magnetar-driver"));
        assert!(gated.contains("magnetar-fakes"));
        assert!(!gated.contains("magnetar-admin"));
    }

    /// A `#[cfg(test)]` item near the top of a file must not exempt the
    /// production code below it.
    ///
    /// The regression this pins was measured, not imagined. Until ADR-0092 the
    /// gate cut at the file's first `#[cfg(test)]` line and dropped everything
    /// after it, which exempted 48% of all gated lines and 71% of the gated
    /// lines added over ten merged pull requests — worst case
    /// `magnetar-runtime-tokio/src/driver.rs`, where a `#[cfg(test)] use` on
    /// line 48 exempted the remaining 2781 of 2828 lines. The shape below is
    /// that file in miniature.
    #[test]
    fn cfg_test_semantics_are_shared_by_clock_log_and_coverage_scanners() {
        let contents = r#"
#[cfg(not(test))]
fn production_not_test() {
    let _ = Instant::now();
    warn!("production not(test)");
}

#[cfg(feature = "contest")]
fn production_contest_feature() {
    let _ = Instant::now();
    warn!("production contest feature");
}

#[cfg(any(test, feature = "contest"))]
fn production_any_test_or_feature() {
    let _ = Instant::now();
    warn!("production any(test, feature)");
}

#[cfg(test)]
fn exact_test_only() {
    let _ = Instant::now();
    warn!("test only");
    let inert = "{";
    // }
}

pub fn production_after_inert_braces() {
    let _ = Instant::now();
    warn!("production after inert braces");
}

#[cfg(all(
    test,
    feature = "fixture",
))]
fn all_test_only() {
    let _ = Instant::now();
    warn!("all(test, ...) only");
}
"#;
        let line = |needle: &str| {
            contents
                .lines()
                .position(|candidate| candidate.contains(needle))
                .map_or_else(
                    || panic!("fixture line `{needle}` exists"),
                    |index| index + 1,
                )
        };
        let production_log_lines: std::collections::BTreeSet<_> = [
            line("production not(test)"),
            line("production contest feature"),
            line("production any(test, feature)"),
            line("production after inert braces"),
        ]
        .into_iter()
        .collect();
        let production_clock_lines: std::collections::BTreeSet<_> =
            production_log_lines.iter().map(|line| line - 1).collect();
        let test_log_lines: std::collections::BTreeSet<_> =
            [line("test only"), line("all(test, ...) only")]
                .into_iter()
                .collect();
        let test_clock_lines: std::collections::BTreeSet<_> =
            test_log_lines.iter().map(|line| line - 1).collect();

        let flags = cfg_test_line_flags(contents);
        let coverage_flags = sim_coverage_cfg_test_lines(contents);
        for production_line in production_clock_lines.iter().chain(&production_log_lines) {
            assert!(!flags[*production_line - 1]);
            assert!(!coverage_flags.contains(&(*production_line as u32)));
        }
        for test_line in test_clock_lines.iter().chain(&test_log_lines) {
            assert!(flags[*test_line - 1]);
            assert!(coverage_flags.contains(&(*test_line as u32)));
        }

        let clock_lines: std::collections::BTreeSet<_> = scan_clock_violations(contents)
            .into_iter()
            .map(|(line, _)| line)
            .collect();
        assert_eq!(clock_lines, production_clock_lines);

        let log_lines: std::collections::BTreeSet<_> = scan_log_field_violations(contents)
            .into_iter()
            .map(|(line, _, _)| line)
            .collect();
        assert_eq!(log_lines, production_log_lines);
        assert!(test_log_lines.is_disjoint(&log_lines));
    }

    #[test]
    fn sim_coverage_cfg_test_import_does_not_exempt_the_rest_of_the_file() {
        let contents = "\
use std::io::Write;

#[cfg(test)]
use std::io::IoSlice;

pub fn production_one() -> u32 {
    1
}

#[cfg(test)]
fn test_helper() -> u32 {
    2
}

pub fn production_two() -> u32 {
    3
}

#[cfg(test)]
mod tests {
    #[test]
    fn t() {}
}
";
        // Assert on what the gate actually applies, not on the underlying
        // scanner — the scanner was always correct; it was this gate that
        // reinvented a worse one.
        let cfg_test = sim_coverage_cfg_test_lines(contents);
        let gated = |line: u32| cfg_test.contains(&line);

        // The two gated non-`mod` items are excluded, and nothing else is.
        assert!(
            gated(3) && gated(4),
            "the `#[cfg(test)] use` itself is test-only"
        );
        assert!(
            gated(11) && gated(12) && gated(13),
            "the gated helper is test-only"
        );
        assert!(
            gated(20) && gated(23),
            "the bottom `mod tests` is test-only"
        );

        // The production code BELOW the first `#[cfg(test)]` survives. Under
        // the old first-line cut every one of these was silently exempt.
        for line in [1, 6, 7, 8, 16, 17, 18] {
            assert!(
                !gated(line),
                "line {line} is production code and must stay gated by \
                 sim coverage; a `#[cfg(test)]` item above it must not exempt it"
            );
        }
    }

    /// The uncovered-line verdict is ENFORCED, and both arms behave as their
    /// messages claim.
    ///
    /// Nothing else pins this. The per-PR `check-sim-coverage` job in
    /// `.github/workflows/ci.yml` passes `--enforce`, which ORs into the
    /// constant, so CI would stay green through a silent revert of ADR-0092 to
    /// ADR-0090's advisory landing and every *other* caller — the local
    /// validation chain in `CLAUDE.md`, the scheduled `xtask-gates.yml` job —
    /// would quietly stop failing. That is exactly the fail-open shape ADR-0088
    /// was written to stop, so it gets a test rather than a comment.
    #[test]
    fn sim_coverage_enforces_uncovered_by_default() {
        // A `const` block, so reverting the constant breaks compilation rather
        // than waiting for someone to run the test. `assert!` on a constant is
        // a clippy error otherwise (`assertions_on_constants`), and the lint is
        // right: the compile-time form is the stronger tripwire.
        const {
            assert!(
                SIM_COVERAGE_ENFORCES_UNCOVERED,
                "ADR-0092 enforces uncovered added lines; flipping this back to \
                 `false` reverts the gate to ADR-0090's advisory landing, where \
                 a green run proves only that the gate ran. Write an ADR \
                 superseding ADR-0092 before changing it."
            );
        }

        // The helper's semantics: an invocation with no flag still enforces.
        // This does NOT catch the call site being cut — verified 2026-08-01,
        // `let enforcing = enforce;` keeps this test green. What catches that
        // is `dead_code` under `-D warnings`, since the constant and the helper
        // both become unreachable from production code. See the note on
        // `sim_coverage_enforcing`.
        assert!(
            sim_coverage_enforcing(false),
            "an invocation with no `--enforce` must still enforce; the flag is \
             redundant since ADR-0092, not load-bearing"
        );
        assert!(sim_coverage_enforcing(true), "`--enforce` can only tighten");

        let root = Path::new("/ws");
        let uncovered = [("crates/magnetar-proto/src/conn.rs".to_owned(), 41)];

        assert!(
            report_uncovered_domain("test", root, &uncovered, true).is_err(),
            "an uncovered added line must fail the check when enforcing"
        );
        assert!(
            report_uncovered_domain("test", root, &uncovered, false).is_ok(),
            "the advisory arm must still exit 0 — it is what the constant \
             selects, and keeping it working is what makes a revert one line"
        );
    }

    /// `magnetar-admin` is deliberately outside every domain's report packages,
    /// so a record-less admin file stays
    /// advisory and the check still exits 0. The façade and fakes are the
    /// counterexample: differential public-aggregate tests now compile both and
    /// their source prefixes are hard-gated.
    #[test]
    fn sim_coverage_keeps_an_admin_file_with_no_records_advisory() {
        let root = Path::new("/ws");
        let tracked = tracked_of(&[("crates/magnetar-admin/src/lib.rs", &[10])]);
        let covered = coverage_of(root, &[]);

        let (missing_gated, ungated) =
            classify_uninstrumented_for(coverage_domain("moonpool"), root, &tracked, &covered);
        assert!(
            missing_gated.is_empty(),
            "magnetar-admin is not a gated crate — it must not hard-fail"
        );
        assert_eq!(
            ungated,
            vec![("crates/magnetar-admin/src/lib.rs".to_owned(), 1)]
        );
        // `report_ungated` returns `()`: advisory only, the check exits 0.
        report_ungated_for(coverage_domain("moonpool"), &ungated);
    }

    /// Generated proto is dropped diff-side by `SIM_COVERAGE_EXCLUDE_PREFIXES`,
    /// so it reaches neither bucket. Without that it would land in the
    /// hard-failing one — `crates/magnetar-proto/src/` is a gated prefix, and
    /// the report explicitly filters `src/pb/` back out.
    #[test]
    fn sim_coverage_never_buckets_generated_proto() {
        let root = Path::new("/ws");
        assert!(
            is_sim_coverage_excluded("crates/magnetar-proto/src/pb/pulsar_api.rs"),
            "generated proto must be dropped before the bucket split"
        );

        let tracked: Vec<_> = tracked_of(&[
            ("crates/magnetar-proto/src/pb/pulsar_api.rs", &[10, 11]),
            ("crates/magnetar-proto/src/conn.rs", &[40]),
        ])
        .into_iter()
        .filter(|(relpath, _)| !is_sim_coverage_excluded(relpath))
        .collect();
        let covered = coverage_of(root, &[("crates/magnetar-proto/src/conn.rs", &[40], &[40])]);

        let (missing_gated, ungated) =
            classify_uninstrumented_for(coverage_domain("moonpool"), root, &tracked, &covered);
        assert!(
            missing_gated.is_empty(),
            "generated proto must not hard-fail"
        );
        assert!(ungated.is_empty(), "generated proto must not be advisory");
        assert!(intersect_diff_with_coverage(root, &tracked, &covered).is_empty());
    }

    /// Regression: the diff side keys on `workspace_root.join(relpath)` while
    /// the LCOV side keys on whatever `llvm-cov` printed after `SF:`. Reaching
    /// the checkout through a symlink is enough to make the two spellings
    /// diverge — and a divergence does not fail loudly, it degrades EVERY file
    /// to "no LCOV record" and passes the whole gate. Both sides go through
    /// `coverage_key`, which canonicalizes.
    #[cfg(unix)]
    #[test]
    fn sim_coverage_matches_lcov_paths_through_a_symlinked_checkout() {
        use std::os::unix::fs::symlink;

        let tmp = tempfile::tempdir().expect("create temp dir");
        // The checkout `llvm-cov` walked: `SF:` carries this spelling.
        let real_root = tmp.path().join("real-checkout");
        let src_dir = real_root.join("crates/magnetar-proto/src");
        fs::create_dir_all(&src_dir).expect("create source tree");
        let real_file = src_dir.join("conn.rs");
        fs::write(&real_file, "fn covered() {}\nfn missed() {}\n").expect("write source");

        // The spelling the gate was invoked through.
        let link_root = tmp.path().join("linked-checkout");
        symlink(&real_root, &link_root).expect("symlink the checkout");

        let lcov = format!(
            "SF:{}\nDA:1,1\nDA:2,0\nend_of_record\n",
            real_file.display()
        );
        let covered = parse_lcov_coverage(&lcov);
        let tracked = tracked_of(&[("crates/magnetar-proto/src/conn.rs", &[1, 2])]);

        assert!(
            uninstrumented_files(&link_root, &tracked, &covered).is_empty(),
            "the file IS instrumented — a symlinked checkout must not read as \
             'no LCOV record'"
        );
        assert_eq!(
            intersect_diff_with_coverage(&link_root, &tracked, &covered),
            vec![("crates/magnetar-proto/src/conn.rs".to_owned(), 2)],
            "the uncovered line must still be caught through the symlink"
        );
    }

    /// The mirror of the case above, and the one that actually pins the
    /// normalisation inside `parse_lcov_coverage`: here the SYMLINKED spelling
    /// is what `llvm-cov` printed after `SF:` and the REAL one is what the diff
    /// side derives from `workspace_root()`. Which side is which is not a
    /// choice the gate makes — `workspace_root()` is baked from
    /// `CARGO_MANIFEST_DIR` at compile time while `llvm-cov` prints whatever
    /// cargo handed it, so either spelling can land on either side. Without
    /// canonicalizing the `SF:` key too, this direction degrades every file to
    /// "no LCOV record" and the whole gate passes silently.
    #[cfg(unix)]
    #[test]
    fn sim_coverage_matches_lcov_paths_when_the_symlink_is_on_the_lcov_side() {
        use std::os::unix::fs::symlink;

        let tmp = tempfile::tempdir().expect("create temp dir");
        let real_root = tmp.path().join("real-checkout");
        let src_dir = real_root.join("crates/magnetar-proto/src");
        fs::create_dir_all(&src_dir).expect("create source tree");
        fs::write(src_dir.join("conn.rs"), "fn covered() {}\nfn missed() {}\n")
            .expect("write source");

        let link_root = tmp.path().join("linked-checkout");
        symlink(&real_root, &link_root).expect("symlink the checkout");

        // `SF:` carries the SYMLINKED spelling this time.
        let lcov = format!(
            "SF:{}\nDA:1,1\nDA:2,0\nend_of_record\n",
            link_root
                .join("crates/magnetar-proto/src/conn.rs")
                .display()
        );
        let covered = parse_lcov_coverage(&lcov);
        let tracked = tracked_of(&[("crates/magnetar-proto/src/conn.rs", &[1, 2])]);

        // …and the diff side is invoked on the REAL one.
        assert!(
            uninstrumented_files(&real_root, &tracked, &covered).is_empty(),
            "equality must be reachable from either spelling, not just one"
        );
        assert_eq!(
            intersect_diff_with_coverage(&real_root, &tracked, &covered),
            vec![("crates/magnetar-proto/src/conn.rs".to_owned(), 2)],
            "the uncovered line must still be caught with the symlink on the LCOV side"
        );
    }
}

#[cfg(test)]
mod known_failing_seeds_tests {
    use super::parse_known_failing_seeds;

    /// The scanner extracts every `[[seed]]` entry's value + status and
    /// is not confused by comment lines or `"""` multiline notes whose
    /// prose could resemble `key = value` assignments.
    #[test]
    fn parses_registry_entries_and_skips_multiline_notes() {
        let contents = r#"
# Known failing seeds — header prose with status = "open" inside a comment.

[[seed]]
value        = "0x56201ccaba82dbc1"
discovered   = "2026-06-02"
status       = "open"
note         = """
Multi-line narrative. This line has value = 42 and
status = "wontfix" inside the note body, which the scanner
must NOT interpret as entry keys.
"""

[[seed]]
value        = 12345
status       = "investigating"
note         = """single entry note"""
"#;
        let seeds = parse_known_failing_seeds(contents).expect("registry parses");
        assert_eq!(seeds.len(), 2);
        assert_eq!(seeds[0].value, "0x56201ccaba82dbc1");
        assert_eq!(seeds[0].status, "open");
        assert_eq!(seeds[1].value, "12345");
        assert_eq!(seeds[1].status, "investigating");
    }

    /// An entry missing its `value` or `status` is a hard error — a
    /// registry the replay cannot act on must fail loudly rather than
    /// silently skipping the seed (the gate-that-cannot-measure rule).
    #[test]
    fn missing_value_or_status_is_an_error() {
        assert!(parse_known_failing_seeds("[[seed]]\nstatus = \"open\"\n").is_err());
        assert!(parse_known_failing_seeds("[[seed]]\nvalue = \"0x1\"\n").is_err());
    }

    /// The real registry file parses and yields only `open` anchors —
    /// the shape the per-PR `seed-replay` job (and this xtask) replay.
    #[test]
    fn real_registry_parses_with_open_entries() {
        let root = super::workspace_root().expect("workspace root");
        let contents = std::fs::read_to_string(
            root.join("crates/magnetar-runtime-moonpool/seeds/known-failing.toml"),
        )
        .expect("registry readable");
        let seeds = parse_known_failing_seeds(&contents).expect("registry parses");
        assert!(!seeds.is_empty(), "registry currently carries anchors");
        for seed in &seeds {
            assert!(
                ["open", "investigating", "wontfix"].contains(&seed.status.as_str()),
                "unexpected status {:?} for seed {}",
                seed.status,
                seed.value
            );
        }
    }
}
