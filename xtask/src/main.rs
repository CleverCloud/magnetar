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
//! - `check-sim-coverage`: assert that every line added relative to `git merge-base origin/main
//!   HEAD` is executed by at least one moonpool test (`cargo-llvm-cov` patch-coverage style).
//!   Mirrors ADR-0024.
//! - `check-runtime-test-parity`: assert `magnetar-runtime-tokio` and `magnetar-runtime-moonpool`
//!   carry the same number of `#[test]` / `#[tokio::test]` / `#[moonpool::test]` items. Mirrors
//!   ADR-0024.
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

/// Protobuf fully-qualified message paths whose `bytes` fields should be
/// generated as `bytes::Bytes` instead of `Vec<u8>`. These are the payload-
/// bearing messages on the hot path — zero-copy decode matters here.
const BYTES_MESSAGES: &[&str] = &[
    ".pulsar.proto.MessageMetadata",
    ".pulsar.proto.SingleMessageMetadata",
    ".pulsar.proto.CommandSend",
    ".pulsar.proto.CommandMessage",
];

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
        Cmd::CheckSimCoverage { base } => check_sim_coverage(&base),
        Cmd::CheckRuntimeTestParity => check_runtime_test_parity(),
        Cmd::VendorProto { rev, source: _ } => {
            bail!("xtask vendor-proto: not implemented yet (lands in M1). Requested rev: {rev}");
        }
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

fn visit(root: &Path, callback: &mut dyn FnMut(&Path, &str)) -> Result<()> {
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
        if let Some(rest) = line.strip_prefix("DA:") {
            if let Some(file) = current_file.as_deref() {
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

/// Count test attributes (`#[test]`, `#[tokio::test]`, `#[moonpool::test]`)
/// inside a crate's `src` and `tests` directories.
///
/// Attributes are recognised by trimmed-line prefix. Composite attributes
/// like `#[tokio::test(flavor = "multi_thread")]` are matched on the
/// `#[tokio::test` prefix so they count once.
fn count_test_attributes(crate_root: &Path) -> Result<usize> {
    let mut total = 0usize;
    for subdir in ["src", "tests"] {
        let dir = crate_root.join(subdir);
        if !dir.exists() {
            continue;
        }
        visit(&dir, &mut |path, contents| {
            if path.extension().is_none_or(|ext| ext != "rs") {
                return;
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
