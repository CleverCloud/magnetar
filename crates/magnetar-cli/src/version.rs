// SPDX-License-Identifier: Apache-2.0
//
//! `--version` banner for the `magnetar` CLI binary.
//!
//! Two entry points, both returning `&'static str` (clap's
//! `version` / `long_version` derive attributes require it):
//!
//! - [`short`] — one-liner payload: `X.Y.Z (sha-dirty)`. Bound to `-V`.
//! - [`long`]  — multi-line banner with build metadata. Bound to `--version`.
//!
//! clap prepends the binary name (from `#[command(name = "magnetar")]`)
//! to the payload, so these strings deliberately omit `"magnetar"` from
//! their first token. The user sees `magnetar X.Y.Z (sha)` end-to-end.
//!
//! The long form is colorized when stdout is a TTY and `NO_COLOR` is unset
//! (see <https://no-color.org>). When piped or `NO_COLOR=1`, output is plain
//! ASCII.
//!
//! Build-time inputs come from `build.rs` via `cargo:rustc-env=`:
//!
//! - `MAGNETAR_BUILD_GIT_SHA`     — 12-char short SHA, `"unknown"` outside git.
//! - `MAGNETAR_BUILD_GIT_DIRTY`   — `"yes"` / `"no"`.
//! - `MAGNETAR_BUILD_TIMESTAMP`   — RFC-3339 UTC; honors `SOURCE_DATE_EPOCH`.
//! - `MAGNETAR_BUILD_PROFILE`     — `debug` / `release`.
//! - `MAGNETAR_BUILD_TARGET`      — target triple.
//! - `MAGNETAR_BUILD_RUSTC`       — `rustc --version` first line.
//! - `MAGNETAR_BUILD_FEATURES`    — `+feat +feat …` for enabled cargo features.

use std::fmt::Write as _;
use std::io::IsTerminal;
use std::sync::OnceLock;

use anstyle::{AnsiColor, Style};

/// Pulsar wire-protocol version this driver speaks.
///
/// Mirrors the hard-coded `protocol_version` in
/// `magnetar-proto/src/conn.rs`. Tracked as a follow-up in
/// `docs/follow-ups.md` (expose as a typed constant from
/// `magnetar-proto`).
const PULSAR_PROTOCOL_VERSION: u32 = 21;

/// Short `-V` payload. One line, never colorized.
///
/// Clap prepends `"magnetar "` (the `name` attribute); this returns
/// `0.1.0-dev.0 (a1b2c3d4e5f6-dirty)`.
pub(crate) fn short() -> &'static str {
    static CACHE: OnceLock<&'static str> = OnceLock::new();
    CACHE.get_or_init(|| {
        let sha = env!("MAGNETAR_BUILD_GIT_SHA");
        let dirty = if env!("MAGNETAR_BUILD_GIT_DIRTY") == "yes" {
            "-dirty"
        } else {
            ""
        };
        let s = format!("{} ({sha}{dirty})", env!("CARGO_PKG_VERSION"));
        Box::leak(s.into_boxed_str()) as &'static str
    })
}

/// Long `--version` banner. Multi-line, colorized when stdout is a TTY
/// and `NO_COLOR` is unset.
pub(crate) fn long() -> &'static str {
    static CACHE: OnceLock<&'static str> = OnceLock::new();
    CACHE.get_or_init(|| {
        let s = render_long(should_color());
        Box::leak(s.into_boxed_str()) as &'static str
    })
}

/// `true` iff color escapes should be emitted on this run.
///
/// Honors `NO_COLOR` (any non-empty value disables, per
/// <https://no-color.org>) and falls back to a stdout `is_terminal()`
/// check. Cached per-process — the banner is built once at parse time.
fn should_color() -> bool {
    if std::env::var_os("NO_COLOR").is_some_and(|v| !v.is_empty()) {
        return false;
    }
    std::io::stdout().is_terminal()
}

/// Build the multi-line banner. `colored=true` wraps select tokens in
/// ANSI escapes; `false` produces pure ASCII.
///
/// Note: clap prepends the binary `name = "magnetar"` to whatever we return,
/// so line 1 here starts with the version, not the program name.
fn render_long(colored: bool) -> String {
    let version = env!("CARGO_PKG_VERSION");
    let sha = env!("MAGNETAR_BUILD_GIT_SHA");
    let dirty = env!("MAGNETAR_BUILD_GIT_DIRTY") == "yes";
    let timestamp = env!("MAGNETAR_BUILD_TIMESTAMP");
    let profile = env!("MAGNETAR_BUILD_PROFILE");
    let target = env!("MAGNETAR_BUILD_TARGET");
    let rustc = env!("MAGNETAR_BUILD_RUSTC");
    let features = env!("MAGNETAR_BUILD_FEATURES");
    let repository = env!("CARGO_PKG_REPOSITORY");

    // Styles. `Style::new()` is a no-op when wrapped in `Paint::new`-style
    // formatting; we render conditionally so the un-colorized branch never
    // emits an empty `\x1b[0m` sequence.
    let bold = Style::new().bold();
    let dim = Style::new().dimmed();
    let green = Style::new().fg_color(Some(AnsiColor::Green.into()));
    let red = Style::new().fg_color(Some(AnsiColor::Red.into()));
    let cyan = Style::new().fg_color(Some(AnsiColor::Cyan.into()));

    let dirty_marker = if dirty { "-dirty" } else { "" };

    let mut out = String::with_capacity(512);

    // Line 1: version + (git sha[-dirty]). Clap glues "magnetar " on front.
    // `write!` against a `String` is infallible — `expect` only documents intent.
    if colored {
        write!(out, "{bold}{version}{bold:#} ").expect("infallible String write");
        writeln!(out, "{dim}({sha}{dirty_marker}){dim:#}").expect("infallible String write");
    } else {
        writeln!(out, "{version} ({sha}{dirty_marker})").expect("infallible String write");
    }

    // Line 2: built timestamp + profile + rustc + target.
    let built_line =
        format!("built {timestamp} · profile={profile} · rustc={rustc} · target={target}");
    if colored {
        writeln!(out, "{dim}{built_line}{dim:#}").expect("infallible String write");
    } else {
        out.push_str(&built_line);
        out.push('\n');
    }

    // Line 3: features (each `+name` token green; future `-name` red).
    out.push_str("features:");
    for token in features.split_whitespace() {
        out.push(' ');
        if colored {
            if let Some(rest) = token.strip_prefix('+') {
                write!(out, "{green}+{rest}{green:#}").expect("infallible String write");
            } else if let Some(rest) = token.strip_prefix('-') {
                write!(out, "{red}-{rest}{red:#}").expect("infallible String write");
            } else {
                out.push_str(token);
            }
        } else {
            out.push_str(token);
        }
    }
    out.push('\n');

    // Line 4: Pulsar wire-protocol version.
    let proto_line = format!("pulsar wire protocol: v{PULSAR_PROTOCOL_VERSION}");
    if colored {
        writeln!(out, "{cyan}{proto_line}{cyan:#}").expect("infallible String write");
    } else {
        out.push_str(&proto_line);
        out.push('\n');
    }

    // Line 5: host OS + bug-report URL.
    let footer = format!(
        "os: {os} · report bugs at {repository}",
        os = std::env::consts::OS,
    );
    if colored {
        write!(out, "{dim}{footer}{dim:#}").expect("infallible String write");
    } else {
        out.push_str(&footer);
    }

    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn short_is_single_line_starting_with_version() {
        let s = short();
        assert_eq!(s.lines().count(), 1);
        assert!(s.starts_with(env!("CARGO_PKG_VERSION")));
    }

    #[test]
    fn long_plain_has_no_ansi_and_required_fields() {
        let s = render_long(false);
        assert!(!s.contains('\x1b'), "plain render must be ANSI-free");
        assert!(s.starts_with(env!("CARGO_PKG_VERSION")));
        assert!(s.contains("built "));
        assert!(s.contains("profile="));
        assert!(s.contains("rustc="));
        assert!(s.contains("target="));
        assert!(s.contains("features:"));
        assert!(s.contains("pulsar wire protocol: v21"));
        assert!(s.contains("os: "));
        assert!(s.contains("report bugs at "));
    }

    #[test]
    fn long_colored_emits_ansi_escapes() {
        let s = render_long(true);
        assert!(
            s.contains('\x1b'),
            "colored render must include ANSI escapes"
        );
        // Same skeleton — colors decorate, do not replace.
        assert!(s.contains(env!("CARGO_PKG_VERSION")));
        assert!(s.contains("pulsar wire protocol: v21"));
    }
}
