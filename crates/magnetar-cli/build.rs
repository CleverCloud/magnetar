// SPDX-License-Identifier: Apache-2.0
//
// Build script for `magnetar-cli`.
//
// Emits build-time metadata into `cargo:rustc-env=` so the binary can surface
// a rich `--version` banner (git short SHA, dirty bit, build timestamp, target
// triple, rustc version, enabled features, build profile). Pure `std`; no
// `build-dependencies`. Modeled after sozu's `bin/build.rs`.
//
// Sans-io invariant note: `SystemTime::now()` runs here on the build host,
// not in the shipped binary. ADR-0011 (clock-injection sans-io) governs the
// runtime hot path; build scripts are out of scope and `magnetar-cli` is not
// in `magnetar-proto`.

use std::process::Command;
use std::time::{SystemTime, UNIX_EPOCH};

fn main() {
    println!("cargo:rerun-if-changed=build.rs");
    println!("cargo:rerun-if-env-changed=SOURCE_DATE_EPOCH");
    println!("cargo:rerun-if-env-changed=RUSTC");
    println!("cargo:rerun-if-env-changed=PROFILE");
    println!("cargo:rerun-if-env-changed=TARGET");

    // Guard `rerun-if-changed` against tarball installs where `.git` is
    // absent — Cargo emits a noisy warning if the path doesn't exist.
    let git_head = std::path::Path::new("../../.git/HEAD");
    if git_head.try_exists().unwrap_or(false) {
        println!("cargo:rerun-if-changed=../../.git/HEAD");
    }
    let git_refs = std::path::Path::new("../../.git/refs/heads");
    if git_refs.try_exists().unwrap_or(false) {
        println!("cargo:rerun-if-changed=../../.git/refs/heads");
    }

    let sha = run_git(&["rev-parse", "--short=12", "HEAD"]).unwrap_or_else(|| "unknown".to_owned());
    let dirty = match run_git(&["status", "--porcelain"]) {
        Some(s) if !s.is_empty() => "yes",
        _ => "no",
    };
    let timestamp = build_timestamp();
    let profile = std::env::var("PROFILE").unwrap_or_else(|_| "unknown".to_owned());
    let target = std::env::var("TARGET").unwrap_or_else(|_| "unknown".to_owned());
    let rustc = rustc_version();
    let features = enabled_features();

    println!("cargo:rustc-env=MAGNETAR_BUILD_GIT_SHA={sha}");
    println!("cargo:rustc-env=MAGNETAR_BUILD_GIT_DIRTY={dirty}");
    println!("cargo:rustc-env=MAGNETAR_BUILD_TIMESTAMP={timestamp}");
    println!("cargo:rustc-env=MAGNETAR_BUILD_PROFILE={profile}");
    println!("cargo:rustc-env=MAGNETAR_BUILD_TARGET={target}");
    println!("cargo:rustc-env=MAGNETAR_BUILD_RUSTC={rustc}");
    println!("cargo:rustc-env=MAGNETAR_BUILD_FEATURES={features}");
}

/// Run `git <args>` and return its trimmed stdout on success, `None` otherwise.
fn run_git(args: &[&str]) -> Option<String> {
    let out = Command::new("git").args(args).output().ok()?;
    if !out.status.success() {
        return None;
    }
    let s = String::from_utf8(out.stdout).ok()?;
    Some(s.trim().to_owned())
}

/// RFC-3339 UTC timestamp string (second precision, `Z` suffix).
///
/// Honors `SOURCE_DATE_EPOCH` (see
/// <https://reproducible-builds.org/specs/source-date-epoch/>) for
/// reproducible builds; falls back to `SystemTime::now()`.
fn build_timestamp() -> String {
    let secs = std::env::var("SOURCE_DATE_EPOCH")
        .ok()
        .and_then(|s| s.parse::<i64>().ok())
        .unwrap_or_else(|| {
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .map_or(0, |d| i64::try_from(d.as_secs()).unwrap_or(i64::MAX))
        });
    format_rfc3339_utc(secs)
}

/// Format a Unix timestamp (seconds since 1970-01-01T00:00:00Z) as
/// `YYYY-MM-DDTHH:MM:SSZ`.
///
/// Uses Howard Hinnant's civil-from-days algorithm
/// (see <http://howardhinnant.github.io/date_algorithms.html#civil_from_days>).
/// Public-domain.
fn format_rfc3339_utc(unix_secs: i64) -> String {
    let secs_per_day: i64 = 86_400;
    let mut days = unix_secs.div_euclid(secs_per_day);
    let secs_of_day = unix_secs.rem_euclid(secs_per_day);

    // Hinnant civil_from_days: input is days since 1970-01-01.
    days += 719_468;
    let era = if days >= 0 { days } else { days - 146_096 } / 146_097;
    let doe = (days - era * 146_097) as u32; // [0, 146_096]
    let yoe = (doe - doe / 1_460 + doe / 36_524 - doe / 146_096) / 365; // [0, 399]
    let y = i64::from(yoe) + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100); // [0, 365]
    let mp = (5 * doy + 2) / 153; // [0, 11]
    let d = doy - (153 * mp + 2) / 5 + 1; // [1, 31]
    let m = if mp < 10 { mp + 3 } else { mp - 9 }; // [1, 12]
    let year = y + i64::from(m <= 2);

    let hour = secs_of_day / 3_600;
    let minute = (secs_of_day % 3_600) / 60;
    let second = secs_of_day % 60;

    format!("{year:04}-{m:02}-{d:02}T{hour:02}:{minute:02}:{second:02}Z")
}

/// First line of `rustc --version`, trimmed; `"unknown"` on failure.
fn rustc_version() -> String {
    let rustc = std::env::var("RUSTC").unwrap_or_else(|_| "rustc".to_owned());
    Command::new(rustc)
        .arg("--version")
        .output()
        .ok()
        .and_then(|o| String::from_utf8(o.stdout).ok())
        .map_or_else(|| "unknown".to_owned(), |s| s.trim().to_owned())
}

/// Space-joined `+name` tokens for every cargo feature Cargo enabled on this
/// crate. Cargo sets `CARGO_FEATURE_<NAME>=1` for each enabled feature.
///
/// `magnetar-cli` has no `[features]` table today; emits `+default` so the
/// rendered version line has a stable shape.
fn enabled_features() -> String {
    let mut on: Vec<String> = std::env::vars()
        .filter_map(|(k, _)| {
            k.strip_prefix("CARGO_FEATURE_")
                .map(|s| s.to_ascii_lowercase().replace('_', "-"))
        })
        .collect();
    on.sort();
    if on.is_empty() {
        "+default".to_owned()
    } else {
        on.into_iter()
            .map(|f| format!("+{f}"))
            .collect::<Vec<_>>()
            .join(" ")
    }
}
