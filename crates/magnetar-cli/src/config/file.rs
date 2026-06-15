// SPDX-License-Identifier: Apache-2.0

//! Locate, load, and save the pulsarctl `~/.config/pulsar/config` file.
//!
//! Path resolution (most-specific first):
//! 1. `--config <path>` (explicit) — a missing file here IS an error.
//! 2. `MAGNETAR_CONFIG` env (explicit) — a missing file here IS an error.
//! 3. `$XDG_CONFIG_HOME/pulsar/config` — only when `XDG_CONFIG_HOME` is set.
//! 4. `$HOME/.config/pulsar/config` — the pulsarctl-hardcoded default.
//!
//! The default path (3 / 4) is allowed to be absent: a missing file there
//! falls back to today's built-in localhost defaults (backward compatible).
//! Only an EXPLICIT `--config` / `MAGNETAR_CONFIG` path that does not exist is
//! a hard error — the operator named a file that isn't there.
//!
//! `$HOME` / `$XDG_CONFIG_HOME` are read via `std::env::var`. The
//! no-internal-clock / env allowlist (ADR-0011) governs `magnetar-proto` only;
//! the CLI is free to read the environment.

use std::path::{Path, PathBuf};

use super::model::PulsarConfig;

/// Errors from locating / parsing / writing the config file.
#[derive(Debug, thiserror::Error)]
pub(crate) enum ConfigError {
    /// An explicit `--config` / `MAGNETAR_CONFIG` path does not exist.
    #[error("config file not found: {0}")]
    ExplicitNotFound(PathBuf),
    /// Could not resolve a default path (neither `$HOME` nor `$XDG_CONFIG_HOME`
    /// is set and no explicit path was given).
    #[error(
        "cannot locate config: neither --config/MAGNETAR_CONFIG nor $HOME/$XDG_CONFIG_HOME set"
    )]
    NoDefaultPath,
    /// I/O error reading or writing the file.
    #[error("config io ({path}): {source}")]
    Io {
        /// The path the I/O was attempted on.
        path: PathBuf,
        /// The underlying I/O error.
        #[source]
        source: std::io::Error,
    },
    /// YAML parse / serialize error.
    #[error("config yaml ({path}): {source}")]
    Yaml {
        /// The path the YAML belongs to.
        path: PathBuf,
        /// The underlying `serde_norway` error.
        #[source]
        source: serde_norway::Error,
    },
}

/// Where a resolved config path came from. Drives the missing-file policy:
/// an explicit source errors on absence, a default source falls back.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum PathSource {
    /// `--config <path>` or `MAGNETAR_CONFIG` — explicitly named by the user.
    Explicit,
    /// `$XDG_CONFIG_HOME/pulsar/config` or `$HOME/.config/pulsar/config`.
    Default,
}

/// A resolved config path plus where it came from.
#[derive(Debug, Clone)]
pub(crate) struct ResolvedPath {
    /// The absolute (or as-given) path to the config file.
    pub(crate) path: PathBuf,
    /// Whether the path was explicit (errors if missing) or a default.
    pub(crate) source: PathSource,
}

/// Resolve the config file path from the explicit override (if any) and the
/// environment. Pure over its `explicit` argument + an env-reader closure so
/// it is unit-testable without mutating the process environment.
pub(crate) fn resolve_path(
    explicit: Option<&str>,
    env: impl Fn(&str) -> Option<String>,
) -> Result<ResolvedPath, ConfigError> {
    if let Some(path) = explicit {
        return Ok(ResolvedPath {
            path: PathBuf::from(path),
            source: PathSource::Explicit,
        });
    }
    if let Some(path) = env("MAGNETAR_CONFIG") {
        return Ok(ResolvedPath {
            path: PathBuf::from(path),
            source: PathSource::Explicit,
        });
    }
    if let Some(xdg) = env("XDG_CONFIG_HOME").filter(|s| !s.is_empty()) {
        return Ok(ResolvedPath {
            path: Path::new(&xdg).join("pulsar").join("config"),
            source: PathSource::Default,
        });
    }
    if let Some(home) = env("HOME").filter(|s| !s.is_empty()) {
        return Ok(ResolvedPath {
            path: Path::new(&home)
                .join(".config")
                .join("pulsar")
                .join("config"),
            source: PathSource::Default,
        });
    }
    Err(ConfigError::NoDefaultPath)
}

/// Read the process environment. Thin wrapper so call sites pass
/// `super::file::std_env` to [`resolve_path`] in production.
pub(crate) fn std_env(key: &str) -> Option<String> {
    std::env::var(key).ok()
}

/// Load the config from a resolved path.
///
/// Returns `Ok(None)` when a DEFAULT-sourced file is absent (fall back to
/// built-in localhost defaults). Returns [`ConfigError::ExplicitNotFound`]
/// when an EXPLICIT path is absent.
pub(crate) fn load(resolved: &ResolvedPath) -> Result<Option<PulsarConfig>, ConfigError> {
    let exists = resolved.path.exists();
    if !exists {
        return match resolved.source {
            PathSource::Explicit => Err(ConfigError::ExplicitNotFound(resolved.path.clone())),
            PathSource::Default => Ok(None),
        };
    }
    let text = std::fs::read_to_string(&resolved.path).map_err(|source| ConfigError::Io {
        path: resolved.path.clone(),
        source,
    })?;
    // An empty file parses as an empty document — treat it as defaults so a
    // freshly `touch`ed config behaves like an absent one.
    if text.trim().is_empty() {
        return Ok(Some(PulsarConfig::default()));
    }
    let cfg = serde_norway::from_str(&text).map_err(|source| ConfigError::Yaml {
        path: resolved.path.clone(),
        source,
    })?;
    Ok(Some(cfg))
}

/// Serialize a config to YAML.
pub(crate) fn to_yaml(cfg: &PulsarConfig) -> Result<String, ConfigError> {
    serde_norway::to_string(cfg).map_err(|source| ConfigError::Yaml {
        path: PathBuf::new(),
        source,
    })
}

/// Save the config to a path, creating the parent directory if needed.
///
/// On Unix the file is created `0600` (best-effort — it carries bearer tokens
/// and client secrets). pulsarctl itself does not chmod, so we tighten rather
/// than loosen; an existing file's mode is left untouched.
pub(crate) fn save(path: &Path, cfg: &PulsarConfig) -> Result<(), ConfigError> {
    if let Some(parent) = path.parent() {
        if !parent.as_os_str().is_empty() {
            std::fs::create_dir_all(parent).map_err(|source| ConfigError::Io {
                path: parent.to_path_buf(),
                source,
            })?;
        }
    }
    let yaml = to_yaml(cfg)?;
    write_private(path, yaml.as_bytes()).map_err(|source| ConfigError::Io {
        path: path.to_path_buf(),
        source,
    })
}

/// Write bytes to `path`, creating the file `0600` on Unix.
#[cfg(unix)]
fn write_private(path: &Path, bytes: &[u8]) -> std::io::Result<()> {
    use std::io::Write;
    use std::os::unix::fs::OpenOptionsExt;

    let mut file = std::fs::OpenOptions::new()
        .write(true)
        .create(true)
        .truncate(true)
        .mode(0o600)
        .open(path)?;
    file.write_all(bytes)
}

/// Non-Unix fallback — no mode bits.
#[cfg(not(unix))]
fn write_private(path: &Path, bytes: &[u8]) -> std::io::Result<()> {
    std::fs::write(path, bytes)
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use super::*;

    fn env_map(pairs: &[(&str, &str)]) -> impl Fn(&str) -> Option<String> {
        let map: BTreeMap<String, String> = pairs
            .iter()
            .map(|(k, v)| ((*k).to_owned(), (*v).to_owned()))
            .collect();
        move |k: &str| map.get(k).cloned()
    }

    /// `--config` wins over everything and is marked Explicit.
    #[test]
    fn resolve_path_explicit_flag_wins() {
        let env = env_map(&[("MAGNETAR_CONFIG", "/env/cfg"), ("HOME", "/home/u")]);
        let r = resolve_path(Some("/flag/cfg"), &env).expect("resolve");
        assert_eq!(r.path, PathBuf::from("/flag/cfg"));
        assert_eq!(r.source, PathSource::Explicit);
    }

    /// `MAGNETAR_CONFIG` is Explicit and beats the XDG/HOME defaults.
    #[test]
    fn resolve_path_env_is_explicit() {
        let env = env_map(&[
            ("MAGNETAR_CONFIG", "/env/cfg"),
            ("XDG_CONFIG_HOME", "/xdg"),
            ("HOME", "/home/u"),
        ]);
        let r = resolve_path(None, &env).expect("resolve");
        assert_eq!(r.path, PathBuf::from("/env/cfg"));
        assert_eq!(r.source, PathSource::Explicit);
    }

    /// `XDG_CONFIG_HOME` (when set) yields `$XDG/pulsar/config` as a Default.
    #[test]
    fn resolve_path_xdg_default() {
        let env = env_map(&[("XDG_CONFIG_HOME", "/xdg"), ("HOME", "/home/u")]);
        let r = resolve_path(None, &env).expect("resolve");
        assert_eq!(r.path, PathBuf::from("/xdg/pulsar/config"));
        assert_eq!(r.source, PathSource::Default);
    }

    /// Falls back to `$HOME/.config/pulsar/config` (pulsarctl default) when XDG
    /// is unset.
    #[test]
    fn resolve_path_home_default() {
        let env = env_map(&[("HOME", "/home/u")]);
        let r = resolve_path(None, &env).expect("resolve");
        assert_eq!(r.path, PathBuf::from("/home/u/.config/pulsar/config"));
        assert_eq!(r.source, PathSource::Default);
    }

    /// An explicit-but-missing path is a hard error; a default-but-missing path
    /// falls back to `None`.
    #[test]
    fn load_missing_explicit_errors_default_falls_back() {
        let dir = tempfile::tempdir().expect("tempdir");
        let missing = dir.path().join("does-not-exist");

        let explicit = ResolvedPath {
            path: missing.clone(),
            source: PathSource::Explicit,
        };
        assert!(matches!(
            load(&explicit),
            Err(ConfigError::ExplicitNotFound(_))
        ));

        let default = ResolvedPath {
            path: missing,
            source: PathSource::Default,
        };
        assert!(load(&default).expect("default load ok").is_none());
    }

    /// save → load round-trips a config and creates the parent dir.
    #[test]
    fn save_then_load_round_trips() {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("nested").join("pulsar").join("config");

        let mut cfg = PulsarConfig::default();
        cfg.contexts.insert(
            "dev".to_owned(),
            super::super::model::Context {
                admin_service_url: "http://localhost:8080".to_owned(),
                ..Default::default()
            },
        );
        cfg.current_context = "dev".to_owned();

        save(&path, &cfg).expect("save");
        assert!(path.exists());

        let resolved = ResolvedPath {
            path: path.clone(),
            source: PathSource::Default,
        };
        let loaded = load(&resolved).expect("load").expect("present");
        assert_eq!(loaded, cfg);

        // 0600 on Unix.
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let mode = std::fs::metadata(&path).unwrap().permissions().mode();
            assert_eq!(mode & 0o777, 0o600);
        }
    }
}
