// SPDX-License-Identifier: Apache-2.0

//! pulsarctl-compatible `~/.config/pulsar/config` support.
//!
//! - [`model`] — the serde structs with pulsarctl's exact key casing, plus `#[serde(flatten)]`
//!   extra maps so unknown keys round-trip.
//! - [`file`] — path resolution, load, and save (`serde_norway`).
//! - [`resolve`] — select the active context and lift it into connection settings (admin URL,
//!   derived data-plane URL, auth, TLS).
//!
//! See [ADR-0068](../../../specs/adr/0068-pulsarctl-config-and-context-management.md)
//! and [`docs/cli.md`](../../../docs/cli.md) ("Config file & contexts").

pub(crate) mod file;
pub(crate) mod model;
pub(crate) mod resolve;

pub(crate) use file::{ConfigError, ResolvedPath, load, resolve_path, save, std_env};
pub(crate) use model::PulsarConfig;
pub(crate) use resolve::{ResolveError, ResolvedAuth, ResolvedContext, resolve};
