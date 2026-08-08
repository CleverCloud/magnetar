// SPDX-License-Identifier: Apache-2.0

//! Swarm-testing configuration for the moonpool deterministic-simulation
//! campaign (ADR-0097, after Groce et al., ISSTA 2012).
//!
//! A [`SwarmConfig`] names the subset of optional campaign features one
//! simulation iteration runs: the four ADR-0048 buggify labels (fault
//! features) and the optional `ProducerConsumerWorkload` operations
//! (operation features). The sim workloads draw one config per iteration
//! in `Workload::setup`, before the first operation, from
//! `moonpool_sim::current_sim_seed()`.
//!
//! # Determinism contract
//!
//! [`SwarmConfig::from_seed`] is a **pure function of the seed**: a
//! splitmix64 hash chain over `(seed XOR per-purpose salt)`. It reads no
//! environment and consumes no RNG stream — neither moonpool's in-run
//! `SIM_RNG` nor its `CONFIG_RNG`, and nothing from `Providers::Random` —
//! so drawing a config cannot shift any later draw of the run, and
//! `MOONPOOL_SEED=<s>` reproduces the same subset and the same run
//! bit-for-bit (ADR-0036 / ADR-0047).
//!
//! # Campaign composition
//!
//! 1 in 4 seeds runs the inclusive [`SwarmConfig::full`] configuration —
//! the ISSTA 2012 data is explicit that swarm subsets complement, never
//! replace, the inclusive run (a bug needing `k` features together
//! appears in a coin-toss subset with probability `1/2^k`). The rest
//! draw each feature independently at 50% inclusion, the paper's default
//! coin-toss; a draw that comes up all-off collapses to `full()` so the
//! subset is never empty. Sub-seed-pinning regression tests force
//! [`SwarmConfig::baseline`] — the pre-swarm workload shape — so their
//! recorded trajectories stay byte-identical.

use core::fmt;

/// The four ADR-0048 buggify labels, in canonical (declaration) order.
/// Index positions match [`SwarmConfig`]'s internal label array.
pub const SWARM_LABELS: [&str; 4] = [
    magnetar_proto::buggify::labels::CONNECTION_RESET_DELAY,
    magnetar_proto::buggify::labels::BATCH_CONTAINER_FLUSH_SPLIT,
    magnetar_proto::buggify::labels::HANDLE_BYTES_SHORT_READ,
    magnetar_proto::buggify::labels::RETRY_CLOCK_SKEW,
];

/// Which campaign slot a seed landed in. Carried in the printed config
/// line so a failing seed's report states whether it ran the inclusive
/// configuration, a swarm subset, or a pinned baseline replay.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SwarmSlot {
    /// Every feature enabled — the inclusive configuration.
    Full,
    /// A per-seed 50%-inclusion subset of the optional features.
    Swarm,
    /// The pre-swarm workload shape (ack on, close off, buggify
    /// unarmed), forced by sub-seed-pinning regression tests to keep
    /// their recorded trajectories byte-identical.
    Baseline,
}

impl SwarmSlot {
    fn as_str(self) -> &'static str {
        match self {
            Self::Full => "full",
            Self::Swarm => "swarm",
            Self::Baseline => "baseline",
        }
    }
}

/// Per-iteration swarm configuration: which optional campaign features
/// this run enables. See the module docs for the determinism contract
/// and campaign composition.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SwarmConfig {
    seed: u64,
    slot: SwarmSlot,
    /// Buggify labels, indexed as [`SWARM_LABELS`].
    labels: [bool; 4],
    /// `op.client_ack` — the workload acks each received message. Off,
    /// the durable cursor never advances, redelivery covers the full
    /// ledger, and `AckAfterReceiveInvariant` + `NoDupOnAckedInvariant`
    /// are knowingly vacuous (printed in the config line).
    client_ack: bool,
    /// `op.client_close` — the workload explicitly closes its producer
    /// and consumer after its gates, exercising the `CLOSE_PRODUCER` /
    /// `CLOSE_CONSUMER` broker paths the workload never drove before.
    client_close: bool,
}

/// splitmix64 finalizer — the same public-domain construction
/// `tests/common/mod.rs::sweep_seeds` uses for seed expansion, so the
/// whole campaign derives from one hash family with no new dependency.
fn splitmix64(x: u64) -> u64 {
    let mut z = x.wrapping_add(0x9E37_79B9_7F4A_7C15);
    z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
    z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
    z ^ (z >> 31)
}

/// Salt for the campaign-slot decision (full vs swarm). Distinct from
/// [`FEATURE_SALT`] so the slot choice and the feature coins are
/// decorrelated per seed.
const SLICE_SALT: u64 = 0x5357_4152_4D5F_534C; // "SWARM_SL"

/// Salt for the per-feature inclusion coins.
const FEATURE_SALT: u64 = 0x5357_4152_4D5F_4645; // "SWARM_FE"

impl SwarmConfig {
    /// Derive the configuration for `seed` — a pure function of the
    /// seed, per the module-level determinism contract.
    #[must_use]
    pub fn from_seed(seed: u64) -> Self {
        // Slot decision: 1 in 4 seeds runs the inclusive configuration.
        if splitmix64(seed ^ SLICE_SALT).is_multiple_of(4) {
            return Self::full_for(seed);
        }
        // Independent 50% coin per feature: one splitmix64 chain step
        // per feature index, so adding a feature later never reshuffles
        // the coins of the existing ones.
        let coin = |idx: u64| splitmix64(seed ^ FEATURE_SALT ^ idx) & 1 == 1;
        let labels = [coin(0), coin(1), coin(2), coin(3)];
        let client_ack = coin(4);
        let client_close = coin(5);
        // Non-empty-subset rule: an all-off draw collapses to the
        // inclusive configuration.
        if !labels.iter().any(|l| *l) && !client_ack && !client_close {
            return Self::full_for(seed);
        }
        Self {
            seed,
            slot: SwarmSlot::Swarm,
            labels,
            client_ack,
            client_close,
        }
    }

    /// The inclusive configuration — every feature enabled.
    #[must_use]
    pub fn full(seed: u64) -> Self {
        Self::full_for(seed)
    }

    fn full_for(seed: u64) -> Self {
        Self {
            seed,
            slot: SwarmSlot::Full,
            labels: [true; 4],
            client_ack: true,
            client_close: true,
        }
    }

    /// The pre-swarm workload shape: ack enabled, close absent, buggify
    /// unarmed. Sub-seed-pinning regression tests force this so their
    /// recorded trajectories stay byte-identical — [`Self::build_buggify`]
    /// returns the disabled helper and no extra operation runs.
    #[must_use]
    pub fn baseline(seed: u64) -> Self {
        Self {
            seed,
            slot: SwarmSlot::Baseline,
            labels: [false; 4],
            client_ack: true,
            client_close: false,
        }
    }

    /// The seed this configuration was derived from (or pinned to).
    #[must_use]
    pub fn seed(&self) -> u64 {
        self.seed
    }

    /// Which campaign slot the seed landed in.
    #[must_use]
    pub fn slot(&self) -> SwarmSlot {
        self.slot
    }

    /// Whether the buggify label at [`SWARM_LABELS`] position `idx` is
    /// armed for this run.
    #[must_use]
    pub fn label_enabled(&self, idx: usize) -> bool {
        self.labels.get(idx).copied().unwrap_or(false)
    }

    /// Whether any buggify label is armed.
    #[must_use]
    pub fn any_label_enabled(&self) -> bool {
        self.labels.iter().any(|l| *l)
    }

    /// `op.client_ack` for this run.
    #[must_use]
    pub fn client_ack(&self) -> bool {
        self.client_ack
    }

    /// `op.client_close` for this run.
    #[must_use]
    pub fn client_close(&self) -> bool {
        self.client_close
    }

    /// Receive-attempt budget multiplier over `PRODUCE_COUNT`. With
    /// `op.client_ack` off the durable cursor never advances, so every
    /// supervised reconnect redelivers the full ledger and the workload
    /// needs more attempts to collect the distinct set — the effective
    /// weight the printed config line reports.
    #[must_use]
    pub fn receive_budget_multiplier(&self) -> u32 {
        if self.client_ack { 2 } else { 4 }
    }

    /// The invariants this configuration knowingly leaves vacuous —
    /// part of the printed config line, per ADR-0097's "an omitted
    /// operation must be visible" rule.
    #[must_use]
    pub fn vacuous_invariants(&self) -> &'static str {
        if self.client_ack {
            ""
        } else {
            "AckAfterReceiveInvariant,NoDupOnAckedInvariant"
        }
    }

    /// Build the [`magnetar_proto::Buggify`] helper this configuration
    /// arms, from an engine-owned RNG closure (the sim workloads pass a
    /// `Providers::Random`-backed closure). No label enabled — including
    /// the whole [`SwarmSlot::Baseline`] slot — returns the disabled
    /// helper, so a baseline run installs exactly what the pre-swarm
    /// code did and consumes no RNG at any choice point.
    ///
    /// Under `not(feature = "buggify")` the armed branch degrades to the
    /// zero-sized disabled helper by construction, so the drawn label
    /// set is printed but inert (ADR-0097 §campaign composition).
    #[cfg(feature = "buggify")]
    #[must_use]
    pub fn build_buggify(&self, rng: magnetar_proto::BuggifyRng) -> magnetar_proto::Buggify {
        if !self.any_label_enabled() {
            return magnetar_proto::Buggify::disabled();
        }
        let enabled = self.labels;
        magnetar_proto::Buggify::with_rng_and_filter(
            rng,
            std::sync::Arc::new(move |label: &'static str| {
                SWARM_LABELS
                    .iter()
                    .position(|l| *l == label)
                    .is_some_and(|idx| enabled[idx])
            }),
        )
    }

    /// No-feature stub of [`Self::build_buggify`] — always the disabled
    /// helper, keeping one call shape on both feature axes.
    #[cfg(not(feature = "buggify"))]
    #[must_use]
    pub fn build_buggify<R>(&self, _rng: R) -> magnetar_proto::Buggify {
        magnetar_proto::Buggify::disabled()
    }
}

impl fmt::Display for SwarmConfig {
    /// The canonical one-line configuration record. Printed before the
    /// workload runs and embedded in every gate / invariant failure
    /// message — a failing seed whose configuration is not printed is
    /// not reproducible (ADR-0047 triage).
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let labels: Vec<&str> = SWARM_LABELS
            .iter()
            .zip(self.labels.iter())
            .filter_map(|(l, on)| on.then_some(*l))
            .collect();
        let mut ops: Vec<&str> = Vec::new();
        if self.client_ack {
            ops.push("client_ack");
        }
        if self.client_close {
            ops.push("client_close");
        }
        write!(
            f,
            "swarm-config seed={:#018x} slot={} labels=[{}] ops=[{}] receive_budget={}x vacuous=[{}] buggify_compiled={}",
            self.seed,
            self.slot.as_str(),
            labels.join(","),
            ops.join(","),
            self.receive_budget_multiplier(),
            self.vacuous_invariants(),
            cfg!(feature = "buggify"),
        )
    }
}
