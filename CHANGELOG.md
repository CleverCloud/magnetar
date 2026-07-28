# Changelog

All notable changes to this project are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Fixed

- **Moonpool proxy/DIRECT broker-URL parsing no longer truncates a corrupted scheme into a nonsense authority:** `Client::proxy_broker_authority` / `direct_broker_authority` (moonpool engine only) previously fell through to a naive `host_port.split('/')` on any input that didn't match `strip_prefix("pulsar://")` / `strip_prefix("pulsar+ssl://")`, silently deriving authorities like `"ptlsar:"` from a single-bit-corrupted `broker_service_url` (e.g. moonpool-sim's bit-flip chaos) instead of failing the lookup. Both helpers now return `Result<String, ClientError>` and reject an input containing `"://"` with an unrecognised scheme explicitly, while still accepting a genuine scheme-less bare `host:port` unchanged.
  (#362, #363, #364, #367; ADR-0055)
- **`send_timeout` was blind to publishes relocated by a supervised reconnect:** `Connection::reset()` moves every in-flight `OpSend` out of `ProducerState::pending` and into `Connection::in_flight_publish_snapshots` so the supervisor can transparently replay it on the new session — but `ProducerState::drain_timed_out_sends` (the `send_timeout` sweep) only ever walked `pending`, so a publish parked across a reconnect was invisible to timeout enforcement until either a successful rebuild replayed it or the supervisor exhausted its entire reconnect attempt budget.
  With a realistic `max_attempts`, a user's `send().await` could stay `Pending` for hours with `send_timeout` configured and armed.
  `Connection::poll_timeout` and `Connection::handle_timeout` now also sweep `in_flight_publish_snapshots`, using each op's ORIGINAL `enqueued_at` (not a fresh reset()-relative budget) as the deadline base, so a relocated send now surfaces the same `SendError { code: -1, message: "send timeout" }` outcome the live-queue path installs, at the correctly-measured deadline — a previously-silent hang becomes a deterministic error.
  Separately, `Connection::fail_all_pending` now also drains `in_flight_publish_snapshots` (installing `OpOutcome::Terminal`), mirroring `fail_producer_open_with_broker_error`'s existing snapshot drain, so a relocated send resolves promptly when the supervisor gives up even if the woken send future has not yet re-registered its waker.
  (#369)
- **A stalled outbound socket no longer starves the driver's read and timer paths:** the driver's write ran unconditionally at the top of every loop iteration in both engines, ahead of `select!` — safe only as long as the write always completed quickly.
  A peer that accepted the connection and then simply stopped draining its receive window parked that write forever, blocking the ENTIRE loop: the read arm, the `driver_waker` arm, and the timer arm (the sole caller of `Connection::handle_timeout`, which drives the keepalive watchdog, the `send_timeout` sweep, and the `ack_response_timeout` backstop) never ran again, so `mark_disconnected()` was never reached and `is_connected()` kept reporting `true` on a functionally dead connection.
  The write is now its own bounded, cancellation-safe `select!` arm (after read and the waker, before the timer), capped by both `DRIVER_WRITE_BUDGET_BYTES` and `Connection::operation_timeout()` (30s default; NOT `keepalive_interval`, which only detects read-side silence).
  Expiry is treated as an I/O failure routed through the same `mark_disconnected()` path every other write error already takes, so the auto-reconnect supervisor redials exactly as it does for any other write failure, instead of the connection wedging as still-connected forever.
  (#370; ADR-0083, amends ADR-0070 and ADR-0074)

## [1.2.3] - 2026-07-20

### Added

- **Configurable operation retry policy:** `ClientBuilder::operation_retry(OperationRetryConfig)` now applies operation-specific broker retries across lookup, partition metadata, producer-open, and subscribe, independently from transport supervision.
  Producer-open additionally retries both producer-quota variants and `ProducerBusy`; subscribe additionally retries `ConsumerBusy`.
  Provisional attachment retries re-run lookup and routing with a fresh handle, while established reattachment remains driver-owned.
  One provider-backed `OperationDeadline` spans every setup stage and composite child, preserving the newest retryable broker error if a later deadline fires.
  Tokio and Moonpool share the policy with injected-time parity.
  ([#343](https://github.com/CleverCloud/magnetar/issues/343), ADR-0080)
- **`ConsumerEventListener` (Failover becameActive/becameInactive parity):** `ConsumerBuilder::consumer_event_listener(...)` + `subscribe_with_event_listener()` mirror Java's `ConsumerBuilder#consumerEventListener`. `ConsumerEvent::{BecameActive,BecameInactive}` fires from a detached poller task, sequentially, once per broker `CommandActiveConsumerChange` — the same push-delivery poller shape ADR-0064's `MessageListener` uses, driving the new `Consumer::next_active_change()` future instead of `receive()`. `Consumer::is_active()` exposes the last-reported state synchronously (`None` until the first transition). Proto-side, `ConsumerState` gains a bounded per-slot active-change ring (`ACTIVE_CHANGES_CAP = 32`, oldest dropped) recorded under the SAME per-slot lock the #307 reflow predicate already holds — one lock acquisition, not two. Also closes a latent leak: `ConnectionEvent::ActiveConsumerChanged` previously accumulated unbounded in the proto event queue (neither driver drained it); it is now silently consumed like `ChecksumMismatch`.
  (#348; ADR-0081)

### Fixed

- **`Auto` receiver-queue policy never scaled up under real load:** the consumer's permit mirror (`ConsumerState::available_permits`) was purely additive — bumped on every grant but never decremented as messages actually arrived — so the `FlowStats::available_permits == 0` starvation signal `Auto::adjust` needs was reachable only via a churn-window reset, never via a broker genuinely exhausting its grant.
  The field is now split: `ConsumerState::granted_permits` (renamed, semantics unchanged — the additive grant mirror the #307 failover-reflow gate and the want-have delta still use) and a new `ConsumerState::permit_balance` (the REAL balance: grants minus one unit per broker dispatch — plain message, batch member, chunk, or PIP-33 marker), which `flow_stats` now feeds into `FlowStats::available_permits`.
  A new churn-window guard skips the adjust tick entirely when `granted_permits == 0` (reset / terminal-failure / same-broker `CloseConsumer`), so that window is never mistaken for load starvation.
  `Auto::adjust` itself is unchanged — only the signal it was fed was wrong.
  (#349; ADR-0082)
- **Consumer final-clone resource release:** dropping the last clone of a consumer now stages a best-effort `CloseConsumer` and wakes the existing driver, allowing ownership-driven teardown to unregister the broker-side consumer when the frame reaches the broker and is accepted.
  Intermediate clone drops leave surviving consumers usable, explicit `close().await` remains the reliable acknowledgement-bearing path, and forgotten close responses never accumulate undrained outcomes.
  (#342; ADR-0077)
- **`aggregate_stats()` no longer zeroes the rate, latency-percentile, and `pending_batch_acks` fields:** `MultiTopicsConsumer::aggregate_stats` / `PartitionedConsumer::aggregate_stats` and `PartitionedProducer::aggregate_stats` previously summed only a handful of cumulative totals, silently leaving `msgs_per_sec`, `bytes_per_sec`, `receive_latency_p50_ms`/`p99_ms`, `send_latency_p50_ms`/`p99_ms`, and `pending_batch_acks` at their zeroed default regardless of the children's real values.
  New `ConsumerState::receive_latency_histogram` / `ProducerState::send_latency_histogram` accessors expose each child's raw latency distribution, and new `ConsumerStats::fold` / `ProducerStats::fold` associated functions (exhaustive per-field destructuring, so a future field addition is a compile error until this fold picks a rule for it) aggregate every child snapshot: the cumulative totals sum (saturating), the rolling rates sum as `f64`, `*_latency_max_ms` is the exact max, and `*_latency_p50_ms`/`p99_ms` are recomputed from a real `hdrhistogram::Histogram::add` merge of every child's histogram — summing or maxing percentiles directly is not statistically sound.
  Both `ConsumerApi` and `ProducerApi` gain a `{receive,send}_latency_histogram` accessor (implemented on both the tokio and moonpool engines) so the façade rewrites are thin collect-then-fold wrappers.
  (#347)
- **Ack orphaned by same-broker `CloseConsumer` + no deadline:** a same-broker bundle reassignment (`CommandCloseConsumer` with `assigned_broker_service_url = None`, the #307 root cause) tears the old consumer id down without ever answering an ack in flight against it, parking the caller's `ack().await` forever.
  The close-handler now fails every pending ack for the torn-down handle immediately (`code: -1, message: "ack orphaned by broker consumer close"`) before the in-place re-subscribe runs.
  As a generic backstop for any other cause of a dropped `CommandAckResponse`, `Connection::ack` now takes an injected `now: Instant` (ADR-0011) and a new `ConnectionConfig::ack_response_timeout` knob (default `Some(30s)`, mirroring the #304 `send_timeout` default; `None` disables it) reaps a pending ack whose response never arrives, mirroring the existing `send_timeout` sweep.
  **BREAKING CHANGE** (`magnetar-proto` only): `Connection::ack`, `Connection::close_consumer`, and `Connection::close_consumer_forget` are `pub fn` and now take an additional `now: Instant` parameter — any direct caller of these sans-io APIs (outside the `magnetar-runtime-tokio` / `magnetar-runtime-moonpool` engines, which are updated in this changeset) must pass the current instant at the call site.
  The `magnetar`/`magnetar-driver` façade and `magnetar_runtime_{tokio,moonpool}::Consumer::{ack,close}` are unaffected — their own signatures are unchanged.
  (#346)

## [1.2.2] - 2026-07-13

### Fixed

- **Chunked-consumer flow replenishment:** accepted incomplete PIP-37 chunks now repay their broker permits immediately, matching Java's per-chunk accounting and preventing queue-2,000 consumers from reaching zero permits below the logical-message refill threshold.
  (#331; ADR-0076)

## [1.2.1] - 2026-07-06

### Added

- **`ConsumerStats::pending_batch_acks`:** live count of PIP-54 per-batch ack-bitset entries (`batch_ack_tracker`).
  Magnetar-specific gauge (no Java counterpart): bounded by the un-acked window under a correctly-pruning ack path, so a monotonically growing value is the signature of the #326 leak class.
  (#326)

### Changed

- **Dependencies:** bumped workspace manifest floors — `rustls-pki-types` 1.14.1→1.15.0, `uuid` 1.23.3→1.23.4, `aws-lc-rs` 1.17.0→1.17.1, and `anyhow` 1.0.102→1.0.103 — and refreshed `Cargo.lock`; the `anyhow` bump resolves `RUSTSEC-2026-0190` flagged by `cargo-deny`.
  (#325)

### Fixed

- **Cumulative ack now prunes every `batch_ack_tracker` entry it covers:** the cumulative branch of `Connection::ack` removed only the tracker entry keyed on the acked id's exact `(ledger_id, entry_id)`, so a consumer acking exclusively via cumulative watermarks (never an individual ack) leaked one PIP-54 `BatchAckEntry` per batched broker entry for the lifetime of the connection — only a reconnect cleared the map.
  The fix prunes all entries at or below the cumulative position (`retain`, once per cumulative ack).
  Surfaced by the otelgw accesslogs converter OOMing at ~24 GiB every 4-6 h under a batched-topic, cumulative-only workload.
  (#326)
- **Driver write fairness: bounded write turns before reads:** the per-connection driver now retains staged outbound bytes and writes at most 256 KiB per loop turn before returning to the read-first `select!`, so already-persisted `CommandSendReceipt`s stay observable under large producer bursts instead of being starved behind an unbounded write drain — without splitting the single socket owner.
  Matched Tokio/Moonpool unit guards and an e2e burst that crosses the write budget keep both engines in `EventStream` parity; recorded as ADR-0074.
  (#319, #324)

## [1.2.0] - 2026-06-29

### Added

- **Consumer name on the multi-child consumer builders:** `PartitionedConsumerBuilder`, `MultiTopicsConsumerBuilder`, and `PatternConsumerBuilder` gain a `.name(impl Into<String>)` setter that propagates the consumer name verbatim to every per-partition / per-topic child via `ConsumerTemplate` (no per-partition suffix — every child subscribes with the same `consumer_name`, matching the Java client).
  Broker `topics stats` now reports a non-empty `consumerName` for each child, so a multi-instance Failover (or Shared) partitioned consumer is attributable to an instance.
  Previously only the inner per-topic `ConsumerBuilder` exposed `.name()`, leaving partitioned consumers stuck at `consumer_name: None`.
  (#300)
- **Pluggable consumer receiver-queue policy (PIP-74 auto-scaled queue):** a new `ReceiverQueuePolicy` trait in `magnetar-proto` makes the consumer receiver-queue size pluggable.
  `Fixed(usize)` is the DEFAULT and is byte-for-byte identical to the previous client; `Auto { min, max_bytes }` opts into PIP-74 `autoScaledReceiverQueueSizeEnabled` parity — growing the target by bounded doubling under starvation (`available_permits == 0`) and shrinking under a buffered-bytes guard, with hysteresis so it converges without thrashing.
  `adjust` is a pure function of `FlowStats` (no clock/RNG/I/O), so both engines stay bit-reproducible; the adjust tick rides the injected clock inside the per-slot consumer loop.
  Builder sugar: `receiver_queue_size(n)` resolves to `Fixed(n)`; `receiver_queue_policy(Arc::new(Auto::new(min, max_bytes)))` opts in (5s default tick, overridable).
  Threaded through partitioned / multi-topics / pattern consumers.
  (#301)
- **`ClientBuilder::connections_per_broker(n)` (Java `connectionsPerBroker` parity):** magnetar opened a single connection per broker, capping a logical producer fleet at one connection's send pipeline and forcing applications to hand-roll a pool of `PulsarClient`s.
  The new knob (default 1) opens up to `n` connections per broker and deterministically round-robins producers AND consumers across them via a shared cursor, fanning out both data-plane surfaces.
  Runtime-only — never reaches the sans-io proto core; lookups and redirect dials always pin index 0.
  The default of 1 is a byte-identical no-op.
  (#314)

### Changed

- **Producer `send_timeout` now defaults to 30s (Java `sendTimeoutMs = 30000` parity):** `CreateProducerRequest::send_timeout` previously defaulted to `None`, which disabled the per-send timeout sweep — a send whose `CommandSendReceipt` was lost or corrupted in flight (receipts carry no CRC32C) hung `Poll::Pending` forever.
  The canonical default is now `Some(30s)`, inherited by the v4 `ProducerBuilder`, `PartitionedProducerBuilder`, and `TypedProducerBuilder` (the V5 surface already used 30s); a timed-out send resolves with `SendError { code: -1, "send timeout" }` and wakes the parked waker.
  `ProducerBuilder::disable_send_timeout()` restores the previous unbounded behavior.
  (#304)
- **Dependencies:** raised workspace floors — `bytes` 1.11.1→1.12.0, `rustls` 0.23.40→0.23.41, `rustls-native-certs` 0.8.3→0.8.4, `zeroize` 1.8.2→1.9.0, and an explicit `opentelemetry` 0.32.0 patch pin — and refreshed `Cargo.lock`.
  (#312, #315)

### Fixed

- **Consumer wedge on same-broker bundle reassignment (broker-initiated `CloseConsumer`):** a code=6 bundle reassignment closes the consumer on the LIVE socket (no TCP drop, so the supervised reconnect / `rebuild_consumers` path never runs), which previously left a Failover/standby consumer parked at zero permits against a non-empty backlog — `receive()` frozen, `availablePermits=0`, `msgRateOut=0` — until a process restart.
  The sans-io core now: re-syncs the client permit mirror to zero on the broker-initiated close; re-subscribes the running consumer in place on the same connection (resuming from the last acked id, deferring the initial `CommandFlow` to the re-subscribe `Success`); re-arms flow when a Failover standby is promoted to active while holding zero permits; and wakes the driver after a single-message `receive()` so a queued replenishment `CommandFlow` is flushed at a buffered-inbound window boundary.
  Both engines inherit the fix from the proto seam.
  (#307, #317, #318)
- **Reconnect: bounded transient-open retry + recoverable-receive gating:** a transient producer-open / subscribe failure is now retried on bounded exponential backoff (2s initial, 8s cap) off the injected clock and, past the cap, terminalized via `fail_producer_open` / `fail_consumer_subscribe` so `send()` / `receive()` return an error instead of hanging forever (#302); and a `receive()` outstanding across a supervised drop no longer resolves `Err(Closed)` during the recoverable reconnect window — new sans-io predicates re-park it and resolve with the post-reconnect message after the rebuild replays `CommandSubscribe` (#299).
  (#299, #302, #313)
- **Driver read-arm fairness under sustained publish load:** the per-connection driver now polls the inbound read arm BEFORE the waker arm in its biased `select!`, so already-arrived `CommandSendReceipt`s are read promptly instead of sitting behind a near-always-pending send waker — collapsing `send().await` tail latency from hundreds of milliseconds back to broker-persist time.
  The outbound path is not starved (`poll_transmit` + `write_all` run at the top of every loop iteration) and the reorder is identical on both engines, so differential `EventStream` parity holds.
  (#303)

## [1.1.1] - 2026-06-17

### Added

- **`magnetar-admin` topic stats — full rate/throughput/size surface:** `TopicStats` now decodes the high-signal `PersistentTopicStats` metrics it previously dropped: `msgRateIn`, `msgRateOut`, `msgThroughputIn`, `msgThroughputOut`, `averageMsgSize`, `storageSize`, and `backlogSize` (alongside the existing `msgInCounter` / `bytesInCounter`).
  `magnetarctl admin topics stats <topic>` emits all of them in its JSON output, so `jq '.msgRateIn'` (and the out-rate, throughput, and storage/backlog sizes) now work for both non-partitioned and partitioned topics.
  Fields default to `0` when a broker release omits them.
  (#293)
- **`magnetarctl` message-id output — `segmentId` no longer dropped:** under the `scalable-topics` feature, `topics terminate` and `topics get-message-id-by-index` now surface the PIP-460 `segmentId` (JSON `null` when absent) instead of silently omitting it; both commands share one `message_id_to_json` renderer so their shapes can't drift.
  (#293)

### Changed

- **CLI (`magnetarctl`) default log level lowered to `warn`:** the default floor dropped from `magnetar=info` to `magnetar=warn`, so `magnetarctl` is quiet by default and surfaces only degraded-state warnings and errors.
  The whole `-v` ladder shifted down one rung — no capability is lost: `-v` now maps to `info` (the old default), `-vv` to `debug`, `-vvv` to `trace`, and `-vvvv`+ widen into the transport stack (reqwest/hyper/rustls/h2).
  Scripts that relied on the prior `info`-level default output must now pass `-v`.
  `docs/cli.md` and `docs/logging.md` updated to match.
  (#292)
- **Dependencies:** bumped `zeroize` 1.8.2→1.9.0.
  (#288)

## [1.1.0] - 2026-06-16

### Added

- **Admin client (`magnetar-admin`) OAuth2 + TLS:** `AdminClientBuilder` gains `oauth2(...)`, `tls_trust_cert_pem(...)`, and `tls_allow_insecure(...)`; a new `AdminAuth::OAuth2` arm refreshes the cached token on demand and attaches it as a bearer credential (erroring clearly on an empty access token).
  `magnetar-admin` now depends on `magnetar-auth-oauth2` (acyclic) and forwards each `crypto-*` feature to it so the token-exchange client binds the same rustls provider.
  (#281)
- **CLI (`magnetarctl`) pulsarctl config file + contexts:** `magnetarctl` now reads the standard pulsarctl config (`--config` > `MAGNETAR_CONFIG` > `$XDG_CONFIG_HOME` > `$HOME/.config/pulsar/config`) and ships a `context` command group (`use`/`set`/`delete`/`get`/`current`/`rename`, with `create`/`del`/`update` aliases) matching the pulsarctl output strings — a working pulsarctl setup now works with zero extra flags.
  Unknown keys and key casing round-trip untouched, so a magnetarctl-written file stays pulsarctl-readable.
  New global flags `--config`, `--context`, `--token-file`, `--tls-trust-cert-path`, `--tls-allow-insecure`, `--tls-enable-hostname-verification`, `--tls-cert-file`, `--tls-key-file`, and `-s` (short for `--admin-url`); the active context supplies the admin URL + auth + TLS, and the data-plane URL is derived from the admin-service-url (`http` → `pulsar://…:6650`, `https` → `pulsar+ssl://…:6651`) unless an explicit `--service-url` is given.
  (#281, #284; ADR-0068)
- **CLI `context rename --force` (`-f`):** opt into overwriting an existing destination context; the destination then fully becomes the source (endpoint + credentials, clearing any stale destination credential), with a warning printed.
  (#284)

### Changed

- **`magnetar-admin` (`AdminError`):** added a new `AdminError::Decode { method, url, status, content_type, snippet, source }` variant carried by the JSON decoders, and added `method: String` + `url: String` fields to the existing `AdminError::Status` variant.
  `AdminError` stays exhaustive (no `#[non_exhaustive]`), so any exhaustive `match` over it or any `Status { code, body }` destructure without `..` must be updated.
  The existing `AdminError::Json` variant is now reserved for request-body **encode** failures only (its `#[error]` text changed from `json decode: …` to `json encode: …`); response **decode** failures route through `AdminError::Decode`.
  (#282)
- **`magnetar-admin` (`AdminError`):** added an `AdminError::Auth(String)` variant for OAuth2 token-acquisition failures.
  Since `AdminError` is exhaustive, downstream exhaustive matches must add this arm.
  (#281)

### Fixed

- **Admin client (`magnetar-admin`):** non-JSON admin responses now surface the request method, URL, HTTP status, `Content-Type`, and a truncated body snippet instead of the bare `serde_json` message (`json decode: expected value at line 1 column 1`).
  Hitting the wrong endpoint, a reverse proxy, or an auth-redirect on a 2xx is now self-diagnosing.
  Non-success statuses (`AdminError::Status`) also name the method + URL.
  (#282)
- **CLI (`magnetarctl`) config/context correctness:** `context rename` refuses to overwrite an existing destination instead of silently destroying its endpoint + credentials (use `--force` to opt in); `context set` no longer persists an inherited `MAGNETAR_TOKEN` (only an explicit `--token` is written) and clears mutually-exclusive auth fields when switching auth mode so a stale higher-precedence credential cannot shadow the one just configured.
  (#281, #284)

### Security

- **CLI (`magnetarctl`) credential safety:** OAuth2 rejects a non-`https` `issuer_endpoint` up front so the `client_secret` is never POSTed over plaintext; `AuthInfo` carries a redacting `Debug` so a `{:?}` of the config never leaks the bearer token; `config save` forces `0600` on a pre-existing world-readable config before writing credentials; an empty token-file token is rejected rather than sending a malformed `Authorization: Bearer ` header to the broker; and the CLI warns when `tls_allow_insecure` is inherited from a context (silent verification downgrade).
  (#281, #284)

## [1.0.1] - 2026-06-15

### Changed

- Renamed the published crates.io packages to avoid a name collision (the `magnetar` name is held by an unrelated, abandoned crate): the façade ships as **`magnetar-driver`** and the CLI as **`magnetarctl`** (binary command `magnetarctl`).
  The façade's library/import name is unchanged — `use magnetar::*` still works; only the dependency line differs (`magnetar-driver = "1.0.1"`).
  No API, behavior, or wire-format change.
  (ADR-0067)

## [1.0.0] - 2026-06-15

First stable release.
Magnetar is a from-scratch Apache Pulsar client driver for Rust with full Apache Pulsar Java-client parity, a sans-io protocol core, and two interchangeable runtime engines.
See the [parity matrix](README.md#java-client-parity-matrix) for the per-feature status snapshot.

### Added

- Initial public release of magnetar, a from-scratch Apache Pulsar client driver for Rust.
  Targets Apache Pulsar 4.0+ LTS, advertises CONNECT `ProtocolVersion::V21` with downgrade fallback, and ships as a 12-crate workspace (façade `magnetar`, sans-io core `magnetar-proto`, `magnetar-runtime-tokio`, `magnetar-runtime-moonpool`, `magnetar-differential`, `magnetar-admin`, `magnetarctl`, `magnetar-fakes`, `magnetar-auth-oauth2`, `magnetar-auth-sasl`, `magnetar-auth-athenz`, `magnetar-messagecrypto`) plus `xtask`.
  (75f7c16)
- Sans-io protocol core (`magnetar-proto`): a `quinn-proto`-style pure state machine (`handle_bytes` / `poll_transmit` / `poll_event` / `poll_timeout`) for Connection, Producer, and Consumer, with zero I/O dependencies and injected clocks (`now: Instant`, `wall_clock` provider).
  The same state machine drives both engines.
  (123b8db, 10cb025; ADR-0004, ADR-0011)
- Dual runtime engines selected at the type level via `PulsarClient<E: Engine = TokioEngine>`: a production tokio engine and a deterministic-simulation moonpool engine over `moonpool_core::Providers`.
  The moonpool engine reaches full façade parity (driver loop + transport, Client lookup / partitioned-metadata / topic-watch, Producer send/flush/close, Consumer receive/ack/seek) plus a rustls-over-bytepipe TLS adapter.
  (405d2cd, 9555113, 1eba8e1, f59032f, e01d676, 3a119e7)
- No-channels concurrency architecture: all `mpsc`/`broadcast`/`watch`/`oneshot` and third-party channel crates are banned and replaced with `Arc<parking_lot::Mutex<…>>` + `tokio::sync::Notify` + `Waker` slabs.
  A split connection mutex enforces global→per-slot lock ordering so the `Producer::send` hot path takes only the per-slot mutex.
  (3275b41; ADR-0003, ADR-0038)
- Producer Java-client parity: `send`/`sendAsync`, batching with `batchingMaxPublishDelay` flush timer, message chunking (PIP-37, chunks-never-batched with bounded consumer reassembly cap), LZ4/ZSTD/Snappy/ZLIB compression, `initialSequenceId`, `sendTimeout`, producer access modes Shared/Exclusive/WaitForExclusive/Fencing (PIP-68), custom `MessageRouter` with Murmur3/JavaStringHash, an interceptor SPI, `TypedMessageBuilder`, and hdrhistogram p50/p99/max stats.
  Best-effort `CloseProducer` is sent on last-clone drop.
  (#243; ADR-0057, ADR-0063)
- Consumer Java-client parity: Exclusive/Shared/Failover/Key_Shared subscriptions, `receive`/`batchReceive`, the full ack family (individual/cumulative/batch/with-properties/under-txn) including batch-index ack (PIP-54/391), negative-ack with `MultiplierRedeliveryBackoff` and an ack-timeout tracker (PIP-37), `reconsumeLater` retry-letter (PIP-58), dead-letter policy (PIP-22/58/124/409), seek by id/timestamp/earliest/latest and per-partition, pause/resume, `readCompacted`, key-shared sticky/auto-split/hash policy (PIP-34/119/282/379), subscription properties, `replicateSubscriptionState`, force-unsubscribe (PIP-313), and `MessageListener` push delivery across single/typed/multi-topic/partitioned/pattern consumers.
  (fe33784; ADR-0064)
- Reader, partitioned producer/consumer, multi-topic, pattern (regex, PIP-145), and `TableView` surfaces, all generic over `E: Engine`, with `auto_update_partitions_interval` tickers for partition growth.
  (8cfd1e3, 844655b, fe5d8c0, b51680a, 31f9cbe, 2b7570c, f09f23c)
- Transactions (PIP-31) end-to-end: a `TxnClient` coordinator with begin/commit/abort, `ADD_PARTITION_TO_TXN` / `ADD_SUBSCRIPTION_TO_TXN`, publish-under-txn, ack-under-txn, and `END_TXN` cleanup; the `Transaction` surface is engine-generic.
  (71e81e9, 19a8df5, ab9041b)
- Schema layer with full Java parity: Bytes/String/JSON/Avro/Protobuf/ProtobufNative/KeyValue/AutoConsume (PIP-87 broker lookup)/AutoProduceBytes plus all primitives.
  AVRO/JSON are canonicalised via the broker canonical form (apache-avro 0.21); PROTOBUF_NATIVE and KeyValue output is byte-identical to the Java client.
  (f3eb61b, d265a06, 08f5702)
- Authentication provider parity: Token, mTLS, OAuth2 `ClientCredentialsFlow` with token caching, SASL-PLAIN (RFC 4616), SASL-Kerberos via `libgssapi` multi-round `AUTH_CHALLENGE`, and Athenz (pre-fetched role token plus opt-in ZTS round-trip).
  In-band `AUTH_CHALLENGE` credential refresh implements PIP-30/PIP-292.
  (48a65b4, 122298e; ADR-0014, ADR-0029, ADR-0030, ADR-0041)
- Pluggable rustls crypto providers selected at compile time on the façade: `crypto-aws-lc-rs` (default, post-quantum hybrid key exchange), `crypto-ring`, `crypto-openssl` (`rustls-openssl` wrapper), and `crypto-fips` (`aws-lc-fips-sys`). rustls-only — no native-tls.
  (3f392af, b6f9cbe, closes issue #9; ADR-0005, ADR-0035)
- End-to-end message encryption (PIP-4, `magnetar-messagecrypto`): AES-GCM payload encryption with RSA-OAEP key wrapping, `MessageEncryptor`/`MessageDecryptor` traits on producer and consumer, and `cryptoFailureAction` Fail/Discard/Consume wired end-to-end, including a moonpool message-crypto bridge.
  (1bfc7e3, 6039251; ADR-0044)
- Admin REST client (`magnetar-admin`, reqwest + rustls) and a kubectl-style `magnetar` CLI: namespace/topic policy endpoints (retention, backlog-quota, TTL, persistence, dispatch-rate, dedup, compaction, delayed-delivery, max-producers/consumers/unacked), schema registry, rack-aware brokers/bookies, Functions/Sources/Sinks/Packages, subscription ops, and PIP-415 `getMessageIdByIndex`.
  (d315c20, d26028b)
- Resilience: supervised reconnect with `Connection::reset` and transparent producer/consumer rebuild, keepalive watchdog, terminal fast-fail, lookup-retry on session-lost, ack-gated re-attach replay, and a handshake-failure budget.
  Memory limit with `FailImmediately` atomic CAS and a `ProducerBlock` `Waker` slab, cluster failover (PIP-121 `ServiceUrlProvider`, Controlled/Auto), and `TOPIC_MIGRATED` supervised reconnect (PIP-188).
  (#263, 5dcc6f9, 6013320; ADR-0016, ADR-0017, ADR-0018, ADR-0020, ADR-0028, ADR-0060, ADR-0061)
- Additional PIPs: broker-entry metadata (PIP-90), shadow topics (PIP-180 — admin CRUD, producer `send_with_source_message_id`, consumer `MessageReceivedFromShadow`), and replicated subscriptions (PIP-33 — `replicate_subscription_state` field plus marker filter, with a two-cluster e2e fixture).
  (bc7ea94, 01d0afd; ADR-0033, ADR-0034)
- Experimental, default-off surfaces: PIP-466 V5 client (`magnetar::v5` behind `experimental-v5-client`, wraps v4 with no wire change) and the PIP-460 scalable-topics scaffold (behind `scalable-topics`: `topic://` URLs, DAG watch, `StreamConsumer`, `magnetar topic-info`).
  No released broker ships PIP-460.
  (b3c581e, d3684ac; ADR-0031, ADR-0032)
- Observability and proxy support: OpenTelemetry context propagation behind the `opentelemetry` feature (auto-injects `traceparent`/`tracestate` into message properties at the send boundary), and Apache Pulsar Proxy support via a per-broker connection pool with lookup-driven routing.
  (#151, #17; ADR-0039, ADR-0053)
- Structured logging across the driver: every error/warn/info log carries at least one structured field (xtask-enforced), with subscriber-side rate-limiting/sampling guidance.
  (#218, #280; ADR-0054, ADR-0065)
- Cross-runtime test and coverage policy: every behavioral change ships proto-unit + tokio-integration + moonpool-integration + differential-equivalence + e2e tests, with 100% moonpool patch coverage and a strict 1:1 tokio↔moonpool test count, all xtask-gated.
  The deterministic-simulation harness adds buggify fault injection, swizzle-clog workload, bit-flip survivability, and a seed sweep.
  (0c8c26c, fec933b; ADR-0024, ADR-0036, ADR-0048, ADR-0050, ADR-0055)

### Changed

- Migrated to Rust Edition 2024 and raised the MSRV to 1.88.
  (ADR-0007, ADR-0042)
- Refactored the client façade into dedicated `builders.rs` / `client_builder.rs` modules.
  (9b83a00, a69830c)
- Repinned the moonpool dependency to the published crates.io 0.7.0 (from a floating git dependency) and adopted vectored writes.
  (#242, 6a0e24b; ADR-0043, ADR-0056)
- Bumped runtime dependencies: `libgssapi` 0.7.2→0.9.1, `http` 1.4.0→1.4.1, and `rcgen` 0.13.2→0.14.8.
  (#13, #152)

### Removed

- Removed dead scaffolding and consolidated the crypto traits into `magnetar-proto`.
  (2a07f07)
- Removed `tls_trust_certs_file_path` from `ClientBuilder`.
  (1736be8)
- Dropped the earlier multi-step (0.1.0 / 0.2.0) release-planning artifacts, superseded by this single 1.0.0 release.
  (fd6e62d)

### Fixed

- Pre-release audit correctness fixes: decompression-bomb size cap, `Instant`-overflow guard, partition-hash correctness, and multi-topic receive starvation.
  (#279, 9347c39)
- Closed transaction parity gaps so the transactions e2e suite passes.
  (19a8df5)
- Hardened consumer behavior during seek and across transient broker-close, and fixed partitioned-topic auto-detection on topic delete.
  (issue #65, 780349c, 6ec47a1)
- The ack-timeout tracker now drops nacked message ids to prevent double redelivery.
  (7ce5e25)
- Resolved moonpool deterministic-simulation seed-sweep failures and hardened swizzle seed replay.
  (#244, #262, #264)
- The reconnect supervisor now persists its backoff across reconnects and resets only after the drop-grace window is stable.
  (#16)

### Security

- Secrets are redacted from `Debug` output: passwords and private keys (CWE-532), Athenz `private_key_pem`, and `AdminAuth::Token`, each guarded by secret-scan log-capture tests.
  (3406f7d, e92994e, f5ae060, 28711ef)
- All `panic!` and `debug_assert!` calls are removed from `magnetar-proto` production paths; every path returns `Result`/`Option`.
  (a561203, cac2199)
- CRC32C verify-or-drop on frames with magic `0x0e01`: a checksum mismatch emits a `ChecksumMismatch` event and drops the frame.
- Exposed `tls_allow_insecure_connection` and `tls_hostname_verification_enable` for Java parity, and cleared cargo-audit advisories (`time` 0.3.45 CVE, `rustls-pemfile` unmaintained).
  (2a9fafb, abc7aad)

[1.2.3]: https://github.com/CleverCloud/magnetar/releases/tag/v1.2.3
[1.2.2]: https://github.com/CleverCloud/magnetar/releases/tag/v1.2.2
[1.2.1]: https://github.com/CleverCloud/magnetar/releases/tag/v1.2.1
[1.2.0]: https://github.com/CleverCloud/magnetar/releases/tag/v1.2.0
[1.1.1]: https://github.com/CleverCloud/magnetar/releases/tag/v1.1.1
[1.1.0]: https://github.com/CleverCloud/magnetar/releases/tag/v1.1.0
[1.0.1]: https://github.com/CleverCloud/magnetar/releases/tag/v1.0.1
[1.0.0]: https://github.com/CleverCloud/magnetar/releases/tag/v1.0.0
