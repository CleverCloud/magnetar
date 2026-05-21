# Magnetar — Parallel Implementer Swarm Status

**Generated**: 2026-05-21 11:48 (HEAD `15fe1b6`)

## In-flight agents (worktrees locked at 15fe1b6)

| Track | Agent | Task # | Scope | Status |
|---|---|---|---|---|
| TA | `worktree-agent-af146334fe8bb3685` | #69 | UnAckedMessageTracker + AckGroupingTracker behavioral tests | running |
| TB | `worktree-agent-a774f6e7216373caf` | #70 | BatchMessageContainer behavioral tests | running |
| TC | `worktree-agent-ae31e3bde738ead32` | #71 | Moonpool engine M1 (driver loop + transport) | running |
| TD | `worktree-agent-a40c56f75a2791e0e` | #72 | Murmur3 + JavaStringHash hashers | running |
| TF | `worktree-agent-a842215fabac3e8ea` | #73 | CryptoFailureAction on Consumer | running |

## Held files (do not touch in main worktree)

- TA: `crates/magnetar-proto/src/trackers/unacked.rs`, `crates/magnetar-proto/src/trackers/ack.rs`
- TB: `crates/magnetar-proto/src/producer.rs` (`#[cfg(test)] mod tests` section)
- TC: `crates/magnetar-runtime-moonpool/src/**`
- TD: `crates/magnetar/src/partitioned_producer.rs`, `crates/magnetar/src/lib.rs`
- TF: `crates/magnetar-proto/src/conn.rs` (`SubscribeRequest`), `crates/magnetar-proto/src/consumer.rs`, `crates/magnetar-runtime-tokio/src/consumer.rs`, `crates/magnetar/src/client.rs` (`ConsumerBuilder`)

## Free files for direct main work

- `crates/magnetar/src/multi_topics.rs`
- `crates/magnetar/src/pattern_consumer.rs`
- `crates/magnetar/src/table_view.rs`
- `crates/magnetar/src/typed.rs` (caveat: TF may touch ConsumerBuilder mirror)
- `crates/magnetar/src/partitioned_consumer.rs`
- `crates/magnetar/src/transaction.rs`
- `crates/magnetar-runtime-tokio/src/producer.rs`
- `crates/magnetar-runtime-tokio/src/client.rs`
- `crates/magnetar-runtime-tokio/src/driver.rs`
- `crates/magnetar-proto/src/txn.rs`
- `crates/magnetar-proto/src/schema/**`
- `crates/magnetar-proto/src/trackers/nack.rs` (TA explicitly scopes to unacked/ack only)

## Merge order

When each implementer lands:
1. Fetch the agent's branch into local main: `git fetch . worktree-agent-<id>:agent-<id>`
2. Validate locally: `cargo build --all-features && cargo test --all-features --workspace`
3. Merge with `--no-ff`: `git merge --no-ff -S agent-<id> -m "merge(agent-<id>): <track summary>"`
4. Push: `git push origin main`

Merge order matters when scopes overlap. Current scopes are designed to be conflict-free, but the safe order is: TA → TB → TD → TF → TC (TC is the largest, leave it for last).

## Post-swarm follow-up

After all 5 implementers merge, the next batch of bounded chunks:
- Auto-reconnect supervisor (largest remaining gap)
- hdrhistogram latency stats (requires workspace dep proposal first)
- PatternConsumer auto-update background task (blocked on Arc<PulsarClient>)
- MultiTopicsConsumer dynamic add/remove (refactor to Mutex<Vec>)
- Consumer#seek(Function&lt;String, Object&gt;) per-partition function-based seek
- TableView auto-update-partitions-interval
- TableView crypto reader bridge

## Constraints reminder

- **No channels**: `Arc<parking_lot::Mutex<...>>` + `tokio::sync::Notify` + per-future Waker slabs
- **Commits**: GPG-signed via `git commit -s -S`
- **No Claude attribution** on commits / PRs / MRs
- **Branches**: `feat/<scope>`, `fix/<scope>`, etc.
- **Conventional commits**: `<type>(<scope>): <subject>`
