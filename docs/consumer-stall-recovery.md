# Consumer stall detection and recovery

A `Shared` subscription can wedge **broker-side** after consumer churn: the attached consumers stop receiving, permanently, while the connection stays perfectly healthy.
This page is the operator-facing form of [ADR-0101](../specs/adr/0101-consumer-stall-detection-and-in-place-recovery.md) and [ADR-0103](../specs/adr/0103-bounded-automatic-consumer-stall-recovery.md) — what the symptom looks like, how to detect it from the client, and the recovery ladder.

Reported as [issue #414](https://github.com/CleverCloud/magnetar/issues/414).

## The symptom

The reported production shape:

| Observation                                    | Value                                                                               |
| ---------------------------------------------- | ----------------------------------------------------------------------------------- |
| Trigger                                        | cursor reset with consumers attached, 12 → 1 scale-down, instance recycle mid-drain |
| Consumer behaviour                             | each survivor receives ~20 messages, then silence, indefinitely                     |
| Broker `availablePermits` for the subscription | `-177300`                                                                           |
| Broker `acks_failed`                           | `0`                                                                                 |
| Client-side errors                             | none                                                                                |
| Connection health                              | fine — keepalive `PING` / `PONG` keeps passing                                      |
| Recovery that worked                           | superuser `pulsar-admin topics unload`                                              |

**The client cannot cause this.**
The Pulsar wire protocol carries only monotonic client → broker permit increments (`CommandFlow`); there is no decrement on the wire, so no client behaviour drives the broker's counter negative.
Magnetar additionally zeroes its own permit mirrors in lock-step at every churn boundary — reconnect reset, same-broker `CommandCloseConsumer`, terminal subscribe failure.

What the client owes you is the ability to **notice** and a **cheaper first recovery step** than unloading the topic.

## Why the connection keepalive does not catch it

Magnetar's connection watchdog ([ADR-0058](../specs/adr/0058-keepalive-watchdog-progress-based.md)) refreshes one connection-wide `last_activity` baseline off **every** decoded inbound frame.
A broker whose dispatcher has wedged for ONE subscription still answers `PING` with `PONG`, still serves every other subscription on that connection, and still replies to acks.
The baseline never ages, so no connection-level deadline ever fires.

Detecting this needs a **per-consumer** signal. That is what the two mechanisms below are.

## Detection

### 1. Poll `available_permits()`

```rust
let permits = consumer.available_permits();
```

Since [ADR-0101](../specs/adr/0101-consumer-stall-detection-and-in-place-recovery.md) this reports the **real, decrementing** broker permit balance — the grants issued minus one per dispatch unit that actually arrived — matching Java's `ConsumerBase#getAvailablePermits`.

- A **healthy** consumer's balance falls as messages arrive and climbs again on each replenishment `CommandFlow`. It moves.
- A **wedged** consumer's balance sits pinned near the receiver-queue size while nothing arrives.

> **Semantic change.** Before ADR-0101 this accessor read the purely-additive grant mirror, which never moved under dispatch — it read `receiver_queue_size` forever whether the broker was streaming or dead. If you have code that treated it as a cumulative grant total, it now returns the un-spent balance instead. [ADR-0082](../specs/adr/0082-consumer-permit-balance-split.md)'s deferral of exactly this accessor is what ADR-0101 amends.

Two things this rung does NOT cover.

A poison-heavy topic with a dead-letter policy is not a stall.
Until [ADR-0107](../specs/adr/0107-refund-the-flow-permit-of-a-dead-lettered-dispatch-unit.md) it looked exactly like one from the outside — the balance fell to zero and stayed there, because a dead-lettered dispatch unit was debited and never refunded — and no rung on this ladder could recover it.
A dead-lettered unit now returns its permit at routing time, so the balance climbs again on its own and the subscription keeps draining.
What grows instead is the consumer's dead-letter buffer, which only `drain_dead_letter` / `republish_dead_letters` empties.

And `permit_balance == 0` was never in scope for the watchdog below: `is_stall_candidate` requires `permit_balance > 0`, since a consumer holding no permits has told the broker nothing it is failing to honour.
A balance pinned at zero is therefore read here, by polling, and not reported as a `ConsumerStalled` event.

### 2. Arm the stall watchdog

```rust
use std::time::Duration;

let client = PulsarClient::builder()
    .service_url(service_url)
    // 30 s matches the keepalive and ack-response cadences.
    .consumer_stall_timeout(Duration::from_secs(30))
    .build()
    .await?;
```

A consumer that holds un-spent broker permits over an **empty** receive queue, in a dispatch-eligible state, for the whole window without a single dispatch unit arriving surfaces:

- one `WARN` on target `magnetar_proto::conn` carrying `handle`, `permit_balance` and `stalled_for_ms`;
- one `ConnectionEvent::ConsumerStalled { handle, permit_balance, stalled_for }`.

Exactly **once per stall episode**: the next dispatch unit re-arms the watchdog, so a consumer that recovers and wedges again reports again.
The window opens when the broker is granted its permits (every subscribe ack, reconnect rebuild, post-seek resubscribe, and recovery routes through the same `initial_flow`), so a consumer that is granted and then handed nothing reports one window later — not one window plus a keepalive interval.
Every state that has its own explanation for the silence suppresses it — `pause`, an in-flight seek, end-of-topic, a terminal subscribe failure, a re-attach in progress, and a non-empty local queue (there the user, not the broker, owes the progress).

`Duration::ZERO` disables it. **The knob is off by default**: an armed deadline perturbs the deterministic-simulation engine's wake schedule even when it never fires, and Java has no per-consumer dispatch watchdog to inherit a default from.

> **The event reports silence, not fault.**
> A consumer that has drained its backlog on an idle topic satisfies the predicate exactly as a wedged one does — the client cannot see the broker's backlog, so it cannot tell them apart.
> Correlate before acting (next section). This is also why the watchdog does nothing on its own unless you arm rung 0 below.

### 3. Confirm against broker truth

```rust
let admin = AdminClient::builder().service_url(admin_url).build()?;
let stats = admin.topic_stats(&topic).await?;
// `subscriptions` is raw JSON: the broker's own per-subscription view.
let subscription = &stats.subscriptions[&subscription_name];
```

Two fields settle it:

- `msgBacklog` — messages the broker is holding for this subscription. A stall with a **zero** backlog is an idle topic, not a fault.
- `availablePermits` — the broker's own counter for the subscription. A **negative** value is the issue #414 signature, and it is the one thing no amount of client-side inspection can infer.

Also worth a glance: `msgRateOut` at `0` alongside a non-zero `msgBacklog`, and `consumers[].availablePermits` per attached consumer.

## Recovery ladder

Climb it in order — each rung is more disruptive than the last.

### Rung 0 — let the watchdog climb rung 1 for you

```rust
use std::time::Duration;

let client = PulsarClient::builder()
    .service_url(service_url)
    // Required: automatic recovery is inert without a stall window.
    .consumer_stall_timeout(Duration::from_secs(30))
    // At most 3 in-place re-subscribes per stall streak, then escalate.
    .consumer_stall_auto_recovery(3)
    .build()
    .await?;
```

When a stall episode closes, the client performs rung 1 itself — the identical call, the identical effects — instead of leaving it to you.

- **At most one attempt per stall episode**, and an episode closes at most once per `consumer_stall_timeout`. With a 30 s window, `3` spends three re-subscribes over ninety seconds and then stops.
- **The budget resets on real progress only**: one broker dispatch unit actually arriving. A consumer that recovers, runs healthily, and later wedges again gets its full budget back; a consumer the broker acks but never dispatches to does not, because the recovery's own re-subscribe would otherwise refund every attempt that paid for it.
- **An ineligible consumer spends no budget** — closed, unsubscribing, terminally failed, mid-seek, already re-attaching, already awaiting a recovery close, `Failover`, or non-durable. Nothing is mutated in that case.
- **A Failover standby is skipped entirely.** Once the broker has reported this consumer as standby (`Consumer::is_active() == Some(false)`), the stall is still reported but no recovery is attempted and no budget is spent. A standby holds its initial grant over an empty queue forever, which is the stall predicate exactly — and since it never receives a dispatch unit, nothing would ever give the budget back. Without the skip, arming recovery on a failover group would burn every standby's whole budget on the broker behaving correctly. A consumer promoted to active keeps the full budget it never spent, so a genuine wedge after promotion still gets the complete ladder.
- **The diagnosis is never suppressed.** The `WARN` and the `ConsumerStalled` event fire on every episode whether or not recovery acts, each attempt logs its own `INFO` carrying `attempt` and `max_attempts`, and exhausting the budget logs one `WARN` naming `pulsar-admin topics unload`.
- **Unset by default**; `0` disables it explicitly.

**Keep the number small.** An attempt does not pay down a negative aggregate permit counter — since [ADR-0108](../specs/adr/0108-close-then-resubscribe-for-in-place-consumer-recovery.md) one recovery is permit-neutral on the subscription's aggregate (see rung 1) — so a budget is a small number of chances for THIS consumer's own slot to come back, not a countdown toward repairing a subscription-wide fault. Issue #414's production failure was `-177300` deep; the point of the bound is to stop and escalate to `pulsar-admin topics unload`, not to re-subscribe forever against something this client cannot repair.

> **This is opt-in for a reason.** The watchdog reports silence, not fault, so an armed budget will occasionally close and re-subscribe a perfectly healthy consumer that is merely idle on a drained topic. Since ADR-0108 that is no longer free: the close is real, so anything the consumer was holding un-acked is redelivered. Arm it only where a duplicate is cheaper than a wedge, and prefer rung 1 by hand where it is not.

### Rung 1 — `Consumer::resubscribe()`

```rust
consumer.resubscribe()?;
```

**Closes this consumer id and re-subscribes it**, on the live connection, in two phases:

1. `CommandCloseConsumer` for this consumer id;
2. on the broker's `Success` for that close, and only then: zero the permit mirrors, fail every in-flight ack, re-emit `CommandSubscribe` for the same consumer id, and let ITS `Success` release a fresh initial `CommandFlow`.

- No transport reconnect. No other consumer, producer, or subscription is disturbed.
- The client-side consumer survives its own close: the handle, the registration, and the receiver queue are all kept, so anything already buffered stays receivable.
- Returns as soon as the `CommandCloseConsumer` is staged and the driver is woken; the grant re-arms two broker replies later. Poll `available_permits()` to watch it land.
- Returns `Err` — mutating nothing — when the consumer is not eligible: closed, unsubscribing, terminally failed, mid-seek, already re-attaching, already awaiting a recovery close, **`Failover`**, or **non-durable** (see below).
- If the broker **rejects** the close, nothing is mutated and no re-subscribe is sent. One `WARN` on `magnetar_proto::conn` records the rejection; the consumer is left exactly as it was — still wedged, still reported by the watchdog, still eligible for another attempt.

> **Why the close is not optional.** A `CommandSubscribe` naming a consumer id that is still live on the connection is a **broker-side no-op**: `ServerCnx.handleSubscribe` finds the id in its own per-connection map, logs a warning, answers `CommandSuccess`, and returns without touching the dispatcher, the cursor, or `availablePermits` (identical at Pulsar v4.0.4 `ServerCnx.java:1320-1326`, v4.2.4 `:1408-1414` and master `:2037-2044`). Before [ADR-0108](../specs/adr/0108-close-then-resubscribe-for-in-place-consumer-recovery.md) this rung sent exactly that, and then granted a second full receiver-queue window on top of a slot the broker had never reset — adding permits the broker never agreed to, once per attempt, while repairing nothing. The close is what makes the re-attach real.

**It costs redelivery.** The broker genuinely drops the consumer, so everything it was holding un-acked goes back to the subscription's redelivery pool — to this consumer after its re-attach, or to a `Shared` sibling meanwhile. Acks still buffered in the ack-grouping tracker when the close lands are dropped by the broker and their messages redelivered. At-least-once is preserved; **duplicates are not**. If your consumer is not idempotent, prefer rung 2, where you control the boundary.

**Two subscription shapes have no rung 1**, and `resubscribe()` returns `Err` for both rather than doing something surprising:

- **`Failover`** — the close really detaches the consumer, so the broker runs an election. An active consumer would hand its partition to a standby and come back at the end of the priority order. That is a subscription-wide reshuffle to repair one slot. Use rung 2 or rung 3.
- **non-durable** — a non-durable subscription's cursor exists only while the consumer does. Closing it discards the cursor and the re-attach restarts from the subscription's configured start position, silently skipping or replaying the backlog. There is no in-place repair for a cursor the close destroys.

**What it repairs:** this client's own slot in the broker's dispatcher.

**What it does not repair:** a dispatcher-WIDE corruption. Issue #414's production failure had the subscription's `availablePermits` at `-177300` across every attached consumer, and this does not clear it. One recovery attempt is **permit-neutral** on the subscription's aggregate counter — the close returns this consumer's remaining permits and the re-subscribe's `CommandFlow` grants them back — so attempts do not add up to a repair the way the pre-ADR-0108 arithmetic claimed. `topics unload` is the answer to a negative aggregate, and rung 0's bound exists to reach it rather than to grind toward it.

Give it a few seconds and re-check `available_permits()` and the broker's `msgRateOut`. If nothing moves, climb.

### Rung 2 — recreate the consumer

Close the consumer and subscribe again. This gets a fresh consumer id rather than reusing the wedged one.
Cheap, still scoped to this client, and worth trying before touching the topic.

### Rung 3 — `topics unload`

```rust
admin.topic_unload(&topic).await?;
```

or

```sh
pulsar-admin topics unload persistent://tenant/namespace/topic
```

Forces the topic off its current broker so ownership is re-elected and the dispatcher is rebuilt from scratch.
This is what recovered the production incident.

**It is disruptive**: every producer and every subscription on that topic is detached and must re-attach. It also needs superuser (or namespace-admin) rights.

## Prevention

Nothing here prevents a broker-side dispatcher fault, but two habits shrink the churn window issue #414 was triggered from:

- **Do not reset a cursor while consumers are attached.** Detach, seek, re-attach. A cursor reset under a live dispatcher is the first of the three reported triggers.
- **Scale down gracefully.** Close each consumer with `Consumer::close().await` and let the broker redistribute before removing the next one, rather than recycling instances mid-drain. A `close()` that completes returns the consumer's un-acked in-flight entries to the subscription in an orderly way; a killed process leaves the broker to time them out.

## See also

- [ADR-0101](../specs/adr/0101-consumer-stall-detection-and-in-place-recovery.md) — the decision, its alternatives, and the ADR-0082 amendment.
- [ADR-0103](../specs/adr/0103-bounded-automatic-consumer-stall-recovery.md) — rung 0: why automatic recovery is opt-in, why it is bounded, and why the budget resets on a dispatch unit and on nothing else.
- [ADR-0108](../specs/adr/0108-close-then-resubscribe-for-in-place-consumer-recovery.md) — why rung 1 closes the consumer before re-subscribing it, and why `Failover` and non-durable subscriptions have no rung 1.
- [ADR-0082](../specs/adr/0082-consumer-permit-balance-split.md) — the `granted_permits` / `permit_balance` split.
- [ADR-0058](../specs/adr/0058-keepalive-watchdog-progress-based.md) — the connection keepalive, and why it cannot see this.
- [`logging.md`](logging.md) — the structured-log field glossary.
- [`observability.md`](observability.md) — OpenTelemetry context propagation.
