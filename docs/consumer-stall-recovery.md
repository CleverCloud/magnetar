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
- **An ineligible consumer spends no budget** — closed, unsubscribing, terminally failed, mid-seek, or already re-attaching. Nothing is mutated in that case.
- **The diagnosis is never suppressed.** The `WARN` and the `ConsumerStalled` event fire on every episode whether or not recovery acts, each attempt logs its own `INFO` carrying `attempt` and `max_attempts`, and exhausting the budget logs one `WARN` naming `pulsar-admin topics unload`.
- **Unset by default**; `0` disables it explicitly.

**Keep the number small.** Each attempt lifts the subscription's aggregate permit counter by exactly one receiver-queue window (see rung 1 for why), and issue #414's production failure was `-177300` deep — roughly 178 windows at a 1000-message queue. No realistic budget reaches that, and the point of the bound is to stop and escalate rather than re-subscribe forever against a fault this client cannot repair.

> **This is opt-in for a reason.** The watchdog reports silence, not fault, so an armed budget will occasionally re-subscribe a perfectly healthy consumer that is merely idle on a drained topic. That is cheap — one `CommandSubscribe`, one `CommandFlow`, the receiver queue untouched, the first dispatch resetting the budget — but it is an action taken against the broker on the client's own initiative, which is not something to do by default.

### Rung 1 — `Consumer::resubscribe()`

```rust
consumer.resubscribe()?;
```

Re-attaches **this consumer id in place**, on the live connection: zero the permit mirrors, fail every in-flight ack (their responses can never arrive against the retired consumer generation), re-emit `CommandSubscribe` for the same consumer id, and let the broker's `Success` release a fresh initial `CommandFlow`.

- No transport reconnect. No other consumer, producer, or subscription is disturbed.
- The receiver queue is left intact, so anything already buffered stays receivable.
- Returns as soon as the `CommandSubscribe` is staged and the driver is woken; the grant re-arms asynchronously on the broker's ack. Poll `available_permits()` to watch it land.
- Returns `Err` — mutating nothing — when the consumer is not eligible: closed, unsubscribing, terminally failed, mid-seek, or already re-attaching.

This is the same machinery issue #307 wired to an inbound same-broker `CommandCloseConsumer`; ADR-0101 made it callable.

**What it repairs:** this client's own slot in the broker's dispatcher.

**What it may not repair:** a dispatcher-WIDE corruption. Issue #414's production failure had the subscription's `availablePermits` at `-177300` across every attached consumer, and one consumer re-attaching does not necessarily clear that.

The arithmetic is worth knowing, because it is what rung 0's bound is chosen against. The re-attach zeroes this consumer's permits broker-side and the client answers the re-subscribe `Success` with one fresh `CommandFlow` of a full receiver-queue window, so **one attempt credits the subscription's aggregate counter by exactly `receiver_queue_size`**. A corruption of `L` therefore needs `ceil(L / receiver_queue_size)` attempts — about 178 for the reported numbers at a 1000-message queue. That is an operator's `topics unload`, not a client's retry loop.

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
- [ADR-0082](../specs/adr/0082-consumer-permit-balance-split.md) — the `granted_permits` / `permit_balance` split.
- [ADR-0058](../specs/adr/0058-keepalive-watchdog-progress-based.md) — the connection keepalive, and why it cannot see this.
- [`logging.md`](logging.md) — the structured-log field glossary.
- [`observability.md`](observability.md) — OpenTelemetry context propagation.
