# ADR-0039 — Adopt a vectored `Transmit` descriptor on the sans-io ↔ runtime boundary

- **Status**: Proposed
- **Date**: 2026-05-27
- **Decider**: Florentin Dubois
- **Tags**: performance, sans-io, runtime, network, zero-copy

## Context

The current sans-io ↔ runtime boundary uses a single `BytesMut` outbound
buffer:

- `magnetar-proto/src/conn.rs::Connection::poll_transmit` drains
  `self.outbound: BytesMut` to a contiguous slice.
- `magnetar-proto/src/frame.rs::encode_payload` builds frames by
  copying every payload into a fresh `BytesMut` accumulator.
- `magnetar-runtime-tokio/src/driver.rs::driver_loop_inner` issues
  `socket.write_all(&write_buf)` against the resulting contiguous
  slice.

For plaintext TCP this means every producer batch performs:

1. `BytesMut::extend_from_slice(payload)` — full memcpy of every
   payload into the wire buffer.
2. `write_all` of the coalesced buffer — kernel copy from user to
   socket-buffer.

Step (1) is wasted work. The kernel's `writev(2)` (and tokio's
`poll_write_vectored` / `IoSlice`) accept a list of disjoint byte
ranges, doing the kernel-side concatenation without a user-space
memcpy. For TLS the picture is different — `tokio_rustls` coalesces
internally to produce a single TLS record, so the vectored ingress
would be flattened anyway. Plaintext is the path that benefits.

`docs/follow-ups.md` flags this under both **zero-copy**
(`encode_payload`) and **syscall reduction** (`writev` / `IoSlice`)
sections. The audit recommends a frame-descriptor pivot
`{head: BytesMut, payload: Bytes}` plus a `Transmit` enum that the
runtime translates into either a `write_all` or a `write_vectored`
call.

The moonpool engine's `Providers::Network` substrate currently mirrors
`tokio::io::AsyncWrite` (single contiguous slice in `write`). Adding
vectored I/O means either:

- **(A)** Extending `Providers::Network` with a vectored entry so both
  the production engine and the moonpool simulator agree on segment
  granularity. moonpool-sim's chaos pack can then drop / re-order
  individual segments, which is a strict superset of what it can do
  with one contiguous buffer today.
- **(B)** Coalescing in the runtime adapter before handing bytes to
  `Providers::Network`. moonpool stays single-slice; only the
  production tokio engine sees segments. Cheaper but loses chaos-pack
  fidelity.

## Decision

**Proposed**: adopt option (A). Add a `Transmit` enum on the sans-io
side and a vectored `Providers::Network` entry, in three landings:

1. **Wave 1 — `Transmit` enum + plaintext write-vectored**. Sans-io:
   `Connection::poll_transmit` returns a `Transmit` enum:
   ```rust
   pub enum Transmit<'a> {
       /// Single contiguous slice — used by TLS (rustls coalesces
       /// internally so segment fidelity is wasted), small handshake
       /// frames, and any path the protocol layer can't trivially
       /// split.
       Contiguous(&'a [u8]),
       /// Segment list — used by producer batches in plaintext mode.
       /// Each `Bytes` carries one frame head + payload. The runtime
       /// passes the list through `poll_write_vectored` /
       /// `Providers::Network::write_vectored`.
       Vectored(&'a [Bytes]),
   }
   ```
   `frame.rs::encode_payload` returns `{head: BytesMut, payload: Bytes}`
   instead of a single coalesced `BytesMut`. The producer state machine
   accumulates segments into `self.outbound_segments: Vec<Bytes>` and
   `poll_transmit` flips between `Contiguous` (handshake / TLS) and
   `Vectored` (plaintext producer batches) at call time. No moonpool
   change in wave 1 — moonpool's tokio backend already speaks the
   same vectored shape.

2. **Wave 2 — moonpool `Providers::Network` vectored entry**.
   `moonpool_core::Providers::Network` grows a `write_vectored`
   method. The chaos pack can drop / re-order individual segments
   (today it can only drop the whole batch). Stays backwards-compatible:
   single-slice `Contiguous` callers go through the existing path.
   Bumps `moonpool-core` minor version.

3. **Wave 3 — `BytesMut` ownership pass-through on read**. The audit's
   "read path double-copy" note (already closed at the proto-side
   `split_to` refactor in commit `bf66a5b`) becomes the dual: the
   driver passes owned `BytesMut` to `handle_bytes`, the proto layer
   keeps a refcount, and `take_decoded_payload` hands `Bytes` slices
   that share the same buffer all the way down to user-facing
   `IncomingMessage::payload`. This wave is independent of (1) / (2)
   and can land separately.

## Consequences

**Easier**:

- Producer batch send-throughput improves. Plaintext path drops one
  full memcpy per payload (proportional to batch payload size). For
  high-throughput compressed batches (1 MiB+ aggregate) this is the
  hot path.
- The frame descriptor `{head, payload}` makes the producer's
  compression-step / encryption-step / metadata-stamp pipeline more
  composable — each step can mutate one segment without touching the
  others.
- moonpool's chaos pack gets segment-granular drop / reorder for free
  once wave 2 lands.

**Harder**:

- Cross-runtime test parity (ADR-0024) requires the wave-2 moonpool
  changes to land before the production tokio engine flips to the
  `Vectored` path — otherwise the differential harness sees
  per-batch byte streams diverging by segment boundary.
- The `Transmit::Vectored` lifetime borrows from `Connection`'s
  internal `Vec<Bytes>`; care needed in the driver loop to not hold
  the lock across the `.await` on `write_vectored`. The existing
  pattern of "drain to owned `Vec<u8>` then drop the lock" needs an
  equivalent — drain to `Vec<Bytes>` (cheap Arc clones) then drop the
  lock.
- TLS coalesces — there's no benefit on `pulsar+ssl://` URLs. The
  runtime must pick `Contiguous` for TLS even if proto could emit
  `Vectored`. The simplest gate: a flag on the driver-loop entry
  paralleling the `flush_after_write` flag landed in
  `perf(driver): skip flush() after write_all on plaintext TCP`.

**Cost**:

- Wave 1: ~300 LOC across `magnetar-proto/src/{conn.rs, frame.rs,
  producer.rs}` + `magnetar-runtime-tokio/src/driver.rs` + the
  matching moonpool adapter.
- Wave 2: ~200 LOC + a `moonpool-core` minor version bump + the
  differential harness chaos-pack update.
- Wave 3: ~150 LOC in proto + 50 LOC in each runtime.
- Test layers per ADR-0024 across all three waves.

**Incompatible with**:

- `BytesMut`-only callers downstream that assume contiguous wire
  layout. The frame-decoder side is already segment-aware (it
  `split_to` against `inbound: BytesMut`), so no change there. The
  audit's "read path double-copy" closure (wave 3) is the symmetric
  follow-on.

## References

- `docs/follow-ups.md` — zero-copy + syscall-reduction bullets that
  this ADR consolidates.
- `crates/magnetar-proto/src/frame.rs::encode_payload` — current
  contiguous-only encoder.
- `crates/magnetar-proto/src/conn.rs::Connection::poll_transmit` —
  current single-slice transmit surface.
- `crates/magnetar-runtime-tokio/src/driver.rs::driver_loop_inner` —
  current `write_all` site that will branch on the new `Transmit`
  shape.
- [ADR-0004](0004-sans-io-protocol-core.md) — sans-io contract that
  the `Transmit` enum has to honour (no I/O types leak into proto).
- [ADR-0024](0024-cross-runtime-test-and-coverage-policy.md) — the
  test layers and parity check the three waves have to satisfy.
- [ADR-0038](0038-split-connection-mutex.md) — per-slot mutex lift
  that already established the "drain under lock, write outside lock"
  pattern this ADR extends.
