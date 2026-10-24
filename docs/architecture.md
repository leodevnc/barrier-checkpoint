# Consistent cuts across FIFO inputs

## Problem

Saving operator state is not enough if input sources resume from offsets that describe a different
point in execution. State may omit pre-barrier records or include post-barrier records that the
sources will replay. Both can happen even though every individual queue preserves order.

The study asks how alignment and channel recording establish the same complete source-prefix
coverage with different waiting and storage costs.

## Decision: finite manifests and explicit cuts

Each source has a fixed sequence and a barrier after `cut` records. Source offsets identify
records even when keys and values repeat. The source cut is persisted alongside operator state.
The manifest is immutable during a simulation; its canonical JSON fingerprint includes records,
source ordering, timing, budgets, and queue capacity.

There is no clock-driven checkpoint coordinator. The cuts are experiment inputs rather than
positions selected by a live coordinator. All sources eventually emit one barrier, including
empty sources and sources whose cut is at their end. The simulator has a hard tick limit.

## Decision: deterministic scheduling

On each logical tick:

1. Each ready source attempts one enqueue. Success sets its next due time to the current tick
   plus its interval. If its queue is full, it remains ready and retries on the next tick.
2. Count channels blocked by alignment before this tick's receive.
3. On an operator service tick, choose one nonempty, nonblocked input by round-robin order.
4. Process the record or barrier and advance the logical clock.

Records and barriers share a bounded FIFO. `source_stall_ticks` counts ready send attempts denied
by a full queue, including ordinary consumer contention. It is not solely checkpoint overhead.
`blocked_channel_ticks` sums alignment-blocked channels over ticks, so it can exceed elapsed time
when several channels wait simultaneously. `barrier_span_ticks` measures the gap from first to
last received barrier. None of these measurements is a real network or disk latency.

## Decision: aligned mode

Once a barrier is consumed on one input, stop receiving from that input. The source can fill the
remaining queue slots, after which its enqueue attempts stall. Other inputs continue to drain.
When the final barrier arrives, every processed offset equals its source cut. Copy the operator
state, mark the checkpoint complete, and unblock all inputs.

No channel records need to be stored. Queued post-cut records are not in the snapshot; recovery
obtains them by replaying sources from their cuts.

## Decision: recording mode

At the first consumed barrier, freeze a copy of the operator state and each input's processed
offset. Keep receiving from every input. On an input whose barrier has not yet arrived, append
each subsequently received record to that input's checkpoint channel log. Stop recording an input
when its barrier arrives. Complete the checkpoint only after all barriers arrive.

Post-barrier data can continue changing the live aggregate, but cannot change the frozen snapshot.
It is not added to a channel log. Such records will be read again from source tails during recovery.
Channel records may not all have been queued at the instant of the first barrier; they represent
the gap between the operator snapshot and each source's recorded cut.

This is a deliberately FIFO recording policy. In particular, it does not model Flink's priority
handling that can let unaligned barriers overtake buffered data, asynchronous state storage, or a
multi-operator acknowledgement graph. The [Flink checkpointing documentation](https://nightlies.apache.org/flink/flink-docs-stable/docs/concepts/stateful-stream-processing/)
describes the broader aligned and unaligned mechanisms; this experiment isolates a smaller
channel-state consistency problem.

## Recovery invariant

For each source with saved operator offset `o`, source cut `c`, and length `n`, recovery must cover
three disjoint ranges:

```text
[0, o)             [o, c)                [c, n)
frozen operator  + recorded channel    + replayed source tail
```

Aligned mode has `o = c`, so the middle range is empty. Recording mode can have `o < c` on inputs
that had not yet delivered a barrier at the operator snapshot. A complete checkpoint contains
exactly the contiguous middle range, with matching keys and values in source order.

The restore validator checks shape and version, scenario fingerprint, cuts, offset bounds,
channel coverage and payloads, mode constraints, and capture budget. It independently recomputes
the expected frozen prefix aggregate to verify the stored state. Only then does recovery load
the stored aggregate, apply stored channel records, and process the source tails.

Full source manifests make these checks possible. This is a stronger, more expensive validation
oracle than a production system with only opaque offsets. It does not authenticate a checkpoint:
someone who controls the manifest and the checkpoint can construct another consistent pair.

Counts and sums are commutative, so cross-channel replay order does not change the final result.
Order-sensitive operators, timers, nondeterministic functions, and external side effects require
additional recovery contracts and are intentionally absent.

## Budgets and failed checkpoints

Input limits are 4 sources, 200 records per source, 32 queue messages per input, 20 ticks for
source/consumer intervals, 100 ticks of source delay, 800 captured records, and 20,000 total ticks.
Record values are integers in [-10^6, 10^6]. Frozen dataclasses detach manifests from caller edits.

If recording would exceed the capture budget, abort the checkpoint before appending that record.
Continue normal aggregation, but never expose that partial snapshot as recoverable. No fallback
checkpoint or automatic retry is invented. Aligned mode needs no capture budget. The single
snapshot and its channel logs remain bounded by the manifest limits.

A tick-budget stop is distinct from checkpoint failure: it can occur before or after a complete
checkpoint. Reports keep those statuses separate. Export requires a complete checkpoint, and
read-time validation rejects partial/truncated or inconsistent artifacts.
