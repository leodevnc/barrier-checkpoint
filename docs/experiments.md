# Skew, storage, and recovery

## Reproduce the skewed-input comparison

```sh
python3 -m barrier_checkpoint compare examples/skewed.json
```

The fast source has 12 records, a cut after 2, and a one-tick send interval. The slow source has
8 records, a cut after 6, a four-tick interval, and an initial delay of 2. Each queue holds 2
messages; the operator receives at most one per tick.

Both modes receive their first barrier at tick 3 and complete their checkpoint at tick 26.
Alignment blocks the fast input for 23 channel-ticks; recording blocks no input and saves 5
channel records. Fast-source full-queue stalls fall from 25 to 2 ticks. The final count is 20
and sum is 158 in uninterrupted runs and both restored runs.

The equal checkpoint completion times matter: avoiding alignment does not remove the slow source's
barrier delay in this FIFO model. The useful observation is the change in waiting and saved state,
not a universal checkpoint-speedup claim. Different queue capacities and operator service rates
can move the contention elsewhere.

## What a missing channel log loses

The recording snapshot contains 3 records totaling 13. Its channel log contains 5 slow-source
records totaling 50, and the source tails contain 12 records totaling 95.

If recovery loads only the frozen operator and source tails, the result is **15 records / 108**,
not **20 / 158**. The corresponding test computes this deliberately incomplete reduction and
checks the loss. The restore command rejects such a truncated channel log rather than silently
returning the incomplete aggregate.

## Exhausting the capture budget

Copy the example manifest and set `capture_limit` to 2, then run recording mode. The simulator
processes all input but reports an aborted checkpoint with reason `capture_limit`. It retains
no usable checkpoint. Set the limit to 5 and this fixture completes. Aligned mode also completes
with a limit of zero because it stores no channel data.

Increasing a budget is not free: it exchanges bounded waiting for additional saved input. This
experiment counts records, not actual storage throughput, encoded bytes, or object-store cost.

## Tests

```sh
python3 -m unittest discover -s tests -v
```

The suite covers:

- Empty and single sources, cuts at zero/end, negative values, repeated keys, FIFO offset checks.
- Frozen-state isolation from later processing and caller changes.
- Failure at every tick in the example: incomplete checkpoints are unavailable; complete ones
  reconstruct the independent batch result.
- Capture and tick budgets, with processing completion separate from checkpoint completion.
- Changed source manifests, modified aggregates, invalid offsets, missing/duplicated/reordered
  channel records, malformed JSON, and overwrite refusal.
- Checkpoint export followed by restore in a separate Python process for both policies.

Generated tests use 100 fixed seeds with 1-4 sources, varying delays, send/service intervals,
queue capacities, cuts, keys, and values. Both modes run on every manifest: 200 simulated runs.
At every tick, queues must remain within capacity. Once a checkpoint exists, its restored result
is compared with a separate batch oracle that groups values and uses Python's `len` and `sum`.
The live final aggregate must match that oracle too.

These bounded checks are not a proof for arbitrary stream topologies. They do not measure network
delivery, real backpressure propagation across workers, persistent-store durability, or external
sink effects. A new-process JSON round trip tests serialization and reconstruction, not crash-safe
atomic file publication.
