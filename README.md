# Barrier Checkpoint

A deterministic experiment in checkpoint alignment, bounded input queues, and channel-state
recovery. Two sources feed one keyed count/sum operator. One source reaches its checkpoint barrier
early; the other is slow. What must stop, and what must be saved, to recover a consistent result?

The simulator compares two policies:

- **Aligned:** stop consuming a channel after its barrier until all input barriers arrive, then
  snapshot the operator state.
- **Recording:** snapshot the operator on the first barrier, keep processing, and record data
  received on other channels until their barriers arrive. Recovery replays that channel data
  before reading source tails.

The recording mode is a FIFO channel-recording model, **not an implementation of Flink unaligned
checkpoints**. It does not let barriers overtake queued data. This keeps the experiment focused
on the consistency cut and its storage cost without claiming production checkpoint latency.

## Run

Python 3.11+; no third-party runtime dependency, model, broker, cloud account, or network call:

```sh
python3 -m barrier_checkpoint demo
python3 -m barrier_checkpoint compare examples/skewed.json
python3 -m unittest discover -s tests -v
```

The demo executes the actual simulator and verifies recovery against a batch reduction:

| Measurement | Aligned | Recording |
| --- | --- | --- |
| First barrier received, tick | 3 | 3 |
| Checkpoint complete, tick | 26 | 26 |
| Blocked-channel ticks | 23 | 0 |
| Fast-source queue-full ticks | 25 | 2 |
| Recorded channel data, records | 0 | 5 |
| All data processed, elapsed ticks | 39 | 35 |
| Recovered count / sum | 20 / 158 | 20 / 158 |

Here the checkpoint completes at the same tick in both modes. Recording removes alignment
blocking and reduces fast-source stalls, but requires additional checkpoint data. Logical ticks
are simulation steps, not milliseconds or a throughput benchmark.

## Export and recover

```sh
python3 -m barrier_checkpoint run examples/skewed.json \
  --mode recording --checkpoint checkpoint.json
python3 -m barrier_checkpoint restore examples/skewed.json checkpoint.json
```

The output path must not already exist. `run` prints the live result, measurements, barrier
timeline, and checkpoint. `restore` validates the checkpoint against the source manifest, loads
the frozen aggregate, replays recorded channels, and processes source records after the saved
cuts. It reconstructs the final aggregate; it does not resume a distributed runtime or write to
an external sink.

Only completed checkpoints can be exported. If a capture budget is exhausted, data processing
continues but that checkpoint is aborted. A tick budget can stop a run before all data is consumed;
a checkpoint completed before that stop remains usable.

| Exit code | Meaning |
| --- | --- |
| 0 | Simulation and checkpoint completed, or restore succeeded |
| 1 | Tick budget exhausted or checkpoint aborted; diagnostic report is on stdout |
| 2 | Invalid input, missing file, rejected restore/export, or filesystem error; JSON error on stderr |

Argument syntax errors use argparse's stderr help and exit 2. No command overwrites an existing
checkpoint. Exports are plain JSON, not an atomic or power-loss-safe checkpoint publication
protocol; truncated files are rejected on read.

## Scenario contract

`examples/skewed.json` is a complete editable manifest. Each source has:

- `name`: unique lowercase identifier.
- `records`: an ordered list of integer `{key, value}` records; list positions are source offsets.
- `cut`: number of source records before its checkpoint barrier, including zero or the full length.
- `interval`: minimum logical ticks between successful sends, including the barrier.
- `delay`: tick when that source becomes ready.

Global fields are `capacity` per input queue, `process_every` ticks between operator receive slots,
`capture_limit` in records, and `max_ticks`. All fields are required. Unknown fields, duplicate JSON
keys, non-finite numbers, and non-integer numeric parameters are rejected. Source manifests and
checkpoint inputs are limited to 1 MiB each.

Sources offer at most one message per tick. Full queues retain their messages and apply
backpressure; they do not drop records. Barriers occupy queue capacity. The operator receives one
eligible message per service slot using deterministic round-robin selection. Source order in the
manifest breaks ties and is part of the scenario fingerprint.

## Boundaries

- One checkpoint, one aggregate operator, 1-4 immutable sources, at most 200 records per source.
- FIFO channels; no network loss, rescaling, concurrent checkpoints, or barrier overtaking.
- Commutative count/sum state only. Replaying channels in source order is not a general solution
  for order-sensitive operators.
- Source manifests remain available during validation and recovery. They allow exact prefix
  checks that a real source-offset-only checkpoint store would not provide.
- A SHA-256 fingerprint detects scenario mismatch; it is not a signature or authentication.
- No external sink is included, so the project makes no end-to-end exactly-once claim.
- No web UI or active GitHub workflow is included. Run the local suite above.

Read [architecture decisions](docs/architecture.md), [experiments and verification](docs/experiments.md),
and the [roadmap](docs/roadmap.md).

Optional installation: `python3 -m pip install .` provides the `barrier-checkpoint` command.
Installation may fetch build tools; direct checkout execution needs no downloads.
