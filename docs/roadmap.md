# Roadmap

## Implemented scope

- Deterministic finite-source simulator with bounded FIFO channels and round-robin consumption.
- One aligned or channel-recording checkpoint with explicit completeness and budget failure.
- Checked source-prefix/channel/tail recovery, JSON export, and separate-process restore.
- Skewed-input experiment, generated schedules, and corrupted-checkpoint rejection tests.

## Independent follow-up questions

1. How does priority barrier delivery change the amount of buffered data that must be saved?
   Specify overtaking and restoration order before extending the model.
2. How should a destination publish a recovered result atomically when it cannot participate in
   the checkpoint transaction? Explore sink commit protocols separately from operator snapshots.
3. What breaks when operators depend on cross-input order or processing-time timers? Define a
   replay contract before replacing the commutative aggregate.

Cloud deployment, live brokers, and external model connections are outside this implementation.
