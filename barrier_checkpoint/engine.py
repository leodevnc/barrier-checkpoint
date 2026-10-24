"""One aggregate operator, FIFO inputs, one checkpoint, deterministic logical ticks."""

from collections import deque
from copy import deepcopy

from .model import InputError, Record, Scenario, add, canonical, fields, integer


class Simulator:
    def __init__(self, scenario: Scenario, mode="aligned"):
        # Revalidate even callers who construct dataclasses directly.
        self.scenario = Scenario.parse(scenario.document())
        if mode not in ("aligned", "recording"):
            raise InputError("mode must be aligned or recording")
        self.mode = mode
        self.tick = 0
        self.turn = 0
        self.state = {}
        self.offsets = {s.name: 0 for s in scenario.sources}
        self.channels = {s.name: deque() for s in scenario.sources}
        self.sent = {s.name: 0 for s in scenario.sources}
        self.due = {s.name: s.delay for s in scenario.sources}
        self.seen = set()
        self.frozen = None
        self.captured = {s.name: [] for s in scenario.sources}
        self.checkpoint = None
        self.first_barrier_tick = None
        self.complete_tick = None
        self.abort_reason = None
        self.source_stalls = {s.name: 0 for s in scenario.sources}
        self.blocked_channel_ticks = 0
        self.peak_queue = 0
        self.peak_capture = 0
        self.timeline = []

    @property
    def done(self):
        return all(self.sent[s.name] == len(s.records) + 1 and not self.channels[s.name]
                   for s in self.scenario.sources)

    def blocked(self, name):
        return (self.mode == "aligned" and name in self.seen and self.checkpoint is None
                and self.abort_reason is None)

    def _barrier(self, source):
        self.timeline.append({"tick": self.tick, "kind": "barrier", "source": source.name})
        if self.first_barrier_tick is None:
            self.first_barrier_tick = self.tick
            if self.mode == "recording":
                self.frozen = {"state": deepcopy(self.state), "offsets": dict(self.offsets)}
        self.seen.add(source.name)
        if len(self.seen) == len(self.scenario.sources) and self.abort_reason is None:
            if self.mode == "aligned":
                self.frozen = {"state": deepcopy(self.state), "offsets": dict(self.offsets)}
            self.checkpoint = {"format_version": 1, "scenario_sha256": self.scenario.fingerprint(),
                               "mode": self.mode, "cuts": {s.name: s.cut for s in self.scenario.sources},
                               "operator": deepcopy(self.frozen), "channels": deepcopy(self.captured)}
            self.complete_tick = self.tick
            self.timeline.append({"tick": self.tick, "kind": "checkpoint_complete"})

    def step(self):
        if self.done or self.tick >= self.scenario.max_ticks:
            return False
        # Sources get one send attempt per tick; barriers occupy FIFO queue capacity too.
        for source in self.scenario.sources:
            name = source.name
            position = self.sent[name]
            if position > len(source.records) or self.tick < self.due[name]:
                continue
            queue = self.channels[name]
            if len(queue) == self.scenario.capacity:
                self.source_stalls[name] += 1
                continue
            offset = None if position == source.cut else position - int(position > source.cut)
            queue.append(offset)
            self.sent[name] += 1
            self.due[name] = self.tick + source.interval
            self.peak_queue = max(self.peak_queue, len(queue))
        self.blocked_channel_ticks += sum(self.blocked(s.name) for s in self.scenario.sources)
        if self.tick % self.scenario.process_every == 0:
            for delta in range(len(self.scenario.sources)):
                index = (self.turn + delta) % len(self.scenario.sources)
                source = self.scenario.sources[index]
                queue = self.channels[source.name]
                if not queue or self.blocked(source.name):
                    continue
                self.turn = (index + 1) % len(self.scenario.sources)
                offset = queue.popleft()
                if offset is None:
                    self._barrier(source)
                else:
                    if (self.mode == "recording" and self.frozen is not None
                            and source.name not in self.seen and self.abort_reason is None):
                        size = sum(map(len, self.captured.values()))
                        if size >= self.scenario.capture_limit:
                            self.abort_reason = "capture_limit"
                            self.timeline.append({"tick": self.tick, "kind": "checkpoint_aborted",
                                                  "reason": self.abort_reason})
                        else:
                            record = source.records[offset]
                            self.captured[source.name].append({"offset": offset, "key": record.key,
                                                               "value": record.value})
                            self.peak_capture = size + 1
                    if offset != self.offsets[source.name]:
                        raise RuntimeError("FIFO offset invariant failed")
                    add(self.state, source.records[offset])
                    self.offsets[source.name] += 1
                break
        self.tick += 1
        return True

    def run(self):
        while self.step():
            pass
        return self.report()

    def report(self):
        return {"mode": self.mode, "finished": self.done, "ticks": self.tick,
                "state": deepcopy(self.state), "processed_offsets": dict(self.offsets),
                "checkpoint": deepcopy(self.checkpoint),
                "checkpoint_status": "complete" if self.checkpoint is not None else
                                     ("aborted" if self.abort_reason else "incomplete"),
                "abort_reason": self.abort_reason, "first_barrier_tick": self.first_barrier_tick,
                "complete_tick": self.complete_tick,
                "barrier_span_ticks": None if self.complete_tick is None else
                                      self.complete_tick - self.first_barrier_tick,
                "source_stall_ticks": dict(self.source_stalls),
                "blocked_channel_ticks": self.blocked_channel_ticks,
                "peak_queue_messages": self.peak_queue, "captured_records": self.peak_capture,
                "timeline": deepcopy(self.timeline)}


def restore(scenario: Scenario, checkpoint):
    """Verify a complete cut, replay recorded channels, then replay source tails.

    Rebuilds aggregate state, not a distributed runtime or an external sink.
    Full source manifests enable stronger checks than a production offset-only snapshot.
    """
    scenario = Scenario.parse(scenario.document())
    fields(checkpoint, ("format_version", "scenario_sha256", "mode", "cuts", "operator", "channels"))
    if type(checkpoint["format_version"]) is not int or checkpoint["format_version"] != 1:
        raise InputError("unsupported checkpoint format")
    if checkpoint["scenario_sha256"] != scenario.fingerprint():
        raise InputError("checkpoint belongs to a different scenario")
    if checkpoint["mode"] not in ("aligned", "recording"):
        raise InputError("invalid checkpoint mode")
    names = [s.name for s in scenario.sources]
    fields(checkpoint["cuts"], names)
    fields(checkpoint["channels"], names)
    fields(checkpoint["operator"], ("state", "offsets"))
    fields(checkpoint["operator"]["offsets"], names)
    expected_state = {}
    captured_count = 0
    for source in scenario.sources:
        cut = integer(checkpoint["cuts"][source.name], 0, len(source.records))
        if cut != source.cut:
            raise InputError("checkpoint source cut does not match scenario")
        offset = integer(checkpoint["operator"]["offsets"][source.name], 0, cut)
        for record in source.records[:offset]:
            add(expected_state, record)
        channel = checkpoint["channels"][source.name]
        if type(channel) is not list or len(channel) != cut - offset:
            raise InputError("channel state does not cover the missing source prefix")
        for index, captured in enumerate(channel, offset):
            record = source.records[index]
            expected = {"offset": index, "key": record.key, "value": record.value}
            if canonical(captured) != canonical(expected):
                raise InputError("channel state contains a changed, duplicated, or reordered record")
        captured_count += len(channel)
    if canonical(checkpoint["operator"]["state"]) != canonical(expected_state):
        raise InputError("operator state does not match its source prefixes")
    if checkpoint["mode"] == "aligned" and captured_count:
        raise InputError("aligned checkpoint must not require channel replay")
    if captured_count > scenario.capture_limit:
        raise InputError("checkpoint exceeds capture budget")
    state = deepcopy(checkpoint["operator"]["state"])
    for source in scenario.sources:
        for captured in checkpoint["channels"][source.name]:
            add(state, Record(captured["key"], captured["value"]))
    for source in scenario.sources:
        for record in source.records[source.cut:]:
            add(state, record)
    return state
