"""Strict finite manifests; no source plugins, URLs, or executable input."""

from dataclasses import asdict, dataclass
import hashlib
import json
import re


class InputError(ValueError):
    pass


def canonical(value):
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=True, allow_nan=False)


def integer(value, low, high):
    if type(value) is not int or not low <= value <= high:
        raise InputError(f"expected an integer in [{low}, {high}]")
    return value


def name(value):
    if type(value) is not str or not re.fullmatch(r"[a-z][a-z0-9-]{0,31}", value):
        raise InputError("names must be 1-32 lowercase letters, digits, or hyphens, starting with a letter")
    return value


def fields(value, expected):
    if type(value) is not dict or set(value) != set(expected):
        raise InputError("object contains missing or unknown fields")


@dataclass(frozen=True)
class Record:
    key: str
    value: int


@dataclass(frozen=True)
class Source:
    name: str
    records: tuple[Record, ...]
    cut: int
    interval: int
    delay: int


@dataclass(frozen=True)
class Scenario:
    sources: tuple[Source, ...]
    capacity: int
    process_every: int
    capture_limit: int
    max_ticks: int

    @classmethod
    def parse(cls, raw):
        fields(raw, ("sources", "capacity", "process_every", "capture_limit", "max_ticks"))
        if type(raw["sources"]) is not list or not 1 <= len(raw["sources"]) <= 4:
            raise InputError("provide 1-4 sources")
        sources = []
        for item in raw["sources"]:
            fields(item, ("name", "records", "cut", "interval", "delay"))
            if type(item["records"]) is not list or len(item["records"]) > 200:
                raise InputError("each source supports up to 200 records")
            records = []
            for record in item["records"]:
                fields(record, ("key", "value"))
                records.append(Record(name(record["key"]), integer(record["value"], -10**6, 10**6)))
            sources.append(Source(name(item["name"]), tuple(records),
                                  integer(item["cut"], 0, len(records)),
                                  integer(item["interval"], 1, 20), integer(item["delay"], 0, 100)))
        if len({source.name for source in sources}) != len(sources):
            raise InputError("source names must be unique")
        return cls(tuple(sources), integer(raw["capacity"], 1, 32),
                   integer(raw["process_every"], 1, 20), integer(raw["capture_limit"], 0, 800),
                   integer(raw["max_ticks"], 1, 20000))

    def document(self):
        return json.loads(canonical(asdict(self)))

    def fingerprint(self):
        return hashlib.sha256(canonical(self.document()).encode()).hexdigest()


def add(state, record):
    current = state.setdefault(record.key, {"count": 0, "sum": 0})
    current["count"] += 1
    current["sum"] += record.value


def batch(scenario):
    state = {}
    for source in scenario.sources:
        for record in source.records:
            add(state, record)
    return state
