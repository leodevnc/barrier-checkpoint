from .engine import Simulator, restore
from .model import Scenario, batch


def scenario():
    return Scenario.parse({
        "capacity": 2, "process_every": 1, "capture_limit": 100, "max_ticks": 1000,
        "sources": [
            {"name": "fast", "cut": 2, "interval": 1, "delay": 0,
             "records": [{"key": "requests", "value": i + 1} for i in range(12)]},
            {"name": "slow", "cut": 6, "interval": 4, "delay": 2,
             "records": [{"key": "requests", "value": 10} for _ in range(8)]},
        ],
    })


def compare(scenario):
    rows = []
    for mode in ("aligned", "recording"):
        result = Simulator(scenario, mode).run()
        recovered = restore(scenario, result["checkpoint"]) if result["checkpoint"] is not None else None
        rows.append({key: result[key] for key in (
            "mode", "finished", "ticks", "checkpoint_status", "first_barrier_tick", "complete_tick",
            "barrier_span_ticks", "blocked_channel_ticks", "source_stall_ticks", "captured_records")})
        rows[-1]["recovery_matches_batch"] = recovered == batch(scenario) if recovered is not None else None
    return rows
