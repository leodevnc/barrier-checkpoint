from copy import deepcopy
import json
from pathlib import Path
import random
import unittest

from barrier_checkpoint import InputError, Scenario, Simulator, restore
from barrier_checkpoint.demo import compare, scenario


def oracle(spec):
    values = {}
    for source in spec.sources:
        for record in source.records:
            values.setdefault(record.key, []).append(record.value)
    return {key: {"count": len(items), "sum": sum(items)} for key, items in values.items()}


def altered(**changes):
    raw = scenario().document()
    raw.update(changes)
    return Scenario.parse(raw)


class SimulationTests(unittest.TestCase):
    def test_demo_measurements(self):
        rows = compare(scenario())
        self.assertEqual([r["blocked_channel_ticks"] for r in rows], [23, 0])
        self.assertEqual([r["captured_records"] for r in rows], [0, 5])
        self.assertEqual([r["complete_tick"] for r in rows], [26, 26])
        self.assertEqual([r["source_stall_ticks"]["fast"] for r in rows], [25, 2])
        self.assertTrue(all(r["recovery_matches_batch"] for r in rows))

    def test_fixture_matches_builtin(self):
        path = Path(__file__).resolve().parents[1] / "examples/skewed.json"
        self.assertEqual(Scenario.parse(json.loads(path.read_text())), scenario())

    def test_both_modes_match_independent_batch(self):
        for mode in ("aligned", "recording"):
            result = Simulator(scenario(), mode).run()
            self.assertEqual(result["state"], oracle(scenario()))
            self.assertEqual(restore(scenario(), result["checkpoint"]), oracle(scenario()))
            self.assertEqual(result["processed_offsets"], {"fast": 12, "slow": 8})

    def test_aligned_snapshot_contains_only_source_prefixes(self):
        result = Simulator(scenario()).run()
        cp = result["checkpoint"]
        self.assertEqual(cp["operator"]["offsets"], {"fast": 2, "slow": 6})
        self.assertEqual(cp["operator"]["state"], {"requests": {"count": 8, "sum": 63}})
        self.assertEqual(cp["channels"], {"fast": [], "slow": []})

    def test_recording_freezes_operator_and_captures_missing_prefix(self):
        sim = Simulator(scenario(), "recording")
        while sim.first_barrier_tick is None:
            sim.step()
        frozen = deepcopy(sim.frozen)
        while sim.checkpoint is None:
            sim.step()
        self.assertEqual(sim.checkpoint["operator"], frozen)
        self.assertGreater(sim.offsets["fast"], scenario().sources[0].cut)
        self.assertEqual([r["offset"] for r in sim.checkpoint["channels"]["slow"]], list(range(1, 6)))
        self.assertEqual(sim.checkpoint["operator"]["offsets"], {"fast": 2, "slow": 1})

    def test_omitting_channel_replay_loses_five_records(self):
        spec = scenario()
        cp = Simulator(spec, "recording").run()["checkpoint"]
        naive_count = cp["operator"]["state"]["requests"]["count"]
        naive_sum = cp["operator"]["state"]["requests"]["sum"]
        for source in spec.sources:
            for record in source.records[source.cut:]:
                naive_count += 1
                naive_sum += record.value
        self.assertEqual((naive_count, naive_sum), (15, 108))
        self.assertEqual(restore(spec, cp)["requests"], {"count": 20, "sum": 158})

    def test_checkpoint_remains_detached_as_live_state_advances(self):
        for mode in ("aligned", "recording"):
            sim = Simulator(scenario(), mode)
            while sim.checkpoint is None:
                sim.step()
            saved = deepcopy(sim.checkpoint)
            sim.run()
            self.assertEqual(sim.checkpoint, saved)
            report = sim.report()
            report["state"].clear()
            report["checkpoint"]["operator"]["state"].clear()
            self.assertEqual(sim.checkpoint, saved)
            self.assertNotEqual(sim.state, {})

    def test_failure_at_every_tick_uses_only_complete_checkpoint(self):
        for mode in ("aligned", "recording"):
            sim = Simulator(scenario(), mode)
            while not sim.done:
                sim.step()
                if sim.complete_tick is None:
                    self.assertIsNone(sim.report()["checkpoint"])
                else:
                    wire = json.loads(json.dumps(sim.report()["checkpoint"]))
                    self.assertEqual(restore(scenario(), wire), oracle(scenario()))

    def test_capture_budget_aborts_checkpoint_not_data_processing(self):
        spec = altered(capture_limit=2)
        result = Simulator(spec, "recording").run()
        self.assertTrue(result["finished"])
        self.assertEqual(result["state"], oracle(spec))
        self.assertEqual(result["checkpoint_status"], "aborted")
        self.assertEqual(result["abort_reason"], "capture_limit")
        self.assertIsNone(result["checkpoint"])
        self.assertEqual(result["captured_records"], 2)
        self.assertEqual(result["blocked_channel_ticks"], 0)

    def test_exact_budget_succeeds_and_zero_budget_aligned(self):
        self.assertEqual(Simulator(altered(capture_limit=5), "recording").run()["checkpoint_status"], "complete")
        self.assertEqual(Simulator(altered(capture_limit=0)).run()["checkpoint_status"], "complete")

    def test_tick_budget_before_and_after_checkpoint(self):
        early = Simulator(altered(max_ticks=4)).run()
        self.assertFalse(early["finished"])
        self.assertIsNone(early["checkpoint"])
        late_spec = altered(max_ticks=28)
        late = Simulator(late_spec).run()
        self.assertFalse(late["finished"])
        self.assertIsNotNone(late["checkpoint"])
        self.assertEqual(restore(late_spec, late["checkpoint"]), oracle(late_spec))

    def test_empty_sources_and_cuts_at_edges(self):
        for count in (0, 1, 8):
            for cut in (0, count):
                for mode in ("aligned", "recording"):
                    raw = scenario().document()
                    raw["sources"] = [{"name": "only", "cut": cut, "interval": 2, "delay": 3,
                                       "records": [{"key": "x", "value": -2}] * count}]
                    spec = Scenario.parse(raw)
                    result = Simulator(spec, mode).run()
                    self.assertTrue(result["finished"])
                    self.assertEqual(result["captured_records"], 0)
                    self.assertEqual(restore(spec, result["checkpoint"]), oracle(spec))

    def test_deterministic_serialization(self):
        for mode in ("aligned", "recording"):
            self.assertEqual(json.dumps(Simulator(scenario(), mode).run(), sort_keys=True),
                             json.dumps(Simulator(scenario(), mode).run(), sort_keys=True))

    def test_unknown_mode(self):
        with self.assertRaises(InputError):
            Simulator(scenario(), "pretend")


class RecoveryValidationTests(unittest.TestCase):
    def setUp(self):
        self.spec = scenario()
        self.cp = Simulator(self.spec, "recording").run()["checkpoint"]

    def test_missing_or_duplicate_channel_record(self):
        for operation in ("remove", "duplicate", "reverse", "change"):
            cp = deepcopy(self.cp)
            records = cp["channels"]["slow"]
            if operation == "remove":
                records.pop()
            elif operation == "duplicate":
                records[1] = records[0]
            elif operation == "reverse":
                records.reverse()
            else:
                records[0]["value"] += 1
            with self.subTest(operation=operation), self.assertRaises(InputError):
                restore(self.spec, cp)

    def test_post_barrier_state_is_rejected(self):
        cp = deepcopy(self.cp)
        cp["operator"]["state"] = Simulator(self.spec, "recording").run()["state"]
        with self.assertRaises(InputError):
            restore(self.spec, cp)

    def test_format_shape_cuts_offsets_and_fingerprint(self):
        broken = []
        for key, value in [("format_version", True), ("format_version", 2), ("mode", "invalid"),
                           ("scenario_sha256", "wrong"), ("operator", {}), ("channels", {})]:
            broken.append({**self.cp, key: value})
        cp = deepcopy(self.cp)
        cp["cuts"]["slow"] = 5
        broken.append(cp)
        cp = deepcopy(self.cp)
        cp["operator"]["offsets"]["fast"] = 3
        broken.append(cp)
        cp = deepcopy(self.cp)
        cp["operator"]["offsets"]["fast"] = True
        broken.append(cp)
        broken.extend([None, {}, {**self.cp, "extra": 1}])
        for cp in broken:
            with self.subTest(checkpoint=cp), self.assertRaises(InputError):
                restore(self.spec, cp)

    def test_source_change_rejected(self):
        raw = self.spec.document()
        raw["sources"][0]["records"][0]["value"] += 1
        with self.assertRaises(InputError):
            restore(Scenario.parse(raw), self.cp)

    def test_recording_cannot_be_relabelled_aligned(self):
        with self.assertRaises(InputError):
            restore(self.spec, {**self.cp, "mode": "aligned"})

    def test_restore_does_not_mutate_snapshot(self):
        before = deepcopy(self.cp)
        state = restore(self.spec, self.cp)
        state.clear()
        self.assertEqual(self.cp, before)


class ManifestTests(unittest.TestCase):
    def test_reject_invalid_bounds_and_shapes(self):
        for key, value in [("capacity", 0), ("capacity", True), ("capacity", 33),
                           ("process_every", 0), ("capture_limit", 801), ("max_ticks", 20001),
                           ("sources", []), ("sources", "bad")]:
            with self.subTest(key=key), self.assertRaises(InputError):
                altered(**{key: value})
        for key, value in [("cut", 13), ("interval", 0), ("delay", -1), ("name", "../bad"),
                           ("records", [{"key": "x", "value": True}]),
                           ("records", [{"key": "x", "value": 0, "unknown": 1}])]:
            raw = scenario().document()
            raw["sources"][0][key] = value
            with self.subTest(key=key), self.assertRaises(InputError):
                Scenario.parse(raw)

    def test_duplicate_sources_and_unknown_fields(self):
        raw = scenario().document()
        raw["sources"][1]["name"] = "fast"
        with self.assertRaises(InputError):
            Scenario.parse(raw)
        with self.assertRaises(InputError):
            Scenario.parse({**scenario().document(), "plugin": "unused"})

    def test_parse_detaches_input(self):
        raw = scenario().document()
        spec = Scenario.parse(raw)
        raw["sources"][0]["records"].clear()
        self.assertEqual(len(spec.sources[0].records), 12)


class GeneratedTests(unittest.TestCase):
    def test_generated_schedules_queue_bounds_and_recovery(self):
        for seed in range(100):
            rng = random.Random(seed)
            raw = {"capacity": rng.randint(1, 5), "process_every": rng.randint(1, 5),
                   "capture_limit": 800, "max_ticks": 20000, "sources": []}
            for i in range(rng.randint(1, 4)):
                count = rng.randint(0, 20)
                raw["sources"].append({"name": f"source-{i}", "cut": rng.randint(0, count),
                    "interval": rng.randint(1, 8), "delay": rng.randint(0, 15),
                    "records": [{"key": rng.choice(["a", "b"]), "value": rng.randint(-10, 10)}
                                for _ in range(count)]})
            spec = Scenario.parse(raw)
            for mode in ("aligned", "recording"):
                with self.subTest(seed=seed, mode=mode):
                    sim = Simulator(spec, mode)
                    while sim.step():
                        self.assertTrue(all(len(q) <= spec.capacity for q in sim.channels.values()))
                        if sim.checkpoint is not None:
                            self.assertEqual(restore(spec, sim.checkpoint), oracle(spec))
                    result = sim.report()
                    self.assertTrue(result["finished"])
                    self.assertEqual(result["checkpoint_status"], "complete")
                    self.assertEqual(result["state"], oracle(spec))
                    self.assertEqual(restore(spec, result["checkpoint"]), oracle(spec))
