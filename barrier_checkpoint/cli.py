import argparse
import json
from pathlib import Path
import sys

from .demo import compare, scenario as demo_scenario
from .engine import Simulator, restore
from .model import InputError, Scenario


def unique_keys(pairs):
    result = {}
    for key, value in pairs:
        if key in result:
            raise InputError("duplicate JSON object key")
        result[key] = value
    return result


def reject_constant(_):
    raise InputError("non-finite JSON number")


def read_json(path):
    with Path(path).open("rb") as stream:
        content = stream.read(1048577)
    if len(content) > 1048576:
        raise InputError("input exceeds 1 MiB")
    return json.loads(content.decode("utf-8"), object_pairs_hook=unique_keys, parse_constant=reject_constant)


def main(argv=None):
    parser = argparse.ArgumentParser(description="Compare barrier alignment with channel recording.")
    sub = parser.add_subparsers(dest="command", required=True)
    sub.add_parser("demo", help="compare modes on the built-in skewed scenario")
    compare_parser = sub.add_parser("compare", help="compare modes on a local manifest")
    compare_parser.add_argument("scenario")
    run = sub.add_parser("run", help="simulate one mode, optionally export a complete checkpoint")
    run.add_argument("scenario")
    run.add_argument("--mode", choices=["aligned", "recording"], default="aligned")
    run.add_argument("--checkpoint", help="new JSON output path; existing files are never overwritten")
    recover = sub.add_parser("restore", help="validate a checkpoint and rebuild the final aggregate")
    recover.add_argument("scenario")
    recover.add_argument("checkpoint")
    args = parser.parse_args(argv)
    try:
        scenario = demo_scenario() if args.command == "demo" else Scenario.parse(read_json(args.scenario))
        if args.command in ("demo", "compare"):
            result = compare(scenario)
            code = 0 if all(r["finished"] and r["recovery_matches_batch"] for r in result) else 1
        elif args.command == "run":
            result = Simulator(scenario, args.mode).run()
            if args.checkpoint:
                if result["checkpoint"] is None:
                    raise InputError("no complete checkpoint to export")
                with Path(args.checkpoint).open("x", encoding="utf-8") as stream:
                    json.dump(result["checkpoint"], stream, sort_keys=True, indent=2, allow_nan=False)
                    stream.write("\n")
            code = 0 if result["finished"] and result["checkpoint"] is not None else 1
        else:
            result = {"state": restore(scenario, read_json(args.checkpoint))}
            code = 0
        print(json.dumps(result, sort_keys=True, indent=2, allow_nan=False))
        return code
    except (ValueError, OSError, RecursionError) as exc:
        print(json.dumps({"error": str(exc)}), file=sys.stderr)
        return 2
