"""
Main script: generate raw game data (CSVs) and load them into Snowflake.

1. Generation (gen/): players → sessions → events (writes to data/*.csv).
2. Ingest (ingest/): loads data/ CSVs into Snowflake RAW_* tables.

Sessions and events are constrained to EVENT_DATE_START..EVENT_DATE_END.

Usage:
    python main.py                           # generate + load (default: last 90 days)
    python main.py --start 2024-01-01 --end 2024-12-31
    python main.py --no-ingest               # generate only
    python main.py --batch 2 --start 2011-02-13 --end 2011-03-15  # incremental: new users, sessions, events
"""

import argparse
import os
import subprocess
import sys
from datetime import datetime, timedelta
from pathlib import Path


DEFAULT_START = "2011-01-13"
DEFAULT_END = "2011-02-12"

CONFIG = {
    "N_PLAYERS": 2000,
    "MAX_SESSIONS_PER_PLAYER": 25,
    "GAME_VERSION": "1.0.3",
    "EVENT_DATE_START": DEFAULT_START,
    "EVENT_DATE_END": DEFAULT_END,
    "GAME_DATA_SEED": "42",  # Fixed seed so every run produces the same data for all users
    "LOAD_BATCH_ID": "1",  # Batch 2+ = new users/sessions/events for incremental APPEND testing
}

SCRIPTS = [
    {
        "name": "players.py",
        "env": {
            "N_PLAYERS": str(CONFIG["N_PLAYERS"]),
            "EVENT_DATE_START": CONFIG["EVENT_DATE_START"],
            "EVENT_DATE_END": CONFIG["EVENT_DATE_END"],
            "GAME_DATA_SEED": CONFIG["GAME_DATA_SEED"],
            "LOAD_BATCH_ID": CONFIG["LOAD_BATCH_ID"],
        },
    },
    {
        "name": "sessions.py",
        "env": {
            "MAX_SESSIONS_PER_PLAYER": str(CONFIG["MAX_SESSIONS_PER_PLAYER"]),
            "EVENT_DATE_START": CONFIG["EVENT_DATE_START"],
            "EVENT_DATE_END": CONFIG["EVENT_DATE_END"],
            "GAME_DATA_SEED": CONFIG["GAME_DATA_SEED"],
            "LOAD_BATCH_ID": CONFIG["LOAD_BATCH_ID"],
        },
    },
    {
        "name": "events.py",
        "env": {
            "GAME_VERSION": CONFIG["GAME_VERSION"],
            "EVENT_DATE_START": CONFIG["EVENT_DATE_START"],
            "EVENT_DATE_END": CONFIG["EVENT_DATE_END"],
            "GAME_DATA_SEED": CONFIG["GAME_DATA_SEED"],
            "LOAD_BATCH_ID": CONFIG["LOAD_BATCH_ID"],
        },
    },
]


def run_generation(project_root: Path, gen_dir: Path) -> None:
    """Run gen/players.py, sessions.py, events.py in order."""
    print("\n" + "=" * 60)
    print("🎮 Step 1: Data generation")
    print("=" * 60)
    print("\n📊 Configuration:")
    for key, value in CONFIG.items():
        print(f"   {key}: {value}")
    print()

    for script_config in SCRIPTS:
        script_name = script_config["name"]
        script_path = gen_dir / script_name
        if not script_path.exists():
            print(f"❌ Script not found: {script_path}")
            sys.exit(1)

        env = os.environ.copy()
        env.update(script_config["env"])
        print(f"\n{'='*60}\n🚀 Running: {script_name}")
        if script_config["env"]:
            print(f"   Config: {', '.join(f'{k}={v}' for k, v in script_config['env'].items())}")
        print(f"{'='*60}\n")

        try:
            subprocess.run(
                [sys.executable, str(script_path)],
                check=True,
                cwd=project_root,
                env=env,
            )
            print(f"\n✅ Completed: {script_name}\n")
        except subprocess.CalledProcessError as e:
            print(f"\n❌ Failed at {script_name}: {e}\n")
            sys.exit(1)
        except Exception as e:
            print(f"\n❌ Error running {script_name}: {e}\n")
            sys.exit(1)

    print("✨ Generation done.\n")


def run_ingest(project_root: Path) -> None:
    """Run ingest/load_to_snowflake.py to load data/ CSVs into Snowflake."""
    ingest_script = project_root / "ingest" / "load_to_snowflake.py"
    if not ingest_script.exists():
        print(f"❌ Ingest script not found: {ingest_script}")
        sys.exit(1)

    print("\n" + "=" * 60)
    print("📤 Step 2: Load to Snowflake")
    print("=" * 60 + "\n")

    try:
        subprocess.run(
            [sys.executable, str(ingest_script)],
            check=True,
            cwd=project_root,
        )
        print("\n✨ Ingest done.\n")
    except subprocess.CalledProcessError as e:
        print(f"\n❌ Ingest failed: {e}\n")
        sys.exit(1)
    except Exception as e:
        print(f"\n❌ Error during ingest: {e}\n")
        sys.exit(1)


def main():
    parser = argparse.ArgumentParser(description="Generate game data and optionally load to Snowflake.")
    parser.add_argument(
        "--no-ingest",
        action="store_true",
        help="Only generate CSVs; do not run Snowflake load",
    )
    parser.add_argument(
        "--start",
        metavar="YYYY-MM-DD",
        default=None,
        help=f"Event date range start (default: {DEFAULT_START})",
    )
    parser.add_argument(
        "--end",
        metavar="YYYY-MM-DD",
        default=None,
        help=f"Event date range end (default: {DEFAULT_END})",
    )
    parser.add_argument(
        "--batch",
        metavar="N",
        type=int,
        default=1,
        help="Load batch ID (default: 1). Use 2+ for incremental: new users, sessions, events with unique IDs.",
    )
    args = parser.parse_args()

    event_start = args.start or CONFIG["EVENT_DATE_START"]
    event_end = args.end or CONFIG["EVENT_DATE_END"]
    CONFIG["EVENT_DATE_START"] = event_start
    CONFIG["EVENT_DATE_END"] = event_end
    CONFIG["LOAD_BATCH_ID"] = str(args.batch)
    for script_config in SCRIPTS:
        script_config["env"]["EVENT_DATE_START"] = event_start
        script_config["env"]["EVENT_DATE_END"] = event_end
        script_config["env"]["LOAD_BATCH_ID"] = CONFIG["LOAD_BATCH_ID"]

    project_root = Path(__file__).resolve().parent
    gen_dir = project_root / "gen"

    run_generation(project_root, gen_dir)
    if not args.no_ingest:
        run_ingest(project_root)
    else:
        print("Skipping ingest (--no-ingest). Data is in data/\n")

    print("=" * 60)
    print("✅ Pipeline finished successfully")
    print("=" * 60 + "\n")


if __name__ == "__main__":
    main()
