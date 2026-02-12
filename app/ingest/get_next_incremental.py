"""
Compute next incremental batch params from Snowflake RAW tables.

Queries max date and max batch from existing data, returns:
  - BATCH_NUM: next batch (max + 1)
  - START_DATE: day after max session/event date
  - END_DATE: start + 31 days

If tables are empty or missing, uses defaults: batch 2, 2011-02-13..2011-03-15.

Output: KEY=value lines for shell eval.
"""

import os
import sys
from datetime import datetime, timedelta
from pathlib import Path

from dotenv import load_dotenv

load_dotenv(Path(__file__).parent.parent / ".env")

# Run from app/ so ingest is a package
sys.path.insert(0, str(Path(__file__).parent.parent))
from ingest.load_to_snowflake import get_snowflake_connection

SNOWFLAKE_DATABASE = os.getenv("SNOWFLAKE_DATABASE", "GAME_ANALYTICS")
SNOWFLAKE_SCHEMA = os.getenv("SNOWFLAKE_SCHEMA", "RAW")

DEFAULT_START = "2011-02-13"
DEFAULT_END = "2011-03-15"
DEFAULT_BATCH = 2
INCREMENT_DAYS = 31


def get_next_incremental_params():
    """Query Snowflake for max date and batch; return next params."""
    conn = None
    try:
        conn = get_snowflake_connection()
        cursor = conn.cursor()

        # Max date from RAW_SESSIONS (SESSION_END) or RAW_GAME_EVENTS (EVENT_TIME)
        max_date = None
        for table, col in [
            ("RAW_SESSIONS", "SESSION_END"),
            ("RAW_GAME_EVENTS", "EVENT_TIME"),
        ]:
            try:
                cursor.execute(
                    f"""
                    SELECT MAX(TRY_TO_DATE(SUBSTR({col}, 1, 10)))
                    FROM {SNOWFLAKE_DATABASE}.{SNOWFLAKE_SCHEMA}.{table}
                    """
                )
                row = cursor.fetchone()
                if row and row[0]:
                    d = row[0]
                    if isinstance(d, str):
                        d = datetime.strptime(d[:10], "%Y-%m-%d").date()
                    if max_date is None or d > max_date:
                        max_date = d
            except Exception:
                pass

        # Max batch from RAW_PLAYERS: player_1 (batch 1) vs player_2_1 (batch 2)
        max_batch = 1
        try:
            cursor.execute(
                f"""
                SELECT MAX(
                    CASE WHEN SPLIT_PART(PLAYER_ID, '_', 3) != ''
                        THEN TRY_CAST(SPLIT_PART(PLAYER_ID, '_', 2) AS INT)
                        ELSE 1
                    END
                )
                FROM {SNOWFLAKE_DATABASE}.{SNOWFLAKE_SCHEMA}.RAW_PLAYERS
                """
            )
            row = cursor.fetchone()
            if row and row[0] is not None:
                max_batch = int(row[0])
        except Exception:
            pass

        cursor.close()

        if max_date:
            start_d = max_date + timedelta(days=1)
            end_d = start_d + timedelta(days=INCREMENT_DAYS)
            start_str = start_d.strftime("%Y-%m-%d")
            end_str = end_d.strftime("%Y-%m-%d")
            batch = max_batch + 1
        else:
            start_str = DEFAULT_START
            end_str = DEFAULT_END
            batch = DEFAULT_BATCH

        return batch, start_str, end_str

    except Exception as e:
        print(f"# Warning: {e}", file=sys.stderr)
        return DEFAULT_BATCH, DEFAULT_START, DEFAULT_END
    finally:
        if conn:
            conn.close()


def main():
    batch, start, end = get_next_incremental_params()
    print(f"INC_BATCH={batch}")
    print(f"INC_START={start}")
    print(f"INC_END={end}")


if __name__ == "__main__":
    main()
