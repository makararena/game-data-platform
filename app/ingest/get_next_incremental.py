"""
Compute next incremental batch params from Snowflake RAW tables.

Queries max date and max numeric IDs from existing data, returns:
  - START_DATE: day after max session/event date
  - END_DATE: start + 31 days
  - PLAYER_ID_OFFSET: max(player_id number) + 1 (e.g. max player_889 → 890)
  - SESSION_ID_OFFSET: max(session_id number) + 1
  - EVENT_ID_OFFSET: max(event_id number) + 1

Uses sequential IDs (player_890, player_891, ...) instead of batch-prefixed (player_2_1).

If tables are empty or missing, uses defaults: 2011-02-13..2011-03-15, offsets 1/1/0.

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
INCREMENT_DAYS = 31


def _query_max_numeric_id(cursor, table: str, id_col: str) -> int:
    """Extract max numeric part from IDs like player_889, player_2_1, session_1234."""
    try:
        cursor.execute(
            f"""
            SELECT MAX(TRY_CAST(REGEXP_SUBSTR({id_col}, '[0-9]+', 1, 1) AS INT))
            FROM {SNOWFLAKE_DATABASE}.{SNOWFLAKE_SCHEMA}.{table}
            """
        )
        row = cursor.fetchone()
        return int(row[0]) if row and row[0] is not None else 0
    except Exception:
        return 0


def get_next_incremental_params():
    """Query Snowflake for max date and max IDs; return next params."""
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

        # Max numeric IDs for sequential generation (player_889 → next is player_890)
        max_player = _query_max_numeric_id(cursor, "RAW_PLAYERS", "PLAYER_ID")
        max_session = _query_max_numeric_id(cursor, "RAW_SESSIONS", "SESSION_ID")
        max_event = _query_max_numeric_id(cursor, "RAW_GAME_EVENTS", "EVENT_ID")

        cursor.close()

        if max_date:
            start_d = max_date + timedelta(days=1)
            end_d = start_d + timedelta(days=INCREMENT_DAYS)
            start_str = start_d.strftime("%Y-%m-%d")
            end_str = end_d.strftime("%Y-%m-%d")
        else:
            start_str = DEFAULT_START
            end_str = DEFAULT_END

        player_offset = max_player + 1
        session_offset = max_session + 1
        event_offset = max_event + 1

        return start_str, end_str, player_offset, session_offset, event_offset

    except Exception as e:
        print(f"# Warning: {e}", file=sys.stderr)
        return DEFAULT_START, DEFAULT_END, 1, 1, 0
    finally:
        if conn:
            conn.close()


def main():
    start, end, player_offset, session_offset, event_offset = get_next_incremental_params()
    print(f"INC_START={start}")
    print(f"INC_END={end}")
    print(f"INC_PLAYER_OFFSET={player_offset}")
    print(f"INC_SESSION_OFFSET={session_offset}")
    print(f"INC_EVENT_OFFSET={event_offset}")


if __name__ == "__main__":
    main()
