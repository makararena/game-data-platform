"""
Script to load raw CSV files into Snowflake.

Loads three tables:
- RAW_PLAYERS
- RAW_SESSIONS  
- RAW_GAME_EVENTS

Requirements:
- snowflake-connector-python[pandas] package (must include [pandas] extra!)
- pandas
- python-dotenv
- Credentials in .env file or environment variables

Installation:
    pip install "snowflake-connector-python[pandas]" pandas python-dotenv

Usage:
    # Set environment variables in .env file or export them:
    SNOWFLAKE_USER="your_username"
    SNOWFLAKE_PASSWORD="your_password"
    SNOWFLAKE_ACCOUNT="cwrlboz-pz37526"
    SNOWFLAKE_WAREHOUSE="COMPUTE_WH"
    
    # Run the script
    python load_to_snowflake.py

The script will:
1. Create or replace tables in GAME_ANALYTICS.RAW schema
2. Load data from data/raw_players.csv, data/raw_sessions.csv, data/raw_game_events.csv
3. Handle VARIANT type for properties column in RAW_GAME_EVENTS

Note: The [pandas] extra is REQUIRED for write_pandas() to work properly.
"""

import os
import sys
import json
from pathlib import Path
from dotenv import load_dotenv

# Load environment variables from .env file first
load_dotenv()

# Import pandas FIRST and ensure it's fully loaded
# This is critical for snowflake connector to detect pandas
import pandas as pd

# Force pandas modules to be loaded and accessible
import pandas.core.indexes.range as pd_range
_ = pd.DataFrame, pd.RangeIndex, pd_range.RangeIndex

# Now import snowflake connector
import snowflake.connector

# Monkey patch snowflake connector's options to ensure pandas is available
# This fixes the lazy import issue
try:
    import snowflake.connector.options as sf_options
    # Ensure pandas is registered in snowflake's options
    if not hasattr(sf_options, 'pandas') or sf_options.pandas is None:
        sf_options.pandas = pd
except Exception:
    pass

# Import write_pandas - this should work now that pandas is fully loaded
from snowflake.connector.pandas_tools import write_pandas

# =====================
# CONFIG
# =====================
# Snowflake connection details (use environment variables)
SNOWFLAKE_ACCOUNT = os.getenv("SNOWFLAKE_ACCOUNT")
SNOWFLAKE_USER = os.getenv("SNOWFLAKE_USER")
SNOWFLAKE_PASSWORD = os.getenv("SNOWFLAKE_PASSWORD")
SNOWFLAKE_WAREHOUSE = os.getenv("SNOWFLAKE_WAREHOUSE")
SNOWFLAKE_DATABASE = os.getenv("SNOWFLAKE_DATABASE")
SNOWFLAKE_SCHEMA = os.getenv("SNOWFLAKE_SCHEMA")

# File paths
DATA_DIR = Path(__file__).parent.parent / "data"
PLAYERS_CSV = DATA_DIR / "raw_players.csv"
SESSIONS_CSV = DATA_DIR / "raw_sessions.csv"
GAME_EVENTS_CSV = DATA_DIR / "raw_game_events.csv"


# =====================
# SNOWFLAKE SCHEMAS
# =====================
RAW_PLAYERS_SCHEMA = """
CREATE OR REPLACE TABLE {database}.{schema}.RAW_PLAYERS (
    PLAYER_ID VARCHAR(255) NOT NULL,
    FIRST_SEEN_AT STRING NOT NULL,
    COUNTRY VARCHAR(10),
    LANGUAGE VARCHAR(10),
    DIFFICULTY_SELECTED VARCHAR(20)
)
"""

RAW_SESSIONS_SCHEMA = """
CREATE OR REPLACE TABLE {database}.{schema}.RAW_SESSIONS (
    SESSION_ID VARCHAR(255) NOT NULL,
    PLAYER_ID VARCHAR(255) NOT NULL,
    SESSION_START STRING NOT NULL,
    SESSION_END STRING NOT NULL,
    PLATFORM VARCHAR(10)
)
"""

RAW_GAME_EVENTS_SCHEMA = """
CREATE OR REPLACE TABLE {database}.{schema}.RAW_GAME_EVENTS (
    EVENT_ID VARCHAR(255) NOT NULL,
    EVENT_TIME STRING NOT NULL,
    PLAYER_ID VARCHAR(255) NOT NULL,
    EVENT_NAME VARCHAR(100) NOT NULL,
    PLATFORM VARCHAR(10),
    GAME_VERSION VARCHAR(20),
    PROPERTIES VARIANT
)
"""


# =====================
# HELPERS
# =====================
def get_snowflake_connection():
    """Create and return a Snowflake connection."""
    if not SNOWFLAKE_USER or not SNOWFLAKE_PASSWORD:
        raise ValueError(
            "SNOWFLAKE_USER and SNOWFLAKE_PASSWORD environment variables must be set"
        )
    
    conn = snowflake.connector.connect(
        user=SNOWFLAKE_USER,
        password=SNOWFLAKE_PASSWORD,
        account=SNOWFLAKE_ACCOUNT,
        warehouse=SNOWFLAKE_WAREHOUSE,
        database=SNOWFLAKE_DATABASE,
        schema=SNOWFLAKE_SCHEMA,
    )
    return conn


def create_table(conn, schema_sql: str, table_name: str):
    """Create or replace a table in Snowflake."""
    print(f"Creating/replacing table: {table_name}...")
    cursor = conn.cursor()
    try:
        cursor.execute(schema_sql.format(
            database=SNOWFLAKE_DATABASE,
            schema=SNOWFLAKE_SCHEMA
        ))
        print(f"✅ Table {table_name} created successfully")
    except Exception as e:
        print(f"❌ Error creating table {table_name}: {e}")
        raise
    finally:
        cursor.close()


def load_dataframe_to_snowflake(
    conn,
    df: pd.DataFrame,
    table_name: str,
):
    """
    Load a pandas DataFrame into Snowflake.
    
    Args:
        conn: Snowflake connection
        df: DataFrame to load
        table_name: Target table name
    """
    print(f"\nLoading {len(df)} rows into {table_name}...")
    
    try:
        # Ensure DataFrame index is RangeIndex (required by write_pandas)
        if not isinstance(df.index, pd.RangeIndex):
            df = df.reset_index(drop=True)

        df.columns = [c.upper() for c in df.columns]

        success, nchunks, nrows, _ = write_pandas(
            conn=conn,
            df=df,
            table_name=table_name,
            database=SNOWFLAKE_DATABASE,
            schema=SNOWFLAKE_SCHEMA,
            auto_create_table=False,
            overwrite=True,
        )
        
        if success:
            print(f"✅ Successfully loaded {nrows} rows into {table_name}")
        else:
            print(f"❌ Failed to load data into {table_name}")
            
    except Exception as e:
        print(f"❌ Error loading data into {table_name}: {e}")
        # Print more debug info
        print(f"   DataFrame type: {type(df)}")
        print(f"   DataFrame index type: {type(df.index)}")
        print(f"   Pandas version: {pd.__version__}")
        raise


def load_players(conn):
    """Load players data."""
    print("\n" + "="*60)
    print("Loading RAW_PLAYERS")
    print("="*60)
    
    # Create table
    create_table(conn, RAW_PLAYERS_SCHEMA, "RAW_PLAYERS")
    
    # Load data
    df = pd.read_csv(PLAYERS_CSV, parse_dates=["first_seen_at"])

    load_dataframe_to_snowflake(conn, df, "RAW_PLAYERS")


def load_sessions(conn):
    """Load sessions data."""
    print("\n" + "="*60)
    print("Loading RAW_SESSIONS")
    print("="*60)
    
    # Create table
    create_table(conn, RAW_SESSIONS_SCHEMA, "RAW_SESSIONS")
    
    # Load data
    df = pd.read_csv(
        SESSIONS_CSV,
        parse_dates=["session_start", "session_end"]
    )
    load_dataframe_to_snowflake(conn, df, "RAW_SESSIONS")


def load_game_events(conn):
    """Load game events data."""
    print("\n" + "="*60)
    print("Loading RAW_GAME_EVENTS")
    print("="*60)
    
    # Create table
    create_table(conn, RAW_GAME_EVENTS_SCHEMA, "RAW_GAME_EVENTS")
    
    # Load data
    df = pd.read_csv(
        GAME_EVENTS_CSV,
        parse_dates=["event_time"]
    )
    
    # For VARIANT type, convert JSON string to dict/object
    # Snowflake's write_pandas expects Python objects for VARIANT columns
    if "properties" in df.columns:
        def parse_properties(x):
            if pd.isna(x) or x == "":
                return {}
            if isinstance(x, str):
                try:
                    return json.loads(x)
                except (json.JSONDecodeError, TypeError):
                    return {}
            return x if isinstance(x, dict) else {}
        
        df["properties"] = df["properties"].apply(parse_properties)
    
    load_dataframe_to_snowflake(conn, df, "RAW_GAME_EVENTS")


# =====================
# MAIN
# =====================
def main():
    """Main function to load all data into Snowflake."""
    print("\n" + "="*60)
    print("🚀 Starting Snowflake Data Load")
    print("="*60)
    print(f"Database: {SNOWFLAKE_DATABASE}")
    print(f"Schema: {SNOWFLAKE_SCHEMA}")
    print(f"Account: {SNOWFLAKE_ACCOUNT}")
    print("="*60)
    
    # Verify files exist
    for file_path, name in [
        (PLAYERS_CSV, "raw_players.csv"),
        (SESSIONS_CSV, "raw_sessions.csv"),
        (GAME_EVENTS_CSV, "raw_game_events.csv"),
    ]:

        if not file_path.exists():
            raise FileNotFoundError(f"❌ File not found: {file_path}")
        print(f"✅ Found {name}")
    
    # Connect to Snowflake
    print("\nConnecting to Snowflake...")
    conn = get_snowflake_connection()
    print("✅ Connected successfully")
    
    try:
        # Load each table
        load_players(conn)
        load_sessions(conn)
        load_game_events(conn)
        
        print("\n" + "="*60)
        print("✨ All data loaded successfully!")
        print("="*60)
        
    except Exception as e:
        print(f"\n❌ Error during data load: {e}")
        raise
    finally:
        conn.close()
        print("\nConnection closed")


if __name__ == "__main__":
    try:
        main()
    except snowflake.connector.errors.HttpError as e:
        # Handle common HTTP connectivity errors (e.g. bad account identifier)
        print("\n❌ Failed to connect to Snowflake (HTTP error).")
        print("   This usually means your SNOWFLAKE_ACCOUNT is incorrect or unreachable.")
        current_account = SNOWFLAKE_ACCOUNT or "<not set>"
        print(f"   SNOWFLAKE_ACCOUNT is currently set to: {current_account}")
        print("   It should look like 'cwrlboz-pz37526' (the course account id).")
        print("   See credentials guide:")
        print("     https://github.com/makararena/game-data-platform/blob/main/instructions/snowflake-credentials.md")
        # Optionally also show the low‑level message for debugging
        print(f"\n   Raw error from driver: {e}")
        sys.exit(1)
    except ValueError as e:
        # Missing required env vars / misconfiguration
        print(f"\n❌ Configuration error: {e}")
        print("   Make sure these variables are set in app/.env (or your environment):")
        print("     SNOWFLAKE_USER, SNOWFLAKE_PASSWORD, SNOWFLAKE_ACCOUNT,")
        print("     SNOWFLAKE_WAREHOUSE, SNOWFLAKE_DATABASE, SNOWFLAKE_SCHEMA")
        sys.exit(1)
    except Exception as e:
        # Catch‑all to avoid an unhandled stack trace for students
        print(f"\n❌ Unexpected error during Snowflake load: {e}")
        print("   If this keeps happening, please share this message with the instructor.")
        sys.exit(1)
