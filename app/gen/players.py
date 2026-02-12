import os
import random
from datetime import datetime, timedelta
from pathlib import Path

import pandas as pd


# =====================
# CONFIG
# =====================
OUTPUT_DIR = Path("data")
OUTPUT_CSV = OUTPUT_DIR / "raw_players.csv"

# Read from environment variable or use default
N_PLAYERS = int(os.getenv("N_PLAYERS", "1500"))
EVENT_DATE_START = os.getenv("EVENT_DATE_START")  # YYYY-MM-DD, optional
EVENT_DATE_END = os.getenv("EVENT_DATE_END")  # YYYY-MM-DD, optional
LOAD_BATCH_ID = int(os.getenv("LOAD_BATCH_ID", "1"))  # For incremental: batch 2+ = new users, unique IDs

# Countries and languages
COUNTRIES = [
    ("US", "en"),
    ("PL", "pl"),
    ("DE", "de"),
    ("FR", "fr"),
    ("ES", "es"),
    ("BY", "be"),
]

# Difficulty distribution wiht a higher chance of choosing normal difficulty
DIFFICULTY_DISTRIBUTION = [
    ("easy", 0.15),
    ("normal", 0.55),
    ("hard", 0.20),
    ("grounded", 0.10),
]


# =====================
# HELPERS
# =====================
def weighted_choice(choices):
    """
    choices: List of (value, weight)
    Return a random choice from the choices with the given weights
    """
    values, weights = zip(*choices)
    return random.choices(values, weights=weights, k=1)[0]


def random_past_timestamp(days_back: int = 90) -> datetime:
    """Generate a random timestamp in the past"""
    return datetime.utcnow() - timedelta(
        days=random.randint(0, days_back),
        hours=random.randint(0, 23),
        minutes=random.randint(0, 59),
    )


def random_timestamp_in_event_range() -> datetime:
    """Random timestamp within [EVENT_DATE_START, end of EVENT_DATE_END]; if not set, use last 90 days."""
    if EVENT_DATE_START and EVENT_DATE_END:
        start = datetime.strptime(EVENT_DATE_START, "%Y-%m-%d")
        end = datetime.strptime(EVENT_DATE_END, "%Y-%m-%d") + timedelta(days=1)
        delta = end - start
        sec = random.randint(0, max(0, int(delta.total_seconds()) - 1))
        return start + timedelta(seconds=sec)
    return random_past_timestamp(90)


# =====================
# PLAYER GENERATION
# =====================
def generate_players(n_players: int) -> pd.DataFrame:
    """Generate a dataframe of players"""
    rows = []

    for i in range(1, n_players + 1):
        country, language = random.choice(COUNTRIES)
        # Batch 1: legacy format (player_1, player_2). Batch 2+: namespaced (player_2_1, player_2_2)
        player_id = f"player_{i}" if LOAD_BATCH_ID == 1 else f"player_{LOAD_BATCH_ID}_{i}"
        player = {
            "player_id": player_id,
            "first_seen_at": random_timestamp_in_event_range(),
            "country": country,
            "language": language,
            "difficulty_selected": weighted_choice(
                DIFFICULTY_DISTRIBUTION
            ),
        }

        rows.append(player)

    return pd.DataFrame(rows)


# =====================
# MAIN
# =====================
def main():
    seed = int(os.getenv("GAME_DATA_SEED", "42"))
    random.seed(seed)

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    df = generate_players(N_PLAYERS)

    # Ensure deterministic column order
    df = df[
        [
            "player_id",
            "first_seen_at",
            "country",
            "language",
            "difficulty_selected",
        ]
    ]

    df.to_csv(OUTPUT_CSV, index=False)

    print(f"🎮 Generated {len(df)} players → {OUTPUT_CSV}")


if __name__ == "__main__":
    main()
