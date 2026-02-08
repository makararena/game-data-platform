import os
import random
from datetime import datetime, timedelta
from pathlib import Path

import pandas as pd


# =====================
# CONFIG
# =====================
INPUT_DIR = Path("data")
OUTPUT_DIR = Path("data")

PLAYERS_CSV = INPUT_DIR / "raw_players.csv"
OUTPUT_CSV = OUTPUT_DIR / "raw_sessions.csv"

# Read from environment variable or use default
MAX_SESSIONS_PER_PLAYER = int(os.getenv("MAX_SESSIONS_PER_PLAYER", "25"))

PLATFORMS = [
    ("ps5", 0.7),
    ("pc", 0.3),
]

SESSION_LENGTH_MINUTES = {
    "short": (5, 20),
    "medium": (20, 60),
    "long": (60, 180),
}

SESSION_LENGTH_DISTRIBUTION = [
    ("short", 0.25),
    ("medium", 0.50),
    ("long", 0.25),
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


def random_session_length() -> int:
    """
    Return a random session length from the given distribution
    """
    category = weighted_choice(SESSION_LENGTH_DISTRIBUTION)
    low, high = SESSION_LENGTH_MINUTES[category]
    return random.randint(low, high)


# =====================
# SESSION GENERATION
# =====================
def generate_sessions(players_df: pd.DataFrame) -> pd.DataFrame:
    sessions = []
    session_counter = 1

    for _, player in players_df.iterrows():
        # Heavy tail: most players few sessions, some many
        n_sessions = max(
            1,
            int(random.expovariate(1 / 5)) # Exponential distribution with mean 5
        )
        n_sessions = min(n_sessions, MAX_SESSIONS_PER_PLAYER) # Cap the number of sessions per player

        last_session_end = pd.to_datetime(player["first_seen_at"])

        for _ in range(n_sessions):
            gap_days = random.randint(0, 5)
            gap_hours = random.randint(0, 12)

            session_start = last_session_end + timedelta(
                days=gap_days,
                hours=gap_hours,
            )

            session_length = random_session_length()

            session_end = session_start + timedelta(
                minutes=session_length
            )

            platform = weighted_choice(PLATFORMS)

            sessions.append(
                {
                    "session_id": f"session_{session_counter}",
                    "player_id": player["player_id"],
                    "session_start": session_start,
                    "session_end": session_end,
                    "platform": platform,
                }
            )

            last_session_end = session_end
            session_counter += 1

    return pd.DataFrame(sessions)


# =====================
# MAIN
# =====================
def main():
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    players_df = pd.read_csv(
        PLAYERS_CSV,
        parse_dates=["first_seen_at"],
    )

    sessions_df = generate_sessions(players_df)

    # Enforce column order
    sessions_df = sessions_df[
        [
            "session_id",
            "player_id",
            "session_start",
            "session_end",
            "platform",
        ]
    ]

    sessions_df.to_csv(OUTPUT_CSV, index=False)

    print(
        f"🕹 Generated {len(sessions_df)} sessions "
        f"for {len(players_df)} players → {OUTPUT_CSV}"
    )


if __name__ == "__main__":
    main()
