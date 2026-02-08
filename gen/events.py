import json
import os
import random
import uuid
from datetime import datetime, timedelta
from pathlib import Path
from typing import List, Dict

import pandas as pd


# =====================
# CONFIG
# =====================
INPUT_DIR = Path("data")
OUTPUT_DIR = Path("data")
PLAYERS_CSV = INPUT_DIR / "raw_players.csv"
SESSIONS_CSV = INPUT_DIR / "raw_sessions.csv"
OUTPUT_CSV = OUTPUT_DIR / "raw_game_events.csv"

# Read from environment variable or use default
GAME_VERSION = os.getenv("GAME_VERSION", "1.0.3")

EVENT_TYPES = [
    "game_started",
    "chapter_started",
    "checkpoint_reached",
    "enemy_killed",
    "player_died",
    "item_crafted",
    "chapter_completed",
    "game_closed",
]

DIFFICULTY_DEATH_MULTIPLIER = {
    "easy": 0.1,
    "normal": 0.3,
    "hard": 0.6,
    "grounded": 0.8,
}

# Additional realistic data options
CHAPTER_NAMES = [
    "The Outskirts", "The Quarantine Zone", "Downtown", "The Suburbs",
    "The University", "The Hospital", "The Financial District", "The Docks",
    "The Bridge", "The Firefly Lab"
]

LOCATIONS = [
    "abandoned_building", "street", "sewer", "rooftop", "warehouse",
    "park", "subway", "apartment", "mall", "school"
]

WEATHER = ["clear", "rain", "fog", "snow"]

ENEMY_NAMES = {
    "infected": ["runner", "clicker", "bloater", "stalker", "shambler"],
    "human": ["hunter", "soldier", "scavenger", "bandit", "merchant"]
}

WEAPON_NAMES = {
    "pistol": ["9mm_pistol", "revolver", "silenced_pistol"],
    "rifle": ["hunting_rifle", "assault_rifle", "sniper_rifle"],
    "bow": ["hunting_bow", "compound_bow"]
}

CRAFTING_MATERIALS = {
    "medkit": ["alcohol", "rag", "scissors"],
    "molotov": ["alcohol", "rag", "bottle"],
    "shiv": ["scissors", "tape", "blade"]
}


# =====================
# HELPERS
# =====================
def random_time(start: datetime, end: datetime) -> datetime:
    """Generate a random time between start and end"""
    delta = end - start
    return start + timedelta(seconds=random.randint(0, int(delta.total_seconds())))


def make_event(
    event_time: datetime,
    player_id: str,
    platform: str,
    event_name: str,
    properties: Dict,
) -> Dict:
    """Make an event json object"""
    return {
        "event_id": str(uuid.uuid4()),
        "event_time": event_time,
        "player_id": player_id,
        "event_name": event_name,
        "platform": platform,
        "game_version": GAME_VERSION,
        "properties": properties,
    }


# =====================
# EVENT GENERATION
# =====================
def generate_events_for_session(session, difficulty) -> List[Dict]:
    events = []

    max_chapter = random.randint(1, 10)
    
    # Randomly take a chapter when the user could rage quit
    rage_quit_chapter = random.choice(
        list(range(1, max_chapter + 1)) + [None]
    )

    # Track session stats
    chapter_start_times = {}

    # game started
    events.append(
        make_event(
            session["session_start"],
            session["player_id"],
            session["platform"],
            "game_started",
            {
                "load_time_ms": random.randint(2000, 8000),
                "resolution": random.choice(["1080p", "1440p", "4K"]),
                "fps_target": random.choice([30, 60, 120]),
                "audio_quality": random.choice(["low", "medium", "high"]),
            },
        )
    )

    # Iterate over each chapter
    for chapter in range(1, max_chapter + 1):
        # Randomly generate a start time for the chapter
        chapter_start_time = random_time(
            session["session_start"], session["session_end"]
        )

        # Track chapter start time
        chapter_start_times[chapter] = chapter_start_time
        
        # Track chapter-specific stats
        chapter_deaths = 0
        chapter_enemies_killed = 0
        
        # Chapter started
        chapter_name = CHAPTER_NAMES[min(chapter - 1, len(CHAPTER_NAMES) - 1)]
        location = random.choice(LOCATIONS)
        weather = random.choice(WEATHER)
        
        events.append(
            make_event(
                chapter_start_time,
                session["player_id"],
                session["platform"],
                "chapter_started",
                {
                    "chapter_id": chapter,
                    "chapter_name": chapter_name,
                    "location": location,
                    "weather": weather,
                    "time_of_day": random.choice(["dawn", "day", "dusk", "night"]),
                },
            )
        )

        # Checkpoints
        for checkpoint in range(1, random.randint(2, 5)):
            checkpoint_time = random_time(chapter_start_time, session["session_end"])
            time_since_start = int((checkpoint_time - chapter_start_time).total_seconds())
            
            events.append(
                make_event(
                    checkpoint_time,
                    session["player_id"],
                    session["platform"],
                    "checkpoint_reached",
                    {
                        "chapter_id": chapter,
                        "checkpoint_id": checkpoint,
                        "time_since_chapter_start_seconds": time_since_start,
                        "health_percentage": random.randint(20, 100),
                        "ammo_count": random.randint(0, 200),
                        "inventory_items": random.randint(5, 25),
                    },
                )
            )

        # enemy kills
        for _ in range(random.randint(2, 10)):
            enemy_type = random.choice(["infected", "human"])
            enemy_name = random.choice(ENEMY_NAMES[enemy_type])
            weapon_type = random.choice(["pistol", "rifle", "bow"])
            weapon_name = random.choice(WEAPON_NAMES[weapon_type])
            is_headshot = random.random() < 0.3
            damage_dealt = random.randint(50, 200) if not is_headshot else random.randint(150, 300)
            
            chapter_enemies_killed += 1
            
            events.append(
                make_event(
                    random_time(chapter_start_time, session["session_end"]),
                    session["player_id"],
                    session["platform"],
                    "enemy_killed",
                    {
                        "chapter_id": chapter,
                        "enemy_type": enemy_type,
                        "enemy_name": enemy_name,
                        "weapon_type": weapon_type,
                        "weapon_name": weapon_name,
                        "damage_dealt": damage_dealt,
                        "headshot": is_headshot,
                        "distance_meters": random.randint(5, 50),
                        "xp_gained": random.randint(10, 50),
                        "stealth_kill": random.random() < 0.2,
                    },
                )
            )

        # deaths (difficulty-driven)
        if random.random() < DIFFICULTY_DEATH_MULTIPLIER[difficulty]:
            death_time = random_time(chapter_start_time, session["session_end"])
            time_survived = int((death_time - chapter_start_time).total_seconds())
            death_reason = random.choice(["combat", "environment", "fall", "explosion"])
            chapter_deaths += 1
            
            events.append(
                make_event(
                    death_time,
                    session["player_id"],
                    session["platform"],
                    "player_died",
                    {
                        "chapter_id": chapter,
                        "death_reason": death_reason,
                        "health_at_death": random.randint(0, 30),
                        "time_survived_seconds": time_survived,
                        "last_enemy_type": random.choice(["infected", "human", "none"]),
                        "location": random.choice(LOCATIONS),
                        "death_count_in_chapter": chapter_deaths,
                    },
                )
            )

            # rage quit
            if rage_quit_chapter == chapter:
                break

        # crafting (optional)
        if random.random() < 0.4:
            item_type = random.choice(["medkit", "molotov", "shiv"])
            materials = CRAFTING_MATERIALS[item_type]
            
            events.append(
                make_event(
                    random_time(chapter_start_time, session["session_end"]),
                    session["player_id"],
                    session["platform"],
                    "item_crafted",
                    {
                        "chapter_id": chapter,
                        "item_type": item_type,
                        "materials_used": materials,
                        "crafting_time_seconds": random.randint(2, 8),
                        "success": random.random() > 0.1,  # 90% success rate
                        "workbench_used": random.random() < 0.3,
                    },
                )
            )

        # chapter completion
        if random.random() < 0.85:
            chapter_end_time = random_time(chapter_start_time, session["session_end"])
            completion_time = int((chapter_end_time - chapter_start_time).total_seconds())
            
            events.append(
                make_event(
                    chapter_end_time,
                    session["player_id"],
                    session["platform"],
                    "chapter_completed",
                    {
                        "chapter_id": chapter,
                        "completion_time_seconds": completion_time,
                        "score": random.randint(500, 5000),
                        "collectibles_found": random.randint(0, 5),
                        "deaths_count": chapter_deaths,
                        "enemies_killed": chapter_enemies_killed,
                        "accuracy_percentage": round(random.uniform(45, 95), 1),
                    },
                )
            )
        else:
            break

    # game closed
    session_duration = int((session["session_end"] - session["session_start"]).total_seconds())
    total_deaths = sum(1 for e in events if e["event_name"] == "player_died")
    total_enemies = sum(1 for e in events if e["event_name"] == "enemy_killed")
    
    events.append(
        make_event(
            session["session_end"],
            session["player_id"],
            session["platform"],
            "game_closed",
            {
                "session_duration_seconds": session_duration,
                "reason": random.choice(["normal", "quit", "menu", "idle_timeout"]),
                "total_deaths": total_deaths,
                "total_enemies_killed": total_enemies,
                "chapters_completed": len([e for e in events if e["event_name"] == "chapter_completed"]),
                "achievements_unlocked": random.randint(0, 3),
                "final_score": random.randint(1000, 50000),
            },
        )
    )

    return events


# =====================
# MAIN
# =====================
def main():
    # Read input CSV files
    players = pd.read_csv(PLAYERS_CSV)
    sessions = pd.read_csv(SESSIONS_CSV)

    # Convert datetime columns
    sessions["session_start"] = pd.to_datetime(sessions["session_start"])
    sessions["session_end"] = pd.to_datetime(sessions["session_end"])

    players_map = dict(
        zip(players.player_id, players.difficulty_selected)
    )

    all_events = []

    for _, session in sessions.iterrows():
        difficulty = players_map.get(
            session["player_id"], "normal"
        )
        all_events.extend(
            generate_events_for_session(session, difficulty)
        )

    df = pd.DataFrame(all_events)
    df["event_time"] = pd.to_datetime(df["event_time"])

    # Serialize properties dict to JSON string for CSV
    df["properties"] = df["properties"].apply(json.dumps)

    # Ensure output directory exists
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    # Export to CSV
    df.to_csv(OUTPUT_CSV, index=False)

    print(f"Exported {len(df)} events to {OUTPUT_CSV}")


if __name__ == "__main__":
    main()

