"""
Main script to run all data generation scripts in sequence.

Execution order:
1. raw_players_events.py - Generates player data
2. raw_sessions_events.py - Generates session data (depends on players)
3. raw_game_events.py - Generates game events (depends on players and sessions)
"""

import os
import subprocess
import sys
from pathlib import Path


# =====================
# CONFIGURATION
# =====================
# Centralized configuration for all data generation scripts
CONFIG = {
    # Players generation
    "N_PLAYERS": 1000,
    
    # Sessions generation
    "MAX_SESSIONS_PER_PLAYER": 25,
    
    # Game events generation
    "GAME_VERSION": "1.0.3",
}


# Script execution order with their required environment variables
SCRIPTS = [
    {
        "name": "players.py",
        "env": {
            "N_PLAYERS": str(CONFIG["N_PLAYERS"]),
        },
    },
    {
        "name": "sessions.py",
        "env": {
            "MAX_SESSIONS_PER_PLAYER": str(CONFIG["MAX_SESSIONS_PER_PLAYER"]),
        },
    },
    {
        "name": "events.py",
        "env": {
            "GAME_VERSION": CONFIG["GAME_VERSION"],
        },
    },
]


def main():
    """Run all data generation scripts in sequence."""
    # Get the directory where this script is located (gen folder)
    script_dir = Path(__file__).parent
    # Get the project root (parent of gen folder)
    project_root = script_dir.parent
    
    print("\n" + "="*60)
    print("🎮 Starting Data Generation Pipeline")
    print("="*60)
    print("\n📊 Configuration:")
    for key, value in CONFIG.items():
        print(f"   {key}: {value}")
    print()
    
    # Run each script in order
    for script_config in SCRIPTS:
        script_name = script_config["name"]
        script_path = script_dir / script_name
        
        if not script_path.exists():
            print(f"❌ Script not found: {script_path}")
            sys.exit(1)
        
        # Prepare environment variables
        env = os.environ.copy()
        env.update(script_config["env"])
        
        print(f"\n{'='*60}")
        print(f"🚀 Running: {script_name}")
        if script_config["env"]:
            print(f"   With config: {', '.join(f'{k}={v}' for k, v in script_config['env'].items())}")
        print(f"{'='*60}\n")
        
        # Run from project root so relative paths in scripts work correctly
        try:
            result = subprocess.run(
                [sys.executable, str(script_path)],
                check=True,
                cwd=project_root,
                env=env,
            )
            print(f"\n✅ Successfully completed: {script_name}\n")
        except subprocess.CalledProcessError as e:
            print(f"\n❌ Error running {script_name}: {e}\n")
            print(f"❌ Pipeline failed at: {script_name}")
            print("Stopping execution.\n")
            sys.exit(1)
        except Exception as e:
            print(f"\n❌ Unexpected error running {script_name}: {e}\n")
            print(f"❌ Pipeline failed at: {script_name}")
            print("Stopping execution.\n")
            sys.exit(1)
    
    print("\n" + "="*60)
    print("✨ All scripts completed successfully!")
    print("="*60 + "\n")


if __name__ == "__main__":
    main()
