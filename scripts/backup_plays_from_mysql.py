"""
Backup follow_the_goat_plays from Windows MySQL to plays_cache.json

Run this BEFORE removing Windows MySQL connection!

Usage:
    python scripts/backup_plays_from_mysql.py
"""

import sys
import json
from pathlib import Path
from datetime import datetime

# Add project root to path
PROJECT_ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from core.database import get_mysql
from core.config import settings

CONFIG_DIR = PROJECT_ROOT / "config"
PLAYS_CACHE_FILE = CONFIG_DIR / "plays_cache.json"
BACKUP_FILE = CONFIG_DIR / f"plays_backup_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"


def backup_plays():
    """Backup plays from MySQL to JSON file."""
    print("=" * 60)
    print("Backup follow_the_goat_plays from Windows MySQL")
    print("=" * 60)
    print(f"MySQL: {settings.mysql.host}/{settings.mysql.database}")
    print(f"Target: {PLAYS_CACHE_FILE}")
    print()
    
    # Ensure config directory exists
    CONFIG_DIR.mkdir(parents=True, exist_ok=True)
    
    try:
        # Fetch plays from MySQL
        print("Connecting to Windows MySQL...")
        with get_mysql() as conn:
            with conn.cursor() as cursor:
                cursor.execute("SELECT * FROM follow_the_goat_plays ORDER BY id")
                plays = cursor.fetchall()
        
        print(f"Found {len(plays)} plays in MySQL")
        
        if not plays:
            print("WARNING: No plays found in MySQL!")
            return False
        
        # Convert datetime objects to strings for JSON serialization
        plays_serializable = []
        for play in plays:
            play_dict = dict(play)
            for key, value in play_dict.items():
                if hasattr(value, 'isoformat'):
                    play_dict[key] = value.isoformat()
            plays_serializable.append(play_dict)
        
        # Create timestamped backup first
        with open(BACKUP_FILE, 'w', encoding='utf-8') as f:
            json.dump(plays_serializable, f, indent=2, ensure_ascii=False)
        print(f"Created timestamped backup: {BACKUP_FILE}")
        
        # Update main cache file
        with open(PLAYS_CACHE_FILE, 'w', encoding='utf-8') as f:
            json.dump(plays_serializable, f, indent=2, ensure_ascii=False)
        print(f"Updated plays cache: {PLAYS_CACHE_FILE}")
        
        # Verify
        with open(PLAYS_CACHE_FILE, 'r', encoding='utf-8') as f:
            verified = json.load(f)
        
        print()
        print("=" * 60)
        print(f"SUCCESS: Backed up {len(verified)} plays")
        print("=" * 60)
        
        # Print play names
        print("\nPlays backed up:")
        for play in verified:
            status = "ACTIVE" if play.get('is_active') else "inactive"
            print(f"  - ID {play['id']}: {play.get('name', 'unnamed')} [{status}]")
        
        return True
        
    except Exception as e:
        print(f"\nERROR: Failed to backup plays: {e}")
        print("\nIf MySQL is not accessible, ensure the existing plays_cache.json is valid.")
        return False


def verify_existing_cache():
    """Verify the existing plays_cache.json file."""
    print("\nVerifying existing cache file...")
    
    if not PLAYS_CACHE_FILE.exists():
        print(f"  No existing cache at {PLAYS_CACHE_FILE}")
        return None
    
    try:
        with open(PLAYS_CACHE_FILE, 'r', encoding='utf-8') as f:
            plays = json.load(f)
        
        print(f"  Existing cache has {len(plays)} plays")
        return plays
    except Exception as e:
        print(f"  ERROR reading cache: {e}")
        return None


if __name__ == "__main__":
    # First check existing cache
    existing = verify_existing_cache()
    
    # Try to backup from MySQL
    success = backup_plays()
    
    if not success and existing:
        print("\nNOTE: Existing plays_cache.json is available as fallback")
        print(f"      Contains {len(existing)} plays")

