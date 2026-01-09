"""
Import plays from JSON backup to PostgreSQL
"""
import json
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).parent
sys.path.insert(0, str(PROJECT_ROOT))

from core.database import get_postgres

def import_plays():
    """Import plays from JSON backup to PostgreSQL."""
    
    # Read the plays JSON file
    json_path = PROJECT_ROOT / "config" / "plays_cache.json"
    
    with open(json_path, 'r') as f:
        plays = json.load(f)
    
    print(f"Found {len(plays)} plays in JSON backup")
    
    imported = 0
    skipped = 0
    
    with get_postgres() as conn:
        with conn.cursor() as cursor:
            for play in plays:
                play_id = play['id']
                
                # Check if play already exists
                cursor.execute("""
                    SELECT id FROM follow_the_goat_plays WHERE id = %s
                """, [play_id])
                
                existing = cursor.fetchone()
                
                if existing:
                    print(f"  Play #{play_id} ({play['name']}) - SKIPPED (already exists)")
                    skipped += 1
                    continue
                
                # Insert the play
                cursor.execute("""
                    INSERT INTO follow_the_goat_plays (
                        id, created_at, find_wallets_sql, max_buys_per_cycle,
                        sell_logic, live_trades, name, description, sorting,
                        short_play, tricker_on_perp, timing_conditions,
                        bundle_trades, play_log, cashe_wallets,
                        cashe_wallets_settings, pattern_validator,
                        pattern_validator_enable, pattern_update_by_ai,
                        pattern_version_id, is_active, project_id,
                        project_ids, project_version
                    ) VALUES (
                        %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                        %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                        %s, %s, %s, %s
                    )
                """, [
                    play_id,
                    play.get('created_at'),
                    play.get('find_wallets_sql'),
                    play.get('max_buys_per_cycle'),
                    play.get('sell_logic'),
                    play.get('live_trades'),
                    play.get('name'),
                    play.get('description'),
                    play.get('sorting'),
                    play.get('short_play'),
                    play.get('tricker_on_perp'),
                    play.get('timing_conditions'),
                    play.get('bundle_trades'),
                    play.get('play_log'),
                    play.get('cashe_wallets'),
                    play.get('cashe_wallets_settings'),
                    play.get('pattern_validator'),
                    play.get('pattern_validator_enable'),
                    play.get('pattern_update_by_ai'),
                    play.get('pattern_version_id'),
                    play.get('is_active'),
                    play.get('project_id'),
                    play.get('project_ids'),
                    play.get('project_version')
                ])
                
                conn.commit()
                print(f"  Play #{play_id} ({play['name']}) - IMPORTED")
                imported += 1
    
    print(f"\n✅ Import complete:")
    print(f"   Imported: {imported}")
    print(f"   Skipped: {skipped}")
    print(f"   Total: {len(plays)}")

if __name__ == '__main__':
    import_plays()
