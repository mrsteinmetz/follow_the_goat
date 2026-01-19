#!/usr/bin/env python3
"""
Test script to trace the is_ratio bug.
Shows why absolute filters are being created when is_ratio=true.
"""

import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).parent
sys.path.insert(0, str(PROJECT_ROOT))

from core.database import get_postgres
from core.filter_cache import get_cached_trades, get_cache_stats
import logging

logging.basicConfig(level=logging.INFO, format="%(message)s")
logger = logging.getLogger(__name__)

def test_cache_columns():
    """Check what columns are in the DuckDB cache."""
    print("="*100)
    print("TESTING DUCKDB CACHE COLUMNS")
    print("="*100)
    
    # Get cache stats
    stats = get_cache_stats()
    print(f"\nCache stats:")
    print(f"  Trades cached: {stats.get('trades_cached', 0)}")
    print(f"  Filter columns: {stats.get('filter_columns', 0)}")
    print(f"  Filter rows: {stats.get('filter_rows', 0)}")
    
    # Load data from cache with ratio_only=False
    print(f"\n\nTest 1: Load with ratio_only=FALSE")
    print("-"*100)
    df_all = get_cached_trades(hours=24, ratio_only=False)
    if len(df_all) > 0:
        filter_cols = [c for c in df_all.columns if c not in ['trade_id', 'play_id', 'followed_at', 'potential_gains', 'our_status', 'minute']]
        print(f"  Loaded {len(df_all)} rows with {len(filter_cols)} filter columns")
        print(f"  First 10 filter columns:")
        for col in sorted(filter_cols)[:10]:
            print(f"    - {col}")
    
    # Load data from cache with ratio_only=True
    print(f"\n\nTest 2: Load with ratio_only=TRUE")
    print("-"*100)
    df_ratio = get_cached_trades(hours=24, ratio_only=True)
    if len(df_ratio) > 0:
        filter_cols = [c for c in df_ratio.columns if c not in ['trade_id', 'play_id', 'followed_at', 'potential_gains', 'our_status', 'minute']]
        print(f"  Loaded {len(df_ratio)} rows with {len(filter_cols)} filter columns")
        print(f"  First 10 filter columns:")
        for col in sorted(filter_cols)[:10]:
            print(f"    - {col}")
    
    # Check what's actually in PostgreSQL for is_ratio
    print(f"\n\nTest 3: Check PostgreSQL for is_ratio status of cached columns")
    print("-"*100)
    
    with get_postgres() as conn:
        with conn.cursor() as cursor:
            # Get sample of filters from cache
            sample_filters = sorted(filter_cols)[:20] if len(df_ratio) > 0 else []
            
            if sample_filters:
                cursor.execute("""
                    SELECT DISTINCT filter_name, is_ratio
                    FROM trade_filter_values
                    WHERE filter_name = ANY(%s)
                    ORDER BY is_ratio, filter_name
                """, [sample_filters])
                
                print(f"  Checking {len(sample_filters)} sample filters from cache:")
                for row in cursor.fetchall():
                    ratio_label = 'RATIO ✅' if row['is_ratio'] == 1 else 'ABSOLUTE ❌'
                    print(f"    {row['filter_name']:<40} is_ratio={row['is_ratio']} ({ratio_label})")


def test_setting():
    """Check the is_ratio setting."""
    print("\n\n" + "="*100)
    print("CHECKING is_ratio SETTING")
    print("="*100)
    
    with get_postgres() as conn:
        with conn.cursor() as cursor:
            cursor.execute("""
                SELECT setting_key, setting_value
                FROM auto_filter_settings
                WHERE setting_key = 'is_ratio'
            """)
            result = cursor.fetchone()
            
            if result:
                print(f"\n  Database setting: is_ratio = {result['setting_value']}")
                
                # Parse as boolean
                value_str = str(result['setting_value']).strip().lower()
                is_ratio_bool = value_str in ['1', 'true', 'yes', 'on']
                print(f"  Parsed as boolean: {is_ratio_bool}")
            else:
                print(f"\n  ⚠ No is_ratio setting found in database!")
                print(f"  Will use default: False")


def main():
    test_setting()
    test_cache_columns()
    
    print("\n\n" + "="*100)
    print("CONCLUSION")
    print("="*100)
    print("""
The bug is in sync_filter_values_incremental() in core/filter_cache.py:

Lines 329, 346, 358 have HARDCODED:
    AND is_ratio = 1

This means the DuckDB cache ONLY loads ratio filters, which is actually CORRECT.

However, the cache contains BOTH ratio and absolute columns because the table
schema includes all columns from the first sync. When ratio_only=False is used,
it returns all columns that exist in the table structure.

The actual issue is that:
1. The cache table was created with BOTH ratio AND absolute filter columns
2. Even though new data syncs only populate is_ratio=1 values, the columns exist
3. When get_cached_trades(ratio_only=False) is called, it returns ALL columns
4. The pattern generator then creates filters from ALL columns it sees

The fix needs to ensure that:
- Either the cache table ONLY has ratio columns when is_ratio=true
- Or get_cached_trades() properly filters out non-ratio columns

Actually, looking at line 487-508 in filter_cache.py, there IS code to filter
columns when ratio_only=True, BUT it queries PostgreSQL to get the list of
ratio columns. This might be including columns that shouldn't be there.
""")


if __name__ == "__main__":
    main()
