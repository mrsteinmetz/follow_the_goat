#!/bin/bash
# Automated PostgreSQL Migration for Trading/Data Modules
# This script updates all Python files to use PostgreSQL instead of DuckDB

set -e  # Exit on error

echo "=================================="
echo "PostgreSQL Migration - Batch Update"
echo "=================================="

# Files to update
FILES=(
    "000trading/follow_the_goat.py"
    "000trading/sell_trailing_stop.py"
    "000trading/train_validator.py"
    "000trading/trail_generator.py"
    "000trading/pattern_validator.py"
    "000trading/trail_data.py"
    "000data_feeds/1_jupiter_get_prices/get_prices_from_jupiter.py"
    "000data_feeds/2_create_price_cycles/create_price_cycles.py"
    "000data_feeds/5_create_profiles/create_profiles.py"
    "000data_feeds/7_create_new_patterns/create_new_paterns.py"
    "features/webhook/app.py"
)

# Counter
UPDATED=0
FAILED=0

for file in "${FILES[@]}"; do
    if [ ! -f "$file" ]; then
        echo "✗ SKIP: $file (not found)"
        continue
    fi
    
    echo ""
    echo "Processing: $file"
    
    # Create backup
    cp "$file" "$file.backup"
    echo "  Backup created: $file.backup"
    
    # Apply transformations
    
    # 1. Remove duckdb imports
    sed -i '/^import duckdb$/d' "$file"
    
    # 2. Update database imports
    sed -i 's/from core\.database import get_duckdb, duckdb_execute_write/from core.database import get_postgres, postgres_execute/g' "$file"
    sed -i 's/from core\.database import get_duckdb, duckdb_insert/from core.database import get_postgres, postgres_insert/g' "$file"
    sed -i 's/from core\.database import get_duckdb, duckdb_update/from core.database import get_postgres, postgres_update/g' "$file"
    sed -i 's/from core\.database import get_duckdb/from core.database import get_postgres/g' "$file"
    sed -i 's/from core\.database import.*duckdb_execute_write/from core.database import get_postgres, postgres_execute/g' "$file"
    sed -i 's/from core\.database import.*duckdb_insert/from core.database import get_postgres, postgres_insert/g' "$file"
    sed -i 's/from core\.database import.*get_trading_engine/from core.database import get_postgres/g' "$file"
    
    # 3. Replace function calls - get_duckdb with read_only=True
    sed -i 's/with get_duckdb("central", read_only=True) as cursor:/with get_postgres() as conn:\n    with conn.cursor() as cursor:/g' "$file"
    sed -i "s/with get_duckdb('central', read_only=True) as cursor:/with get_postgres() as conn:\n    with conn.cursor() as cursor:/g" "$file"
    
    # 4. Replace function calls - get_duckdb without read_only
    sed -i 's/with get_duckdb("central") as conn:/with get_postgres() as conn:/g' "$file"
    sed -i "s/with get_duckdb('central') as conn:/with get_postgres() as conn:/g" "$file"
    sed -i 's/get_duckdb("central")/get_postgres()/g' "$file"
    sed -i "s/get_duckdb('central')/get_postgres()/g" "$file"
    
    # 5. Replace duckdb_execute_write calls
    sed -i 's/duckdb_execute_write("central", /postgres_execute(/g' "$file"
    sed -i "s/duckdb_execute_write('central', /postgres_execute(/g" "$file"
    
    # 6. Replace duckdb_insert calls
    sed -i 's/duckdb_insert("central", /postgres_insert(/g' "$file"
    sed -i "s/duckdb_insert('central', /postgres_insert(/g" "$file"
    
    # 7. Replace duckdb_update calls
    sed -i 's/duckdb_update("central", /postgres_update(/g' "$file"
    sed -i "s/duckdb_update('central', /postgres_update(/g" "$file"
    
    # 8. Replace SQL parameter placeholders ? with %s
    # This is complex - we'll do a simple replacement and manual review might be needed
    python3 << 'PYTHON_SCRIPT'
import sys
import re

file_path = sys.argv[1]

with open(file_path, 'r') as f:
    content = f.read()

# Pattern: .execute("...", [...]) with ? placeholders
# Replace ? with %s in SQL strings
def replace_placeholders(match):
    full_match = match.group(0)
    # Replace ? with %s
    return full_match.replace('?', '%s')

# Match .execute( with strings containing ?
pattern = r'\.execute\s*\(\s*["\']([^"\']*\?[^"\']*)["\']'
content = re.sub(pattern, replace_placeholders, content)

# Match .execute with triple quotes
pattern = r'\.execute\s*\(\s*"""([^"]*\?[^"]*)"""'
content = re.sub(pattern, replace_placeholders, content)

with open(file_path, 'w') as f:
    f.write(content)

print(f"  ✓ Parameter placeholders updated (? → %s)")
PYTHON_SCRIPT
" "$file"
    
    # Check if file was modified
    if ! diff -q "$file" "$file.backup" > /dev/null 2>&1; then
        echo "  ✓ Updated successfully"
        UPDATED=$((UPDATED + 1))
    else
        echo "  - No changes needed"
    fi
done

echo ""
echo "=================================="
echo "Migration Complete"
echo "=================================="
echo "Files updated: $UPDATED"
echo "Files failed: $FAILED"
echo ""
echo "NOTE: Please manually review:"
echo "1. INSERT OR REPLACE → ON CONFLICT DO UPDATE"
echo "2. INSERT OR IGNORE → ON CONFLICT DO NOTHING"
echo "3. Any remaining ? placeholders in complex SQL"
echo ""
echo "Backups created with .backup extension"
