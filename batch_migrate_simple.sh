#!/bin/bash
# Automated PostgreSQL Migration for Trading/Data Modules

set -e

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
)

UPDATED=0

for file in "${FILES[@]}"; do
    if [ ! -f "$file" ]; then
        echo "✗ SKIP: $file (not found)"
        continue
    fi
    
    echo ""
    echo "Processing: $file"
    
    # Create backup
    cp "$file" "$file.backup"
    echo "  Backup: $file.backup"
    
    # 1. Remove duckdb imports
    sed -i '/^import duckdb$/d' "$file"
    
    # 2. Update imports (simple patterns)
    sed -i 's/from core\.database import get_duckdb/from core.database import get_postgres/g' "$file"
    sed -i 's/, duckdb_execute_write/, postgres_execute/g' "$file"
    sed -i 's/, duckdb_insert/, postgres_insert/g' "$file"
    sed -i 's/, get_trading_engine//g' "$file"
    
    # 3. Replace get_duckdb calls
    sed -i 's/get_duckdb("central")/get_postgres()/g' "$file"
    sed -i "s/get_duckdb('central')/get_postgres()/g" "$file"
    
    # 4. Replace duckdb_execute_write
    sed -i 's/duckdb_execute_write("central", /postgres_execute(/g' "$file"
    sed -i "s/duckdb_execute_write('central', /postgres_execute(/g" "$file"
    
    # 5. Replace SQL placeholders
    sed -i 's/execute("\([^"]*\)?\([^"]*\)", /execute("\1%s\2", /g' "$file"
    sed -i "s/execute('\([^']*\)?\([^']*\)', /execute('\1%s\2', /g" "$file"
    
    # Check if modified
    if ! diff -q "$file" "$file.backup" > /dev/null 2>&1; then
        echo "  ✓ Updated"
        UPDATED=$((UPDATED + 1))
    else
        echo "  - No changes"
    fi
done

echo ""
echo "=================================="
echo "Complete: $UPDATED files updated"
echo "=================================="
