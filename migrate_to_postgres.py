#!/usr/bin/env python3
"""
Automated PostgreSQL Migration Script
======================================
This script automatically updates Python files to use PostgreSQL instead of DuckDB.

Changes:
1. Updates import statements
2. Replaces get_duckdb() with get_postgres()
3. Replaces duckdb_execute_write() with postgres_execute()
4. Changes SQL parameter placeholders from ? to %s
5. Updates database function calls

Usage:
    python migrate_to_postgres.py <file_path>
    python migrate_to_postgres.py --all  # Migrate all relevant files
"""

import re
import sys
from pathlib import Path
from typing import List, Tuple

# Files to migrate
MIGRATION_TARGETS = [
    "scheduler/master2.py",
    "scheduler/website_api.py",
    "000trading/follow_the_goat.py",
    "000trading/sell_trailing_stop.py",
    "000trading/train_validator.py",
    "000trading/trail_generator.py",
    "000trading/pattern_validator.py",
    "000data_feeds/1_jupiter_get_prices/get_prices_from_jupiter.py",
    "000data_feeds/2_create_price_cycles/create_price_cycles.py",
    "000data_feeds/3_binance_order_book_data/stream_binance_order_book_data.py",
    "features/webhook/app.py",
]


def migrate_imports(content: str) -> Tuple[str, List[str]]:
    """Update import statements."""
    changes = []
    
    # Remove duckdb imports
    if "import duckdb" in content:
        content = re.sub(r'^import duckdb\s*$', '', content, flags=re.MULTILINE)
        changes.append("Removed: import duckdb")
    
    # Update core.database imports
    old_imports = [
        "get_duckdb", "duckdb_execute_write", "duckdb_insert", "duckdb_update",
        "get_trading_engine", "register_connection"
    ]
    
    for old_import in old_imports:
        if f"from core.database import" in content and old_import in content:
            # Replace individual imports
            content = re.sub(
                rf'\bfrom core\.database import ([^;\n]*\b{old_import}\b[^;\n]*)',
                lambda m: replace_database_imports(m.group(1)),
                content
            )
            changes.append(f"Updated import: {old_import}")
    
    # Remove DataClient imports (master2.py only)
    if "from core.data_client import" in content:
        content = re.sub(r'^from core\.data_client import.*$', '', content, flags=re.MULTILINE)
        changes.append("Removed: core.data_client imports")
    
    return content, changes


def replace_database_imports(import_str: str) -> str:
    """Replace database import names."""
    replacements = {
        'get_duckdb': 'get_postgres',
        'duckdb_execute_write': 'postgres_execute',
        'duckdb_insert': 'postgres_insert',
        'duckdb_update': 'postgres_update',
        'get_trading_engine': '',  # Remove
        'register_connection': '',  # Remove
    }
    
    imports = [i.strip() for i in import_str.split(',')]
    new_imports = []
    
    for imp in imports:
        if imp in replacements:
            if replacements[imp]:  # Only add if not empty string
                new_imports.append(replacements[imp])
        else:
            new_imports.append(imp)
    
    # Add any missing postgres functions
    if 'get_postgres' not in new_imports:
        new_imports.insert(0, 'get_postgres')
    
    return f"from core.database import {', '.join(sorted(set(new_imports)))}"


def migrate_database_calls(content: str) -> Tuple[str, List[str]]:
    """Update database function calls."""
    changes = []
    
    # Replace get_duckdb with get_postgres
    if 'get_duckdb(' in content:
        # Pattern 1: get_duckdb("central", read_only=True)
        content = re.sub(
            r'with get_duckdb\(["\']central["\']\s*,\s*read_only\s*=\s*True\)\s+as\s+(\w+):',
            r'with get_postgres() as conn:\n    with conn.cursor() as \1:',
            content
        )
        
        # Pattern 2: get_duckdb("central") for writes
        content = re.sub(
            r'with get_duckdb\(["\']central["\']\)\s+as\s+(\w+):',
            r'with get_postgres() as \1:',
            content
        )
        
        # Pattern 3: Any other get_duckdb calls
        content = re.sub(
            r'get_duckdb\([^)]+\)',
            'get_postgres()',
            content
        )
        
        changes.append("Replaced: get_duckdb() → get_postgres()")
    
    # Replace duckdb_execute_write with postgres_execute
    if 'duckdb_execute_write' in content:
        content = re.sub(
            r'duckdb_execute_write\(\s*["\']central["\']\s*,',
            'postgres_execute(',
            content
        )
        changes.append("Replaced: duckdb_execute_write() → postgres_execute()")
    
    # Remove get_trading_engine calls
    if 'get_trading_engine()' in content:
        changes.append("WARNING: get_trading_engine() calls need manual review")
    
    return content, changes


def migrate_sql_placeholders(content: str) -> Tuple[str, List[str]]:
    """Change SQL parameter placeholders from ? to %s."""
    changes = []
    
    # Pattern: .execute("...", [...]) with ? placeholders
    pattern = r'\.execute\s*\(\s*(["\'])((?:(?!\1).)*?)\1\s*,\s*\[([^\]]*)\]\s*\)'
    
    def replace_placeholders(match):
        quote = match.group(1)
        sql = match.group(2)
        params = match.group(3)
        
        # Count ? placeholders
        count = sql.count('?')
        if count > 0:
            # Replace ? with %s
            new_sql = sql.replace('?', '%s')
            return f'.execute({quote}{new_sql}{quote}, [{params}])'
        return match.group(0)
    
    new_content = re.sub(pattern, replace_placeholders, content, flags=re.DOTALL)
    
    if new_content != content:
        changes.append(f"Replaced SQL placeholders: ? → %s")
        content = new_content
    
    return content, changes


def migrate_upsert_syntax(content: str) -> Tuple[str, List[str]]:
    """Update DuckDB-specific SQL syntax."""
    changes = []
    
    # INSERT OR REPLACE
    if 'INSERT OR REPLACE' in content:
        # This needs manual review as PostgreSQL syntax is different
        changes.append("WARNING: INSERT OR REPLACE needs manual conversion to ON CONFLICT")
    
    # INSERT OR IGNORE
    if 'INSERT OR IGNORE' in content:
        content = content.replace('INSERT OR IGNORE', 'INSERT ... ON CONFLICT DO NOTHING')
        changes.append("NOTE: INSERT OR IGNORE needs ON CONFLICT clause added manually")
    
    return content, changes


def migrate_file(file_path: Path, dry_run: bool = False) -> Tuple[bool, List[str]]:
    """Migrate a single file."""
    if not file_path.exists():
        return False, [f"File not found: {file_path}"]
    
    print(f"\n{'='*60}")
    print(f"Migrating: {file_path}")
    print(f"{'='*60}")
    
    # Read file
    try:
        content = file_path.read_text(encoding='utf-8')
    except Exception as e:
        return False, [f"Failed to read file: {e}"]
    
    original_content = content
    all_changes = []
    
    # Apply migrations
    content, changes = migrate_imports(content)
    all_changes.extend(changes)
    
    content, changes = migrate_database_calls(content)
    all_changes.extend(changes)
    
    content, changes = migrate_sql_placeholders(content)
    all_changes.extend(changes)
    
    content, changes = migrate_upsert_syntax(content)
    all_changes.extend(changes)
    
    # Print changes
    if all_changes:
        print("\nChanges:")
        for change in all_changes:
            print(f"  - {change}")
    else:
        print("\nNo changes needed.")
        return True, []
    
    # Write file (if not dry run)
    if not dry_run:
        try:
            # Create backup
            backup_path = file_path.with_suffix(file_path.suffix + '.backup')
            backup_path.write_text(original_content, encoding='utf-8')
            print(f"\nBackup created: {backup_path}")
            
            # Write migrated content
            file_path.write_text(content, encoding='utf-8')
            print(f"✓ File updated successfully")
            
            return True, all_changes
        except Exception as e:
            return False, [f"Failed to write file: {e}"]
    else:
        print("\n[DRY RUN] File not modified")
        return True, all_changes


def main():
    """Main migration entry point."""
    if len(sys.argv) < 2:
        print("Usage: python migrate_to_postgres.py <file_path>")
        print("       python migrate_to_postgres.py --all")
        print("       python migrate_to_postgres.py --dry-run <file_path>")
        sys.exit(1)
    
    project_root = Path(__file__).parent
    dry_run = '--dry-run' in sys.argv
    
    if '--all' in sys.argv:
        # Migrate all target files
        print(f"Migrating {len(MIGRATION_TARGETS)} files...")
        
        results = []
        for target in MIGRATION_TARGETS:
            file_path = project_root / target
            success, changes = migrate_file(file_path, dry_run)
            results.append((target, success, len(changes)))
        
        # Summary
        print(f"\n{'='*60}")
        print("Migration Summary")
        print(f"{'='*60}")
        
        for target, success, change_count in results:
            status = "✓" if success else "✗"
            print(f"{status} {target}: {change_count} changes")
        
        total_success = sum(1 for _, success, _ in results if success)
        print(f"\n{total_success}/{len(results)} files migrated successfully")
        
    else:
        # Migrate single file
        file_arg = [arg for arg in sys.argv[1:] if not arg.startswith('--')][0]
        file_path = Path(file_arg) if Path(file_arg).is_absolute() else project_root / file_arg
        
        success, changes = migrate_file(file_path, dry_run)
        
        if not success:
            sys.exit(1)


if __name__ == "__main__":
    main()
