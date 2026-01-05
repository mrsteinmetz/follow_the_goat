#!/usr/bin/env python3
"""
Pattern Projects Migration Script
==================================
ONE-TIME migration: Fetches pattern_config_projects and pattern_config_filters 
from the old MySQL database and saves them to PostgreSQL (permanent storage).

On master2.py startup, data is automatically loaded from PostgreSQL to DuckDB.

Usage:
    python scripts/migrate_pattern_projects_from_mysql.py
"""

import sys
import json
import logging
from pathlib import Path
from datetime import datetime
from typing import List, Dict, Any

# Add project root to Python path
PROJECT_ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from core.database import get_postgres
import pymysql
import pymysql.cursors

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Old MySQL database credentials
OLD_MYSQL_CONFIG = {
    'host': '116.202.51.115',
    'user': 'solcatcher',
    'password': 'jjJH!la9823JKJsdfjk76jH',
    'database': 'solcatcher',
    'port': 3306,
    'cursorclass': pymysql.cursors.DictCursor,
    'connect_timeout': 10
}

# Backup directory
BACKUP_DIR = PROJECT_ROOT / "backups" / "pattern_projects"
BACKUP_DIR.mkdir(parents=True, exist_ok=True)


def fetch_from_mysql() -> Dict[str, List[Dict[str, Any]]]:
    """Fetch pattern projects and filters from old MySQL database."""
    logger.info("Connecting to old MySQL database...")
    
    try:
        conn = pymysql.connect(**OLD_MYSQL_CONFIG)
        logger.info("✓ Connected to MySQL")
        
        with conn.cursor() as cursor:
            # Fetch projects
            logger.info("Fetching pattern_config_projects...")
            cursor.execute("""
                SELECT id, name, description, created_at, updated_at
                FROM pattern_config_projects
                ORDER BY id
            """)
            projects = cursor.fetchall()
            logger.info(f"✓ Found {len(projects)} projects")
            
            # Fetch filters
            logger.info("Fetching pattern_config_filters...")
            cursor.execute("""
                SELECT id, project_id, name, section, minute, field_name, field_column,
                       from_value, to_value, include_null, exclude_mode, play_id, 
                       is_active, created_at, updated_at
                FROM pattern_config_filters
                ORDER BY id
            """)
            filters = cursor.fetchall()
            logger.info(f"✓ Found {len(filters)} filters")
        
        conn.close()
        
        # Convert datetime objects to strings for JSON serialization
        for project in projects:
            if project.get('created_at'):
                project['created_at'] = project['created_at'].strftime('%Y-%m-%d %H:%M:%S')
            if project.get('updated_at'):
                project['updated_at'] = project['updated_at'].strftime('%Y-%m-%d %H:%M:%S')
        
        for filter_obj in filters:
            if filter_obj.get('created_at'):
                filter_obj['created_at'] = filter_obj['created_at'].strftime('%Y-%m-%d %H:%M:%S')
            if filter_obj.get('updated_at'):
                filter_obj['updated_at'] = filter_obj['updated_at'].strftime('%Y-%m-%d %H:%M:%S')
            # Convert Decimal to float for JSON serialization
            if filter_obj.get('from_value') is not None:
                filter_obj['from_value'] = float(filter_obj['from_value'])
            if filter_obj.get('to_value') is not None:
                filter_obj['to_value'] = float(filter_obj['to_value'])
        
        return {
            'projects': projects,
            'filters': filters
        }
        
    except Exception as e:
        logger.error(f"Failed to fetch from MySQL: {e}")
        raise


def save_json_backup(data: Dict[str, List[Dict[str, Any]]]) -> Path:
    """Save data to JSON backup file."""
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    backup_file = BACKUP_DIR / f"pattern_projects_backup_{timestamp}.json"
    
    with open(backup_file, 'w') as f:
        json.dump(data, f, indent=2, default=str)
    
    logger.info(f"✓ Saved JSON backup: {backup_file}")
    return backup_file


def ensure_postgres_schema():
    """Create pattern config tables in PostgreSQL if they don't exist."""
    logger.info("Ensuring PostgreSQL schema exists...")
    
    with get_postgres() as conn:
        if not conn:
            raise Exception("PostgreSQL connection not available")
        
        with conn.cursor() as cursor:
            # Create projects table
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS pattern_config_projects (
                    id SERIAL PRIMARY KEY,
                    name VARCHAR(255) NOT NULL,
                    description TEXT,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)
            cursor.execute("""
                CREATE INDEX IF NOT EXISTS idx_pattern_projects_name 
                ON pattern_config_projects(name)
            """)
            cursor.execute("""
                CREATE INDEX IF NOT EXISTS idx_pattern_projects_created_at 
                ON pattern_config_projects(created_at)
            """)
            
            # Create filters table
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS pattern_config_filters (
                    id SERIAL PRIMARY KEY,
                    project_id INTEGER,
                    name VARCHAR(255) NOT NULL,
                    section VARCHAR(100),
                    minute SMALLINT,
                    field_name VARCHAR(100) NOT NULL,
                    field_column VARCHAR(100),
                    from_value DECIMAL(20,8),
                    to_value DECIMAL(20,8),
                    include_null SMALLINT DEFAULT 0,
                    exclude_mode SMALLINT DEFAULT 0,
                    play_id INTEGER,
                    is_active SMALLINT DEFAULT 1,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)
            cursor.execute("""
                CREATE INDEX IF NOT EXISTS idx_pattern_filters_project_id 
                ON pattern_config_filters(project_id)
            """)
            cursor.execute("""
                CREATE INDEX IF NOT EXISTS idx_pattern_filters_section_minute 
                ON pattern_config_filters(section, minute)
            """)
            cursor.execute("""
                CREATE INDEX IF NOT EXISTS idx_pattern_filters_is_active 
                ON pattern_config_filters(is_active)
            """)
            
            conn.commit()
    
    logger.info("✓ PostgreSQL schema ready")


def import_to_postgres(data: Dict[str, List[Dict[str, Any]]]):
    """Import projects and filters into PostgreSQL (permanent storage)."""
    projects = data['projects']
    filters = data['filters']
    
    logger.info("Importing to PostgreSQL (permanent storage)...")
    
    with get_postgres() as conn:
        if not conn:
            raise Exception("PostgreSQL connection not available")
        
        with conn.cursor() as cursor:
            # Import projects
            logger.info(f"Importing {len(projects)} projects...")
            for project in projects:
                try:
                    cursor.execute("""
                        INSERT INTO pattern_config_projects 
                        (id, name, description, created_at, updated_at)
                        VALUES (%s, %s, %s, %s, %s)
                        ON CONFLICT (id) DO UPDATE SET
                            name = EXCLUDED.name,
                            description = EXCLUDED.description,
                            updated_at = EXCLUDED.updated_at
                    """, (
                        project['id'],
                        project['name'],
                        project['description'],
                        project['created_at'],
                        project['updated_at']
                    ))
                except Exception as e:
                    logger.warning(f"Failed to import project {project['id']}: {e}")
            
            logger.info(f"✓ Imported {len(projects)} projects")
            
            # Import filters
            logger.info(f"Importing {len(filters)} filters...")
            for filter_obj in filters:
                try:
                    cursor.execute("""
                        INSERT INTO pattern_config_filters 
                        (id, project_id, name, section, minute, field_name, field_column,
                         from_value, to_value, include_null, exclude_mode, play_id,
                         is_active, created_at, updated_at)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                        ON CONFLICT (id) DO UPDATE SET
                            project_id = EXCLUDED.project_id,
                            name = EXCLUDED.name,
                            section = EXCLUDED.section,
                            minute = EXCLUDED.minute,
                            field_name = EXCLUDED.field_name,
                            field_column = EXCLUDED.field_column,
                            from_value = EXCLUDED.from_value,
                            to_value = EXCLUDED.to_value,
                            include_null = EXCLUDED.include_null,
                            exclude_mode = EXCLUDED.exclude_mode,
                            play_id = EXCLUDED.play_id,
                            is_active = EXCLUDED.is_active,
                            updated_at = EXCLUDED.updated_at
                    """, (
                        filter_obj['id'],
                        filter_obj['project_id'],
                        filter_obj['name'],
                        filter_obj['section'],
                        filter_obj['minute'],
                        filter_obj['field_name'],
                        filter_obj['field_column'],
                        filter_obj['from_value'],
                        filter_obj['to_value'],
                        filter_obj['include_null'],
                        filter_obj['exclude_mode'],
                        filter_obj['play_id'],
                        filter_obj['is_active'],
                        filter_obj['created_at'],
                        filter_obj['updated_at']
                    ))
                except Exception as e:
                    logger.warning(f"Failed to import filter {filter_obj['id']}: {e}")
            
            logger.info(f"✓ Imported {len(filters)} filters")
            conn.commit()
            
            # Verify import
            cursor.execute("SELECT COUNT(*) as count FROM pattern_config_projects")
            result = cursor.fetchone()
            project_count = result['count'] if isinstance(result, dict) else result[0]
            
            cursor.execute("SELECT COUNT(*) as count FROM pattern_config_filters")
            result = cursor.fetchone()
            filter_count = result['count'] if isinstance(result, dict) else result[0]
            
            logger.info(f"\nVerification:")
            logger.info(f"  Projects in PostgreSQL: {project_count}")
            logger.info(f"  Filters in PostgreSQL: {filter_count}")


def print_summary(data: Dict[str, List[Dict[str, Any]]]):
    """Print a summary of the migrated data."""
    projects = data['projects']
    filters = data['filters']
    
    logger.info("\n" + "="*80)
    logger.info("MIGRATION SUMMARY")
    logger.info("="*80)
    logger.info(f"Total Projects: {len(projects)}")
    logger.info(f"Total Filters: {len(filters)}")
    
    if projects:
        logger.info("\nProjects:")
        for project in projects:
            filter_count = len([f for f in filters if f['project_id'] == project['id']])
            desc = project['description'] or '(no description)'
            logger.info(f"  • {project['name']} (ID: {project['id']}) - {filter_count} filters")
            logger.info(f"    {desc}")
    
    logger.info("\n" + "="*80)


def main():
    """Main migration function."""
    logger.info("="*80)
    logger.info("PATTERN PROJECTS MIGRATION - MySQL to PostgreSQL")
    logger.info("="*80)
    logger.info("This is a ONE-TIME migration. Data will live in PostgreSQL.")
    logger.info("master2.py will load data from PostgreSQL to DuckDB on startup.")
    logger.info("")
    
    try:
        # Step 1: Fetch from MySQL
        data = fetch_from_mysql()
        
        # Step 2: Save JSON backup
        backup_file = save_json_backup(data)
        
        # Step 3: Ensure PostgreSQL schema exists
        ensure_postgres_schema()
        
        # Step 4: Import to PostgreSQL (permanent storage)
        import_to_postgres(data)
        
        # Step 5: Print summary
        print_summary(data)
        
        logger.info("\n✓ Migration completed successfully!")
        logger.info(f"  JSON backup saved to: {backup_file}")
        logger.info(f"  Data imported to PostgreSQL (permanent storage)")
        logger.info(f"\nNext steps:")
        logger.info(f"  1. MySQL can now be shut down")
        logger.info(f"  2. Restart master2.py to load data from PostgreSQL to DuckDB")
        
        return 0
        
    except Exception as e:
        logger.error(f"\n✗ Migration failed: {e}")
        import traceback
        traceback.print_exc()
        return 1


if __name__ == "__main__":
    sys.exit(main())

