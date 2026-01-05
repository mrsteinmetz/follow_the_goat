"""
Pattern Projects Loader
========================
Loads pattern_config_projects and pattern_config_filters from PostgreSQL 
into DuckDB on startup.

This is called by master2.py during initialization.
"""

import logging
from typing import List, Dict, Any

logger = logging.getLogger("pattern_loader")


def load_pattern_projects_from_postgres(duckdb_conn) -> bool:
    """
    Load pattern projects and filters from PostgreSQL to DuckDB.
    
    This is called on master2.py startup to populate the in-memory DuckDB
    with pattern configuration data from permanent PostgreSQL storage.
    
    Args:
        duckdb_conn: DuckDB connection to insert data into
        
    Returns:
        True if successful, False otherwise
    """
    from core.database import get_postgres
    
    try:
        logger.info("Loading pattern projects from PostgreSQL...")
        
        with get_postgres() as pg_conn:
            if not pg_conn:
                logger.warning("PostgreSQL not available - skipping pattern projects load")
                return False
            
            with pg_conn.cursor() as cursor:
                # Load projects
                cursor.execute("""
                    SELECT id, name, description, created_at, updated_at
                    FROM pattern_config_projects
                    ORDER BY id
                """)
                projects = cursor.fetchall()
                
                # Load filters
                cursor.execute("""
                    SELECT id, project_id, name, section, minute, field_name, field_column,
                           from_value, to_value, include_null, exclude_mode, play_id,
                           is_active, created_at, updated_at
                    FROM pattern_config_filters
                    ORDER BY id
                """)
                filters = cursor.fetchall()
        
        if not projects and not filters:
            logger.info("No pattern projects found in PostgreSQL")
            return True
        
        # Insert projects into DuckDB
        logger.info(f"Inserting {len(projects)} projects into DuckDB...")
        for project in projects:
            try:
                duckdb_conn.execute("""
                    INSERT INTO pattern_config_projects 
                    (id, name, description, created_at, updated_at)
                    VALUES (?, ?, ?, ?, ?)
                    ON CONFLICT (id) DO UPDATE SET
                        name = EXCLUDED.name,
                        description = EXCLUDED.description,
                        updated_at = EXCLUDED.updated_at
                """, [
                    project['id'],
                    project['name'],
                    project['description'],
                    project['created_at'],
                    project['updated_at']
                ])
            except Exception as e:
                logger.warning(f"Failed to insert project {project['id']}: {e}")
        
        # Insert filters into DuckDB
        logger.info(f"Inserting {len(filters)} filters into DuckDB...")
        for filter_obj in filters:
            try:
                duckdb_conn.execute("""
                    INSERT INTO pattern_config_filters 
                    (id, project_id, name, section, minute, field_name, field_column,
                     from_value, to_value, include_null, exclude_mode, play_id,
                     is_active, created_at, updated_at)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
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
                """, [
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
                ])
            except Exception as e:
                logger.warning(f"Failed to insert filter {filter_obj['id']}: {e}")
        
        logger.info(f"✓ Loaded {len(projects)} projects and {len(filters)} filters from PostgreSQL")
        return True
        
    except Exception as e:
        logger.error(f"Failed to load pattern projects from PostgreSQL: {e}")
        return False

