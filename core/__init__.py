"""
Core utilities for Follow The Goat project.
"""

from .database import get_postgres, postgres_execute, postgres_query
from .config import settings

__all__ = ["get_postgres", "postgres_execute", "postgres_query", "settings"]

