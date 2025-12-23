"""
Configuration Management
========================
Centralized settings for all features.
"""

import os
from pathlib import Path
from dataclasses import dataclass, field
from typing import Optional

# Load .env file if python-dotenv is available
PROJECT_ROOT = Path(__file__).parent.parent
try:
    from dotenv import load_dotenv
    env_path = PROJECT_ROOT / ".env"
    if env_path.exists():
        # Try utf-16 first (common on Windows), fallback to utf-8
        try:
            load_dotenv(env_path, encoding='utf-16')
        except:
            load_dotenv(env_path, encoding='utf-8')
    else:
        load_dotenv()
except ImportError:
    pass  # dotenv not installed


@dataclass
class MySQLSettings:
    """MySQL database connection settings."""
    host: str = "116.202.51.115"
    user: str = "solcatcher"
    password: str = "jjJH!la9823JKJsdfjk76jH"
    database: str = "solcatcher"
    port: int = 3306
    charset: str = "utf8mb4"
    
    @classmethod
    def from_env(cls) -> "MySQLSettings":
        """Load MySQL settings from environment variables."""
        return cls(
            host=os.getenv("DB_HOST", "116.202.51.115"),
            user=os.getenv("DB_USER", "solcatcher"),
            password=os.getenv("DB_PASSWORD", "jjJH!la9823JKJsdfjk76jH"),
            database=os.getenv("DB_DATABASE", "solcatcher"),
            port=int(os.getenv("DB_PORT", "3306")),
            charset=os.getenv("DB_CHARSET", "utf8mb4"),
        )


@dataclass
class Settings:
    """Application settings loaded from environment or defaults."""
    
    # Project paths
    project_root: Path = Path(__file__).parent.parent
    
    # Hot/Cold storage threshold
    hot_storage_hours: int = 24
    
    # Scheduler settings
    scheduler_timezone: str = "UTC"
    
    # Logging
    log_level: str = "INFO"
    
    # MySQL settings
    mysql: MySQLSettings = field(default_factory=MySQLSettings.from_env)
    
    # Jupiter API key (required as of Jan 31, 2026)
    # Get free key at: https://portal.jup.ag
    # Free tier: 60 requests/minute
    jupiter_api_key: str = ""
    
    # DuckDB central database path
    @property
    def central_db_path(self) -> Path:
        return self.project_root / "000data_feeds" / "central.duckdb"
    
    @classmethod
    def from_env(cls) -> "Settings":
        """Load settings from environment variables."""
        return cls(
            hot_storage_hours=int(os.getenv("HOT_STORAGE_HOURS", "24")),
            scheduler_timezone=os.getenv("SCHEDULER_TZ", "UTC"),
            log_level=os.getenv("LOG_LEVEL", "INFO"),
            mysql=MySQLSettings.from_env(),
            jupiter_api_key=os.getenv("JUPITER_API_KEY", "") or os.getenv("jupiter_api_key", ""),
        )


# Global settings instance
settings = Settings.from_env()

