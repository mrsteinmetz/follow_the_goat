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
        # Try utf-8 first, fallback to utf-16 (older Windows format)
        try:
            load_dotenv(env_path, encoding='utf-8')
        except:
            load_dotenv(env_path, encoding='utf-16')
    else:
        load_dotenv()
except ImportError:
    pass  # dotenv not installed


@dataclass
class PostgresSettings:
    """PostgreSQL database connection settings for archive database.
    
    This connects to a LOCAL PostgreSQL for archiving old data.
    The archive database stores data that has expired from DuckDB hot storage.
    """
    # Default to localhost for PostgreSQL
    host: str = "127.0.0.1"
    user: str = "ftg_user"
    password: str = ""  # Set via DB_PASSWORD env var
    database: str = "solcatcher"
    port: int = 5432
    
    @classmethod
    def from_env(cls) -> "PostgresSettings":
        """Load PostgreSQL settings from environment variables."""
        return cls(
            host=os.getenv("DB_HOST", "127.0.0.1"),
            user=os.getenv("DB_USER", "ftg_user"),
            password=os.getenv("DB_PASSWORD", ""),
            database=os.getenv("DB_DATABASE", "solcatcher"),
            port=int(os.getenv("DB_PORT", "5432")),
        )


@dataclass
class Settings:
    """Application settings loaded from environment or defaults."""
    
    # Project paths
    project_root: Path = Path(__file__).parent.parent
    
    # Hot/Cold storage threshold (default for most tables)
    hot_storage_hours: int = 24
    
    # Trades-specific hot storage (72 hours for buyins/trades)
    trades_hot_storage_hours: int = 72
    
    # Scheduler settings
    scheduler_timezone: str = "UTC"
    
    # Logging
    log_level: str = "INFO"
    
    # PostgreSQL settings (for archive database)
    postgres: PostgresSettings = field(default_factory=PostgresSettings.from_env)
    
    # Jupiter API key (required; get free at https://portal.jup.ag)
    jupiter_api_key: str = ""

    # Jupiter Ultra Swap API (recommended; lite-api.jup.ag deprecated Jan 31 2026)
    # https://dev.jup.ag/docs/ultra/get-started
    # GET /ultra/v1/order (quote + unsigned tx), POST /ultra/v1/execute (submit signed tx)
    jupiter_ultra_api_base: str = "https://api.jup.ag"
    
    # DuckDB central database path
    @property
    def central_db_path(self) -> Path:
        return self.project_root / "000data_feeds" / "central.duckdb"
    
    @classmethod
    def from_env(cls) -> "Settings":
        """Load settings from environment variables."""
        return cls(
            hot_storage_hours=int(os.getenv("HOT_STORAGE_HOURS", "24")),
            trades_hot_storage_hours=int(os.getenv("TRADES_HOT_STORAGE_HOURS", "72")),
            scheduler_timezone=os.getenv("SCHEDULER_TZ", "UTC"),
            log_level=os.getenv("LOG_LEVEL", "INFO"),
            postgres=PostgresSettings.from_env(),
            jupiter_api_key=os.getenv("JUPITER_API_KEY", "") or os.getenv("jupiter_api_key", ""),
        )


# Global settings instance
settings = Settings.from_env()

