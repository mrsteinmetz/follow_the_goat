"""
Configuration Management
========================
Centralized settings for all features.
"""

import os
from pathlib import Path
from dataclasses import dataclass
from typing import Optional


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
    
    @classmethod
    def from_env(cls) -> "Settings":
        """Load settings from environment variables."""
        return cls(
            hot_storage_hours=int(os.getenv("HOT_STORAGE_HOURS", "24")),
            scheduler_timezone=os.getenv("SCHEDULER_TZ", "UTC"),
            log_level=os.getenv("LOG_LEVEL", "INFO"),
        )


# Global settings instance
settings = Settings.from_env()

