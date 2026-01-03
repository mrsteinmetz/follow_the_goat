"""
Utility helpers for webhook payload normalization.
"""

from datetime import datetime
from typing import Optional


def parse_timestamp(value: Optional[object]) -> Optional[datetime]:
    """Parse various timestamp inputs into datetime."""
    if value is None:
        return None
    if isinstance(value, datetime):
        return value
    if isinstance(value, (int, float)):
        try:
            return datetime.fromtimestamp(value)
        except Exception:
            return None
    if isinstance(value, str):
        for fmt in ("%Y-%m-%dT%H:%M:%S", "%Y-%m-%d %H:%M:%S"):
            try:
                return datetime.strptime(value.replace("Z", "").replace("T", " "), fmt)
            except Exception:
                continue
        try:
            return datetime.fromisoformat(value.replace("Z", ""))
        except Exception:
            return None
    return None
