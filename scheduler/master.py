"""
Master Scheduler - APScheduler
==============================
Single entry point for all scheduled tasks.
NO .bat files - everything runs through here.

Usage:
    python scheduler/master.py
"""

import sys
from pathlib import Path

# Add project root to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from apscheduler.schedulers.blocking import BlockingScheduler
from apscheduler.triggers.interval import IntervalTrigger
from apscheduler.triggers.cron import CronTrigger
import logging

from core.database import archive_old_data
from core.config import settings

# Configure logging
logging.basicConfig(
    level=getattr(logging, settings.log_level),
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)
logger = logging.getLogger("scheduler")


def create_scheduler() -> BlockingScheduler:
    """Create and configure the scheduler."""
    scheduler = BlockingScheduler(timezone=settings.scheduler_timezone)
    
    # =====================================================
    # CORE MAINTENANCE JOBS
    # =====================================================
    
    # Archive old price data (every hour)
    scheduler.add_job(
        func=lambda: archive_old_data("price_points", "prices", settings.hot_storage_hours),
        trigger=IntervalTrigger(hours=1),
        id="archive_price_points",
        name="Archive price_points older than 24h",
        replace_existing=True,
    )
    
    # =====================================================
    # FEATURE JOBS - Add new features here
    # =====================================================
    
    # Example: Uncomment and modify when adding new features
    # from features.my_feature.main import run as my_feature_run
    # scheduler.add_job(
    #     func=my_feature_run,
    #     trigger=IntervalTrigger(minutes=5),
    #     id="my_feature",
    #     name="My Feature Description",
    #     replace_existing=True,
    # )
    
    return scheduler


def main():
    """Start the scheduler."""
    logger.info("Starting Follow The Goat Scheduler")
    logger.info(f"Timezone: {settings.scheduler_timezone}")
    logger.info(f"Hot storage window: {settings.hot_storage_hours} hours")
    
    scheduler = create_scheduler()
    
    # Log all registered jobs
    jobs = scheduler.get_jobs()
    logger.info(f"Registered {len(jobs)} jobs:")
    for job in jobs:
        logger.info(f"  - {job.id}: {job.name}")
    
    try:
        scheduler.start()
    except (KeyboardInterrupt, SystemExit):
        logger.info("Scheduler stopped")


if __name__ == "__main__":
    main()

