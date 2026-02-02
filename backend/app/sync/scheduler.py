# app/sync/scheduler.py

import logging
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger

from app.sync.service import SyncService
from app.models.database import SessionLocal

logger = logging.getLogger(__name__)

scheduler = BackgroundScheduler()


def sync_all_tables():
    """
    Auto-sync job for all tables
    Runs nightly via scheduler or manually via API
    """
    db = SessionLocal()
    service = SyncService(db)

    tables = ["bsk_master", "deo_master", "service_master", "provision"]

    try:
        logger.info("🌙 Nightly auto-sync started")

        for table in tables:
            logger.info(f"🔄 Syncing {table}")
            service.sync_table(table)

        logger.info("🎉 Nightly auto-sync completed")

    except Exception as e:
        logger.error(f"❌ Auto-sync failed: {e}")
        raise

    finally:
        db.close()


def start_scheduler():
    """
    Start APScheduler
    """
    scheduler.add_job(
        sync_all_tables,
        CronTrigger(hour=2, minute=0),
        id="auto_sync_all",
        replace_existing=True
    )

    scheduler.start()
    logger.info("✅ Scheduler started (runs daily at 2 AM)")


def stop_scheduler():
    """
    Shutdown scheduler gracefully
    """
    scheduler.shutdown()
    logger.info("🛑 Scheduler stopped")
