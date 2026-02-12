# app/sync/scheduler.py

import logging
from datetime import datetime
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger

from app.sync.service import SyncService
from app.models.database import SessionLocal
from app.models import models
from app.utility.training_helper_function import compute_and_cache_recommendations
from app.utility.video_cleanup import (
    cleanup_old_videos,
    analyze_video_storage,
    emergency_cleanup,
)

logger = logging.getLogger(__name__)

scheduler = BackgroundScheduler()

# ============================================================================
# VIDEO CLEANUP CONFIGURATION
# ============================================================================

# Cleanup parameters
VIDEO_CLEANUP_CONFIG = {
    "retention_days": 30,  # Delete videos older than 30 days
    "keep_latest_n": 2,  # Always keep latest 2 versions
    "dry_run": False,  # Set to True for testing
}

# Emergency cleanup threshold (GB)
EMERGENCY_THRESHOLD_GB = 5  # Trigger emergency cleanup if free space < 5GB


def sync_all_tables():
    """
    🌙 NIGHTLY AUTO-SYNC JOB (Enhanced with Checkpoint Tracking)

    Syncs all tables with detailed tracking:
    - Master tables: Full reload (truncate + insert)
    - Provisions: Incremental sync (auto-detects last end_date)

    Features:
    ✅ Per-table error handling (one failure doesn't stop others)
    ✅ Detailed success/failure tracking in checkpoints
    ✅ Auto-incremental provision sync (no manual dates needed)
    ✅ Performance monitoring

    Runs: Every day at 2:00 AM
    """
    db = SessionLocal()
    service = SyncService(db)

    # Track overall sync statistics
    sync_start_time = datetime.now()
    tables_succeeded = []
    tables_failed = []

    try:
        logger.info("=" * 80)
        logger.info("🌙 NIGHTLY AUTO-SYNC STARTED")
        logger.info("=" * 80)
        logger.info(f"⏰ Start Time: {sync_start_time.strftime('%Y-%m-%d %H:%M:%S')}")
        logger.info("")

        # ====================================================================
        # SYNC MASTER TABLES (Full Reload)
        # ====================================================================
        master_tables = ["bsk_master", "deo_master", "service_master"]

        for table in master_tables:
            try:
                logger.info("-" * 80)
                logger.info(f"📊 SYNCING: {table.upper()}")
                logger.info("-" * 80)

                service.sync_master_table(table)
                tables_succeeded.append(table)

                # Display checkpoint summary
                _log_checkpoint_summary(db, table)
                logger.info("")

            except Exception as e:
                logger.error(f"❌ {table} sync FAILED: {e}")
                tables_failed.append(table)
                logger.info("")
                # Continue with next table
                continue

        # ====================================================================
        # SYNC PROVISION TABLE (Incremental)
        # ====================================================================
        try:
            logger.info("-" * 80)
            logger.info("📊 SYNCING: PROVISION (Incremental)")
            logger.info("-" * 80)

            # Auto-incremental: reads last provision_end_date from checkpoint
            service.sync_provisions()
            tables_succeeded.append("provision")

            # Display checkpoint summary
            _log_checkpoint_summary(db, "provision")
            logger.info("")

        except Exception as e:
            logger.error(f"❌ provision sync FAILED: {e}")
            tables_failed.append("provision")
            logger.info("")

        # ====================================================================
        # FINAL SUMMARY
        # ====================================================================
        sync_end_time = datetime.now()
        total_duration = (sync_end_time - sync_start_time).total_seconds()

        logger.info("=" * 80)
        logger.info("🎉 NIGHTLY AUTO-SYNC COMPLETED")
        logger.info("=" * 80)
        logger.info(f"⏰ End Time: {sync_end_time.strftime('%Y-%m-%d %H:%M:%S')}")
        logger.info(
            f"⏱️  Total Duration: {int(total_duration)}s ({total_duration/60:.1f} minutes)"
        )
        logger.info("")
        logger.info(
            f"✅ Succeeded ({len(tables_succeeded)}): {', '.join(tables_succeeded) if tables_succeeded else 'None'}"
        )
        logger.info(
            f"❌ Failed ({len(tables_failed)}): {', '.join(tables_failed) if tables_failed else 'None'}"
        )
        logger.info("=" * 80)

        # Raise exception if any table failed (for monitoring/alerting)
        if tables_failed:
            raise RuntimeError(f"Sync failed for tables: {', '.join(tables_failed)}")

    except Exception as e:
        logger.error(f"❌ NIGHTLY AUTO-SYNC FAILED: {e}")
        raise

    finally:
        db.close()


def _log_checkpoint_summary(db, table_name: str):
    """
    Log summary of checkpoint data for a table

    Shows:
    - Last sync status and counts
    - Date range (for provision)
    - Performance metrics
    """
    try:
        cp = db.query(models.SyncCheckpoint).filter_by(table_name=table_name).first()

        if not cp:
            logger.warning(f"⚠️ No checkpoint found for {table_name}")
            return

        logger.info("📍 Checkpoint Summary:")
        logger.info(f"   Status: {cp.sync_status or 'N/A'}")
        logger.info(f"   Success: {cp.last_sync_success_count or 0:,} records")
        logger.info(f"   Failed: {cp.last_sync_failed_count or 0:,} records")
        logger.info(f"   Duration: {cp.last_sync_duration_seconds or 0}s")

        # Show provision-specific date range
        if (
            table_name == "provision"
            and cp.provision_start_date
            and cp.provision_end_date
        ):
            logger.info(
                f"   Date Range: {cp.provision_start_date} to {cp.provision_end_date}"
            )
            days_synced = (cp.provision_end_date - cp.provision_start_date).days + 1
            logger.info(f"   Days Covered: {days_synced}")

        # Show cumulative stats
        logger.info(f"   Total Synced (All Time): {cp.total_records_synced:,}")
        logger.info(f"   Total Runs: {cp.total_sync_runs or 0}")

        # Show error if any
        if cp.error_message:
            logger.warning(f"   ⚠️ Last Error: {cp.error_message}")

    except Exception as e:
        logger.error(f"❌ Failed to log checkpoint summary: {e}")


def precompute_training_recommendations():
    """
    🚀 WEEKLY TRAINING RECOMMENDATIONS PRECOMPUTE (Optimized)

    This function:
    1. Analyzes provision data from LAST 365 DAYS ONLY (sliding window)
    2. Identifies underperforming BSKs compared to nearby centers
    3. Generates personalized training recommendations
    4. Caches results in TrainingRecommendationCache table

    ✅ OPTIMIZATION:
    - Uses 365-day sliding window (last 1 year of data)
    - Keeps computation time CONSTANT regardless of database age
    - Performance stays consistent even after 5+ years of operation

    Runs: Every Sunday at 3:00 AM (after nightly data sync completes)
    """
    db = SessionLocal()

    # Configuration: Sliding window settings
    LOOKBACK_DAYS = 365  # Analyze last 1 year of provisions

    try:
        logger.info("=" * 80)
        logger.info("📚 WEEKLY TRAINING RECOMMENDATIONS PRECOMPUTE STARTED")
        logger.info("🚀 OPTIMIZATION: Using 365-day sliding window")
        logger.info("=" * 80)

        # Run the OPTIMIZED precomputation with sliding window
        result = compute_and_cache_recommendations(
            db=db,
            n_neighbors=10,  # Compare with 10 nearby BSKs
            top_n_services=10,  # Recommend top 10 services per BSK
            min_provision_threshold=5,  # Minimum provisions to consider
            lookback_days=LOOKBACK_DAYS,  # Only analyze last 365 days
        )

        logger.info("=" * 80)
        logger.info("✅ TRAINING RECOMMENDATIONS PRECOMPUTE COMPLETED")
        logger.info("=" * 80)
        logger.info("📊 Results:")
        logger.info(f"   - BSKs analyzed: {result.get('bsks_analyzed', 'N/A')}")
        logger.info(
            f"   - Provisions processed: {result.get('provisions_processed', 'N/A'):,}"
        )
        logger.info(f"   - Lookback period: {result.get('lookback_days', 'N/A')} days")
        logger.info(f"   - Cutoff date: {result.get('cutoff_date', 'N/A')}")
        logger.info(
            f"   - Recommendations generated: {result.get('recommendations_generated', 'N/A')}"
        )
        logger.info(
            f"   - Computation time: {result.get('computation_time_seconds', 'N/A')}s"
        )
        logger.info(
            f"   - Optimization: {result.get('optimization_note', 'Sliding window used')}"
        )
        logger.info("=" * 80)

        return result

    except Exception as e:
        logger.error("=" * 80)
        logger.error("❌ TRAINING RECOMMENDATIONS PRECOMPUTE FAILED")
        logger.error("=" * 80)
        logger.error(f"Error: {e}")
        import traceback

        traceback.print_exc()
        raise

    finally:
        db.close()


def scheduled_video_cleanup():
    """
    🎥 WEEKLY VIDEO CLEANUP JOB

    This function:
    1. Checks if emergency cleanup is needed (low disk space)
    2. Runs normal cleanup to delete old videos
    3. Logs detailed statistics

    ✅ FEATURES:
    - Removes videos older than configured retention period
    - Always keeps the latest N versions per service
    - Emergency cleanup if disk space is critically low
    - Detailed logging and statistics

    Runs: Every Sunday at 4:00 AM (after training precompute completes)
    """
    logger.info("=" * 80)
    logger.info(f"🎥 WEEKLY VIDEO CLEANUP STARTED: {datetime.now()}")
    logger.info("=" * 80)

    try:
        # Check if emergency cleanup is needed first
        stats = analyze_video_storage()
        free_space_gb = stats.get("free_space_gb", 0)

        if free_space_gb < EMERGENCY_THRESHOLD_GB:
            logger.warning(
                f"🚨 Low disk space detected: {free_space_gb:.2f} GB - "
                f"Running emergency cleanup"
            )
            emergency_cleanup(target_free_gb=EMERGENCY_THRESHOLD_GB * 2)

        # Run normal cleanup
        cleanup_stats = cleanup_old_videos(
            retention_days=VIDEO_CLEANUP_CONFIG["retention_days"],
            keep_latest_n=VIDEO_CLEANUP_CONFIG["keep_latest_n"],
            dry_run=VIDEO_CLEANUP_CONFIG["dry_run"],
        )

        logger.info("=" * 80)
        logger.info("✅ WEEKLY VIDEO CLEANUP COMPLETED")
        logger.info("=" * 80)
        logger.info("📊 Results:")
        logger.info(f"   - Videos deleted: {cleanup_stats['cleanup']['deleted_count']}")
        logger.info(
            f"   - Space freed: {cleanup_stats['cleanup']['space_freed_mb']:.2f} MB"
        )
        logger.info(
            f"   - Videos retained: {cleanup_stats['cleanup']['retained_count']}"
        )
        logger.info(
            f"   - Services cleaned: {cleanup_stats['cleanup']['services_processed']}"
        )
        logger.info("=" * 80)

        return cleanup_stats

    except Exception as e:
        logger.error("=" * 80)
        logger.error("❌ WEEKLY VIDEO CLEANUP FAILED")
        logger.error("=" * 80)
        logger.error(f"Error: {e}")
        import traceback

        traceback.print_exc()
        raise


def scheduled_storage_check():
    """
    💾 DAILY STORAGE MONITORING JOB

    This function:
    1. Analyzes video storage usage
    2. Logs statistics and health metrics
    3. Warns if disk space is getting low

    Runs: Every day at midnight (00:00)
    """
    try:
        stats = analyze_video_storage()

        logger.info("=" * 80)
        logger.info(f"💾 DAILY STORAGE CHECK: {datetime.now()}")
        logger.info("=" * 80)
        logger.info("📊 Storage Statistics:")
        logger.info(f"   - Total videos: {stats['total_videos']}")
        logger.info(
            f"   - Total size: {stats['total_size_mb']:.2f} MB ({stats['total_size_gb']:.2f} GB)"
        )
        logger.info(f"   - Free space: {stats['free_space_gb']:.2f} GB")
        logger.info(f"   - Services with videos: {stats['services_count']}")

        # Check if space is getting low
        if stats["free_space_gb"] < EMERGENCY_THRESHOLD_GB:
            logger.warning(
                f"⚠️ WARNING: Low disk space ({stats['free_space_gb']:.2f} GB). "
                f"Emergency cleanup may be triggered during next weekly cleanup."
            )
        elif stats["free_space_gb"] < EMERGENCY_THRESHOLD_GB * 2:
            logger.warning(
                f"⚠️ NOTICE: Disk space below recommended "
                f"({stats['free_space_gb']:.2f} GB)"
            )
        else:
            logger.info(f"✅ Disk space healthy ({stats['free_space_gb']:.2f} GB)")

        logger.info("=" * 80)

    except Exception as e:
        logger.error(f"❌ Storage check failed: {e}")


def start_scheduler():
    """
    🚀 START APSCHEDULER WITH AUTOMATED JOBS

    Scheduled Jobs:

    1. DAILY STORAGE CHECK (12:00 AM) 💾
       ├── Monitors video storage usage
       ├── Logs storage statistics and health metrics
       ├── Warns if disk space is low
       └── Runs before data sync

    2. DAILY DATA SYNC (2:00 AM) 🌙
       ├── Syncs: bsk_master, deo_master, service_master (full reload)
       ├── Syncs: provision (incremental - auto-detects date range)
       ├── Tracks: Success/failure counts, duration, errors
       └── Features: Per-table error handling, detailed checkpoints

    3. WEEKLY TRAINING RECOMMENDATIONS (Sunday 3:00 AM) 📚
       ├── Precomputes training recommendations for all BSKs
       ├── Uses 365-day sliding window 
       ├── Runs after data sync completes

    4. WEEKLY VIDEO CLEANUP (Sunday 4:00 AM) 🎥
       ├── Deletes videos older than retention period (30 days)
       ├── Always keeps latest N versions per service
       ├── Emergency cleanup if disk space critically low
       └── Runs after training precompute completes

    ✅ ENHANCED FEATURES:
    - Automatic incremental provision syncs (no manual date management)
    - Resilient error handling 
    - Performance monitoring and optimization
    - Automated video storage management
    """

    # ========================================================================
    # JOB 1: DAILY DATA SYNC (2:00 AM)
    # ========================================================================
    scheduler.add_job(
        sync_all_tables,
        CronTrigger(hour=2, minute=0),
        id="auto_sync_all",
        name="Daily Data Sync (Enhanced)",
        replace_existing=True,
    )
    logger.info("✅ Scheduled: Daily Data Sync - Every day at 2:00 AM")
    logger.info("   ├── Master tables: Full reload")
    logger.info("   ├── Provision: Incremental (auto-detects date range)")
    logger.info("   └── Enhanced checkpoint tracking")

    # ========================================================================
    # JOB 2: WEEKLY TRAINING RECOMMENDATIONS (Sunday 3:00 AM)
    # ========================================================================
    scheduler.add_job(
        precompute_training_recommendations,
        CronTrigger(day_of_week="sun", hour=3, minute=0),
        id="weekly_training_precompute",
        name="Weekly Training Recommendations (365-day window)",
        replace_existing=True,
    )
    logger.info("✅ Scheduled: Training Precompute - Every Sunday at 3:00 AM")
    logger.info("   └── Uses 365-day sliding window for constant performance")

    # ========================================================================
    # JOB 3: DAILY STORAGE CHECK (Midnight)
    # ========================================================================
    scheduler.add_job(
        scheduled_storage_check,
        CronTrigger(hour=00, minute=00),
        id="daily_storage_check",
        name="Daily Storage Check",
        replace_existing=True,
    )
    logger.info("✅ Scheduled: Storage Check - Every day at 00:00")
    logger.info("   └── Monitors video storage and logs health metrics")

    # ========================================================================
    # JOB 4: WEEKLY VIDEO CLEANUP (Sunday 4:00 AM)
    # ========================================================================
    scheduler.add_job(
        scheduled_video_cleanup,
        CronTrigger(day_of_week="sun", hour=4, minute=00),
        id="weekly_video_cleanup",
        name="Weekly Video Cleanup",
        replace_existing=True,
    )
    logger.info("✅ Scheduled: Video Cleanup - Every Sunday at 4:00 AM")
    logger.info(
        "   ├── Retention: {} days".format(VIDEO_CLEANUP_CONFIG["retention_days"])
    )
    logger.info(
        "   ├── Keep latest: {} versions".format(VIDEO_CLEANUP_CONFIG["keep_latest_n"])
    )
    logger.info("   └── Emergency threshold: {} GB".format(EMERGENCY_THRESHOLD_GB))

    # ========================================================================
    # START SCHEDULER
    # ========================================================================
    scheduler.start()
    logger.info("=" * 80)
    logger.info("🚀 SCHEDULER STARTED SUCCESSFULLY")
    logger.info("=" * 80)
    logger.info("Active Jobs:")
    logger.info("  1. 💾 Daily Storage Check")
    logger.info("     ├── Time: Every day at 00:00 (Midnight)")
    logger.info("     └── Function: Monitor video storage and disk space")
    logger.info("")
    logger.info("  2. 🌙 Daily Data Sync")
    logger.info("     ├── Time: Every day at 2:00 AM")
    logger.info("     ├── Tables: bsk_master, deo_master, service_master, provision")
    logger.info(
        "     └── Features: Enhanced checkpoints, auto-incremental provision sync"
    )
    logger.info("")
    logger.info("  3. 📚 Weekly Training Recommendations")
    logger.info("     ├── Time: Every Sunday at 3:00 AM")
    logger.info("     ├── Optimization: 365-day sliding window")
    logger.info("     └── Performance: Constant 10-30s regardless of database age")
    logger.info("")
    logger.info("  4. 🎥 Weekly Video Cleanup")
    logger.info("     ├── Time: Every Sunday at 4:00 AM")
    logger.info(
        f"     ├── Retention: {VIDEO_CLEANUP_CONFIG['retention_days']} days (keep latest {VIDEO_CLEANUP_CONFIG['keep_latest_n']} versions)"
    )
    logger.info(
        f"     └── Emergency cleanup: Triggered if < {EMERGENCY_THRESHOLD_GB} GB free"
    )
    logger.info("=" * 80)


def stop_scheduler():
    """
    🛑 Shutdown scheduler gracefully
    """
    scheduler.shutdown()
    logger.info("🛑 Scheduler stopped")


# ============================================================================
# MANUAL TRIGGER FUNCTIONS (for testing or emergency syncs)
# ============================================================================


def trigger_manual_sync(table_name: str = None):
    """
    Manually trigger sync for a specific table or all tables

    Args:
        table_name: Specific table to sync, or None for all tables

    Usage:
        from app.sync.scheduler import trigger_manual_sync

        # Sync all tables
        trigger_manual_sync()

        # Sync specific table
        trigger_manual_sync("provision")
    """
    db = SessionLocal()
    service = SyncService(db)

    try:
        if table_name:
            logger.info(f"🔧 Manual sync triggered for: {table_name}")

            if table_name == "provision":
                service.sync_provisions()
            else:
                service.sync_master_table(table_name)

            _log_checkpoint_summary(db, table_name)
        else:
            logger.info("🔧 Manual sync triggered for: ALL TABLES")
            sync_all_tables()

    finally:
        db.close()


def check_sync_status():
    """
    Check current sync status for all tables

    Usage:
        from app.sync.scheduler import check_sync_status
        check_sync_status()
    """
    db = SessionLocal()

    try:
        logger.info("=" * 80)
        logger.info("📊 SYNC STATUS CHECK")
        logger.info("=" * 80)

        checkpoints = db.query(models.SyncCheckpoint).all()

        for cp in checkpoints:
            logger.info("")
            logger.info(f"📋 Table: {cp.table_name}")
            logger.info(f"   Status: {cp.sync_status or 'N/A'}")
            logger.info(f"   Last Sync: {cp.last_sync_date or 'Never'}")
            logger.info(
                f"   Success: {cp.last_sync_success_count or 0:,} | Failed: {cp.last_sync_failed_count or 0:,}"
            )
            logger.info(
                f"   Total Synced: {cp.total_records_synced:,} (over {cp.total_sync_runs or 0} runs)"
            )

            if cp.table_name == "provision" and cp.provision_end_date:
                logger.info(f"   Last Date: {cp.provision_end_date}")

            if cp.error_message:
                logger.warning(f"   ⚠️ Error: {cp.error_message}")

        logger.info("")
        logger.info("=" * 80)

    finally:
        db.close()
