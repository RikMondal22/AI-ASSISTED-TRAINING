"""
Automated Data Synchronization Script

This script runs automatically via:
- Windows Task Scheduler (Windows)
- Cron (Linux/Unix)

Usage:
    python scripts/cron_sync.py --table all
    python scripts/cron_sync.py --table bsk_master
    python scripts/cron_sync.py --table provision
"""

import sys
import os
from pathlib import Path
from datetime import datetime

# Add project root to Python path
project_root = Path(__file__).parent.parent
sys.path.append(str(project_root))

from app.models.database import SessionLocal
from app.services.sync_service import SyncService
import logging

# Setup logging
log_dir = project_root / "logs"
log_dir.mkdir(exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler(log_dir / f"sync_{datetime.now().strftime('%Y%m%d')}.log"),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)


def run_master_tables_sync():
    """
    Sync all master tables (BSK, DEO, Service)
    
    Each table calls its own specific API endpoint
    """
    db = SessionLocal()
    sync_service = SyncService(db)
    
    try:
        logger.info("=" * 80)
        logger.info("🚀 STARTING MASTER TABLES SYNC")
        logger.info("=" * 80)
        
        # Authenticate once
        if not sync_service.authenticate():
            logger.error("❌ Authentication failed - aborting sync")
            return
        
        tables = ["bsk_master", "deo_master", "service_master"]
        results = {}
        
        for table in tables:
            logger.info(f"\n🔄 Syncing {table}...")
            result = sync_service.sync_master_table(table)
            results[table] = result
            
            if result["success"]:
                logger.info(
                    f"✅ {table}: "
                    f"+{result['records_inserted']} inserted, "
                    f"~{result['records_updated']} updated, "
                    f"❌{result['records_failed']} failed"
                )
            else:
                logger.error(f"❌ {table} sync failed: {result.get('message')}")
        
        # Summary
        logger.info("\n" + "=" * 80)
        logger.info("📊 MASTER TABLES SYNC SUMMARY")
        logger.info("=" * 80)
        for table, result in results.items():
            status = "✅ SUCCESS" if result["success"] else "❌ FAILED"
            logger.info(f"{table}: {status} ({result.get('duration_seconds', 0):.2f}s)")
        logger.info("=" * 80)
    
    except Exception as e:
        logger.error(f"❌ Critical error in master sync: {e}")
    finally:
        db.close()


def run_provision_sync():
    """
    Sync provision table (incremental, paginated)
    
    Automatically continues from last checkpoint
    """
    db = SessionLocal()
    sync_service = SyncService(db)
    
    try:
        logger.info("=" * 80)
        logger.info("🚀 STARTING PROVISION SYNC (Incremental)")
        logger.info("=" * 80)
        
        # Authenticate
        if not sync_service.authenticate():
            logger.error("❌ Authentication failed - aborting sync")
            return
        
        # Sync provisions (automatically uses checkpoint for start_date)
        result = sync_service.sync_provisions(page_size=1000)
        
        if result["success"]:
            logger.info("\n" + "=" * 80)
            logger.info("📊 PROVISION SYNC SUMMARY")
            logger.info("=" * 80)
            logger.info(f"✅ Records inserted: {result['records_inserted']:,}")
            logger.info(f"~ Records updated: {result['records_updated']:,}")
            logger.info(f"❌ Records failed: {result['records_failed']:,}")
            logger.info(f"⏱️ Duration: {result['duration_seconds']:.2f}s")
            logger.info("=" * 80)
        else:
            logger.error(f"❌ Provision sync failed: {result.get('message')}")
    
    except Exception as e:
        logger.error(f"❌ Critical error in provision sync: {e}")
    finally:
        db.close()


if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description="BSK Data Sync - Automated Scheduler")
    parser.add_argument(
        "--table", 
        choices=["master", "provision", "all"], 
        default="all",
        help="Which tables to sync: master (BSK/DEO/Service), provision, or all"
    )
    
    args = parser.parse_args()
    
    start_time = datetime.now()
    logger.info(f"\n{'=' * 80}")
    logger.info(f"BSK DATA SYNC - Started at {start_time.strftime('%Y-%m-%d %H:%M:%S')}")
    logger.info(f"{'=' * 80}\n")
    
    if args.table in ["master", "all"]:
        run_master_tables_sync()
    
    if args.table in ["provision", "all"]:
        run_provision_sync()
    
    end_time = datetime.now()
    duration = (end_time - start_time).total_seconds()
    logger.info(f"\n{'=' * 80}")
    logger.info(f"✅ SYNC COMPLETED - Total time: {duration:.2f}s")
    logger.info(f"{'=' * 80}\n")