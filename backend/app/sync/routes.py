# app/sync/routes.py

import logging
from fastapi import APIRouter, BackgroundTasks, Depends, HTTPException
from sqlalchemy.orm import Session
import sys
sys.path.append("..")  # Adjust the path as necessary
from app.models.database import get_db
from app.sync.service import SyncService
from app.sync.scheduler import sync_all_tables

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/sync", tags=["Sync"])

VALID_TABLES = [
    "bsk_master",
    "deo_master",
    "service_master",
    "provision"
]


@router.get("/{table_name}")
def sync_table(
    table_name: str,
    background_tasks: BackgroundTasks,
    db: Session = Depends(get_db)
):
    """
    Trigger sync for a single table
    """
    if table_name not in VALID_TABLES:
        raise HTTPException(
            status_code=400,
            detail=f"Invalid table. Choose from {VALID_TABLES}"
        )

    logger.info(f"🚀 Manual sync triggered for {table_name}")

    service = SyncService(db)
    background_tasks.add_task(service.sync_table, table_name)

    return {
        "success": True,
        "message": f"Sync started for {table_name}",
        "table": table_name
    }


@router.post("/all")
def sync_all(background_tasks: BackgroundTasks):
    """
    Trigger sync for all tables
    """
    logger.info("🚀 Manual sync triggered for ALL tables")

    background_tasks.add_task(sync_all_tables)

    return {
        "success": True,
        "message": "All tables syncing in background",
        "tables": VALID_TABLES
    }



# app/sync/status_routes.py

from fastapi import APIRouter, Depends
from sqlalchemy.orm import Session
from sqlalchemy import desc

from app.models.database import get_db
from app.models import models

router = APIRouter(prefix="/sync", tags=["Sync"])


@router.get("/status")
def get_status(
    table_name: str | None = None,
    limit: int = 10,
    db: Session = Depends(get_db)
):
    query = db.query(models.SyncLog).order_by(desc(models.SyncLog.started_at))

    if table_name:
        query = query.filter(models.SyncLog.table_name == table_name)

    logs = query.limit(limit).all()

    return {"logs": [
        {
            "table": log.table_name,
            "status": log.status,
            "type": log.sync_type,
            "started": log.started_at,
            "completed": log.completed_at,
            "duration": log.duration_seconds,
            "inserted": log.records_inserted,
            "updated": log.records_updated,
            "failed": log.records_failed,
            "error": log.error_message
        }
        for log in logs
    ]}


@router.get("/checkpoints")
def get_checkpoints(db: Session = Depends(get_db)):
    cps = db.query(models.SyncCheckpoint).all()

    return {"checkpoints": [
        {
            "table": cp.table_name,
            "last_sync": cp.last_sync_date,
            "last_timestamp": cp.last_sync_timestamp,
            "total_synced": cp.total_records_synced
        }
        for cp in cps
    ]}
