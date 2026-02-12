import os
import logging
import requests
import time
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple
from sqlalchemy.orm import Session
from sqlalchemy import text
from requests.adapters import HTTPAdapter
from urllib3.util.ssl_ import create_urllib3_context

from app.models import models
from app.models.database import engine

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# SSL ADAPTER (legacy govt servers)
# ---------------------------------------------------------------------------
class SSLContextAdapter(HTTPAdapter):
    def init_poolmanager(self, *args, **kwargs):
        context = create_urllib3_context()
        context.load_default_certs()
        context.options |= 0x4  # OP_LEGACY_SERVER_CONNECT
        kwargs['ssl_context'] = context
        return super().init_poolmanager(*args, **kwargs)


# ---------------------------------------------------------------------------
# API CONFIG (POST everywhere)
# ---------------------------------------------------------------------------
class BSKAPIConfig:
    BSK_API_BASE_URL = os.getenv(
        "BSK_API_BASE_URL", "https://bsk.wb.gov.in/aiapi"
    )

    USERNAME = os.getenv("BSK_API_USERNAME")
    PASSWORD = os.getenv("BSK_API_PASSWORD")

    AUTH_URL = f"{BSK_API_BASE_URL}/generate_token"

    ENDPOINTS = {
        "bsk_master": f"{BSK_API_BASE_URL}/api/sync/bsk_master",
        "deo_master": f"{BSK_API_BASE_URL}/api/sync/deo_master",
        "service_master": f"{BSK_API_BASE_URL}/api/sync/service_master",
        "provision": f"{BSK_API_BASE_URL}/api/sync/provision",
    }


# ---------------------------------------------------------------------------
# SYNC SERVICE
# ---------------------------------------------------------------------------
class SyncService:
    def __init__(self, db: Session):
        self.db = db
        self.session = requests.Session()

        adapter = SSLContextAdapter()
        self.session.mount("https://", adapter)
        self.session.mount("http://", adapter)

        self.config = BSKAPIConfig()
        self._is_authenticated = False

        logger.info("🔒 SSL adapter configured")

    # -------------------------------------------------------------------
    # AUTH
    # -------------------------------------------------------------------
    def authenticate(self):
        payload = {
            "username": self.config.USERNAME,
            "password": self.config.PASSWORD,
        }

        response = self.session.post(self.config.AUTH_URL, json=payload, timeout=30)
        response.raise_for_status()
        data = response.json()

        token = data.get("token") or data.get("access_token") or data.get("jwt")
        if not token:
            raise RuntimeError("No JWT received from auth API")

        self.session.headers.update({
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json",
        })

        self._is_authenticated = True
        logger.info("✅ Authenticated")

    def ensure_authenticated(self):
        if not self._is_authenticated:
            self.authenticate()

    # -------------------------------------------------------------------
    # SAFE POST JSON CALL
    # -------------------------------------------------------------------
    def _post_json(self, url: str, payload: Dict) -> Dict:
        response = self.session.post(url, json=payload, timeout=60)
        response.raise_for_status()
        try:
            return response.json()
        except ValueError:
            logger.error("❌ Non-JSON response received")
            logger.error(response.text[:500])
            raise RuntimeError("API did not return JSON")

    # -------------------------------------------------------------------
    # MASTER TABLES (DROP & RELOAD)
    # -------------------------------------------------------------------
    def sync_master_table(self, table_name: str):
        """
        Sync master tables with enhanced checkpoint tracking
        """
        # 📊 START TRACKING
        start_time = time.time()
        self._mark_sync_running(table_name)
        
        try:
            self.ensure_authenticated()
            url = self.config.ENDPOINTS[table_name]

            logger.info(f"🌐 Fetching {table_name}")
            data = self._post_json(url, {})

            records = data.get("data") or data.get("results") or data.get("records") or []
            logger.info(f"📦 Fetched {len(records)} records")

            if not records:
                logger.warning(f"⚠️ No records received for {table_name}")
                duration = int(time.time() - start_time)
                self._update_checkpoint_enhanced(
                    table_name=table_name,
                    success_count=0,
                    failed_count=0,
                    duration_seconds=duration,
                    status='success',
                    error_message='No records to sync'
                )
                return

            # Truncate table before inserting
            logger.info(f"🗑️ Truncating table ml_{table_name}")
            self._truncate_table(table_name)
            
            # Insert records with detailed logging
            inserted, failed = self._bulk_insert_records(table_name, records)
            
            # Calculate duration
            duration = int(time.time() - start_time)
            
            # Determine sync status
            if failed == 0:
                status = 'success'
                error_msg = None
            elif inserted > 0:
                status = 'partial'
                error_msg = f'{failed} out of {len(records)} records failed'
            else:
                status = 'failed'
                error_msg = f'All {failed} records failed to insert'
            
            # Update enhanced checkpoint
            self._update_checkpoint_enhanced(
                table_name=table_name,
                success_count=inserted,
                failed_count=failed,
                duration_seconds=duration,
                status=status,
                error_message=error_msg
            )
            
            # Log final result
            if status == 'success':
                logger.info(f"✅ {table_name} synced successfully: {inserted} inserted in {duration}s")
            elif status == 'partial':
                logger.warning(f"⚠️ {table_name} partially synced: {inserted} inserted, {failed} failed in {duration}s")
            else:
                logger.error(f"❌ {table_name} sync failed: {failed} failed in {duration}s")
                raise RuntimeError(f"Failed to insert any records for {table_name}")
                
        except Exception as e:
            duration = int(time.time() - start_time)
            logger.error(f"❌ {table_name} sync exception: {e}")
            
            # Mark as failed
            self._update_checkpoint_enhanced(
                table_name=table_name,
                success_count=0,
                failed_count=0,
                duration_seconds=duration,
                status='failed',
                error_message=str(e)
            )
            raise

    # -------------------------------------------------------------------
    # PROVISION (2-STEP FLOW: META → PAGINATION) - INSERT ONLY
    # -------------------------------------------------------------------
    def sync_provisions(self, start_date: Optional[str] = None, end_date: Optional[str] = None):
        """
        Sync provisions with enhanced tracking including date ranges
        """
        # 📊 START TRACKING
        start_time = time.time()
        self._mark_sync_running("provision")
        
        try:
            self.ensure_authenticated()
            url = self.config.ENDPOINTS["provision"]

            # ---------------- STEP 1: DETERMINE DATE RANGE ----------------
            if not start_date:
                cp = self.db.query(models.SyncCheckpoint).filter_by(
                    table_name="provision"
                ).first()
                
                # Use provision_end_date + 1 day as start_date for incremental sync
                if cp and cp.provision_end_date:
                    start_date = (cp.provision_end_date + timedelta(days=1)).strftime("%Y-%m-%d")
                else:
                    start_date = (datetime.now() - timedelta(days=30)).strftime("%Y-%m-%d")

            end_date = end_date or datetime.now().strftime("%Y-%m-%d")
            
            logger.info(f"📅 Provision date range: {start_date} to {end_date}")

            # ---------------- STEP 2: META CALL ----------------
            meta_payload = {
                "start_date": start_date,
                "end_date": end_date,
            }

            logger.info(f"🔍 Provision META: {meta_payload}")
            meta = self._post_json(url, meta_payload)

            total_records = meta.get("total_no_of_records", 0)
            logger.info(f"📊 Total provision records: {total_records}")

            if total_records == 0:
                duration = int(time.time() - start_time)
                logger.info("ℹ️ No provision records to sync")
                
                # Still update checkpoint with date range
                self._update_checkpoint_enhanced(
                    table_name="provision",
                    success_count=0,
                    failed_count=0,
                    duration_seconds=duration,
                    status='success',
                    error_message='No records in date range',
                    provision_start_date=start_date,
                    provision_end_date=end_date
                )
                return

            # ---------------- STEP 3: PAGINATION (INSERT ONLY) ----------------
            page_size = 1000
            page = 1
            synced = 0
            total_failed = 0
            errors = []

            while True:
                page_payload = {
                    "start_date": start_date,
                    "end_date": end_date,
                    "Page": page,
                    "Pagesize": page_size,
                }

                try:
                    logger.info(f"📄 Provision page {page} (Records so far: {synced}/{total_records})")
                    data = self._post_json(url, page_payload)
                    records = data.get("records", [])

                    if not records:
                        logger.info("✅ No more provision records")
                        break

                    # INSERT ONLY (no upsert)
                    inserted, failed = self._bulk_insert_records("provision", records)
                    synced += inserted
                    total_failed += failed

                    logger.info(f"   ✓ Page {page}: {inserted} inserted, {failed} failed")
                    page += 1

                except Exception as e:
                    # PAGINATION FAILURE HANDLER
                    error_msg = f"Page {page} failed: {str(e)}"
                    logger.error(f"❌ {error_msg}")
                    errors.append(error_msg)
                    
                    logger.info("➡️ Skipping page and continuing")
                    total_failed += page_size  # Assume all records in page failed
                    page += 1
                    continue

            # ---------------- FINAL STATUS ----------------
            duration = int(time.time() - start_time)
            
            # Determine status
            if total_failed == 0:
                status = 'success'
                error_msg = None
            elif synced > 0:
                status = 'partial'
                error_msg = f'{total_failed} records failed. Errors: {"; ".join(errors[:3])}'
            else:
                status = 'failed'
                error_msg = f'All records failed. Errors: {"; ".join(errors[:3])}'
            
            # Update enhanced checkpoint with provision date range
            self._update_checkpoint_enhanced(
                table_name="provision",
                success_count=synced,
                failed_count=total_failed,
                duration_seconds=duration,
                status=status,
                error_message=error_msg,
                provision_start_date=start_date,
                provision_end_date=end_date
            )
            
            logger.info(f"✅ Provision sync complete: {synced} inserted, {total_failed} failed in {duration}s")
            
        except Exception as e:
            duration = int(time.time() - start_time)
            logger.error(f"❌ Provision sync exception: {e}")
            
            # Mark as failed
            self._update_checkpoint_enhanced(
                table_name="provision",
                success_count=0,
                failed_count=0,
                duration_seconds=duration,
                status='failed',
                error_message=str(e),
                provision_start_date=start_date if 'start_date' in locals() else None,
                provision_end_date=end_date if 'end_date' in locals() else None
            )
            raise

    # -------------------------------------------------------------------
    # DB OPS
    # -------------------------------------------------------------------
    def _truncate_table(self, table: str):
        """Truncate table to start fresh"""
        try:
            with engine.begin() as conn:
                conn.execute(text(f"TRUNCATE TABLE dbo.ml_{table}"))
            logger.info(f"🗑️ Table ml_{table} truncated successfully")
        except Exception as e:
            logger.error(f"❌ Failed to truncate table ml_{table}: {e}")
            raise

    def _bulk_insert_records(self, table: str, records: List[Dict]) -> Tuple[int, int]:
        """
        Insert records with DETAILED ERROR LOGGING
        Returns: (inserted_count, failed_count)
        """
        inserted = 0
        failed = 0
        
        for idx, record in enumerate(records):
            try:
                self._insert_record(table, record)
                inserted += 1
            except Exception as e:
                failed += 1
                # DETAILED ERROR LOGGING (only first 5 failures to avoid log spam)
                if failed <= 5:
                    logger.error(f"❌ Insert failed for record {idx+1}/{len(records)} in {table}")
                    logger.error(f"   Error: {str(e)}")
                    logger.error(f"   Record keys: {list(record.keys())}")
                    sample_values = {k: str(v)[:50] for k, v in list(record.items())[:3]}
                    logger.error(f"   Sample values: {sample_values}")
        
        # Log summary if many failures
        if failed > 5:
            logger.error(f"⚠️ {failed - 5} additional insert failures (not logged in detail)")
        
        return inserted, failed

    def _insert_record(self, table: str, record: Dict):
        """Insert a single record into the database"""
        if not record:
            raise ValueError("Cannot insert empty record")
            
        cols = ",".join(record.keys())
        vals = ",".join(f":{k}" for k in record.keys())
        q = text(f"INSERT INTO dbo.ml_{table} ({cols}) VALUES ({vals})")
        
        with engine.begin() as conn:
            conn.execute(q, record)

    # -------------------------------------------------------------------
    # ENHANCED CHECKPOINT MANAGEMENT
    # -------------------------------------------------------------------
    def _mark_sync_running(self, table: str):
        """Mark sync as running before starting"""
        try:
            cp = self.db.query(models.SyncCheckpoint).filter_by(
                table_name=table
            ).first()

            if not cp:
                cp = models.SyncCheckpoint(
                    table_name=table,
                    total_records_synced=0,
                    total_sync_runs=0,
                    total_failures=0,
                )
                self.db.add(cp)

            cp.sync_status = 'running'
            self.db.commit()
            
        except Exception as e:
            logger.error(f"❌ Failed to mark sync as running for {table}: {e}")
            self.db.rollback()

    def _update_checkpoint_enhanced(
        self,
        table_name: str,
        success_count: int,
        failed_count: int,
        duration_seconds: int,
        status: str,
        error_message: Optional[str] = None,
        provision_start_date: Optional[str] = None,
        provision_end_date: Optional[str] = None
    ):
        """
        Update sync checkpoint with comprehensive tracking
        
        Args:
            table_name: Name of the table
            success_count: Number of successfully inserted records
            failed_count: Number of failed inserts
            duration_seconds: Time taken for sync
            status: 'success', 'partial', 'failed'
            error_message: Error details if any
            provision_start_date: Start date for provision sync (YYYY-MM-DD)
            provision_end_date: End date for provision sync (YYYY-MM-DD)
        """
        try:
            cp = self.db.query(models.SyncCheckpoint).filter_by(
                table_name=table_name
            ).first()

            if not cp:
                cp = models.SyncCheckpoint(
                    table_name=table_name,
                    total_records_synced=0,
                    total_sync_runs=0,
                    total_failures=0,
                    first_sync_date=datetime.now()
                )
                self.db.add(cp)

            # Update last sync statistics
            cp.last_sync_date = datetime.now()
            cp.last_sync_success_count = success_count
            cp.last_sync_failed_count = failed_count
            cp.last_sync_duration_seconds = duration_seconds
            cp.sync_status = status
            cp.error_message = error_message

            # Update cumulative statistics
            cp.total_records_synced += success_count
            cp.total_failures += failed_count
            cp.total_sync_runs += 1

            # Calculate average duration
            if cp.avg_sync_duration_seconds:
                cp.avg_sync_duration_seconds = (
                    (cp.avg_sync_duration_seconds * (cp.total_sync_runs - 1) + duration_seconds)
                    // cp.total_sync_runs
                )
            else:
                cp.avg_sync_duration_seconds = duration_seconds

            # Update last successful sync timestamp (only if status is success)
            if status == 'success':
                cp.last_successful_sync = datetime.now()

            # Update provision-specific date range
            if table_name == "provision" and provision_start_date and provision_end_date:
                cp.provision_start_date = datetime.strptime(provision_start_date, "%Y-%m-%d").date()
                cp.provision_end_date = datetime.strptime(provision_end_date, "%Y-%m-%d").date()

            self.db.commit()
            
            logger.info(
                f"📍 Checkpoint updated: {table_name} | "
                f"Status: {status} | "
                f"Success: {success_count} | "
                f"Failed: {failed_count} | "
                f"Duration: {duration_seconds}s"
            )
            
        except Exception as e:
            logger.error(f"❌ Failed to update checkpoint for {table_name}: {e}")
            self.db.rollback()
            raise