# """
# Enhanced Sync Service with Multiple API Endpoints Support
# """

# import os
# import logging
# import requests
# from datetime import datetime, timedelta
# from typing import Dict, List, Optional, Tuple
# from sqlalchemy.orm import Session
# from sqlalchemy import text

# from app.models import models
# from app.models.database import engine

# logger = logging.getLogger(__name__)

# # ============================================================================
# # API CONFIGURATION - DIFFERENT ENDPOINTS FOR EACH TABLE
# # ============================================================================

# class BSKAPIConfig:
#     """Configuration for BSK Portal APIs - Different endpoint for each table"""
    
#     BASE_URL = os.getenv("BSK_API_BASE_URL", "https://bsk-portal.example.com/api")
    
    
#     # Different API endpoints for each table
#     ENDPOINTS = {
#         "bsk_master": {
#             "url": f"{BASE_URL}/v1/bsk-centers",  # Different API
#             "method": "GET",
#             "needs_auth": True
#         },
#         "deo_master": {
#             "url": f"{BASE_URL}/v1/deo-operators",  # Different API
#             "method": "GET",
#             "needs_auth": True
#         },
#         "service_master": {
#             "url": f"{BASE_URL}/v1/services-catalog",  # Different API
#             "method": "GET",
#             "needs_auth": True
#         },
#         "provision": {
#             "url": f"{BASE_URL}/v1/provisions",  # Different API with pagination
#             "metadata_url": f"{BASE_URL}/v1/provisions/count",  # Metadata endpoint
#             "method": "GET",
#             "needs_auth": True,
#             "supports_pagination": True,
#             "supports_date_range": True
#         }
#     }


# class SyncService:
#     """Enhanced service with multi-API support"""
    
#     def __init__(self, db: Session):
#         self.db = db
#         self.session = requests.Session()
#         self.config = BSKAPIConfig()
#         self.auth_token = None
    
#     # ========================================================================
#     # AUTHENTICATION
#     # ========================================================================
    
#     def authenticate(self) -> bool:
#         """Authenticate with BSK Portal API"""
#         try:
#             auth_url = f"{self.config.BASE_URL}/auth/login"
#             response = self.session.post(auth_url, json={
#                 "username": self.config.USERNAME,
#                 "password": self.config.PASSWORD
#             }, timeout=30)
#             response.raise_for_status()
            
#             data = response.json()
#             self.auth_token = data.get("token") or data.get("access_token")
            
#             if self.auth_token:
#                 self.session.headers.update({
#                     "Authorization": f"Bearer {self.auth_token}",
#                     "X-API-Key": self.config.API_KEY,
#                     "Content-Type": "application/json"
#                 })
#                 logger.info("✅ Successfully authenticated with BSK Portal")
#                 return True
            
#             logger.error("❌ No token received from authentication")
#             return False
            
#         except Exception as e:
#             logger.error(f"❌ Authentication failed: {e}")
#             return False
    
#     # ========================================================================
#     # MASTER TABLES - FULL SYNC (Different APIs for each table)
#     # ========================================================================
    
#     def sync_master_table(self, table_name: str) -> Dict:
#         """
#         Sync small master tables from their specific API endpoint
        
#         Each table has its own API URL defined in BSKAPIConfig.ENDPOINTS
#         """
#         log_entry = self._create_sync_log(table_name, "full")
        
#         try:
#             logger.info(f"📥 Starting full sync for {table_name}")
            
#             # Get table-specific API configuration
#             endpoint_config = self.config.ENDPOINTS.get(table_name)
#             if not endpoint_config:
#                 raise ValueError(f"No API configuration found for {table_name}")
            
#             api_url = endpoint_config["url"]
#             logger.info(f"🌐 Calling API: {api_url}")
            
#             # Fetch data from table-specific API
#             response = self.session.get(api_url, timeout=60)
#             response.raise_for_status()
            
#             data = response.json()
            
#             # Handle different response formats
#             # Some APIs return {"data": [...]}
#             # Others return {"results": [...]}
#             # Adjust based on actual API response
#             records = data.get("data") or data.get("results") or data.get("records") or []
            
#             if not records:
#                 logger.warning(f"⚠️ No records returned from API for {table_name}")
#                 return self._complete_sync_log(log_entry, "completed", 0, 0, 0)
            
#             logger.info(f"✅ Fetched {len(records)} records from {table_name} API")
            
#             # UPSERT to database
#             inserted, updated, failed = self._upsert_records(table_name, records)
            
#             # Update checkpoint
#             self._update_checkpoint(table_name, datetime.now(), inserted + updated)
            
#             logger.info(f"📊 {table_name} sync complete: +{inserted} inserted, ~{updated} updated, ❌{failed} failed")
            
#             return self._complete_sync_log(log_entry, "completed", len(records), inserted, updated, failed)
            
#         except Exception as e:
#             logger.error(f"❌ Sync failed for {table_name}: {e}")
#             return self._complete_sync_log(log_entry, "failed", 0, 0, 0, error=str(e))
    
#     # ========================================================================
#     # PROVISION TABLE - INCREMENTAL PAGINATION SYNC
#     # ========================================================================
    
#     def sync_provisions(
#         self, 
#         start_date: Optional[str] = None,
#         end_date: Optional[str] = None,
#         page_size: int = 1000
#     ) -> Dict:
#         """
#         Sync provision table with pagination from its specific API
        
#         Steps:
#         1. Call metadata API to get total record count
#         2. Calculate total pages needed
#         3. Fetch each page and UPSERT
#         4. Track progress with checkpoints
#         """
        
#         # Determine date range for incremental sync
#         if not start_date:
#             checkpoint = self.db.query(models.SyncCheckpoint).filter(
#                 models.SyncCheckpoint.table_name == "provision"
#             ).first()
            
#             if checkpoint and checkpoint.last_sync_date:
#                 # Continue from last sync date
#                 start_date = checkpoint.last_sync_date.strftime("%Y-%m-%d")
#                 logger.info(f"📅 Resuming from last sync: {start_date}")
#             else:
#                 # Default: Last 30 days for first sync
#                 start_date = (datetime.now() - timedelta(days=30)).strftime("%Y-%m-%d")
#                 logger.info(f"📅 First sync - starting from: {start_date}")
        
#         if not end_date:
#             end_date = datetime.now().strftime("%Y-%m-%d")
        
#         logger.info(f"📅 Syncing provisions: {start_date} → {end_date}")
        
#         log_entry = self._create_sync_log("provision", "incremental", start_date, end_date)
        
#         try:
#             # Get provision API config
#             endpoint_config = self.config.ENDPOINTS["provision"]
#             metadata_url = endpoint_config["metadata_url"]
#             data_url = endpoint_config["url"]
            
#             # STEP 1: Get metadata (total records count)
#             logger.info(f"🌐 Calling metadata API: {metadata_url}")
#             metadata_response = self.session.get(
#                 metadata_url,
#                 params={
#                     "start_date": start_date,
#                     "end_date": end_date
#                 },
#                 timeout=30
#             )
#             metadata_response.raise_for_status()
            
#             metadata = metadata_response.json()
#             total_records = metadata.get("total_records") or metadata.get("count") or metadata.get("total") or 0
            
#             if total_records == 0:
#                 logger.info("ℹ️ No new provisions in date range")
#                 return self._complete_sync_log(log_entry, "completed", 0, 0, 0)
            
#             total_pages = (total_records + page_size - 1) // page_size
#             logger.info(f"📊 Total provisions: {total_records:,} records ({total_pages} pages)")
            
#             # STEP 2: Paginate and sync
#             total_inserted = 0
#             total_updated = 0
#             total_failed = 0
            
#             for page in range(1, total_pages + 1):
#                 logger.info(f"📄 Processing page {page}/{total_pages}")
                
#                 # Fetch page from provision API
#                 logger.info(f"🌐 Calling data API: {data_url} (page={page})")
#                 page_response = self.session.get(
#                     data_url,
#                     params={
#                         "start_date": start_date,
#                         "end_date": end_date,
#                         "page": page,
#                         "page_size": page_size
#                     },
#                     timeout=120  # Longer timeout for large pages
#                 )
#                 page_response.raise_for_status()
                
#                 page_data = page_response.json()
#                 records = page_data.get("data") or page_data.get("results") or page_data.get("records") or []
                
#                 if not records:
#                     logger.warning(f"⚠️ No data returned for page {page}")
#                     continue
                
#                 logger.info(f"✅ Fetched {len(records)} records from page {page}")
                
#                 # UPSERT batch
#                 inserted, updated, failed = self._upsert_records("provision", records)
                
#                 total_inserted += inserted
#                 total_updated += updated
#                 total_failed += failed
                
#                 # Update progress in log
#                 log_entry.page_number = page
#                 log_entry.records_fetched = total_inserted + total_updated + total_failed
#                 self.db.commit()
                
#                 logger.info(f"✅ Page {page}: +{inserted} inserted, ~{updated} updated, ❌{failed} failed")
            
#             # STEP 3: Update checkpoint with end_date
#             self._update_checkpoint(
#                 "provision", 
#                 datetime.strptime(end_date, "%Y-%m-%d"), 
#                 total_inserted + total_updated
#             )
            
#             logger.info(f"🎉 Provision sync complete: Total +{total_inserted} inserted, ~{total_updated} updated")
            
#             return self._complete_sync_log(
#                 log_entry, "completed", 
#                 total_inserted + total_updated + total_failed,
#                 total_inserted, total_updated, total_failed
#             )
            
#         except Exception as e:
#             logger.error(f"❌ Provision sync failed: {e}")
#             return self._complete_sync_log(log_entry, "failed", 0, 0, 0, error=str(e))
    
#     # ========================================================================
#     # DATABASE OPERATIONS
#     # ========================================================================
    
#     def _upsert_records(self, table_name: str, records: List[Dict]) -> Tuple[int, int, int]:
#         """
#         UPSERT records to database
        
#         Returns: (inserted_count, updated_count, failed_count)
#         """
#         inserted = 0
#         updated = 0
#         failed = 0
        
#         # Primary key mapping
#         pk_map = {
#             "bsk_master": "bsk_id",
#             "deo_master": "agent_id",
#             "service_master": "service_id",
#             "provision": "customer_id"  # You might need composite key logic
#         }
        
#         primary_key = pk_map.get(table_name)
        
#         for record in records:
#             try:
#                 # Map API response fields to database columns
#                 db_record = self._map_api_to_db_fields(table_name, record)
                
#                 if not db_record:
#                     failed += 1
#                     continue
                
#                 if primary_key and primary_key in db_record:
#                     # Check if record exists
#                     check_query = text(
#                         f"SELECT COUNT(*) FROM dbo.ml_{table_name} WHERE {primary_key} = :pk"
#                     )
                    
#                     with engine.connect() as conn:
#                         exists = conn.execute(check_query, {"pk": db_record[primary_key]}).scalar() > 0
                    
#                     if exists:
#                         # UPDATE existing record
#                         set_clause = ", ".join([f"{k} = :{k}" for k in db_record.keys() if k != primary_key])
#                         update_query = text(
#                             f"UPDATE dbo.ml_{table_name} SET {set_clause} WHERE {primary_key} = :{primary_key}"
#                         )
                        
#                         with engine.connect() as conn:
#                             conn.execute(update_query, db_record)
#                             conn.commit()
                        
#                         updated += 1
#                     else:
#                         # INSERT new record
#                         self._insert_record(table_name, db_record)
#                         inserted += 1
#                 else:
#                     # No primary key - always insert
#                     self._insert_record(table_name, db_record)
#                     inserted += 1
                    
#             except Exception as e:
#                 logger.error(f"❌ Failed to upsert record: {e}")
#                 failed += 1
#                 continue
        
#         return (inserted, updated, failed)
    
#     def _insert_record(self, table_name: str, record: Dict):
#         """Insert a single record"""
#         columns = ", ".join(record.keys())
#         placeholders = ", ".join([f":{k}" for k in record.keys()])
        
#         insert_query = text(
#             f"INSERT INTO dbo.ml_{table_name} ({columns}) VALUES ({placeholders})"
#         )
        
#         with engine.connect() as conn:
#             conn.execute(insert_query, record)
#             conn.commit()
    
#     def _map_api_to_db_fields(self, table_name: str, api_record: Dict) -> Optional[Dict]:
#         """
#         Map API response fields to database column names
        
#         IMPORTANT: Customize this based on ACTUAL API response format from BSK Portal
#         """
        
#         try:
#             if table_name == "bsk_master":
#                 return {
#                     "bsk_id": api_record.get("id") or api_record.get("bsk_id"),
#                     "bsk_name": api_record.get("name") or api_record.get("bsk_name"),
#                     "bsk_code": api_record.get("code") or api_record.get("bsk_code"),
#                     "bsk_type": api_record.get("type") or api_record.get("bsk_type"),
#                     "district_name": api_record.get("district"),
#                     "bsk_address": api_record.get("address"),
#                     "bsk_lat": api_record.get("latitude"),
#                     "bsk_long": api_record.get("longitude"),
#                     "is_active": api_record.get("is_active", True),
#                     # ... map other fields
#                 }
            
#             elif table_name == "deo_master":
#                 return {
#                     "agent_id": api_record.get("id") or api_record.get("agent_id"),
#                     "user_name": api_record.get("name") or api_record.get("user_name"),
#                     "agent_code": api_record.get("code") or api_record.get("agent_code"),
#                     "agent_email": api_record.get("email"),
#                     "agent_phone": api_record.get("phone"),
#                     "bsk_id": api_record.get("bsk_id"),
#                     "is_active": api_record.get("is_active", True),
#                     # ... map other fields
#                 }
            
#             elif table_name == "service_master":
#                 return {
#                     "service_id": api_record.get("id") or api_record.get("service_id"),
#                     "service_name": api_record.get("name") or api_record.get("service_name"),
#                     "service_type": api_record.get("type") or api_record.get("service_type"),
#                     "department_name": api_record.get("department"),
#                     "service_desc": api_record.get("description"),
#                     "how_to_apply": api_record.get("application_process"),
#                     "is_active": api_record.get("is_active", 1),
#                     # ... map other fields
#                 }
            
#             elif table_name == "provision":
#                 return {
#                     "customer_id": api_record.get("customer_id"),
#                     "bsk_id": api_record.get("bsk_id"),
#                     "bsk_name": api_record.get("bsk_name"),
#                     "customer_name": api_record.get("customer_name"),
#                     "customer_phone": api_record.get("customer_phone") or api_record.get("phone"),
#                     "service_id": api_record.get("service_id"),
#                     "service_name": api_record.get("service_name"),
#                     "prov_date": api_record.get("provision_date") or api_record.get("prov_date"),
#                     "docket_no": api_record.get("docket_number") or api_record.get("docket_no"),
#                 }
            
#             return api_record  # Fallback: return as-is
            
#         except Exception as e:
#             logger.error(f"❌ Failed to map API fields: {e}")
#             return None
    
#     # ========================================================================
#     # LOGGING & CHECKPOINTS
#     # ========================================================================
    
#     def _create_sync_log(
#         self, table_name: str, sync_type: str, 
#         start_date: Optional[str] = None, end_date: Optional[str] = None
#     ) -> models.SyncLog:
#         """Create a new sync log entry"""
#         log = models.SyncLog(
#             table_name=table_name,
#             sync_type=sync_type,
#             start_date=datetime.strptime(start_date, "%Y-%m-%d") if start_date else None,
#             end_date=datetime.strptime(end_date, "%Y-%m-%d") if end_date else None,
#             status="running",
#             triggered_by="cron"
#         )
#         self.db.add(log)
#         self.db.commit()
#         self.db.refresh(log)
#         return log
    
#     def _complete_sync_log(
#         self, log_entry: models.SyncLog, status: str,
#         fetched: int, inserted: int, updated: int, failed: int = 0, error: Optional[str] = None
#     ) -> Dict:
#         """Complete sync log with results"""
#         log_entry.status = status
#         log_entry.records_fetched = fetched
#         log_entry.records_inserted = inserted
#         log_entry.records_updated = updated
#         log_entry.records_failed = failed
#         log_entry.completed_at = datetime.now()
#         log_entry.duration_seconds = (log_entry.completed_at - log_entry.started_at).total_seconds()
#         log_entry.error_message = error
        
#         self.db.commit()
        
#         return {
#             "success": status == "completed",
#             "table_name": log_entry.table_name,
#             "records_fetched": fetched,
#             "records_inserted": inserted,
#             "records_updated": updated,
#             "records_failed": failed,
#             "duration_seconds": log_entry.duration_seconds,
#             "message": f"Sync {'completed successfully' if status == 'completed' else 'failed'}"
#         }
    
#     def _update_checkpoint(self, table_name: str, last_date: datetime, records_synced: int):
#         """Update sync checkpoint"""
#         checkpoint = self.db.query(models.SyncCheckpoint).filter(
#             models.SyncCheckpoint.table_name == table_name
#         ).first()
        
#         if checkpoint:
#             checkpoint.last_sync_date = last_date
#             checkpoint.total_records_synced += records_synced
#             checkpoint.last_successful_sync = datetime.now()
#         else:
#             checkpoint = models.SyncCheckpoint(
#                 table_name=table_name,
#                 last_sync_date=last_date,
#                 total_records_synced=records_synced,
#                 last_successful_sync=datetime.now()
#             )
#             self.db.add(checkpoint)
        
#         self.db.commit()






"""
Ultra-Simple BSK Sync Service
- Single API endpoint pattern: /{table_name}
- Auto-syncs all 4 tables every night
- Timestamp-based incremental for provision
"""

import os
import logging
import requests
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple
from sqlalchemy.orm import Session
from sqlalchemy import text

from app.models import models
from app.models.database import engine

logger = logging.getLogger(__name__)

# ============================================================================
# CONFIG
# ============================================================================

class Config:
    BASE_URL = os.getenv("BSK_API_BASE_URL", "https://bsk-portal.example.com/api")
    PAGE_SIZE = 10000


class SyncService:
    
    def __init__(self, db: Session):
        self.db = db
        self.session = requests.Session()
        self.base_url = Config.BASE_URL
        self.page_size = Config.PAGE_SIZE
    
    # ========================================================================
    # MAIN SYNC METHOD - Handles all 4 tables
    # ========================================================================
    
    def sync_table(self, table_name: str) -> Dict:
        """
        Sync any table
        
        For bsk_master, deo_master, service_master:
          - Simple GET /{table_name}
          - Get all data
          - Push to DB
        
        For provision:
          - Step 1: GET /provision → get total_no_of_records
          - Step 2: Loop with Page/Pagesize params
          - Push to DB
        """
        
        if table_name == "provision":
            return self._sync_provision()
        else:
            return self._sync_master(table_name)
    
    # ========================================================================
    # MASTER TABLES (simple sync)
    # ========================================================================
    
    def _sync_master(self, table_name: str) -> Dict:
        """Sync master table - simple GET and push"""
        
        log = self._create_log(table_name, "full")
        
        try:
            logger.info(f"📥 Syncing {table_name}...")
            
            url = f"{self.base_url}/{table_name}"
            response = self.session.get(url, timeout=60)
            response.raise_for_status()
            
            data = response.json()
            records = data.get("records", [])
            
            if not records:
                logger.warning(f"No data for {table_name}")
                return self._complete_log(log, "completed", 0, 0, 0)
            
            logger.info(f"✅ Got {len(records)} records")
            
            # Push to DB
            inserted, updated, failed = self._upsert(table_name, records)
            
            # Update checkpoint
            self._update_checkpoint(table_name, datetime.now(), inserted + updated)
            
            logger.info(f"✅ {table_name}: +{inserted} new, ~{updated} updated")
            
            return self._complete_log(log, "completed", len(records), inserted, updated, failed)
            
        except Exception as e:
            logger.error(f"❌ {table_name} failed: {e}")
            return self._complete_log(log, "failed", 0, 0, 0, error=str(e))
    
    # ========================================================================
    # PROVISION (two-step paginated sync)
    # ========================================================================
    
    def _sync_provision(self) -> Dict:
        """
        Provision sync with pagination
        
        Step 1: GET /provision → get total count
        Step 2: Loop GET /provision?Page=N&Pagesize=10000
        """
        
        table_name = "provision"
        
        # Get last sync timestamp
        checkpoint = self.db.query(models.SyncCheckpoint).filter(
            models.SyncCheckpoint.table_name == table_name
        ).first()
        
        if checkpoint and checkpoint.last_sync_timestamp:
            start_dt = checkpoint.last_sync_timestamp.isoformat()
            logger.info(f"📅 Resuming from: {start_dt}")
        else:
            start_dt = (datetime.now() - timedelta(days=7)).isoformat()
            logger.info(f"📅 First sync from: {start_dt}")
        
        end_dt = datetime.now().isoformat()
        
        log = self._create_log(table_name, "incremental", start_dt, end_dt)
        
        try:
            logger.info(f"📥 Syncing provision from {start_dt} to {end_dt}")
            
            # ================================================================
            # STEP 1: Get total count
            # ================================================================
            url = f"{self.base_url}/provision"
            params = {
                "start_date": start_dt,
                "end_date": end_dt
            }
            
            response = self.session.get(url, params=params, timeout=60)
            response.raise_for_status()
            
            metadata = response.json()
            total_records = metadata.get("total_no_of_records", 0)
            
            if total_records == 0:
                logger.info("No new provision records")
                return self._complete_log(log, "completed", 0, 0, 0)
            
            logger.info(f"📊 Total: {total_records} records")
            
            # ================================================================
            # STEP 2: Paginate and sync
            # ================================================================
            total_pages = (total_records + self.page_size - 1) // self.page_size
            logger.info(f"📄 Pages: {total_pages}")
            
            total_inserted = 0
            total_updated = 0
            total_failed = 0
            
            for page in range(1, total_pages + 1):
                logger.info(f"📄 Page {page}/{total_pages}")
                
                params = {
                    "start_date": start_dt,
                    "end_date": end_dt,
                    "Page": page,
                    "Pagesize": self.page_size
                }
                
                response = self.session.get(url, params=params, timeout=120)
                response.raise_for_status()
                
                data = response.json()
                
                if not data.get("success"):
                    logger.error(f"❌ Page {page} failed")
                    continue
                
                records = data.get("records", [])
                
                if not records:
                    continue
                
                # Push to DB
                inserted, updated, failed = self._upsert(table_name, records)
                
                total_inserted += inserted
                total_updated += updated
                total_failed += failed
                
                logger.info(f"💾 Page {page}: +{inserted} new, ~{updated} updated")
            
            # Update checkpoint
            self._update_checkpoint(
                table_name,
                datetime.now(),
                total_inserted + total_updated,
                datetime.fromisoformat(end_dt)
            )
            
            logger.info(f"🎉 Provision complete! +{total_inserted} new, ~{total_updated} updated")
            
            return self._complete_log(
                log, "completed",
                total_records, total_inserted, total_updated, total_failed
            )
            
        except Exception as e:
            logger.error(f"❌ Provision failed: {e}")
            return self._complete_log(log, "failed", 0, 0, 0, error=str(e))
    
    # ========================================================================
    # DATABASE OPS
    # ========================================================================
    
    def _upsert(self, table_name: str, records: List[Dict]) -> Tuple[int, int, int]:
        """Insert or update records"""
        
        pk = self._get_pk(table_name)
        
        inserted = 0
        updated = 0
        failed = 0
        
        for record in records:
            try:
                if pk and pk in record:
                    exists = self._exists(table_name, pk, record[pk])
                    
                    if exists:
                        self._update(table_name, pk, record)
                        updated += 1
                    else:
                        self._insert(table_name, record)
                        inserted += 1
                else:
                    self._insert(table_name, record)
                    inserted += 1
                    
            except Exception as e:
                logger.error(f"❌ Record failed: {e}")
                failed += 1
        
        return (inserted, updated, failed)
    
    def _exists(self, table_name: str, pk: str, value) -> bool:
        query = text(f"SELECT COUNT(*) FROM dbo.ml_{table_name} WHERE {pk} = :pk")
        with engine.connect() as conn:
            return conn.execute(query, {"pk": value}).scalar() > 0
    
    def _insert(self, table_name: str, record: Dict):
        cols = ", ".join(record.keys())
        vals = ", ".join([f":{k}" for k in record.keys()])
        query = text(f"INSERT INTO dbo.ml_{table_name} ({cols}) VALUES ({vals})")
        with engine.connect() as conn:
            conn.execute(query, record)
            conn.commit()
    
    def _update(self, table_name: str, pk: str, record: Dict):
        set_clause = ", ".join([f"{k} = :{k}" for k in record.keys() if k != pk])
        query = text(f"UPDATE dbo.ml_{table_name} SET {set_clause} WHERE {pk} = :{pk}")
        with engine.connect() as conn:
            conn.execute(query, record)
            conn.commit()
    
    def _get_pk(self, table_name: str) -> str:
        pks = {
            "bsk_master": "bsk_id",
            "deo_master": "agent_id",
            "service_master": "service_id",
            "provision": "customer_id"
        }
        return pks.get(table_name)
    
    # ========================================================================
    # LOGGING
    # ========================================================================
    
    def _create_log(self, table: str, sync_type: str, start: str = None, end: str = None):
        log = models.SyncLog(
            table_name=table,
            sync_type=sync_type,
            start_datetime=start,
            end_datetime=end,
            status="running",
            triggered_by="auto"
        )
        self.db.add(log)
        self.db.commit()
        self.db.refresh(log)
        return log
    
    def _complete_log(self, log, status: str, fetched: int, inserted: int, 
                     updated: int, failed: int = 0, error: str = None):
        log.status = status
        log.records_fetched = fetched
        log.records_inserted = inserted
        log.records_updated = updated
        log.records_failed = failed
        log.completed_at = datetime.now()
        log.duration_seconds = (log.completed_at - log.started_at).total_seconds()
        log.error_message = error
        self.db.commit()
        
        return {
            "success": status == "completed",
            "table": log.table_name,
            "fetched": fetched,
            "inserted": inserted,
            "updated": updated,
            "failed": failed,
            "duration": log.duration_seconds
        }
    
    def _update_checkpoint(self, table: str, sync_date: datetime, 
                          records: int, last_ts: datetime = None):
        cp = self.db.query(models.SyncCheckpoint).filter(
            models.SyncCheckpoint.table_name == table
        ).first()
        
        if cp:
            cp.last_sync_date = sync_date
            cp.total_records_synced += records
            cp.last_successful_sync = datetime.now()
            if last_ts:
                cp.last_sync_timestamp = last_ts
        else:
            cp = models.SyncCheckpoint(
                table_name=table,
                last_sync_date=sync_date,
                total_records_synced=records,
                last_successful_sync=datetime.now(),
                last_sync_timestamp=last_ts or datetime.now()
            )
            self.db.add(cp)
        
        self.db.commit()