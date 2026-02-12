# """
# Database service utilities for fetching data for service recommendations.
# This module provides functions to fetch data from the database tables.
# """

# import pandas as pd
# from sqlalchemy.orm import Session
# from typing import Optional, List, Dict
# import sys
# import os

# # Add backend to path for imports
# sys.path.append(
#     os.path.abspath(os.path.join(os.path.dirname(os.path.dirname(__file__)), "backend"))
# )
# sys.path.append(
#     os.path.abspath(os.path.join(os.path.dirname(os.path.dirname(__file__))))
# )

# try:
#     from app.models.database import SessionLocal
#     from app.models.models import ServiceMaster, BSKMaster, DEOMaster, Provision

#     print("✅ Database models imported successfully")
# except ImportError as e:
#     try:
#         # Try alternative import
#         from backend.app.models.database import SessionLocal
#         from backend.app.models.models import (
#             ServiceMaster,
#             BSKMaster,
#             DEOMaster,
#             Provision,
#         )

#         print("✅ Database models imported successfully (alternative path)")
#     except ImportError as e2:
#         print(f"Warning: Could not import database models. Error: {e2}")
#         print(
#             "Make sure you're running from the project root and backend dependencies are installed."
#         )
#         ServiceMaster = BSKMaster = DEOMaster = Provision = SessionLocal = None


# def get_database_session() -> Optional[Session]:
#     """Get a database session."""
#     if SessionLocal is None:
#         print("SessionLocal not available - database models not imported")
#         return None

#     try:
#         return SessionLocal()
#     except Exception as e:
#         print(f"Error creating database session: {e}")
#         return None


# def fetch_services_from_db(include_inactive: bool = False) -> Optional[pd.DataFrame]:
#     """
#     Fetch services data from ServiceMaster table.

#     Args:
#         include_inactive: Whether to include inactive services

#     Returns:
#         DataFrame with service data or None if error
#     """
#     if ServiceMaster is None:
#         print("ServiceMaster model not available")
#         return None

#     db = get_database_session()
#     if db is None:
#         return None

#     try:
#         # Build query
#         query = db.query(ServiceMaster)
#         if not include_inactive:
#             query = query.filter(ServiceMaster.is_active == 1)

#         services = query.all()

#         if not services:
#             print("No services found in database")
#             return None

#         # Convert to DataFrame
#         services_data = []
#         for service in services:
#             services_data.append(
#                 {
#                     "service_id": service.service_id,
#                     "service_name": service.service_name or "",
#                     "service_type": service.service_type or "",
#                     "service_desc": service.service_desc or "",
#                     "common_name": service.common_name or "",
#                     "department_name": service.department_name or "",
#                     "department_id": service.department_id,
#                     "how_to_apply": service.how_to_apply or "",
#                     "eligibility_criteria": service.eligibility_criteria or "",
#                     "required_doc": service.required_doc or "",
#                     "is_active": service.is_active,
#                     "is_paid_service": service.is_paid_service,
#                 }
#             )

#         return pd.DataFrame(services_data)

#     except Exception as e:
#         print(f"Error fetching services: {e}")
#         return None
#     finally:
#         db.close()


# def fetch_bsks_from_db(include_inactive: bool = False) -> Optional[pd.DataFrame]:
#     """
#     Fetch BSK data from BSKMaster table.

#     Args:
#         include_inactive: Whether to include inactive BSKs

#     Returns:
#         DataFrame with BSK data or None if error
#     """
#     if BSKMaster is None:
#         print("BSKMaster model not available")
#         return None

#     db = get_database_session()
#     if db is None:
#         return None

#     try:
#         # Build query
#         query = db.query(BSKMaster)
#         if not include_inactive:
#             query = query.filter(BSKMaster.is_active == True)

#         bsks = query.all()

#         if not bsks:
#             print("No BSKs found in database")
#             return None

#         # Convert to DataFrame
#         bsk_data = []
#         for bsk in bsks:
#             bsk_data.append(
#                 {
#                     "bsk_id": bsk.bsk_id,
#                     "bsk_name": bsk.bsk_name or "",
#                     "bsk_code": bsk.bsk_code or "",
#                     "district_name": bsk.district_name or "",
#                     "district_id": bsk.district_id,
#                     "block_municipalty_name": bsk.block_municipalty_name or "",
#                     "bsk_lat": bsk.bsk_lat,
#                     "bsk_long": bsk.bsk_long,
#                     "bsk_address": bsk.bsk_address or "",
#                     "is_active": bsk.is_active,
#                     "no_of_deos": bsk.no_of_deos,
#                 }
#             )

#         return pd.DataFrame(bsk_data)

#     except Exception as e:
#         print(f"Error fetching BSKs: {e}")
#         return None
#     finally:
#         db.close()


# def fetch_deos_from_db(include_inactive: bool = False) -> Optional[pd.DataFrame]:
#     """
#     Fetch DEO data from DEOMaster table.

#     Args:
#         include_inactive: Whether to include inactive DEOs

#     Returns:
#         DataFrame with DEO data or None if error
#     """
#     if DEOMaster is None:
#         print("DEOMaster model not available")
#         return None

#     db = get_database_session()
#     if db is None:
#         return None

#     try:
#         # Build query
#         query = db.query(DEOMaster)
#         if not include_inactive:
#             query = query.filter(DEOMaster.is_active == True)

#         deos = query.all()

#         if not deos:
#             print("No DEOs found in database")
#             return None

#         # Convert to DataFrame
#         deo_data = []
#         for deo in deos:
#             deo_data.append(
#                 {
#                     "agent_id": deo.agent_id,
#                     "user_name": deo.user_name or "",
#                     "agent_code": deo.agent_code or "",
#                     "agent_email": deo.agent_email or "",
#                     "agent_phone": deo.agent_phone or "",
#                     "bsk_id": deo.bsk_id,
#                     "bsk_name": deo.bsk_name or "",
#                     "date_of_engagement": deo.date_of_engagement or "",
#                     "bsk_post": deo.bsk_post or "",
#                     "is_active": deo.is_active,
#                 }
#             )

#         return pd.DataFrame(deo_data)

#     except Exception as e:
#         print(f"Error fetching DEOs: {e}")
#         return None
#     finally:
#         db.close()


# def fetch_provisions_from_db(limit: Optional[int] = None) -> Optional[pd.DataFrame]:
#     """
#     Fetch provisions data from Provision table.

#     Args:
#         limit: Maximum number of records to fetch (None for all)

#     Returns:
#         DataFrame with provisions data or None if error
#     """
#     if Provision is None:
#         print("Provision model not available")
#         return None

#     db = get_database_session()
#     if db is None:
#         return None

#     try:
#         query = db.query(Provision)
#         if limit:
#             query = query.limit(limit)

#         provisions = query.all()

#         if not provisions:
#             print("No provisions found in database")
#             return None

#         # Convert to DataFrame
#         provision_data = []
#         for provision in provisions:
#             provision_data.append(
#                 {
#                     "bsk_id": provision.bsk_id,
#                     "bsk_name": provision.bsk_name or "",
#                     "customer_id": provision.customer_id,
#                     "customer_name": provision.customer_name or "",
#                     "customer_phone": provision.customer_phone or "",
#                     "service_id": provision.service_id,
#                     "service_name": provision.service_name or "",
#                     "prov_date": provision.prov_date or "",
#                     "docket_no": provision.docket_no or "",
#                 }
#             )

#         return pd.DataFrame(provision_data)

#     except Exception as e:
#         print(f"Error fetching provisions: {e}")
#         return None
#     finally:
#         db.close()


# def fetch_all_data_for_recommendations(
#     include_inactive: bool = False,
# ) -> Dict[str, Optional[pd.DataFrame]]:
#     """
#     Fetch all data needed for service recommendations.

#     Args:
#         include_inactive: Whether to include inactive records

#     Returns:
#         Dictionary with DataFrames for services, bsks, deos, and provisions
#     """
#     print("Fetching all data from database...")

#     data = {
#         "services_df": fetch_services_from_db(include_inactive),
#         "bsk_df": fetch_bsks_from_db(include_inactive),
#         "deos_df": fetch_deos_from_db(include_inactive),
#         "provisions_df": fetch_provisions_from_db(),
#     }

#     # Print summary
#     for key, df in data.items():
#         if df is not None:
#             print(f"Loaded {len(df)} records for {key}")
#         else:
#             print(f"Failed to load {key}")

#     return data


# def test_database_connection() -> bool:
#     """Test if database connection is working."""
#     try:
#         db = get_database_session()
#         if db is None:
#             return False

#         # Try a simple query
#         if ServiceMaster is not None:
#             count = db.query(ServiceMaster).count()
#             print(f"Database connection successful. Found {count} services.")
#             db.close()
#             return True
#         else:
#             print("Database models not available")
#             return False

#     except Exception as e:
#         print(f"Database connection failed: {e}")
#         return False
