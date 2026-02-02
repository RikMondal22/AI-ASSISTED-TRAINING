"""
BSK Training Optimization API
A comprehensive FastAPI application for managing and optimizing training programs
for Bangla Sahayata Kendra (BSK) DEO Operators. This API provides endpoints for managing BSK
master data, service catalogs, DEO (Data Entry Operator) information, and provisions.
It also includes AI-powered analytics for identifying underperforming BSKs and
generating training recommendations and video Generation.

Author: AI ASSISTED TRAINING TEAM
Version: 1.0.0
"""

# ============================================================================
# STANDARD LIBRARY IMPORTS
# ============================================================================
import os
import sys
import uuid
import shutil
import logging
from pathlib import Path
from datetime import datetime, timezone
from contextlib import asynccontextmanager
from typing import Optional, List
from fastapi.responses import FileResponse
from fastapi import UploadFile, File, Form, BackgroundTasks, status
from datetime import datetime

# ============================================================================
# THIRD-PARTY IMPORTS
# ============================================================================
import pandas as pd
from dotenv import load_dotenv
from fastapi import (
    FastAPI,
    APIRouter,
    Depends,
    HTTPException,
    Query,
    UploadFile,
    File,
    Form,
    BackgroundTasks,
)
from fastapi.responses import FileResponse, JSONResponse
from fastapi.middleware.cors import CORSMiddleware
from sqlalchemy.orm import Session
from sqlalchemy import desc, func
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger

# ============================================================================
# LOCAL APPLICATION IMPORTS
# ============================================================================
from app.models import models
from app.models import schemas
from app.models.database import engine, get_db
from app.utility.helper_functions import fetch_all_master_data
from app.utility.video_generation_helper import (
    generate_video_from_slides,
    validate_and_match_service,
    get_next_version,
    save_video_to_filesystem,
    cleanup_old_versions,
    get_video_file_path,
)
from app.utility.training_helper_function import (
    enrich_recommendation,
    compute_and_cache_recommendations,
)
from app.sync.scheduler import start_scheduler, stop_scheduler, sync_all_tables

# from app.sync.routes import router as sync_router
# ============================================================================
# EXTERNAL / AI & TRAINING MODULES
# ============================================================================
# Configure module paths
sys.path.append(
    os.path.abspath(os.path.join(os.path.dirname(__file__), "../../ai_service"))
)
from bsk_analytics import find_underperforming_bsks

# ============================================================================
# VIDEO / CONTENT GENERATION UTILITIES
# ============================================================================
from utils.pdf_extractor import extract_raw_content
from services.gemini_service import generate_slides_from_raw, generate_slides_from_form
from utils.service_utils import validate_form_content
from app.sync.service import SyncService
from utils.pdf_validator import validate_pdf_content
# from app.models.schemas import SyncRequest, SyncResponse, SyncStatus
# ============================================================================
# Authentication related imports
# ============================================================================
from app.auth.auth_utils import (
    get_current_user,
    require_superuser,
    authenticate_deo,
    authenticate_superuser,
    create_access_token,
    change_password,
    get_password_hash,
    TokenData,
    UserAuth,
    TokenResponse,
    PasswordChange,
)

# Loading the environment variables
load_dotenv()
# APPLICATION CONFIGURATION

# Setup to print the dubug the modes
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)

# Initialize database tables (idempotent)
logger.info("Initializing database tables...")
models.Base.metadata.create_all(bind=engine)
logger.info("Database initialization complete")


# ============================================================================
# APPLICATION LIFECYCLE MANAGEMENT
# ============================================================================


@asynccontextmanager
async def lifespan(app: FastAPI):
    logger.info("🚀 Application startup")
    start_scheduler()
    yield
    logger.info("🛑 Application shutdown")
    stop_scheduler()


# Register routers
# app.include_router(sync_router)

# Confighguration
UPLOAD_DIR = Path("uploads")

# # Create directories if they don't exist
UPLOAD_DIR.mkdir(exist_ok=True)

# Video storage configuration
VIDEO_BASE_DIR = Path("videos")
VIDEO_BASE_DIR.mkdir(exist_ok=True)
# ============================================================================
# FASTAPI APPLICATION SETUP
# ============================================================================

app = FastAPI(
    title="BSK Training Optimization API",
    description="API for AI-Assisted Training Optimization System for Bangla Sahayata Kendra",
    version="1.0.0",
    docs_url="/docs",
    redoc_url="/redoc",
    lifespan=lifespan,
)

# Configure CORS middleware to allow cross-origin requests
# TODO: In production, replace allow_origins=["*"] with specific domain list
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # SECURITY: Update this in production!
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# ============================================================================
# AUTHENTICATION ENDPOINTS
# ============================================================================


# @app.post("/auth/login", response_model=TokenResponse, tags=["Authentication"])
# def login(auth_request: UserAuth, db: Session = Depends(get_db)):
#     """
#     🔐 **USER LOGIN - Database-Backed Authentication**

#     **Authentication Types:**

#     1. **SUPERUSER (Admin)**
#        - Username: From superusers table
#        - Password: Stored in user_credentials (hashed)
#        - Access: ALL recommendations

#     2. **DEO (Data Entry Operator)**
#        - Username: DEO agent_code (e.g., "DEO001")
#        - Password:
#          * First login: "password123" (default)
#          * After change: User-set password
#        - Access: ONLY assigned BSK

#     **First-Time DEO Login:**
#     - System auto-creates credentials with password "password123"
#     - Must change password after first login

#     **Security Features:**
#     - 🔒 Bcrypt password hashing
#     - 🔒 Account lockout after 5 failed attempts (30 min)
#     - 🔒 Last login tracking
#     - 🔒 Password change enforcement

#     **Example Response:**
#     ```json
#     {
#         "access_token": "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9...",
#         "token_type": "bearer",
#         "role": "deo",
#         "bsk_id": 45,
#         "must_change_password": true
#     }
#     ```
#     """
#     logger.info(f"🔐 Login attempt for user: {auth_request.username}")

#     # Try superuser authentication first
#     user_data = authenticate_superuser(db, auth_request.username, auth_request.password)

#     if user_data:
#         logger.info(f"✅ Superuser authenticated: {auth_request.username}")

#         access_token = create_access_token(
#             data={
#                 "user_id": user_data["user_id"],
#                 "role": user_data["role"],
#                 "username": user_data["username"],
#                 "must_change_password": user_data.get("must_change_password", False),
#             }
#         )

#         return TokenResponse(
#             access_token=access_token,
#             role="superuser",
#             must_change_password=user_data.get("must_change_password", False),
#         )

#     # Try DEO authentication
#     user_data = authenticate_deo(db, auth_request.username, auth_request.password)

#     if user_data:
#         logger.info(
#             f"✅ DEO authenticated: {user_data['agent_code']} (BSK: {user_data['bsk_id']})"
#         )

#         access_token = create_access_token(
#             data={
#                 "user_id": user_data["user_id"],
#                 "role": user_data["role"],
#                 "bsk_id": user_data["bsk_id"],
#                 "agent_code": user_data["agent_code"],
#                 "must_change_password": user_data.get("must_change_password", False),
#             }
#         )

#         return TokenResponse(
#             access_token=access_token,
#             role="deo",
#             bsk_id=user_data["bsk_id"],
#             must_change_password=user_data.get("must_change_password", False),
#         )

#     # Authentication failed
#     logger.warning(f"❌ Login failed for user: {auth_request.username}")
#     raise HTTPException(
#         status_code=status.HTTP_401_UNAUTHORIZED,
#         detail="Incorrect username or password",
#         headers={"WWW-Authenticate": "Bearer"},
#     )


# @app.get("/auth/me", tags=["Authentication"])
# def get_current_user_info(user: TokenData = Depends(get_current_user)):
#     """
#     👤 **GET CURRENT USER INFO**
#     Returns information about the currently authenticated user.
#     """
#     logger.info(f"👤 User info requested: {user.role} (ID: {user.user_id})")

#     response = {
#         "user_id": user.user_id,
#         "role": user.role,
#         "must_change_password": user.must_change_password,
#     }

#     if user.role == "deo":
#         response.update(
#             {
#                 "bsk_id": user.bsk_id,
#                 "agent_code": user.agent_code,
#                 "message": f"You have access to BSK ID: {user.bsk_id}",
#             }
#         )
#     else:
#         response["message"] = "You have access to all recommendations"

#     return response


# @app.post("/auth/change-password", tags=["Authentication"])
# def change_user_password(
#     password_change: PasswordChange,
#     user: TokenData = Depends(get_current_user),
#     db: Session = Depends(get_db),
# ):
#     """
#     🔑 **CHANGE PASSWORD**

#     Allows users to change their password.

#     **Requirements:**
#     - Must provide current password
#     - New password must be at least 8 characters

#     **Request Body:**
#     ```json
#     {
#         "current_password": "password123",
#         "new_password": "MyNewSecurePassword123!"
#     }
#     ```

#     **Use Cases:**
#     - First-time DEO changing from default password
#     - Regular password updates
#     - Security compliance
#     """
#     logger.info(f"🔑 Password change request for {user.role}: {user.user_id}")

#     success = change_password(
#         db=db,
#         user_id=user.user_id,
#         user_type=user.role if user.role == "deo" else "superuser",
#         current_password=password_change.current_password,
#         new_password=password_change.new_password,
#     )

#     if success:
#         return {
#             "success": True,
#             "message": "Password changed successfully. Please login again with your new password.",
#         }

#     raise HTTPException(status_code=500, detail="Password change failed")


# # ============================================================================
# # ADMIN ENDPOINTS - Superuser Management
# # ============================================================================


# @app.post("/admin/create-superuser", tags=["Admin Management"])
# def create_superuser(
#     full_name: str = Form(...),
#     email: str = Form(...),
#     username: str = Form(...),
#     password: str = Form(...),
#     role: str = Form("admin", description="admin, super_admin, analyst"),
#     phone: Optional[str] = Form(None),
#     current_user: TokenData = Depends(require_superuser),
#     db: Session = Depends(get_db),
# ):
#     """
#     👑 **CREATE NEW SUPERUSER (Admin Only)**

#     Only existing superusers can create new superuser accounts.

#     **Fields:**
#     - full_name: Full name
#     - email: Email address (unique)
#     - username: Login username (unique)
#     - password: Initial password (min 8 chars)
#     - role: admin, super_admin, analyst
#     - phone: Optional phone number
#     """
#     # from app.models import models

#     logger.info(f"👑 Creating new superuser: {username} by {current_user.user_id}")

#     # Check if username already exists
#     existing = (
#         db.query(models.UserCredentials)
#         .filter(models.UserCredentials.username == username)
#         .first()
#     )

#     if existing:
#         raise HTTPException(status_code=400, detail="Username already exists")

#     # Check if email already exists
#     existing_email = (
#         db.query(models.Superuser).filter(models.Superuser.email == email).first()
#     )

#     if existing_email:
#         raise HTTPException(status_code=400, detail="Email already exists")

#     # Create superuser profile
#     superuser = models.Superuser(
#         full_name=full_name,
#         email=email,
#         phone=phone,
#         role=role,
#         is_active=True,
#         created_by=current_user.user_id,
#     )
#     db.add(superuser)
#     db.flush()  # Get superuser_id

#     # Create credentials
#     creds = models.UserCredentials(
#         user_id=superuser.superuser_id,
#         user_type="superuser",
#         username=username,
#         password_hash=get_password_hash(password),
#         must_change_password=True,  # Force change on first login
#         is_active=True,
#     )
#     db.add(creds)
#     db.commit()

#     logger.info(f"✅ Superuser created: {username} (ID: {superuser.superuser_id})")

#     return {
#         "success": True,
#         "message": f"Superuser '{username}' created successfully",
#         "superuser_id": superuser.superuser_id,
#         "username": username,
#         "must_change_password": True,
#     }


# @app.get("/admin/list-superusers", tags=["Admin Management"])
# def list_superusers(
#     current_user: TokenData = Depends(require_superuser), db: Session = Depends(get_db)
# ):
#     """
#     **LIST ALL SUPERUSERS (Admin Only)**

#     Returns list of all superuser accounts.
#     """
#     from app.models import models

#     superusers = db.query(models.Superuser).all()

#     return {
#         "total": len(superusers),
#         "superusers": [
#             {
#                 "superuser_id": su.superuser_id,
#                 "full_name": su.full_name,
#                 "email": su.email,
#                 "role": su.role,
#                 "is_active": su.is_active,
#                 "created_at": su.created_at.isoformat() if su.created_at else None,
#             }
#             for su in superusers
#         ],
#     }


# @app.post("/admin/reset-deo-password/{agent_code}", tags=["Admin Management"])
# def reset_deo_password(
#     agent_code: str,
#     new_password: str = Form("password123"),
#     current_user: TokenData = Depends(require_superuser),
#     db: Session = Depends(get_db),
# ):
#     """
#     🔄 **RESET DEO PASSWORD (Admin Only)**

#     Resets a DEO's password to a new value (default: "password123").
#     DEO will be forced to change password on next login.

#     **Use Cases:**
#     - DEO forgot password
#     - Security incident
#     - Account recovery
#     """
#     from app.models import models

#     logger.info(
#         f"🔄 Password reset for DEO: {agent_code} by superuser {current_user.user_id}"
#     )

#     # Find DEO
#     deo = (
#         db.query(models.DEOMaster)
#         .filter(models.DEOMaster.agent_code == agent_code)
#         .first()
#     )

#     if not deo:
#         raise HTTPException(status_code=404, detail=f"DEO not found: {agent_code}")

#     # Get or create credentials
#     creds = (
#         db.query(models.UserCredentials)
#         .filter(
#             models.UserCredentials.user_id == deo.agent_id,
#             models.UserCredentials.user_type == "deo",
#         )
#         .first()
#     )

#     if not creds:
#         # Create new credentials
#         creds = models.UserCredentials(
#             user_id=deo.agent_id,
#             user_type="deo",
#             username=agent_code,
#             password_hash=get_password_hash(new_password),
#             must_change_password=True,
#             is_active=True,
#         )
#         db.add(creds)
#     else:
#         # Reset existing credentials
#         creds.password_hash = get_password_hash(new_password)
#         creds.must_change_password = True
#         creds.failed_login_attempts = 0
#         creds.account_locked_until = None
#         creds.is_active = True

#     db.commit()

#     logger.info(f"✅ Password reset successful for DEO: {agent_code}")

#     return {
#         "success": True,
#         "message": f"Password reset for DEO '{agent_code}'",
#         "new_password": new_password,
#         "must_change_password": True,
#     }


@app.post("/auth/login", response_model=TokenResponse, tags=["Authentication"])
def login(auth_request: UserAuth, db: Session = Depends(get_db)):
    """
    🔐 **USER LOGIN - Database-Backed Authentication**

    **Authentication Types:**

    1. **SUPERUSER (Admin)**
       - Username: From superusers table
       - Password: Stored in user_credentials (hashed)
       - Access: ALL recommendations

    2. **DEO (Data Entry Operator)**
       - Username: DEO agent_email (e.g., "DEO001")
       - Password:
         * First login: "password123" (default)
         * After change: User-set password
       - Access: ONLY assigned BSK

    **First-Time DEO Login:**
    - System auto-creates credentials with password "password123"
    - Must change password after first login

    **Security Features:**
    - 🔒 Bcrypt password hashing
    - 🔒 Account lockout after 5 failed attempts (30 min)
    - 🔒 Last login tracking
    - 🔒 Password change enforcement

    **Example Response:**
    ```json
    {
        "access_token": "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9...",
        "token_type": "bearer",
        "role": "deo",
        "bsk_id": 45,
        "must_change_password": true
    }
    ```
    """
    logger.info(f"🔐 Login attempt for user: {auth_request.username}")

    # Try superuser authentication first
    user_data = authenticate_superuser(db, auth_request.username, auth_request.password)

    if user_data:
        logger.info(f"✅ Superuser authenticated: {auth_request.username}")

        access_token = create_access_token(
            data={
                "user_id": user_data["user_id"],
                "role": user_data["role"],
                "username": user_data["username"],
                "must_change_password": user_data.get("must_change_password", False),
            }
        )

        return TokenResponse(
            access_token=access_token,
            role="superuser",
            must_change_password=user_data.get("must_change_password", False),
        )

    # Try DEO authentication
    user_data = authenticate_deo(db, auth_request.username, auth_request.password)

    if user_data:
        logger.info(
            f"✅ DEO authenticated: {user_data['agent_email']} (BSK: {user_data['bsk_id']})"
        )

        access_token = create_access_token(
            data={
                "user_id": user_data["user_id"],
                "role": user_data["role"],
                "bsk_id": user_data["bsk_id"],
                "agent_email": user_data["agent_email"],
                "must_change_password": user_data.get("must_change_password", False),
            }
        )

        return TokenResponse(
            access_token=access_token,
            role="deo",
            bsk_id=user_data["bsk_id"],
            must_change_password=user_data.get("must_change_password", False),
        )

    # Authentication failed
    logger.warning(f"❌ Login failed for user: {auth_request.username}")
    raise HTTPException(
        status_code=status.HTTP_401_UNAUTHORIZED,
        detail="Incorrect username or password",
        headers={"WWW-Authenticate": "Bearer"},
    )


@app.get("/auth/me", tags=["Authentication"])
def get_current_user_info(user: TokenData = Depends(get_current_user)):
    """
    👤 **GET CURRENT USER INFO**

    Returns information about the currently authenticated user.
    """
    logger.info(f"👤 User info requested: {user.role} (ID: {user.user_id})")

    response = {
        "user_id": user.user_id,
        "role": user.role,
        "must_change_password": user.must_change_password,
    }

    if user.role == "deo":
        response.update(
            {
                "bsk_id": user.bsk_id,
                "agent_email": user.agent_email,
                "message": f"You have access to BSK ID: {user.bsk_id}",
            }
        )
    else:
        response["message"] = "You have access to all recommendations"

    return response


@app.post("/auth/change-password", tags=["Authentication"])
def change_user_password(
    password_change: PasswordChange,
    user: TokenData = Depends(get_current_user),
    db: Session = Depends(get_db),
):
    """
    🔑 **CHANGE PASSWORD**

    Allows users to change their password.

    **Requirements:**
    - Must provide current password
    - New password must be at least 8 characters

    **Request Body:**
    ```json
    {
        "current_password": "password123",
        "new_password": "MyNewSecurePassword123!"
    }
    ```

    **Use Cases:**
    - First-time DEO changing from default password
    - Regular password updates
    - Security compliance
    """
    logger.info(f"🔑 Password change request for {user.role}: {user.user_id}")

    success = change_password(
        db=db,
        user_id=user.user_id,
        user_type=user.role if user.role == "deo" else "superuser",
        current_password=password_change.current_password,
        new_password=password_change.new_password,
    )

    if success:
        return {
            "success": True,
            "message": "Password changed successfully. Please login again with your new password.",
        }

    raise HTTPException(status_code=500, detail="Password change failed")


# ============================================================================
# ADMIN ENDPOINTS - Superuser Management
# ============================================================================


@app.post("/admin/create-superuser", tags=["Admin Management"])
def create_superuser(
    full_name: str = Form(...),
    email: str = Form(...),
    username: str = Form(...),
    password: str = Form(...),
    role: str = Form("admin", description="admin, super_admin, analyst"),
    phone: Optional[str] = Form(None),
    current_user: TokenData = Depends(require_superuser),
    db: Session = Depends(get_db),
):
    """
    👑 **CREATE NEW SUPERUSER (Admin Only)**

    Only existing superusers can create new superuser accounts.

    **Fields:**
    - full_name: Full name
    - email: Email address (unique)
    - username: Login username (unique)
    - password: Initial password (min 8 chars)
    - role: admin, super_admin, analyst
    - phone: Optional phone number
    """
    # from app.models import models

    logger.info(f"👑 Creating new superuser: {username} by {current_user.user_id}")

    # Check if username already exists
    existing = (
        db.query(models.UserCredentials)
        .filter(models.UserCredentials.username == username)
        .first()
    )

    if existing:
        raise HTTPException(status_code=400, detail="Username already exists")

    # Check if email already exists
    existing_email = (
        db.query(models.Superuser).filter(models.Superuser.email == email).first()
    )

    if existing_email:
        raise HTTPException(status_code=400, detail="Email already exists")

    # Create superuser profile
    superuser = models.Superuser(
        full_name=full_name,
        email=email,
        phone=phone,
        role=role,
        is_active=True,
        created_by=current_user.user_id,
    )
    db.add(superuser)
    db.flush()  # Get superuser_id

    # Create credentials
    creds = models.UserCredentials(
        user_id=superuser.superuser_id,
        user_type="superuser",
        username=username,
        password_hash=get_password_hash(password),
        must_change_password=True,  # Force change on first login
        is_active=True,
    )
    db.add(creds)
    db.commit()

    logger.info(f"✅ Superuser created: {username} (ID: {superuser.superuser_id})")

    return {
        "success": True,
        "message": f"Superuser '{username}' created successfully",
        "superuser_id": superuser.superuser_id,
        "username": username,
        "must_change_password": True,
    }


@app.get("/admin/list-superusers", tags=["Admin Management"])
def list_superusers(
    current_user: TokenData = Depends(require_superuser), db: Session = Depends(get_db)
):
    """
    📋 **LIST ALL SUPERUSERS (Admin Only)**

    Returns list of all superuser accounts.
    """
    from app.models import models

    superusers = db.query(models.Superuser).all()

    return {
        "total": len(superusers),
        "superusers": [
            {
                "superuser_id": su.superuser_id,
                "full_name": su.full_name,
                "email": su.email,
                "role": su.role,
                "is_active": su.is_active,
                "created_at": su.created_at.isoformat() if su.created_at else None,
            }
            for su in superusers
        ],
    }


@app.post("/admin/reset-deo-password/{agent_email}", tags=["Admin Management"])
def reset_deo_password(
    agent_email: str,
    new_password: str = Form("password123"),
    current_user: TokenData = Depends(require_superuser),
    db: Session = Depends(get_db),
):
    """
    🔄 **RESET DEO PASSWORD (Admin Only)**

    Resets a DEO's password to a new value (default: "password123").
    DEO will be forced to change password on next login.

    **Use Cases:**
    - DEO forgot password
    - Security incident
    - Account recovery
    """
    from app.models import models

    logger.info(
        f"🔄 Password reset for DEO: {agent_email} by superuser {current_user.user_id}"
    )

    # Find DEO
    deo = (
        db.query(models.DEOMaster)
        .filter(models.DEOMaster.agent_email == agent_email)
        .first()
    )

    if not deo:
        raise HTTPException(status_code=404, detail=f"DEO not found: {agent_email}")

    # Get or create credentials
    creds = (
        db.query(models.UserCredentials)
        .filter(
            models.UserCredentials.user_id == deo.agent_id,
            models.UserCredentials.user_type == "deo",
        )
        .first()
    )

    if not creds:
        # Create new credentials
        creds = models.UserCredentials(
            user_id=deo.agent_id,
            user_type="deo",
            username=agent_email,
            password_hash=get_password_hash(new_password),
            must_change_password=True,
            is_active=True,
        )
        db.add(creds)
    else:
        # Reset existing credentials
        creds.password_hash = get_password_hash(new_password)
        creds.must_change_password = True
        creds.failed_login_attempts = 0
        creds.account_locked_until = None
        creds.is_active = True

    db.commit()

    logger.info(f"✅ Password reset successful for DEO: {agent_email}")

    return {
        "success": True,
        "message": f"Password reset for DEO '{agent_email}'",
        "new_password": new_password,
        "must_change_password": True,
    }



# ============================================================================
# SERVICE TRAINING RECOMMENDATION ENDPOINT WITH AUTH
# ============================================================================


# ENDPOINT 1: Training Recommendation Endpoint
@app.get("/service_training_recommendation/", tags=["Analytics"])
def service_training_recommendation(
    # Authentication (REQUIRED)
    user: TokenData = Depends(get_current_user),
    # Computation control
    precompute: bool = Query(
        False,
        description="If TRUE: Recompute from  provisions (30-180s). If FALSE: Use precomputed data (50-100ms)",
    ),
    # Response format
    summary_only: bool = Query(
        False, description="Return summary statistics instead of full details"
    ),
    # Filters
    district_filter: Optional[str] = Query(None, description="Filter by district name"),
    min_priority: Optional[float] = Query(None, description="Minimum priority score"),
    min_training_services: Optional[int] = Query(
        None, description="Minimum services needing training"
    ),
    db: Session = Depends(get_db),
):
    """
    🔐 **AUTHENTICATED TRAINING RECOMMENDATIONS**
    
    **AUTHENTICATION REQUIRED:**
    - Must include JWT token in Authorization header
    - `Authorization: Bearer <token>`
    
    **ROLE-BASED ACCESS:**
    
    **SUPERUSER:**
    - Sees ALL recommendations across all BSKs
    - Can apply filters (district, priority, etc.)
    - Can trigger precompute
    
    **DEO:**
    - Sees ONLY recommendations for their assigned BSK
    - Filters are ignored (always shows their BSK)
    - Cannot trigger precompute (returns error)
    
    **Response Structure:**
    - Superuser: Full list of all BSKs
    - DEO: Single recommendation for their BSK 
    
    **Example Usage:**
    ```bash
    # 1. Login first
    curl -X POST http://api/auth/login \
      -H "Content-Type: application/json" \
      -d '{"username": "admin", "password": "admin123"}'
    
    # 2. Use token in subsequent requests
    curl -X GET http://api/service_training_recommendation/ \
      -H "Authorization: Bearer <your-token>"
    ```
    """
    logger.info(
        f"🔐 GET /service_training_recommendation/ - "
        f"User: {user.role} (ID: {user.user_id}), "
        f"precompute={precompute}"
    )

    # AUTHORIZATION CHECK: Precompute only for superuser
    if precompute and user.role != "superuser":
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Only superusers can trigger precompute. Please contact admin.",
        )

    # MODE 1: PRECOMPUTE (Superuser only)
    if precompute:
        logger.info("PRECOMPUTE MODE: Starting full computation from scratch...")
        computation_result = compute_and_cache_recommendations(
            db=db, n_neighbors=10, top_n_services=10, min_provision_threshold=5
        )
        logger.info(
            "Precomputation complete, now fetching results from recently completed analysis..."
        )

    # MODE 2: FETCH FROM CACHE

    # Build base query
    base_query = db.query(models.TrainingRecommendationCache)

    # ROLE-BASED FILTERING
    if user.role == "deo":
        # DEO: Force filter to their BSK only
        if user.bsk_id is None:
            raise HTTPException(
                status_code=400,
                detail="DEO user has no BSK assignment. Please contact admin.",
            )

        base_query = base_query.filter(
            models.TrainingRecommendationCache.bsk_id == user.bsk_id
        )

        logger.info(f"DEO mode: Filtering to BSK {user.bsk_id} only")

    else:  # Superuser
        # Apply optional filters for super user
        if district_filter:
            bsks_in_district = (
                db.query(models.BSKMaster.bsk_id)
                .filter(models.BSKMaster.district_name.ilike(f"%{district_filter}%"))
                .all()
            )
            bsk_ids = [b[0] for b in bsks_in_district]
            base_query = base_query.filter(
                models.TrainingRecommendationCache.bsk_id.in_(bsk_ids)
            )
            logger.info(
                f"Filtering by district: {district_filter} ({len(bsk_ids)} BSKs)"
            )

        if min_priority is not None:
            base_query = base_query.filter(
                models.TrainingRecommendationCache.priority_score >= min_priority
            )

        if min_training_services is not None:
            base_query = base_query.filter(
                models.TrainingRecommendationCache.total_training_services
                >= min_training_services
            )

    # Get total count
    total_count = base_query.count()

    if total_count == 0:
        return {
            "status": "success",
            "message": (
                "No recommendations found matching your filters."
                if user.role == "superuser"
                else "No recommendations found for your BSK."
            ),
            "user_role": user.role,
            "recommendations": [],
        }

    # Fetch all results (no pagination)
    precomp_res = base_query.order_by(
        desc(models.TrainingRecommendationCache.priority_score)
    ).all()

    logger.info(f"Retrieved {len(precomp_res)} recommendations for {user.role}")

    # Enrich with master table data
    recommendations = [enrich_recommendation(rec, db) for rec in precomp_res]

    # Get cache timestamp
    cache_timestamp = precomp_res[0].timestamp.isoformat() if precomp_res else None

    # SUMMARY MODE
    if summary_only:
        all_matching = base_query.all()

        return {
            "status": "success",
            "user_role": user.role,
            "summary": {
                "total_bsks_needing_training": total_count,
                "total_provisions": sum(r.total_provisions for r in all_matching),
                "total_training_gaps": sum(
                    r.total_training_services for r in all_matching
                ),
                "avg_priority_score": (
                    round(
                        sum(r.priority_score for r in all_matching) / len(all_matching),
                        2,
                    )
                    if all_matching
                    else 0
                ),
                "highest_priority_score": (
                    round(max(r.priority_score for r in all_matching), 2)
                    if all_matching
                    else 0
                ),
            },
            "top_10_bsks": [
                {
                    "bsk_id": r["bsk_id"],
                    "bsk_name": r["bsk_name"],
                    "priority_score": r["priority_score"],
                    "total_training_services": r["total_training_services"],
                }
                for r in recommendations[:10]
            ],
            "cache_info": {
                "last_updated": cache_timestamp,
                "using_cached_data": not precompute,
            },
        }

    # FULL RESPONSE
    response = {
        "status": "success",
        "user_role": user.role,
        "total_recommendations": total_count,
        "recommendations": recommendations,
        "cache_info": {
            "last_updated": cache_timestamp,
            "using_cached_data": not precompute,
        },
    }

    # Add DEO-specific info
    if user.role == "deo":
        response["deo_info"] = {
            "agent_email": user.agent_email,
            "assigned_bsk_id": user.bsk_id,
            "message": f"Showing recommendations for your assigned BSK only (ID: {user.bsk_id})",
        }

    # Add computation result if precompute was run
    if precompute:
        response["cache_info"]["computation_result"] = computation_result

    return response


# ENDPOINT 2: Training Recommendation Status Endpoint
@app.get("/training_recommendation_status/", tags=["Analytics"])
def get_cache_status(db: Session = Depends(get_db)):
    """
    Check current stored status and find details about it.
    Shows:
    - When precompute=True was last computed
    - How long computation took
    - Number of total precompute=True recommendations
    """
    # Get last successful computation whose status is completed
    last_comp = (
        db.query(models.RecommendationComputationLog)
        .filter(models.RecommendationComputationLog.status == "completed")
        .order_by(desc(models.RecommendationComputationLog.computation_timestamp))
        .first()
    )

    # Count cached recommendations
    total_cached = db.query(models.TrainingRecommendationCache).count()
    if last_comp and last_comp.computation_timestamp:
        # Use timezone-aware datetime for comparison
        now = (
            datetime.now(timezone.utc)
            if last_comp.computation_timestamp.tzinfo
            else datetime.now()
        )
        cache_age_hours = (now - last_comp.computation_timestamp).total_seconds() / 3600
        # is_stale = cache_age_hours > 24  # Consider stale if older than 24 hours
        return {
            "last_computation": {
                "timestamp": last_comp.computation_timestamp.isoformat(),
                "duration_seconds": (
                    round(last_comp.computation_duration_seconds, 2)
                    if last_comp.computation_duration_seconds
                    else None
                ),
                "bsks_analyzed": last_comp.total_bsks_analyzed,
                "provisions_processed": last_comp.total_provisions_processed,
                "recommendations_generated": last_comp.total_recommendations_generated,
            },
            "details": {
                "total_recommendations": total_cached,
                "age_hours": round(cache_age_hours, 1),
            },
        }
    # case where no valid computation exists
    return {
        "cache_status": "empty",
        "last_computation": None,
        "cache": {
            "total_recommendations": total_cached,
            "age_hours": None,
            "is_stale": None,
        },
        "message": "No cache found. Run /service_training_recommendation/?precompute=true to generate recommendations.",
    }


# ENDPOINT 3: Training Recommendation all history logs Endpoint
@app.get("/training_recommendation_history/", tags=["Analytics"])
def get_computation_history(
    limit: int = Query(10, ge=1, description="Number of logs to return"),
    db: Session = Depends(get_db),
):
    """
    Get history of all computation runs.
    Useful for monitoring precompute=True refresh patterns and troubleshooting failures.
    """
    logs = (
        db.query(models.RecommendationComputationLog)
        .order_by(desc(models.RecommendationComputationLog.computation_timestamp))
        .limit(limit)
        .all()
    )

    return {
        "history": [
            {
                "log_id": log.log_id,
                "timestamp": log.computation_timestamp.isoformat(),
                "status": log.status,
                "duration_seconds": (
                    round(log.computation_duration_seconds, 2)
                    if log.computation_duration_seconds
                    else None
                ),
                "bsks_analyzed": log.total_bsks_analyzed,
                "provisions_processed": log.total_provisions_processed,
                "recommendations_generated": log.total_recommendations_generated,
                "triggered_by": log.triggered_by,
                "error": log.error_message if log.status == "failed" else None,
            }
            for log in logs
        ],
        "total_logs": len(logs),
    }


# ENDPOINT 4: Underperforming BSKs Endpoint
@app.get("/underperforming_bsks/", tags=["Analytics"])
def get_underperforming_bsks(
    num_bsks: int = Query(50, description="Number of BSKs to return"),
    sort_order: str = Query(
        "asc", pattern="^(asc|desc)$", description="Sort order: 'asc' or 'desc'"
    ),
    db: Session = Depends(get_db),
):
    """
    Identify and retrieve underperforming BSKs based on AI analytics.

    This endpoint uses machine learning algorithms to analyze BSK performance
    across multiple dimensions (provision volume, service diversity, efficiency, etc.)
    and returns a ranked list of underperforming BSKs that require attention.

    Args:
        num_bsks: Number of BSKs to return (1-1000)
        sort_order: Sort order for results - 'asc' (lowest performing first) or 'desc'
        db: Database session dependency

    Returns:
        List[dict]: Ranked list of underperforming BSKs with performance scores

    Example Response:
        [
            {
                "bsk_id": 123,
                "bsk_name": "BSK Branch Name",
                "score": 45.2,
                "metrics": {...}
            },
            ...
        ]
    """
    logger.info(
        f"GET /underperforming_bsks/ - Analyzing with num_bsks={num_bsks}, "
        f"sort_order={sort_order}"
    )

    # Fetch all master data from database
    bsks_df, provisions_df, deos_df, services_df = fetch_all_master_data(db)

    # Execute AI analytics to identify underperforming BSKs
    logger.info("Running underperformance analysis...")
    result_df = find_underperforming_bsks(bsks_df, provisions_df, deos_df, services_df)

    # Sort results by performance score
    ascending = sort_order == "asc"
    result_df = result_df.sort_values(by="score", ascending=ascending).head(num_bsks)

    logger.info(f"Analysis complete. Returning {len(result_df)} underperforming BSKs")

    # Convert DataFrame to list of dictionaries for JSON response
    return result_df.to_dict(orient="records")


# ============================================================================
# VIDEO GENERATION Endpoints
# ============================================================================
# ENDPOINT 1: Generate Video from Form
@app.post("/bsk_portal/generate_video_from_form/", tags=["BSK Portal Integration"])
async def bsk_generate_video_from_form(
    service_name: str = Form(
        ..., description="Service name (must match service_master)"
    ),
    service_description: str = Form(
        ..., description="Brief description of the service"
    ),
    how_to_apply: str = Form(..., description="Step-by-step application process"),
    eligibility_criteria: str = Form(..., description="Who can apply for this service"),
    required_documents: str = Form(..., description="List of required documents"),
    fees_and_timeline: Optional[str] = Form(
        None, description="Fees and processing time"
    ),
    operator_tips: Optional[str] = Form(None, description="Tips for BSK operators"),
    troubleshooting: Optional[str] = Form(
        None, description="Common issues and solutions"
    ),
    service_link: Optional[str] = Form(
        None, description="Official service website URL"
    ),
    db: Session = Depends(get_db),
):
    """
    🎥 **GENERATE VIDEO FROM FORM WITH AI ENHANCEMENT**

    NEW Process:
    1. Validates required form fields (5 mandatory fields)
    2. Validates service name against service_master
    3. **USES GEMINI AI to enhance and structure content into professional slides**
    4. Generates video IN-MEMORY from AI-enhanced slides
    5. Saves video to local filesystem
    6. Returns video URL and metadata

    Required Fields:
    - service_name
    - service_description
    - how_to_apply
    - eligibility_criteria
    - required_documents

    Optional Fields (will be included in AI processing if provided):
    - fees_and_timeline
    - operator_tips
    - troubleshooting
    - service_link
    """

    logger.info("=" * 80)
    logger.info("🎬 BSK PORTAL - AI-ENHANCED VIDEO GENERATION FROM FORM")
    logger.info("=" * 80)

    try:
        # ================================================================
        # STEP 1: Build service content dictionary
        # ================================================================
        service_content = {
            "service_name": service_name.strip(),
            "service_description": service_description.strip(),
            "how_to_apply": how_to_apply.strip(),
            "eligibility_criteria": eligibility_criteria.strip(),
            "required_docs": required_documents.strip(),
            "fees_and_timeline": fees_and_timeline.strip() if fees_and_timeline else "",
            "operator_tips": operator_tips.strip() if operator_tips else "",
            "troubleshooting": troubleshooting.strip() if troubleshooting else "",
            "service_link": service_link.strip() if service_link else "",
        }

        # ================================================================
        # STEP 2: Validate required fields
        # ================================================================
        logger.info("🔍 Validating form content...")
        is_valid, validation_msg = validate_form_content(service_content)

        if not is_valid:
            logger.error(f"❌ Validation failed: {validation_msg}")
            raise HTTPException(status_code=400, detail=validation_msg)

        logger.info("✅ Form validation passed")

        # ================================================================
        # STEP 3: Validate service name against database
        # ================================================================
        logger.info(f"🔍 Validating service name: {service_name}")
        matched_service_id, official_service_name = validate_and_match_service(
            service_name, db
        )

        if not matched_service_id:
            raise HTTPException(
                status_code=400,
                detail=f"Service '{service_name}' not found in service_master. Please use exact service name.",
            )

        logger.info(
            f"✅ Matched Service: {official_service_name} (ID: {matched_service_id})"
        )

        # Update service_content with official name
        service_content["service_name"] = official_service_name

        # ================================================================
        # STEP 4: USE GEMINI AI TO CREATE ENHANCED SLIDES
        # ================================================================
        logger.info("🤖 Enhancing content with Gemini AI...")
        logger.info(f"   📝 Service: {official_service_name}")
        logger.info(
            f"   📋 Processing {len([k for k, v in service_content.items() if v])} filled fields"
        )

        try:
            slide_data = generate_slides_from_form(service_content)
            slides = slide_data.get("slides", [])

            if not slides:
                raise ValueError("No slides generated by AI")

            logger.info(f"✅ AI generated {len(slides)} professional training slides")

            # Log slide titles for verification
            for i, slide in enumerate(slides, 1):
                logger.info(f"   Slide {i}: {slide.get('title', 'Untitled')}")

        except Exception as e:
            logger.error(f"❌ AI enhancement failed: {str(e)}")
            raise HTTPException(
                status_code=500, detail=f"AI content enhancement failed: {str(e)}"
            )

        # ================================================================
        # STEP 5: Generate video from AI-enhanced slides
        # ================================================================
        logger.info("🎬 Generating video from AI-enhanced slides...")
        result = await generate_video_from_slides(slides, official_service_name)
        video_bytes = result["video_bytes"]

        logger.info(f"✅ Video generated successfully")
        logger.info(f"   📊 Size: {result['file_size_mb']} MB")
        logger.info(f"   ⏱️  Duration: ~{result['duration_estimate']} seconds")
        logger.info(f"   🎞️  Slides: {result['total_slides']}")

        # ================================================================
        # STEP 6: Calculate next version
        # ================================================================
        next_version = get_next_version(matched_service_id, official_service_name, db)
        logger.info(f"📌 Video version: v{next_version}")

        # ================================================================
        # STEP 7: Save video to filesystem
        # ================================================================
        logger.info("💾 Saving video to filesystem...")
        video_info = save_video_to_filesystem(
            video_bytes, official_service_name, next_version
        )
        logger.info(f"✅ Video saved: {video_info['video_path']}")

        # ================================================================
        # STEP 8: Create database record
        # ===============================
        # =================================
        logger.info("📝 Creating database record...")
        video_record = models.ServiceVideo(
            service_id=matched_service_id,
            service_name_metadata=official_service_name,
            video_version=next_version,
            source_type="form_ai_enhanced",  # New source type
            video_path=video_info["video_path"],
            video_url=f"/api/videos/{official_service_name.replace(' ', '_')}/{next_version}",
            file_size_mb=result["file_size_mb"],
            duration_seconds=result["duration_estimate"],
            total_slides=result["total_slides"],
            is_new=True,
            is_done=True,
            created_at=datetime.now(),
        )

        db.add(video_record)
        db.commit()
        db.refresh(video_record)

        # Mark previous versions as old
        db.query(models.ServiceVideo).filter(
            models.ServiceVideo.service_id == matched_service_id,
            models.ServiceVideo.video_version != next_version,
        ).update({"is_new": False}, synchronize_session=False)
        db.commit()

        logger.info(f"✅ Database record created (ID: {video_record.video_id})")

        # ================================================================
        # STEP 9: Return response
        # ================================================================
        logger.info("=" * 80)
        logger.info("✅ VIDEO GENERATION COMPLETE")
        logger.info("=" * 80)

        return {
            "success": True,
            "message": "Video generated successfully with AI enhancement",
            "service_id": matched_service_id,
            "service_name": official_service_name,
            "video_version": next_version,
            "video_url": f"/api/videos/{official_service_name.replace(' ', '_')}/{next_version}",
            "video_id": video_record.video_id,
            "total_slides": result["total_slides"],
            "file_size_mb": result["file_size_mb"],
            "duration_seconds": result["duration_estimate"],
            "ai_enhanced": True,  # Flag to indicate AI was used
        }

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"❌ Unexpected error: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


# ENDPOINT 2: Generate Video from PDF
# @app.post("/bsk_portal/generate_video_from_pdf/", tags=["BSK Portal Integration"])
# async def bsk_generate_video_from_pdf(
#     pdf_file: UploadFile = File(...),
#     service_name: str = Form(
#         ..., description="Service name (must match service_master)"
#     ),
#     use_openai: bool = Form(False),
#     db: Session = Depends(get_db),
# ):
#     """
#     🎥 **GENERATE AND SAVE VIDEO FROM PDF**

#     Process:
#     1. Validates service name
#     2. Extracts content from PDF
#     3. AI generates slides
#     4. Generates video IN-MEMORY
#     5. SAVES video to local filesystem (videos/<service_name>/<version>.mp4)
#     6. STREAMS video to client
#     7. Logs generation metadata with file path

#     Returns: StreamingResponse with video bytes + metadata headers
#     """

#     logger.info("=" * 80)
#     logger.info("🎬 BSK PORTAL - PDF VIDEO GENERATION (WITH LOCAL STORAGE)")
#     logger.info("=" * 80)

#     temp_pdf_path = None

#     try:
#         # STEP 1: Validate service
#         matched_service_id, official_service_name = validate_and_match_service(
#             service_name, db
#         )

#         if not matched_service_id:
#             raise HTTPException(
#                 status_code=400,
#                 detail=f"Service '{service_name}' not found in service_master",
#             )

#         logger.info(
#             f"✅ Matched Service: {official_service_name} (ID: {matched_service_id})"
#         )

#         # STEP 2: Validate and save PDF temporarily
#         if not pdf_file.filename.lower().endswith(".pdf"):
#             raise HTTPException(status_code=400, detail="Only PDF files are supported")

#         temp_id = str(uuid.uuid4())[:8]
#         pdf_filename = f"{temp_id}_{pdf_file.filename}"
#         temp_pdf_path = UPLOAD_DIR / pdf_filename

#         with open(temp_pdf_path, "wb") as buffer:
#             shutil.copyfileobj(pdf_file.file, buffer)

#         file_size_mb = os.path.getsize(temp_pdf_path) / (1024 * 1024)
#         if file_size_mb > 50:
#             raise HTTPException(status_code=400, detail="PDF too large (max 50MB)")

#         # STEP 3: Extract text from PDF
#         logger.info("📖 Extracting text from PDF...")
#         raw_pages = extract_raw_content(str(temp_pdf_path))
#         raw_text = "\n\n".join(["\n".join(page["lines"]) for page in raw_pages])

#         if not raw_text.strip():
#             raise ValueError("No text content extracted from PDF")

#         logger.info(f"✅ Extracted {len(raw_text)} characters")

#         # STEP 4: Generate slides with AI
#         logger.info("🤖 Generating slides with AI...")
#         if use_openai:
#             from services.openai_service import (
#                 generate_slides_from_raw as openai_generate,
#             )

#             slide_data = openai_generate(raw_text)
#         else:
#             slide_data = generate_slides_from_raw(raw_text)

#         slides = slide_data.get("slides", [])
#         logger.info(f"✅ Generated {len(slides)} slides")

#         # STEP 5: Generate video IN-MEMORY
#         logger.info("🎬 Generating video...")
#         result = await generate_video_from_slides(slides, official_service_name)
#         video_bytes = result["video_bytes"]

#         # STEP 6: Calculate next version
#         next_version = get_next_version(matched_service_id, official_service_name, db)

#         # STEP 7: Save video to filesystem
#         video_info = save_video_to_filesystem(
#             video_bytes, official_service_name, next_version
#         )

#         # STEP 8: Log generation metadata WITH FILE PATH
#         video_record = models.ServiceVideo(
#             service_id=matched_service_id,
#             service_name_metadata=official_service_name,
#             video_version=next_version,
#             source_type="pdf_automatic",
#             video_path=video_info["video_path"],  # ✅ Store file path
#             video_url=f"/api/videos/{video_info['relative_path']}",  # ✅ Store URL path
#             file_size_mb=result["file_size_mb"],
#             duration_seconds=result["duration_estimate"],
#             total_slides=result["total_slides"],
#             is_new=True,
#             is_done=True,
#             created_at=datetime.now(),
#         )

#         db.add(video_record)
#         db.commit()
#         db.refresh(video_record)

#         # STEP 9: Mark previous versions as old
#         db.query(models.ServiceVideo).filter(
#             models.ServiceVideo.service_id == matched_service_id,
#             models.ServiceVideo.video_version != next_version,
#         ).update({"is_new": False}, synchronize_session=False)
#         db.commit()

#         logger.info(
#             f"✅ Video generated and saved! Log ID: {video_record.video_id}, Path: {video_info['video_path']}"
#         )
#         return {
#             "success": True,
#             "message": "Video generated successfully with AI enhancement",
#             "service_id": matched_service_id,
#             "service_name": official_service_name,
#             "video_version": next_version,
#             "video_url": f"/api/videos/{official_service_name.replace(' ', '_')}/{next_version}",
#             "video_id": video_record.video_id,
#             "total_slides": result["total_slides"],
#             "file_size_mb": result["file_size_mb"],
#             "duration_seconds": result["duration_estimate"],
#             "ai_enhanced": True,  # Flag to indicate AI was used
#         }

#     except HTTPException:
#         raise
#     except Exception as e:
#         logger.error(f"❌ Unexpected error: {str(e)}")
#         raise HTTPException(status_code=500, detail=str(e))
#     #     # STEP 11: Stream video to client
#     #     video_bytes.seek(0)  # Reset buffer to start

#     #     return StreamingResponse(
#     #         video_bytes,
#     #         media_type="video/mp4",
#     #         headers={
#     #             "Content-Disposition": f"attachment; filename={official_service_name.replace(' ', '_')}_v{next_version}.mp4",
#     #             # Metadata headers
#     #             "X-Video-ID": str(video_record.video_id),
#     #             "X-Service-ID": str(matched_service_id),
#     #             "X-Video-Version": str(next_version),
#     #             "X-Total-Slides": str(result["total_slides"]),
#     #             "X-Duration-Seconds": str(result["duration_estimate"]),
#     #             "X-Video-URL": video_record.video_url,  # ✅ Include URL in headers
#     #         },
#     #     )

#     # except HTTPException:
#     #     raise
#     # except Exception as e:
#     #     logger.error(f"❌ Error: {str(e)}")
#     #     # Cleanup PDF on error
#     #     if temp_pdf_path and os.path.exists(temp_pdf_path):
#     #         os.remove(temp_pdf_path)
#     #     raise HTTPException(status_code=500, detail=str(e))

@app.post("/generate_video_from_pdf")
async def generate_video_from_pdf(pdf_file: UploadFile = File(...),service_name: str = Form(
        ..., description="Service name (must match service_master)"
    ),
    use_openai: bool = Form(False),
    db: Session = Depends(get_db),):
    """
    Generate training video from government service PDF
    """
    
    try:
        matched_service_id, official_service_name = validate_and_match_service(
            service_name, db
        )

        if not matched_service_id:
            raise HTTPException(
                status_code=400,
                detail=f"Service '{service_name}' not found in service_master",
            )

        logger.info(
            f"✅ Matched Service: {official_service_name} (ID: {matched_service_id})"
        )
        # Save uploaded PDF
        pdf_path = f"uploads/{pdf_file.filename}"
        os.makedirs("uploads", exist_ok=True)
        
        with open(pdf_path, "wb") as f:
            f.write(await pdf_file.read())
        
        # -------------------------------------------------
        # STEP 1: EXTRACT CONTENT
        # -------------------------------------------------
        print("📄 Extracting PDF content...")
        # pdf_pages = extract_raw_content(pdf_path)
        raw_pages = extract_raw_content(str(pdf_path))
        raw_text = "\n\n".join(["\n".join(page["lines"]) for page in raw_pages])

        # -------------------------------------------------
        # STEP 2: VALIDATE CONTENT (NEW!)
        # -------------------------------------------------
        print("🔍 Validating PDF content...")
        is_valid, validation_message = validate_pdf_content(raw_pages)
        
        if not is_valid:
            raise HTTPException(
                status_code=400,
                detail={
                    "error": "Invalid PDF Content",
                    "message": validation_message,
                    "hint": "Please upload a government service document containing: service name, description, application process, eligibility criteria, and required documents."
                }
            )
        
        print(f"✅ {validation_message}")
        
        # STEP 4: Generate slides with AI
        logger.info("🤖 Generating slides with AI...")
        if use_openai:
            from services.openai_service import (
                generate_slides_from_raw as openai_generate,
            )

            slide_data = openai_generate(raw_text)
        else:
            slide_data = generate_slides_from_raw(raw_text)

        slides = slide_data.get("slides", [])
        logger.info(f"✅ Generated {len(slides)} slides")

        # STEP 5: Generate video IN-MEMORY
        logger.info("🎬 Generating video...")
        result = await generate_video_from_slides(slides, official_service_name)
        video_bytes = result["video_bytes"]

        # STEP 6: Calculate next version
        next_version = get_next_version(matched_service_id, official_service_name, db)

        # STEP 7: Save video to filesystem
        video_info = save_video_to_filesystem(
            video_bytes, official_service_name, next_version
        )

        # STEP 8: Log generation metadata WITH FILE PATH
        video_record = models.ServiceVideo(
            service_id=matched_service_id,
            service_name_metadata=official_service_name,
            video_version=next_version,
            source_type="pdf_automatic",
            video_path=video_info["video_path"],  # ✅ Store file path
            video_url=f"/api/videos/{video_info['relative_path']}",  # ✅ Store URL path
            file_size_mb=result["file_size_mb"],
            duration_seconds=result["duration_estimate"],
            total_slides=result["total_slides"],
            is_new=True,
            is_done=True,
            created_at=datetime.now(),
        )

        db.add(video_record)
        db.commit()
        db.refresh(video_record)

        # STEP 9: Mark previous versions as old
        db.query(models.ServiceVideo).filter(
            models.ServiceVideo.service_id == matched_service_id,
            models.ServiceVideo.video_version != next_version,
        ).update({"is_new": False}, synchronize_session=False)
        db.commit()

        logger.info(
            f"✅ Video generated and saved! Log ID: {video_record.video_id}, Path: {video_info['video_path']}"
        )
        return {
            "success": True,
            "message": "Video generated successfully with AI enhancement",
            "service_id": matched_service_id,
            "service_name": official_service_name,
            "video_version": next_version,
            "video_url": f"/api/videos/{official_service_name.replace(' ', '_')}/{next_version}",
            "video_id": video_record.video_id,
            "total_slides": result["total_slides"],
            "file_size_mb": result["file_size_mb"],
            "duration_seconds": result["duration_estimate"],
            "ai_enhanced": True,  # Flag to indicate AI was used
        }

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"❌ Unexpected error: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

# ENDPOINT 3: GET VIDEO BY CLEAN URL
@app.get("/api/videos/{service_name}/{version}", tags=["Video Access"])
async def get_video_by_url(
    service_name: str,
    version: int,
    db: Session = Depends(get_db),
):
    """
    🎬 **SERVE VIDEO VIA URL**

    Access videos using clean URLs:
    - /api/videos/<service_name>/<version>

    Example:
    - /api/videos/Birth_Certificate/1
    - /api/videos/Ration_Card/2

    Returns: Video file (MP4) for streaming/download
    """

    try:
        # Build video path
        video_path = VIDEO_BASE_DIR / service_name / f"{version}.mp4"

        # Check if file exists
        if not video_path.exists():
            raise HTTPException(
                status_code=404,
                detail=f"Video not found: {service_name} version {version}",
            )

        logger.info(f"📹 Serving video: {video_path}")

        # Return video file
        return FileResponse(
            path=str(video_path),
            media_type="video/mp4",
            filename=f"{service_name}_v{version}.mp4",
            headers={
                "Accept-Ranges": "bytes",  # Enable video seeking
                "Cache-Control": "public, max-age=31536000",  # Cache for 1 year
            },
        )

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"❌ Error serving video: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


# ============================================================================
# DATA SYNC ENDPOINTS
# ============================================================================
scheduler = BackgroundScheduler()

# ============================================================================
# SYNC ENDPOINTS (One endpoint per table)
# ============================================================================


@app.get("/sync/{table_name}", tags=["Sync"])
def sync_table(
    table_name: str, background_tasks: BackgroundTasks, db: Session = Depends(get_db)
):
    """
    Sync a specific table
    Available tables:
    - bsk_master
    - deo_master
    - service_master
    - provision

    Examples:
    GET /sync/bsk_master
    GET /sync/provision
    """

    valid_tables = ["bsk_master", "deo_master", "service_master", "provision"]

    if table_name not in valid_tables:
        raise HTTPException(400, f"Invalid table. Choose: {', '.join(valid_tables)}")

    logger.info(f"Manual sync triggered for {table_name}")

    service = SyncService(db)
    background_tasks.add_task(service.sync_table, table_name)

    return {
        "success": True,
        "message": f"Sync started for {table_name}",
        "table": table_name,
    }


@app.post("/sync/all", tags=["Sync"])
def sync_all(background_tasks: BackgroundTasks):
    """
    Manually trigger sync for all 4 tables

    This does the same thing as the auto-scheduler,
    but you can trigger it manually anytime.

    POST /sync/all
    """
    logger.info("🚀 Manual sync triggered for all tables")

    background_tasks.add_task(sync_all_tables)

    return {
        "success": True,
        "message": "Syncing all tables in background",
        "tables": ["bsk_master", "deo_master", "service_master", "provision"],
    }


@app.get("/sync/status", tags=["Sync"])
def get_status(table_name: str = None, limit: int = 10, db: Session = Depends(get_db)):
    """
    Check sync status

    GET /sync/status
    GET /sync/status?table_name=provision
    GET /sync/status?limit=50
    """
    from app.models import models
    from sqlalchemy import desc

    query = db.query(models.SyncLog).order_by(desc(models.SyncLog.started_at))

    if table_name:
        query = query.filter(models.SyncLog.table_name == table_name)

    logs = query.limit(limit).all()

    return {
        "logs": [
            {
                "table": log.table_name,
                "type": log.sync_type,
                "status": log.status,
                "started": log.started_at.isoformat(),
                "completed": log.completed_at.isoformat() if log.completed_at else None,
                "duration": log.duration_seconds,
                "fetched": log.records_fetched,
                "inserted": log.records_inserted,
                "updated": log.records_updated,
                "failed": log.records_failed,
                "error": log.error_message,
            }
            for log in logs
        ]
    }


@app.get("/sync/checkpoints", tags=["Sync"])
def get_checkpoints(db: Session = Depends(get_db)):
    """
    View checkpoints for all tables

    GET /sync/checkpoints
    """
    from app.models import models

    checkpoints = db.query(models.SyncCheckpoint).all()

    return {
        "checkpoints": [
            {
                "table": cp.table_name,
                "last_sync": (
                    cp.last_sync_date.isoformat() if cp.last_sync_date else None
                ),
                "last_timestamp": (
                    cp.last_sync_timestamp.isoformat()
                    if cp.last_sync_timestamp
                    else None
                ),
                "total_synced": cp.total_records_synced,
            }
            for cp in checkpoints
        ]
    }


# ============================================================================
# Our system Required APIs
# ============================================================================


# BSK MASTER
@app.get("/bsk/", response_model=List[schemas.BSKMaster], tags=["BSK Master"])
def get_bsk_list(
    skip: int = Query(0, ge=0, description="Number of records to skip"),
    limit: int = Query(None, ge=1, description="Maximum number of records to return"),
    db: Session = Depends(get_db),
):
    """
    Retrieve a paginated list of Bangla Sahayata Kendra (BSK) records.
    This endpoint supports pagination through skip and limit parameters,
    allowing efficient retrieval of large datasets.
    Args:
        skip: Number of records to skip (for pagination)
        limit: Maximum number of records to return (None = all records)
        db: Database session dependency
    Returns:
        List[BSKMaster]: List of BSK master records
    """
    logger.info(f"GET /bsk/ - Fetching BSK list with skip={skip}, limit={limit}")

    # Build query with offset
    query = db.query(models.BSKMaster).offset(skip)

    # Apply limit if specified
    if limit is not None:
        query = query.limit(limit)

    bsk_list = query.all()
    logger.info(f"Successfully retrieved {len(bsk_list)} BSK records")

    return bsk_list


# BSK idividual
@app.get("/bsk/{bsk_code}", response_model=schemas.BSKMaster, tags=["BSK Master"])
def get_bsk(bsk_id: int, db: Session = Depends(get_db)):
    """
    Retrieve a specific BSK record by its ID.
    Args:
        bsk_id: The unique identifier for the BSK
        db: Database session dependency
    Returns:
        BSKMaster: The requested BSK record
    Raises:
        HTTPException: 404 if BSK not found
    """
    logger.info(f"GET /bsk/{bsk_id} - Fetching BSK with ID: {bsk_id}")

    bsk = db.query(models.BSKMaster).filter(models.BSKMaster.bsk_id == bsk_id).first()

    if bsk is None:
        logger.warning(f"BSK not found with ID: {bsk_id}")
        raise HTTPException(status_code=404, detail=f"BSK not found with ID: {bsk_id}")

    logger.info(f"Successfully retrieved BSK: {bsk_id}")
    return bsk


# SERVICE MASTER
@app.get(
    "/services/", response_model=List[schemas.ServiceMaster], tags=["Service Master"]
)
def get_services(
    skip: int = Query(0, ge=0, description="Number of records to skip"),
    limit: int = Query(None, ge=1, description="Maximum number of records to return"),
    db: Session = Depends(get_db),
):
    """
    Retrieve a paginated list of service records.
    Services represent the different banking services that can be provided
    by BSK agents (e.g., account opening, loan applications, etc.).
    Args:
        skip: Number of records to skip (for pagination)
        limit: Maximum number of records to return (None = all records)
        db: Database session dependency
    Returns:
        List[ServiceMaster]: List of service master records
    """
    logger.info(f"GET /services/ - Fetching services with skip={skip}, limit={limit}")

    # Build query with offset
    query = db.query(models.ServiceMaster).offset(skip)

    # Apply limit if specified
    if limit is not None:
        query = query.limit(limit)

    services = query.all()
    logger.info(f"Successfully retrieved {len(services)} service records")

    return services


# SERVICE individual
@app.get(
    "/services/{service_id}",
    response_model=schemas.ServiceMaster,
    tags=["Service Master"],
)
def get_service(service_id: int, db: Session = Depends(get_db)):
    """
    Retrieve a specific service record by its ID.
    Args:
        service_id: The unique identifier for the service
        db: Database session dependency
    Returns:
        ServiceMaster: The requested service record
    Raises:
        HTTPException: 404 if service not found
    """
    logger.info(f"GET /services/{service_id} - Fetching service with ID: {service_id}")

    service = (
        db.query(models.ServiceMaster)
        .filter(models.ServiceMaster.service_id == service_id)
        .first()
    )

    if service is None:
        logger.warning(f"Service not found with ID: {service_id}")
        raise HTTPException(
            status_code=404, detail=f"Service not found with ID: {service_id}"
        )

    logger.info(f"Successfully retrieved service: {service_id}")
    return service


# DEO MASTER
@app.get("/deo/", response_model=List[schemas.DEOMaster], tags=["DEO Master"])
def get_deo_list(
    skip: int = Query(0, ge=0, description="Number of records to skip"),
    limit: int = Query(None, ge=1, description="Maximum number of records to return"),
    db: Session = Depends(get_db),
):
    """
    Retrieve a paginated list of Data Entry Operator (DEO) records.
    DEOs are responsible for managing and overseeing BSK agents in their
    assigned territories.
    Args:
        skip: Number of records to skip (for pagination)
        limit: Maximum number of records to return (None = all records)
        db: Database session dependency
    Returns:
        List[DEOMaster]: List of DEO master records
    """
    logger.info(f"GET /deo/ - Fetching DEO list with skip={skip}, limit={limit}")

    # Build query with offset
    query = db.query(models.DEOMaster).offset(skip)

    # Apply limit if specified
    if limit is not None:
        query = query.limit(limit)

    deo_list = query.all()
    logger.info(f"Successfully retrieved {len(deo_list)} DEO records")

    return deo_list


# DEO individual
@app.get("/deo/{agent_id}", response_model=schemas.DEOMaster, tags=["DEO Master"])
def get_deo(agent_id: int, db: Session = Depends(get_db)):
    """
    Retrieve a specific DEO record by agent ID.
    Args:
        agent_id: The unique identifier for the DEO agent
        db: Database session dependency
    Returns:
        DEOMaster: The requested DEO record
    Raises:
        HTTPException: 404 if DEO not found
    """
    logger.info(f"GET /deo/{agent_id} - Fetching DEO with agent ID: {agent_id}")

    deo = (
        db.query(models.DEOMaster).filter(models.DEOMaster.agent_id == agent_id).first()
    )

    if deo is None:
        logger.warning(f"DEO not found with agent ID: {agent_id}")
        raise HTTPException(
            status_code=404, detail=f"DEO not found with agent ID: {agent_id}"
        )

    logger.info(f"Successfully retrieved DEO: {agent_id}")
    return deo


# PROVISION ENDPOINTS
@app.get("/provisions/", response_model=List[schemas.Provision], tags=["Provisions"])
def get_provisions(
    skip: int = Query(0, ge=0, description="Number of records to skip"),
    limit: int = Query(
        None,
        ge=1,
        description="Maximum number of records to return(Choose a small number as the number of data is too huge)",
    ),
    db: Session = Depends(get_db),
):
    """
    Retrieve a paginated list of provision records.
    Provisions represent service transactions or activations performed by
    BSK agents for customers.
    Args:
        skip: Number of records to skip (for pagination)
        limit: Maximum number of records to return (None = all records)
        db: Database session dependency
    Returns:
        List[Provision]: List of provision records
    """
    logger.info(
        f"GET /provisions/ - Fetching provisions with skip={skip}, limit={limit}"
    )

    # Build query with offset
    query = db.query(models.Provision).offset(skip)

    # Apply limit if specified
    if limit is not None:
        query = query.limit(limit)

    provisions = query.all()
    logger.info(f"Successfully retrieved {len(provisions)} provision records")

    return provisions


# PROVISION individual
@app.get(
    "/provisions/{customer_id}", response_model=schemas.Provision, tags=["Provisions"]
)
def get_provision(customer_id: str, db: Session = Depends(get_db)):
    """
    Retrieve a specific provision record by customer ID.
    Args:
        customer_id: The unique identifier for the customer
        db: Database session dependency
    Returns:
        Provision: The requested provision record
    Raises:
        HTTPException: 404 if provision not found
    """
    logger.info(
        f"GET /provisions/{customer_id} - Fetching provision for customer: {customer_id}"
    )
    provision = (
        db.query(models.Provision)
        .filter(models.Provision.customer_id == customer_id)
        .first()
    )
    if provision is None:
        logger.warning(f"Provision not found for the customer ID: {customer_id}")
        raise HTTPException(
            status_code=404,
            detail=f"Provision not found for the customer ID: {customer_id}",
        )
    logger.info(f"Successfully retrieved provision for the customer: {customer_id}")
    return provision
