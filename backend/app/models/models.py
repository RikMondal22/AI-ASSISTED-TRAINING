"""
SQLAlchemy ORM Models for BSK Training Optimization API

This module defines all database models (tables) using SQLAlchemy ORM.
Each class represents a table in the database.

Author: AI ASSISTED TRAINING TEAM
Version: 2.0.0
"""

from sqlalchemy import (
    Column,
    Integer,
    String,
    Boolean,
    Float,
    Text,
    DateTime,
    JSON,
    Index,
)
from sqlalchemy.sql import func
from datetime import datetime
from .database import Base

# ============================================================================
# BSK MASTER MODEL
# ============================================================================


class BSKMaster(Base):
    """
    BSK Master table
    Stores information about all BSK centers including location,
    contact details, and operational information.
    """

    __tablename__ = "ml_bsk_master"
    __table_args__ = {"schema": "dbo"}

    # Primary Key
    bsk_id = Column(
        Integer, primary_key=True, index=True, comment="Unique BSK identifier"
    )

    # Basic Information
    bsk_name = Column(String(200), comment="Name of the BSK center")
    bsk_code = Column(String(50), index=True, comment="Unique BSK code")
    bsk_type = Column(String, comment="Type of BSK (e.g., Urban, Rural)")
    bsk_sub_type = Column(String, comment="Sub-type classification")

    # Location Information
    district_name = Column(String(50), comment="District name")
    district_id = Column(Integer, index=True, comment="District ID reference")
    sub_division_name = Column(String, comment="Sub-division name")
    sub_div_id = Column(Integer, comment="Sub-division ID reference")
    block_municipalty_name = Column(String, comment="Block/Municipality name")
    block_mun_id = Column(Integer, comment="Block/Municipality ID reference")
    gp_ward = Column(String, comment="GP/Ward name")
    gp_id = Column(Integer, comment="GP/Ward ID reference")
    gp_ward_distance = Column(String(50), comment="Distance from GP/Ward")
    pin = Column(String(10), comment="PIN code")

    # Contact & Address
    bsk_address = Column(String(500), comment="Full address of BSK")
    bsk_lat = Column(String(50), comment="Latitude coordinate")
    bsk_long = Column(String(50), comment="Longitude coordinate")
    bsk_account_no = Column(String(30), comment="Bank account number")
    bsk_landline_no = Column(String(20), comment="Landline phone number")

    # Operational Information
    no_of_deos = Column(Integer, default=0, comment="Number of DEOs assigned")
    is_aadhar_center = Column(Integer, default=0, comment="Aadhaar center flag (0/1)")
    is_saturday_open = Column(Text, comment="Saturday operation status")
    is_active = Column(Boolean, default=True, comment="Active status flag")

    def __repr__(self):
        return f"<BSKMaster(bsk_id={self.bsk_id}, bsk_name='{self.bsk_name}', bsk_code='{self.bsk_code}')>"


# ============================================================================
# DEO MASTER MODEL
# ============================================================================


class DEOMaster(Base):
    """
    Data Entry Operator (DEO) Master table

    Stores information about DEO agents who manage BSK operations.
    """

    __tablename__ = "ml_deo_master"
    __table_args__ = {"schema": "dbo"}

    # Primary Key
    agent_id = Column(
        Integer, primary_key=True, index=True, comment="Unique agent identifier"
    )

    # User Information
    user_id = Column(Integer, index=True, comment="User ID reference")
    user_name = Column(String(200), comment="Full name of the agent")
    agent_code = Column(
        String(50), unique=True, index=True, comment="Unique agent code"
    )
    user_emp_no = Column(String, comment="Employee number")
    grp = Column(Text, comment="Group/Category")

    # Contact Information
    agent_email = Column(String(250), comment="Email address")
    agent_phone = Column(String(50), comment="Phone number")

    # BSK Assignment
    bsk_id = Column(Integer, index=True, comment="Assigned BSK ID")
    bsk_name = Column(String(200), comment="Assigned BSK name")
    bsk_code = Column(String(50), comment="Assigned BSK code")
    bsk_post = Column(String(100), comment="Post/Position at BSK")

    # Location References
    bsk_distid = Column(Integer, comment="District ID reference")
    bsk_subdivid = Column(Integer, comment="Sub-division ID reference")
    bsk_blockid = Column(Integer, comment="Block ID reference")
    bsk_gpwdid = Column(Integer, comment="GP/Ward ID reference")

    # Status & Dates
    date_of_engagement = Column(Text, comment="Date of joining/engagement")
    user_islocked = Column(Boolean, default=False, comment="Account locked status")
    is_active = Column(Boolean, default=True, comment="Active status flag")

    def __repr__(self):
        return f"<DEOMaster(agent_id={self.agent_id}, user_name='{self.user_name}', agent_code='{self.agent_code}')>"


# ============================================================================
# SERVICE MASTER MODEL
# ============================================================================


class ServiceMaster(Base):
    """
    Service Master table

    Catalog of all services that can be provided by BSK centers.
    """

    __tablename__ = "ml_service_master"
    __table_args__ = {"schema": "dbo"}

    # Primary Key
    service_id = Column(
        Integer, primary_key=True, index=True, comment="Unique service identifier"
    )

    # Service Information
    service_name = Column(String(600), comment="Official name of the service")
    common_name = Column(Text, comment="Common/popular name")
    action_name = Column(Text, comment="Action/process name")
    service_link = Column(String(600), comment="URL link to service details")
    service_desc = Column(Text, comment="Detailed description of the service")

    # Department Information
    department_id = Column(Integer, index=True, comment="Department ID reference")
    department_name = Column(Text, comment="Department name")

    # Service Classification
    service_type = Column(String(1), comment="Service type code")
    is_new = Column(Integer, default=0, comment="New service flag (0/1)")
    is_active = Column(Integer, default=1, comment="Active status (0/1)")
    is_paid_service = Column(Boolean, default=False, comment="Paid service flag")

    # Application Information
    how_to_apply = Column(Text, comment="Application process description")
    eligibility_criteria = Column(Text, comment="Eligibility requirements")
    required_doc = Column(Text, comment="Required documents list")

    def __repr__(self):
        return f"<ServiceMaster(service_id={self.service_id}, service_name='{self.service_name}')>"


# ============================================================================
# PROVISION MODEL
# ============================================================================


class Provision(Base):
    """
    Provision/Transaction table

    Records of services provided to customers at BSK centers.
    """

    __tablename__ = "ml_provision"
    __table_args__ = {"schema": "dbo"}

    # Primary Key
    customer_id = Column(Text, primary_key=True, comment="Unique customer identifier")

    # BSK Information
    bsk_id = Column(Integer, index=True, comment="BSK ID where service was provided")
    bsk_name = Column(String(200), comment="BSK name")

    # Customer Information
    customer_name = Column(String, comment="Customer name")
    customer_phone = Column(String, comment="Customer phone number")

    # Service Information
    service_id = Column(Integer, index=True, comment="Service ID reference")
    service_name = Column(String(600), comment="Service name")

    # Transaction Details
    prov_date = Column(Text, comment="Date of service provision")
    docket_no = Column(String, comment="Docket/reference number")

    def __repr__(self):
        return f"<Provision(customer_id='{self.customer_id}', service_id={self.service_id}, bsk_id={self.bsk_id})>"


class CitizenMasterV2(Base):
    """
    Citizen Master table (Version 2)

    Stores demographic and contact information for citizens.
    """

    __tablename__ = "ml_citizen_master_v2"
    __table_args__ = {"schema": "dbo"}

    # Primary Key
    citizen_id = Column(
        Text, primary_key=True, index=True, comment="Unique citizen identifier"
    )

    # Contact Information
    citizen_phone = Column(String, index=True, comment="Primary phone number")
    alt_phone = Column(String, comment="Alternative phone number")
    email = Column(String, comment="Email address")

    # Personal Information
    citizen_name = Column(String, comment="Full name")
    guardian_name = Column(String(200), comment="Father/Guardian name")
    gender = Column(String(10), comment="Gender")
    dob = Column(String(30), comment="Date of birth")
    age = Column(Integer, comment="Age")

    # Location References
    district_id = Column(Integer, index=True, comment="District ID reference")
    sub_div_id = Column(Integer, comment="Sub-division ID reference")
    gp_id = Column(Integer, comment="GP/Ward ID reference")

    # Demographics
    caste = Column(String(50), comment="Caste category")
    religion = Column(String(30), comment="Religion")

    def __repr__(self):
        return f"<CitizenMasterV2(citizen_id='{self.citizen_id}', citizen_name='{self.citizen_name}')>"


class DepartmentMaster(Base):
    """Department Master - Catalog of government departments"""

    __tablename__ = "ml_department_master"
    __table_args__ = {"schema": "dbo"}

    dept_id = Column(Integer, primary_key=True, index=True)
    dept_name = Column(String(600))

    def __repr__(self):
        return (
            f"<DepartmentMaster(dept_id={self.dept_id}, dept_name='{self.dept_name}')>"
        )


class District(Base):
    """District Master - List of districts"""

    __tablename__ = "ml_district"
    __table_args__ = {"schema": "dbo"}

    district_id = Column(Integer, primary_key=True, index=True)
    district_name = Column(String(50))
    district_code = Column(String(20), unique=True)
    grp = Column(String(10))

    def __repr__(self):
        return f"<District(district_id={self.district_id}, district_name='{self.district_name}')>"


class BlockMunicipality(Base):
    """Block/Municipality Master"""

    __tablename__ = "ml_block_municipality"
    __table_args__ = {"schema": "dbo"}

    block_muni_id = Column(Integer, primary_key=True, index=True)
    block_muni_name = Column(String)
    sub_div_id = Column(Integer, index=True)
    district_id = Column(Integer, index=True)
    bm_type = Column(String)

    def __repr__(self):
        return f"<BlockMunicipality(block_muni_id={self.block_muni_id}, block_muni_name='{self.block_muni_name}')>"


class GPWardMaster(Base):
    """Gram Panchayat/Ward Master"""

    __tablename__ = "ml_gp_ward_master"
    __table_args__ = {"schema": "dbo"}

    gp_id = Column(Integer, primary_key=True, index=True)
    district_id = Column(String)
    sub_div_id = Column(Integer)
    block_muni_id = Column(String)
    gp_ward_name = Column(String)

    def __repr__(self):
        return f"<GPWardMaster(gp_id={self.gp_id}, gp_ward_name='{self.gp_ward_name}')>"


class PostOfficeMaster(Base):
    """Post Office Master"""

    __tablename__ = "ml_post_office_master"
    __table_args__ = {"schema": "dbo"}

    post_office_id = Column(Integer, primary_key=True, index=True)
    post_office_name = Column(String(250))
    pin_code = Column(String(7), index=True)
    district_id = Column(Integer, index=True)

    def __repr__(self):
        return f"<PostOfficeMaster(post_office_id={self.post_office_id}, post_office_name='{self.post_office_name}')>"


class TrainingRecommendationCache(Base):
    """
    Optimized cache storing ONLY provision computations using parallel arrays.
    BSK/Service/DEO details fetched real-time from master tables.

    Storage Strategy:
    - Uses parallel arrays instead of nested JSON for better performance
    - Order matters across arrays (same index = same entity)
    - Only stores IDs, not names (names fetched from master tables)
    """

    __tablename__ = "training_recommendation_cache"
    __table_args__ = (
        Index("idx_bsk_timestamp", "bsk_id", "timestamp"),
        {"schema": "dbo"},
    )

    # ========================================================================
    # PRIMARY KEY
    # ========================================================================
    bsk_id = Column(Integer, primary_key=True, index=True, comment="BSK identifier")

    # ========================================================================
    # PROVISION METRICS (Computed from 1.66M provisions)
    # ========================================================================
    total_provisions = Column(
        Integer, default=0, comment="Total number of provisions at this BSK"
    )

    unique_services_provided = Column(
        Integer, default=0, comment="Number of unique services provided"
    )

    priority_score = Column(
        Float, default=0.0, comment="Training priority score (higher = more urgent)"
    )

    # ========================================================================
    # NEAREST NEIGHBORS (Parallel Arrays - Order Matters!)
    # ========================================================================
    nearest_bsks_id = Column(
        JSON, comment="List[int] - Neighbor BSK IDs in order [78, 373, 363, ...]"
    )

    distance_km = Column(
        JSON,
        comment="List[float] - Distances in km, same order as nearest_bsks_id [3.18, 7.57, 8.35, ...]",
    )

    # ========================================================================
    # NEIGHBORHOOD CONTEXT
    # ========================================================================
    neigh_top_services_id = Column(
        JSON, comment="List[int] - Top service IDs in neighborhood [352, 4, 408, ...]"
    )

    # ========================================================================
    # TRAINING RECOMMENDATIONS (Parallel Arrays - Order Matters!)
    # ========================================================================
    total_training_services = Column(
        Integer, default=0, comment="Number of services needing training"
    )

    recom_service_id = Column(
        JSON, comment="List[int] - Recommended service IDs [4, 408, 164, ...]"
    )

    recom_service_prov = Column(
        JSON,
        comment="List[int] - Current provisions at THIS BSK for each service [0, 0, 2, ...]",
    )

    recom_service_neigh_prov = Column(
        JSON,
        comment="List[int] - Total provisions in NEIGHBORHOOD for each service [757, 645, 531, ...]",
    )

    # NOTE: Gap can be calculated as: (neigh_prov / num_neighbors) - current_prov

    # ========================================================================
    # METADATA
    # ========================================================================
    timestamp = Column(
        DateTime(timezone=True),
        server_default=func.now(),
        comment="When this cache entry was created/updated",
    )

    def __repr__(self):
        return f"<TrainingRecommendationCache(bsk_id={self.bsk_id}, priority={self.priority_score}, services={self.total_training_services})>"


class RecommendationComputationLog(Base):
    """
    Track computation runs for monitoring and debugging.

    Helps answer:
    - When was cache last refreshed?
    - How long did computation take?
    - Did any computations fail?
    """

    __tablename__ = "recommendation_computation_log"
    __table_args__ = {"schema": "dbo"}

    # Primary Key
    log_id = Column(Integer, primary_key=True, autoincrement=True)

    # Timestamps
    computation_timestamp = Column(
        DateTime(timezone=True),
        server_default=func.now(),
        comment="When computation started",
    )

    completion_timestamp = Column(
        DateTime(timezone=True), comment="When computation finished"
    )

    # Status
    status = Column(
        String(50), default="running", comment="running, completed, or failed"
    )

    # Parameters used
    n_neighbors = Column(Integer, comment="Number of neighbors analyzed")
    top_n_services = Column(Integer, comment="Top N services considered")
    min_provision_threshold = Column(Integer, comment="Minimum provision threshold")

    # Results
    total_bsks_analyzed = Column(Integer, comment="Number of BSKs processed")
    total_provisions_processed = Column(
        Integer, comment="Number of provisions processed"
    )
    total_recommendations_generated = Column(
        Integer, comment="Number of recommendations generated"
    )
    computation_duration_seconds = Column(Float, comment="How long it took")

    # Metadata
    triggered_by = Column(String(100), comment="Who/what triggered this computation")
    error_message = Column(Text, comment="Error details if failed")

    def __repr__(self):
        return f"<ComputationLog(id={self.log_id}, status={self.status}, duration={self.computation_duration_seconds}s)>"


class SyncLog(Base):
    """Track all sync operations for audit and incremental sync"""

    __tablename__ = "sync_logs"
    __table_args__ = {"schema": "dbo"}

    log_id = Column(Integer, primary_key=True, autoincrement=True)
    table_name = Column(String(100), index=True, comment="Table being synced")
    sync_type = Column(String(50), comment="full, incremental, or pagination")

    # Sync parameters
    start_date = Column(DateTime, comment="Date range start (for provision)")
    end_date = Column(DateTime, comment="Date range end (for provision)")
    page_number = Column(Integer, comment="Current page being processed")

    # Results
    records_fetched = Column(Integer, default=0)
    records_inserted = Column(Integer, default=0)
    records_updated = Column(Integer, default=0)
    records_failed = Column(Integer, default=0)

    # Status
    status = Column(String(50), default="running", comment="running, completed, failed")
    error_message = Column(Text, comment="Error details if failed")

    # Timing
    started_at = Column(DateTime, default=datetime.now)
    completed_at = Column(DateTime)
    duration_seconds = Column(Float)

    # Metadata
    triggered_by = Column(String(100), comment="cron, manual, or api")

    def __repr__(self):
        return f"<SyncLog(table={self.table_name}, status={self.status}, records={self.records_fetched})>"


class SyncCheckpoint(Base):
    """Store last successful sync state for each table"""

    __tablename__ = "sync_checkpoints"
    __table_args__ = {"schema": "dbo"}

    table_name = Column(String(100), primary_key=True)
    last_sync_date = Column(DateTime, comment="Last date successfully synced")
    last_record_id = Column(
        String(100), comment="Last record ID synced (for pagination)"
    )
    total_records_synced = Column(Integer, default=0)
    last_successful_sync = Column(DateTime)

    def __repr__(self):
        return f"<SyncCheckpoint(table={self.table_name}, last_sync={self.last_sync_date})>"


class UserCredentials(Base):
    """
    User credentials table for DEO and Superuser authentication.

    Stores hashed passwords and authentication metadata.
    Supports password changes and account management.
    """

    __tablename__ = "user_credentials"
    __table_args__ = {"schema": "dbo"}

    # Primary Key
    credential_id = Column(Integer, primary_key=True, autoincrement=True)

    # User References
    user_id = Column(
        Integer,
        unique=True,
        index=True,
        comment="References agent_id from DEO or superuser_id",
    )
    user_type = Column(String(20), nullable=False, comment="'deo' or 'superuser'")
    username = Column(
        String(100),
        unique=True,
        index=True,
        comment="Login username (agent_code for DEO)",
    )

    # Password (HASHED with bcrypt)
    password_hash = Column(Text, nullable=False, comment="Bcrypt hashed password")

    # Password Management
    password_last_changed = Column(
        DateTime, default=datetime.now, comment="When password was last changed"
    )
    must_change_password = Column(
        Boolean, default=True, comment="Force password change on next login"
    )
    failed_login_attempts = Column(
        Integer, default=0, comment="Track failed login attempts"
    )
    account_locked_until = Column(
        DateTime, nullable=True, comment="Account lockout timestamp"
    )

    # Status
    is_active = Column(Boolean, default=True, comment="Account active status")

    # Audit
    created_at = Column(DateTime, default=datetime.now)
    updated_at = Column(DateTime, default=datetime.now, onupdate=datetime.now)
    last_login = Column(
        DateTime, nullable=True, comment="Last successful login timestamp"
    )

    def __repr__(self):
        return f"<UserCredentials(username='{self.username}', user_type='{self.user_type}', active={self.is_active})>"


class Superuser(Base):
    """
    Superuser/Admin master table.

    Stores superuser profile information.
    """

    __tablename__ = "superusers"
    __table_args__ = {"schema": "dbo"}

    # Primary Key
    superuser_id = Column(Integer, primary_key=True, autoincrement=True)

    # Profile
    full_name = Column(String(200), nullable=False)
    email = Column(String(250), unique=True, index=True)
    phone = Column(String(50))

    # Role & Permissions
    role = Column(
        String(50), default="admin", comment="admin, super_admin, analyst, etc."
    )
    permissions = Column(JSON, comment="List of permitted actions/modules")

    # Status
    is_active = Column(Boolean, default=True)

    # Audit
    created_at = Column(DateTime, default=datetime.now)
    created_by = Column(Integer, comment="Who created this superuser")

    def __repr__(self):
        return f"<Superuser(id={self.superuser_id}, name='{self.full_name}', role='{self.role}')>"


class PasswordResetToken(Base):
    """
    Temporary tokens for password reset functionality.
    """

    __tablename__ = "password_reset_tokens"
    __table_args__ = {"schema": "dbo"}

    token_id = Column(Integer, primary_key=True, autoincrement=True)
    user_id = Column(Integer, index=True, comment="User requesting reset")
    user_type = Column(String(20), comment="'deo' or 'superuser'")

    reset_token = Column(
        String(100), unique=True, index=True, comment="Unique reset token"
    )

    created_at = Column(DateTime, default=datetime.now)
    expires_at = Column(DateTime, comment="Token expiration (24 hours)")
    used_at = Column(DateTime, nullable=True, comment="When token was used")
    is_used = Column(Boolean, default=False)

    def __repr__(self):
        return f"<PasswordResetToken(user_id={self.user_id}, used={self.is_used})>"


class ServiceVideo(Base):
    """
    Service Video table

    Tracks generated training videos for services.
    Stores metadata and file paths for video access.

    CHANGES:
    - Added 'service_name_metadata' field for exact name matching
    - Changed 'video_file_path' to 'video_path' to match helper functions
    - Added 'video_url' field for easy URL access
    - Updated 'source_type' to include new 'form_ai_enhanced' option
    - Renamed status fields to match API usage
    """

    __tablename__ = "service_videos"
    __table_args__ = {"schema": "dbo"}

    # Primary Key
    video_id = Column(
        Integer, primary_key=True, autoincrement=True, comment="Unique video identifier"
    )

    # Service Reference
    service_id = Column(
        Integer,
        index=True,
        nullable=True,  # ✅ CHANGED: Allow NULL for new services not yet in service_master
        comment="Reference to service_master.service_id (NULL if service not in master)",
    )
    service_name_metadata = Column(  # ✅ NEW: Added for exact service name tracking
        String(600),
        nullable=False,
        index=True,  # ✅ NEW: Indexed for faster lookups
        comment="Exact service name used for video generation (for matching)",
    )

    # Video Version Management
    video_version = Column(
        Integer,
        default=1,
        nullable=False,  # ✅ CHANGED: Made non-nullable
        comment="Version number of the video (increments for each regeneration)",
    )

    # Source Information
    source_type = Column(
        String(
            30
        ),  # ✅ CHANGED: Increased from 20 to 30 to accommodate 'form_ai_enhanced'
        nullable=False,
        comment="Source: 'pdf_automatic', 'pdf_manual', 'form_manual', 'form_ai_enhanced'",
    )

    # ============================================================================
    # FILE STORAGE - UPDATED TO MATCH HELPER FUNCTIONS
    # ============================================================================

    video_path = Column(  # ✅ CHANGED: Renamed from video_file_path
        String(500),
        nullable=False,
        comment="Absolute file path: /path/to/videos/<service_name>/<version>.mp4",
    )
    video_url = Column(  # ✅ NEW: Added for URL access
        String(500),
        nullable=True,
        comment="URL path for accessing video: /api/videos/<service_name>/<version>",
    )

    # File Metadata
    file_size_mb = (
        Column(  # ✅ CHANGED: Renamed from video_file_size_mb for consistency
            Float, comment="File size in megabytes"
        )
    )

    # Video Metadata
    duration_seconds = Column(Float, comment="Video duration in seconds")
    total_slides = Column(Integer, comment="Total number of slides in the video")
    resolution = Column(
        String(20),
        default="1280x720",  # ✅ CHANGED: Updated to match actual video dimensions
        comment="Video resolution",
    )
    fps = Column(Integer, default=30, comment="Frames per second")

    # Generation Metadata
    pdf_file_name = Column(
        String(500), comment="Original PDF filename (if source_type contains 'pdf')"
    )
    form_data = Column(JSON, comment="Form input data (if source_type contains 'form')")
    ai_model_used = Column(  # ✅ NEW: Track which AI model was used
        String(50), comment="AI model used: 'gemini-2.0-flash-exp', 'gpt-4', etc."
    )

    # ============================================================================
    # STATUS FLAGS - SIMPLIFIED
    # ============================================================================

    is_new = Column(  # ✅ MATCHES API: Changed from is_latest
        Boolean,
        default=True,
        comment="Whether this is the newest version (auto-set to false for older versions)",
    )
    is_done = Column(  # ✅ MATCHES API: Changed from generation_status
        Boolean,
        default=False,
        comment="Whether video generation completed successfully",
    )
    is_active = Column(
        Boolean,
        default=True,
        comment="Whether this video is currently published/available",
    )

    # Timestamps
    created_at = Column(
        DateTime(timezone=True),
        server_default=func.now(),
        nullable=False,
        comment="When video generation started",
    )
    updated_at = Column(
        DateTime(timezone=True), onupdate=func.now(), comment="Last update timestamp"
    )

    # Error Tracking (optional but useful)
    error_message = Column(Text, comment="Error details if generation failed")

    # Access Tracking (optional)
    view_count = Column(
        Integer, default=0, comment="Number of times video has been accessed"
    )
    last_accessed_at = Column(
        DateTime(timezone=True), comment="Last time video was accessed"
    )

    # Indexing for performance
    __table_args__ = (
        Index("idx_service_version", "service_id", "video_version"),
        Index(
            "idx_service_name_version", "service_name_metadata", "video_version"
        ),  # ✅ NEW
        Index("idx_service_latest", "service_id", "is_new"),  # ✅ CHANGED: Uses is_new
        Index("idx_source_type", "source_type"),
        Index("idx_is_done", "is_done"),  # ✅ NEW: For filtering completed videos
        {"schema": "dbo"},
    )

    def __repr__(self):
        return f"<ServiceVideo(id={self.video_id}, service='{self.service_name_metadata}', version={self.video_version}, done={self.is_done})>"


class VideoGenerationLog(Base):
    """
    Video Generation Log table

    Tracks the detailed process of video generation for debugging and analytics.
    """

    __tablename__ = "video_generation_logs"
    __table_args__ = {"schema": "dbo"}

    # Primary Key
    log_id = Column(Integer, primary_key=True, autoincrement=True)

    # Video Reference
    video_id = Column(
        Integer, index=True, comment="Reference to service_videos.video_id"
    )

    # Process Tracking
    step_name = Column(
        String(100),
        comment="Generation step: pdf_extraction, content_generation, image_search, audio_gen, video_assembly",
    )
    step_status = Column(String(50), comment="Status: started, completed, failed")
    step_duration_seconds = Column(Float, comment="Time taken for this step")

    # Details
    step_details = Column(JSON, comment="Step-specific metadata and parameters")
    error_message = Column(Text, comment="Error details if step failed")

    # Timestamps
    started_at = Column(DateTime(timezone=True), default=func.now())
    completed_at = Column(DateTime(timezone=True))

    def __repr__(self):
        return f"<VideoGenerationLog(video_id={self.video_id}, step='{self.step_name}', status='{self.step_status}')>"
