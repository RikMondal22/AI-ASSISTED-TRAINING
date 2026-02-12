"""
SQLAlchemy ORM Models for BSK Training Optimization API

This module defines all database models (tables) using SQLAlchemy ORM.
Each class represents a table in the database.

Author: AI ASSISTED TRAINING TEAM
Version: 2.0.0
"""
"""
SQLAlchemy ORM Models for BSK Training Optimization API

This module defines all database models (tables) using SQLAlchemy ORM.
Each class represents a table in the database.

Author: AI ASSISTED TRAINING TEAM
Version: 2.0.0
"""
from sqlalchemy import Column, Integer, String, DateTime, Text, Date
from sqlalchemy.sql import func
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
    PrimaryKeyConstraint
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
    # __table_args__ = (
#     PrimaryKeyConstraint('agent_id', 'agent_code', 'prov_date', 'docket_no'),
#     {"schema": "dbo"}
# )
    __table_args__ = {"schema": "dbo"}

    # Primary Key
    agent_id = Column(
        Integer,  index=True, comment="Unique agent identifier"
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
    agent_email = Column(String(250),primary_key=True, comment="Email address")
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
    __table_args__ = (
    PrimaryKeyConstraint('customer_id', 'service_id', 'prov_date', 'docket_no'),
    {"schema": "dbo"}
)

    # Primary Key
    customer_id = Column(Text, comment="Unique customer identifier")

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


class SyncCheckpoint(Base):
    """
    Enhanced Sync Checkpoint - Tracks detailed sync statistics per table

    Features:
    - Success/failure counts per sync run
    - Date range tracking for provision syncs
    - Sync status monitoring
    - Error message logging
    - Full audit trail
    """

    __tablename__ = "sync_checkpoints"
    __table_args__ = {"schema": "dbo"}

    # =========================================================================
    # PRIMARY KEY
    # =========================================================================
    table_name = Column(
        String(100),
        primary_key=True,
        comment="Table name (bsk_master, deo_master, service_master, provision)",
    )

    # =========================================================================
    # LAST SYNC RUN STATISTICS
    # =========================================================================
    last_sync_date = Column(DateTime, comment="Timestamp when last sync completed")

    last_sync_success_count = Column(
        Integer,
        default=0,
        comment="Number of records successfully inserted in last sync",
    )

    last_sync_failed_count = Column(
        Integer,
        default=0,
        comment="Number of records that failed to insert in last sync",
    )

    sync_status = Column(
        String(20),
        default="pending",
        comment="Status: 'success', 'partial', 'failed', 'running', 'pending'",
    )

    error_message = Column(
        Text, nullable=True, comment="Last error message if sync failed"
    )

    # =========================================================================
    # CUMULATIVE STATISTICS (ALL TIME)
    # =========================================================================
    total_records_synced = Column(
        Integer,
        default=0,
        comment="Cumulative total of all successfully synced records",
    )

    total_sync_runs = Column(
        Integer, default=0, comment="Total number of sync runs executed"
    )

    total_failures = Column(
        Integer, default=0, comment="Cumulative total of all failed record inserts"
    )

    # =========================================================================
    # PROVISION-SPECIFIC DATE RANGE TRACKING
    # =========================================================================
    provision_start_date = Column(
        Date,
        nullable=True,
        comment="Start date for last provision sync (provision table only)",
    )

    provision_end_date = Column(
        Date,
        nullable=True,
        comment="End date for last provision sync (provision table only)",
    )

    # =========================================================================
    # AUDIT TRAIL
    # =========================================================================
    last_successful_sync = Column(
        DateTime, comment="Timestamp of last successful sync (status='success')"
    )

    first_sync_date = Column(
        DateTime, default=func.now(), comment="Timestamp when table was first synced"
    )

    last_record_id = Column(
        String(100),
        nullable=True,
        comment="Last record ID synced (for pagination, if applicable)",
    )

    # =========================================================================
    # PERFORMANCE TRACKING
    # =========================================================================
    last_sync_duration_seconds = Column(
        Integer, nullable=True, comment="Duration of last sync in seconds"
    )

    avg_sync_duration_seconds = Column(
        Integer, nullable=True, comment="Average sync duration across all runs"
    )

    def __repr__(self):
        return (
            f"<SyncCheckpoint("
            f"table={self.table_name}, "
            f"status={self.sync_status}, "
            f"last_run={self.last_sync_date}, "
            f"success={self.last_sync_success_count}, "
            f"failed={self.last_sync_failed_count}"
            f")>"
        )


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


class VideoGenerationQueue(Base):
    """
    Queue table for async video generation requests
    
    Tracks the lifecycle of each video generation:
    - pending → processing → completed → retrieved
    
    Once a video is retrieved by the user, it can be cleaned up after a few days.
    """
    __tablename__ = "video_generation_queue"
    __table_args__ = {"schema": "dbo"}
    # Primary identification
    id = Column(Integer, primary_key=True, index=True, autoincrement=True)
    video_id = Column(String(100), unique=True, index=True, nullable=False)  # UUID
    
    # Service information
    service_id = Column(Integer, nullable=True)  # Foreign key to service_master
    service_name = Column(String(500), nullable=False, index=True)
    source_type = Column(String(50), nullable=False)  # 'form_ai_enhanced', 'pdf_ai_enhanced'
    
    # Request tracking
    status = Column(String(20), nullable=False, index=True)  # pending, processing, completed, retrieved, failed
    request_data = Column(JSON, nullable=True)  # Original request data for debugging
    
    # Timestamps
    created_at = Column(DateTime, nullable=False, index=True)  # When request was created
    started_at = Column(DateTime, nullable=True)               # When processing started
    completed_at = Column(DateTime, nullable=True, index=True) # When video was ready
    retrieved_at = Column(DateTime, nullable=True)             # When user retrieved it
    failed_at = Column(DateTime, nullable=True)                # When it failed (if applicable)
    updated_at = Column(DateTime, nullable=False)              # Last update time
    
    # Video details (populated after completion)
    video_record_id = Column(Integer, nullable=True)  # Foreign key to service_videos table
    video_url = Column(String(500), nullable=True)
    video_path = Column(String(500), nullable=True)
    file_size_mb = Column(Float, nullable=True)
    duration_seconds = Column(Integer, nullable=True)
    total_slides = Column(Integer, nullable=True)
    
    # Error tracking
    error_message = Column(Text, nullable=True)

    # Push tracking (set when result is POSTed to external BSK API)
    pushed_at = Column(DateTime, nullable=True)   # When result was pushed to bsk.wb.gov.in
    

class VideoGenerationTask(Base):
    """
    Video Generation Task table
    
    Tracks asynchronous video generation requests and their status.
    Works with RabbitMQ queue for background processing.
    
    Workflow:
    1. User submits request → task created with status='pending'
    2. Task published to RabbitMQ queue
    3. Worker picks up task → status='processing'
    4. Video generated → status='completed', video_url populated
    5. If error → status='failed', error_message populated
    """
    
    __tablename__ = "video_generation_tasks"
    __table_args__ = {"schema": "dbo"}
    
    # Primary Key
    task_id = Column(
        String(36),  # UUID format
        primary_key=True,
        comment="Unique task identifier (UUID)"
    )
    
    # Service Information
    service_name = Column(
        String(600),
        nullable=False,
        index=True,
        comment="Service name for video generation"
    )
    service_id = Column(
        Integer,
        nullable=True,
        index=True,
        comment="Reference to service_master.service_id (NULL for new services)"
    )
    
    # Task Status
    status = Column(
        String(20),
        nullable=False,
        default='pending',
        index=True,
        comment="Task status: pending, processing, completed, failed"
    )
    
    # Progress Information
    progress_percentage = Column(
        Integer,
        default=0,
        comment="Progress percentage (0-100)"
    )
    current_step = Column(
        String(100),
        comment="Current processing step description"
    )
    
    # Video Information
    video_id = Column(
        Integer,
        nullable=True,
        comment="Reference to service_videos.video_id after completion"
    )
    video_version = Column(
        Integer,
        comment="Video version number"
    )
    video_url = Column(
        String(500),
        comment="URL to access completed video"
    )
    video_path = Column(
        String(500),
        comment="File system path to video"
    )
    
    # Source Information
    source_type = Column(
        String(30),
        nullable=False,
        comment="Source: pdf_automatic, pdf_manual, form_manual, form_ai_enhanced"
    )
    pdf_file_name = Column(
        String(500),
        comment="Original PDF filename (if applicable)"
    )
    form_data = Column(
        JSON,
        comment="Form input data (if applicable)"
    )
    
    # Generation Metadata
    total_slides = Column(
        Integer,
        comment="Total number of slides to generate"
    )
    slides_data = Column(
        JSON,
        comment="Slide data used for generation"
    )
    
    # File Metadata
    file_size_mb = Column(
        Float,
        comment="Video file size in MB"
    )
    duration_seconds = Column(
        Float,
        comment="Video duration in seconds"
    )
    
    # Error Tracking
    error_message = Column(
        Text,
        comment="Error details if generation failed"
    )
    retry_count = Column(
        Integer,
        default=0,
        comment="Number of retry attempts"
    )
    max_retries = Column(
        Integer,
        default=3,
        comment="Maximum retry attempts allowed"
    )
    
    # User Information (Optional)
    user_id = Column(
        String(100),
        comment="User who requested the video (if tracking users)"
    )
    client_ip = Column(
        String(50),
        comment="Client IP address for request tracking"
    )
    
    # Timestamps
    created_at = Column(
        DateTime(timezone=True),
        server_default=func.now(),
        nullable=False,
        index=True,
        comment="When task was created"
    )
    started_at = Column(
        DateTime(timezone=True),
        comment="When processing started"
    )
    completed_at = Column(
        DateTime(timezone=True),
        comment="When task completed (success or failure)"
    )
    updated_at = Column(
        DateTime(timezone=True),
        onupdate=func.now(),
        comment="Last update timestamp"
    )
    
    # Processing Information
    processing_time_seconds = Column(
        Float,
        comment="Total processing time in seconds"
    )
    worker_id = Column(
        String(100),
        comment="ID of worker that processed this task"
    )
    
    # Indexing for performance
    __table_args__ = (
        Index("idx_task_status", "status"),
        Index("idx_task_service", "service_name", "status"),
        Index("idx_task_created", "created_at"),
        Index("idx_task_user", "user_id", "created_at"),
        {"schema": "dbo"},
    )
    
    def __repr__(self):
        return (
            f"<VideoGenerationTask("
            f"task_id='{self.task_id}', "
            f"service='{self.service_name}', "
            f"status='{self.status}', "
            f"progress={self.progress_percentage}%"
            f")>"
        )