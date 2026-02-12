from pydantic import BaseModel, Field
from typing import Optional, Dict, Any, List
from datetime import datetime


class BSKMaster(BaseModel):
    bsk_id: int
    bsk_name: Optional[str]
    district_name: Optional[str]
    sub_division_name: Optional[str]
    block_municipalty_name: Optional[str]
    gp_ward: Optional[str]
    gp_ward_distance: Optional[str]
    bsk_type: Optional[str]
    bsk_sub_type: Optional[str]
    bsk_code: Optional[str]
    no_of_deos: Optional[int]
    is_aadhar_center: Optional[int]
    bsk_address: Optional[str]
    bsk_lat: Optional[str]
    bsk_long: Optional[str]
    bsk_account_no: Optional[str]
    bsk_landline_no: Optional[str]
    is_saturday_open: Optional[str]
    is_active: Optional[bool]
    district_id: Optional[int]
    block_mun_id: Optional[int]
    gp_id: Optional[int]
    sub_div_id: Optional[int]
    pin: Optional[str]

    class Config:
        from_attributes = True


class ServiceMaster(BaseModel):
    service_id: int
    service_name: Optional[str]
    common_name: Optional[str]
    action_name: Optional[str]
    service_link: Optional[str]
    department_id: Optional[int]
    department_name: Optional[str]
    is_new: Optional[int]
    service_type: Optional[str]
    is_active: Optional[int]
    is_paid_service: Optional[bool]
    service_desc: Optional[str]
    how_to_apply: Optional[str]
    eligibility_criteria: Optional[str]
    required_doc: Optional[str]

    class Config:
        from_attributes = True


class DEOMaster(BaseModel):
    agent_id: int
    user_id: Optional[int]
    grp: Optional[str]
    user_name: Optional[str]
    agent_code: Optional[str]
    agent_email: Optional[str]
    agent_phone: Optional[str]
    date_of_engagement: Optional[str]
    user_emp_no: Optional[str]
    bsk_id: Optional[int]
    bsk_name: Optional[str]
    bsk_code: Optional[str]
    bsk_distid: Optional[int]
    bsk_subdivid: Optional[int]
    bsk_blockid: Optional[int]
    bsk_gpwdid: Optional[int]
    user_islocked: Optional[bool]
    is_active: Optional[bool]
    bsk_post: Optional[str]

    class Config:
        from_attributes = True


class Provision(BaseModel):
    bsk_id: Optional[int]
    bsk_name: Optional[str]
    customer_id: str
    customer_name: Optional[str]
    customer_phone: Optional[str]
    service_id: Optional[int]
    service_name: Optional[str]
    prov_date: Optional[str]
    docket_no: Optional[str]

    class Config:
        from_attributes = True


class SyncRequest(BaseModel):
    """Request model for triggering sync operations"""

    table_name: str = Field(
        ...,
        description="Table to sync: bsk_master, deo_master, service_master, or provision",
    )
    sync_type: str = Field("full", description="full or incremental")

    # For provision table with large data
    start_date: Optional[str] = Field(
        None, description="Start date for provision sync (YYYY-MM-DD)"
    )
    end_date: Optional[str] = Field(
        None, description="End date for provision sync (YYYY-MM-DD)"
    )
    page_size: int = Field(1000, ge=100, le=10000, description="Records per page")

    # API credentials (should come from env in production)
    api_username: Optional[str] = None
    api_password: Optional[str] = None
    api_key: Optional[str] = None


class SyncResponse(BaseModel):
    """Response model for sync operations"""

    success: bool
    table_name: str
    sync_type: str

    records_fetched: int
    records_inserted: int
    records_updated: int
    records_failed: int

    duration_seconds: float
    message: str

    # For provision pagination
    pages_processed: Optional[int] = None
    date_range: Optional[Dict[str, str]] = None


class SyncStatus(BaseModel):
    """Status of sync operations"""

    table_name: str
    last_sync_date: Optional[datetime]
    total_records_synced: int
    last_successful_sync: Optional[datetime]
    is_sync_running: bool


class ServiceVideoBase(BaseModel):
    """Base schema for ServiceVideo"""

    service_id: Optional[int] = (
        None  # ✅ CHANGED: Now optional (can be None for new services)
    )
    service_name_metadata: str  # ✅ NEW: Required field for service name tracking
    video_version: int
    source_type: str  # 'pdf_automatic', 'pdf_manual', 'form_manual', 'form_ai_enhanced'

    # File paths
    video_path: str  # ✅ CHANGED: Renamed from video_file_path
    video_url: Optional[str] = None  # ✅ NEW: URL path

    # File metadata
    file_size_mb: Optional[float] = None  # ✅ CHANGED: Renamed from video_file_size_mb
    duration_seconds: Optional[float] = None
    total_slides: Optional[int] = None

    # Status flags
    is_new: bool = True  # ✅ NEW: Replaces is_latest
    is_done: bool = False  # ✅ NEW: Replaces generation_status
    is_active: bool = True

    # AI metadata
    ai_model_used: Optional[str] = None  # ✅ NEW: Track AI model


class ServiceVideoCreate(ServiceVideoBase):
    """Schema for creating a new video record"""

    pass


class ServiceVideoUpdate(BaseModel):
    """Schema for updating a video record"""

    service_id: Optional[int] = None
    service_name_metadata: Optional[str] = None
    video_version: Optional[int] = None
    source_type: Optional[str] = None
    video_path: Optional[str] = None
    video_url: Optional[str] = None
    file_size_mb: Optional[float] = None
    duration_seconds: Optional[float] = None
    total_slides: Optional[int] = None
    is_new: Optional[bool] = None
    is_done: Optional[bool] = None
    is_active: Optional[bool] = None
    ai_model_used: Optional[str] = None


class ServiceVideo(ServiceVideoBase):
    """Complete schema with timestamps"""

    video_id: int
    created_at: datetime
    updated_at: Optional[datetime] = None

    # Optional fields
    pdf_file_name: Optional[str] = None
    form_data: Optional[Dict[str, Any]] = None
    error_message: Optional[str] = None
    view_count: int = 0
    last_accessed_at: Optional[datetime] = None

    class Config:
        from_attributes = True


class VideoGenerationResponse(BaseModel):
    """Response schema for video generation endpoints"""

    success: bool
    message: Optional[str] = None  # ✅ NEW: Optional message

    # Service info
    service_id: Optional[int] = None  # ✅ CHANGED: Optional for new services
    service_name: str

    # Video info
    video_id: Optional[int] = None  # ✅ NEW: Database record ID
    video_version: int
    video_url: str

    # Metadata
    total_slides: Optional[int] = None  # ✅ NEW
    file_size_mb: Optional[float] = None  # ✅ NEW
    duration_seconds: Optional[float] = None  # ✅ NEW
    ai_enhanced: Optional[bool] = None  # ✅ NEW: Flag to indicate AI was used

    class Config:
        from_attributes = True


class FormVideoGenerationRequest(BaseModel):
    """Request schema for form-based video generation"""

    # Required fields
    service_name: str = Field(
        ..., min_length=1, description="Service name (must match service_master)"
    )
    service_description: str = Field(
        ..., min_length=10, description="Brief description of the service"
    )
    how_to_apply: str = Field(
        ..., min_length=10, description="Step-by-step application process"
    )
    eligibility_criteria: str = Field(
        ..., min_length=10, description="Who can apply for this service"
    )
    required_documents: str = Field(
        ..., min_length=5, description="List of required documents"
    )

    # Optional fields
    fees_and_timeline: Optional[str] = Field(
        None, description="Fees and processing time"
    )
    operator_tips: Optional[str] = Field(None, description="Tips for BSK operators")
    troubleshooting: Optional[str] = Field(
        None, description="Common issues and solutions"
    )
    service_link: Optional[str] = Field(
        None, description="Official service website URL"
    )

    class Config:
        json_schema_extra = {
            "example": {
                "service_name": "Ration Card Application",
                "service_description": "Apply for new ration card to access subsidized food grains",
                "how_to_apply": "Fill application form, attach documents, submit to local office",
                "eligibility_criteria": "Must be West Bengal resident, head of family",
                "required_documents": "Aadhaar card, address proof, income certificate, passport photo",
                "fees_and_timeline": "No fees. Processing takes 30-45 days",
                "operator_tips": "Verify Aadhaar carefully, check address proof is recent",
                "troubleshooting": "If Aadhaar not linked, help citizen link it first",
                "service_link": "https://wb.gov.in/ration-card",
            }
        }


class PDFVideoGenerationRequest(BaseModel):
    """Request schema for PDF-based video generation"""

    service_name: str = Field(
        ..., description="Service name (must match service_master)"
    )
    use_openai: bool = Field(False, description="Use OpenAI instead of Gemini")


class VideoListItem(BaseModel):
    """Schema for a single video in list response"""

    video_id: int
    service_id: Optional[int]
    service_name: str
    video_version: int
    source_type: str
    video_url: str
    file_size_mb: Optional[float]
    duration_seconds: Optional[float]
    total_slides: Optional[int]
    is_new: bool
    is_done: bool
    created_at: datetime
    ai_enhanced: bool = Field(False, description="Whether AI enhancement was used")

    class Config:
        from_attributes = True


class VideoListResponse(BaseModel):
    """Response schema for listing videos"""

    total: int
    videos: List[VideoListItem]
    filters_applied: Optional[Dict[str, Any]] = None


class VideoDetailsResponse(BaseModel):
    """Detailed response for a single video"""

    video_id: int
    service_id: Optional[int]
    service_name: str
    video_version: int
    source_type: str

    # Paths
    video_path: str
    video_url: str

    # Metadata
    file_size_mb: Optional[float]
    duration_seconds: Optional[float]
    total_slides: Optional[int]
    resolution: Optional[str]
    fps: Optional[int]

    # Generation info
    pdf_file_name: Optional[str]
    form_data: Optional[Dict[str, Any]]
    ai_model_used: Optional[str]

    # Status
    is_new: bool
    is_done: bool
    is_active: bool

    # Timestamps
    created_at: datetime
    updated_at: Optional[datetime]

    # Access stats
    view_count: int
    last_accessed_at: Optional[datetime]

    # Error info
    error_message: Optional[str]

    class Config:
        from_attributes = True


class SlideContent(BaseModel):
    """Schema for a single slide"""

    slide_no: int
    title: str
    bullets: List[str]
    image_keyword: str


class SlideGenerationResponse(BaseModel):
    """Response schema for AI slide generation (testing endpoint)"""

    success: bool
    total_slides: int
    slides: List[SlideContent]
    ai_model_used: str
    processing_time_seconds: Optional[float] = None


class SourceTypeBreakdown(BaseModel):
    """Breakdown of videos by source type"""
    source_type: str
    count: int
    percentage: float


class ServiceVideoSummary(BaseModel):
    """Summary statistics for a specific service"""
    service_id: Optional[int]
    service_name: str
    total_versions: int
    latest_version: int
    is_active: bool
    is_completed: bool
    created_at: datetime
    source_type: str


class VideoAnalyticsResponse(BaseModel):
    """Comprehensive analytics response for service videos"""
    
    # Overall Statistics
    total_videos: int
    total_services_with_videos: int
    total_unique_services: int  # Services that have videos
    
    # Status Breakdown
    videos_completed: int  # is_done = True
    videos_in_progress: int  # is_done = False
    videos_active: int  # is_active = True
    videos_inactive: int  # is_active = False
    
    # Version Statistics
    total_versions: int  # Sum of all versions
    services_with_multiple_versions: int
    average_versions_per_service: float
    
    # Source Type Breakdown
    source_type_breakdown: List[SourceTypeBreakdown]
    
    # AI Enhancement Statistics
    ai_enhanced_videos: int
    ai_models_used: Dict[str, int]  # {"gemini-2.0-flash-exp": 10, "gpt-4": 5}
    
    # Storage Statistics
    total_storage_mb: float
    average_file_size_mb: float
    total_duration_hours: float
    average_duration_minutes: float
    
    # Access Statistics
    total_views: int
    most_viewed_service: Optional[str]
    most_viewed_count: int
    
    # Recent Activity
    videos_created_last_7_days: int
    videos_created_last_30_days: int
    latest_video_created_at: Optional[datetime]
    
    # Service List (optional, can be paginated)
    services: Optional[List[ServiceVideoSummary]] = None
    
    class Config:
        from_attributes = True


class ServiceWithVideoStatus(BaseModel):
    """Individual service with its video generation status"""
    service_id: int
    service_name: str
    department_name: Optional[str]
    is_active: bool
    
    # Video status
    has_video: bool
    video_count: int  # Number of versions
    latest_video_version: Optional[int]
    video_is_completed: Optional[bool]  # is_done status of latest video
    video_is_active: Optional[bool]  # is_active status of latest video
    video_url: Optional[str]  # URL of latest video
    video_created_at: Optional[datetime]
    source_type: Optional[str]  # How video was generated


class DepartmentVideoStats(BaseModel):
    """Video statistics by department"""
    department_id: Optional[int]
    department_name: str
    total_services: int
    services_with_videos: int
    services_without_videos: int
    coverage_percentage: float
    completed_videos: int
    active_videos: int


class VideoAnalyticsResponse(BaseModel):
    """Comprehensive analytics comparing all services against generated videos"""
    
    # ========================================================================
    # SERVICE OVERVIEW
    # ========================================================================
    total_no_of_services: int  # Total services in service_master
    active_services: int  # Services with is_active=1
    inactive_services: int  # Services with is_active=0
    
    # ========================================================================
    # VIDEO GENERATION COVERAGE
    # ========================================================================
    services_with_videos: int  # Services that have at least one video
    services_without_videos: int  # Services with no videos
    video_coverage_percentage: float  # (services_with_videos / total_services) * 100
    
    # ========================================================================
    # VIDEO STATUS BREAKDOWN
    # ========================================================================
    total_videos_generated: int  # Total video records
    videos_completed: int  # Videos with is_done=True
    videos_in_progress: int  # Videos with is_done=False
    active_videos: int  # Videos with is_active=True
    inactive_videos: int  # Videos with is_active=False
    
    # ========================================================================
    # ACTIVE SERVICE VIDEO STATUS
    # ========================================================================
    active_services_with_videos: int  # Active services that have videos
    active_services_without_videos: int  # Active services without videos
    active_service_video_coverage: float  # Coverage % for active services only
    
    # ========================================================================
    # VERSION STATISTICS
    # ========================================================================
    services_with_multiple_versions: int
    total_video_versions: int
    average_versions_per_service: float
    
    # ========================================================================
    # SOURCE TYPE BREAKDOWN
    # ========================================================================
    videos_by_source: Dict[str, int]  # {"pdf_automatic": 10, "form_ai_enhanced": 5, ...}
    
    # ========================================================================
    # DEPARTMENT-WISE BREAKDOWN
    # ========================================================================
    department_stats: List[DepartmentVideoStats]
    
    # ========================================================================
    # STORAGE & USAGE METRICS
    # ========================================================================
    total_storage_mb: float
    total_video_duration_minutes: float
    total_views: int
    most_viewed_service: Optional[str]
    
    # ========================================================================
    # RECENT ACTIVITY
    # ========================================================================
    videos_created_last_7_days: int
    videos_created_last_30_days: int
    
    # ========================================================================
    # DETAILED SERVICE LIST (Optional)
    # ========================================================================
    services: Optional[List[ServiceWithVideoStatus]] = None
    
    class Config:
        from_attributes = True


