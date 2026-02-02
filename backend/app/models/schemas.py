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


class DepartmentMaster(BaseModel):
    department_id: int
    department_name: str

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


class CitizenMaster(BaseModel):
    citizen_id: int
    citizen_phone_no: str
    citizen_name: str
    alternative_phone_no: Optional[str] = None
    email: Optional[str] = None
    father_guardian_name: str
    district: str
    block_municipality: str
    post_office: str
    police_station: str
    house_no: str
    gender: str
    date_of_birth: datetime
    age: int
    caste: str
    religion: str

    class Config:
        from_attributes = True


class DistrictMaster(BaseModel):
    district_id: int
    district_name: str
    slave_db: str
    district_code: str

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


class BSKTransaction(BaseModel):
    transaction_id: int
    transaction_date: datetime
    bsk_code: str
    transaction_amount: float
    transaction_time: datetime
    deo_code: str
    deo_name: str
    customer_id: int
    customer_name: str
    customer_phone: str
    service_id: int
    service_name: str

    class Config:
        from_attributes = True


class BlockMunicipality(BaseModel):
    block_muni_id: int
    block_muni_name: Optional[str]
    sub_div_id: Optional[int]
    district_id: Optional[int]
    bm_type: Optional[str]

    class Config:
        from_attributes = True


class CitizenMasterV2(BaseModel):
    citizen_id: str
    citizen_phone: Optional[str]
    citizen_name: Optional[str]
    alt_phone: Optional[str]
    email: Optional[str]
    guardian_name: Optional[str]
    district_id: Optional[int]
    sub_div_id: Optional[int]
    gp_id: Optional[int]
    gender: Optional[str]
    dob: Optional[str]
    age: Optional[int]
    caste: Optional[str]
    religion: Optional[str]

    class Config:
        from_attributes = True


class District(BaseModel):
    district_id: int
    district_name: Optional[str]
    district_code: Optional[str]
    grp: Optional[str]

    class Config:
        from_attributes = True


class GPWardMaster(BaseModel):
    gp_id: int
    district_id: Optional[str]
    sub_div_id: Optional[int]
    block_muni_id: Optional[str]
    gp_ward_name: Optional[str]

    class Config:
        from_attributes = True


class PostOfficeMaster(BaseModel):
    post_office_id: int
    post_office_name: Optional[str]
    pin_code: Optional[str]
    district_id: Optional[int]

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


# ============================================================================
# UPDATED: VideoGenerationResponse
# ============================================================================


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


# ============================================================================
# NEW: Form Generation Request Schema
# ============================================================================


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


# ============================================================================
# NEW: PDF Generation Request Schema
# ============================================================================


class PDFVideoGenerationRequest(BaseModel):
    """Request schema for PDF-based video generation"""

    service_name: str = Field(
        ..., description="Service name (must match service_master)"
    )
    use_openai: bool = Field(False, description="Use OpenAI instead of Gemini")


# ============================================================================
# NEW: Video List Response Schema
# ============================================================================


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


# ============================================================================
# NEW: Video Details Response Schema
# ============================================================================


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


# ============================================================================
# NEW: Slide Generation Response (for testing/debugging)
# ============================================================================


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
