import os
import sys
import logging
from pathlib import Path
from datetime import datetime, timezone
from contextlib import asynccontextmanager
from typing import Optional, List

# =============================================================================
# THIRD-PARTY IMPORTS
# =============================================================================
import pandas as pd
from dotenv import load_dotenv

from fastapi import (
    FastAPI,
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

# =============================================================================
# LOCAL APPLICATION IMPORTS
# =============================================================================

# Database & Models
from app.models import models, schemas
from app.models.database import SessionLocal, engine, get_db

# Utility Functions
from app.utility.helper_functions import fetch_all_master_data
from app.utility.training_helper_function import (
    enrich_recommendation,
    compute_and_cache_recommendations,
)

# Video Generation & Queue
from app.utility.video_generation_helper import (
    background_video_generation,
    validate_and_match_service,
)
from app.utility.video_queue_manager import queue_manager

# Sync & Scheduler
from app.sync.scheduler import (
    start_scheduler,
    stop_scheduler,
)
from app.sync.service import SyncService

# PDF & Content Utilities
from utils.pdf_extractor import extract_raw_content
from utils.pdf_validator import validate_pdf_content
from utils.service_utils import validate_form_content

# AI / External Services
from services.gemini_service import (
    generate_slides_from_form,
    generate_slides_from_raw,
)

# =============================================================================
# EXTERNAL / AI & ANALYTICS MODULE PATH CONFIGURATION
# =============================================================================
sys.path.append(
    os.path.abspath(os.path.join(os.path.dirname(__file__), "../../ai_service"))
)

from bsk_analytics import find_underperforming_bsks

# =============================================================================
# LOGGER CONFIGURATION
# =============================================================================
logger = logging.getLogger(__name__)


# Loading the environment variables
load_dotenv()
# APPLICATION CONFIGURATION
BASE_URL = os.getenv("BASE_URL", "http://localhost:54300")
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
# SERVICE TRAINING RECOMMENDATION ENDPOINT WITH AUTH FOR SUPERUSER AND DEO
# ============================================================================

from sqlalchemy import text


@app.get("/health")
async def health_check():
    health_status = {"status": "healthy", "service": "TrainAI", "database": "unknown"}

    try:
        db = SessionLocal()
        db.execute(text("SELECT 1"))
        db.close()
        health_status["database"] = "connected"
    except Exception as e:
        health_status["database"] = "error"
        health_status["status"] = "unhealthy"

    return health_status


# ============================================================================
# SERVICE TRAINING RECOMMENDATION ENDPOINT
# ============================================================================


# ENDPOINT 1: Training Recommendation Endpoint
@app.get("/service_training_recommendation/", tags=["Training Analytics"])
def service_training_recommendation(
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
    Get training recommendations for BSKs from the precomputed cache.
    
    **✨ This endpoint is READ-ONLY and FAST (50-100ms)**
    - Always uses cached data from the last precompute
    - No heavy computation
    - Ideal for dashboards and real-time queries
    
    **🔄 To refresh the data:**
    - Automatic: Every Sunday at 3:00 AM (scheduled)
    - Manual: POST /precompute/training-recommendations
    
    **Features:**
    - View all BSK recommendations
    - Filter by district, priority score, or minimum training services
    - Each service recommendation includes training video URL if available
    - Summary mode for quick statistics

    **Video Integration:**
    - If a service has training videos, the latest video URL is included
    - Video URL format: `/videos/<service_name>/<version>.mp4`
    - Only completed videos (is_done=True) are shown

    **Example Usage:**
    ```bash
    # Get all recommendations (fast - from cache)
    curl http://api/service_training_recommendation/

    # Filter by district
    curl "http://api/service_training_recommendation/?district_filter=Kolkata"

    # Get summary only
    curl "http://api/service_training_recommendation/?summary_only=true"

    # Filter high priority BSKs
    curl "http://api/service_training_recommendation/?min_priority=100"

    # To refresh data, use the POST endpoint:
    curl -X POST http://api/precompute/training-recommendations
    ```

    **Cache Info:**
    - Updated: Every Sunday at 3:00 AM
    - Window: Last 365 days of provisions
    - Manual refresh: POST /precompute/training-recommendations
    """
    logger.info("📊 GET /service_training_recommendation/ - Fetching from cache")

    # Build base query
    base_query = db.query(models.TrainingRecommendationCache)

    # Apply optional filters
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
        logger.info(f"Filtering by district: {district_filter} ({len(bsk_ids)} BSKs)")

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
            "message": "No recommendations found matching your filters. To refresh data, use POST /precompute/training-recommendations",
            "recommendations": [],
            "cache_info": {
                "refresh_endpoint": "POST /precompute/training-recommendations",
                "automatic_schedule": "Every Sunday at 3:00 AM",
            }
        }

    # Fetch all results
    precomp_res = base_query.order_by(
        desc(models.TrainingRecommendationCache.priority_score)
    ).all()

    logger.info(f"Retrieved {len(precomp_res)} recommendations from cache")

    # Enrich with master table data AND video URLs
    recommendations = [enrich_recommendation(rec, db) for rec in precomp_res]

    # Get cache timestamp
    cache_timestamp = precomp_res[0].timestamp.isoformat() if precomp_res else None

    # SUMMARY MODE
    if summary_only:
        all_matching = base_query.all()

        return {
            "status": "success",
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
                "sliding_window": "365 days (automatic)",
                "refresh_endpoint": "POST /precompute/training-recommendations",
                "automatic_schedule": "Every Sunday at 3:00 AM",
            },
        }

    # FULL RESPONSE
    return {
        "status": "success",
        "total_recommendations": total_count,
        "recommendations": recommendations,
        "cache_info": {
            "last_updated": cache_timestamp,
            "sliding_window": "365 days (automatic)",
            "refresh_endpoint": "POST /precompute/training-recommendations",
            "automatic_schedule": "Every Sunday at 3:00 AM",
        },
    }


# ENDPOINT 2: Training Recommendation all history logs Endpoint
@app.get("/training_recommendation_history/", tags=["Training Analytics"])
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


# ENDPOINT 3: Underperforming BSKs Endpoint
@app.get("/underperforming_bsks/", tags=["Training Analytics"])
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


# ==============================================================================================
# VIDEO GENERATION Endpoints
# ==============================================================================================


# ============================================================================
# 1. GENERATE VIDEO FROM FORM (ASYNC)
# ============================================================================
@app.post("/bsk_portal/generate_video_from_form/", tags=["Training video Generation"])
async def bsk_generate_video_from_form_async(
    background_tasks: BackgroundTasks,
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
    🎥 **ASYNC VIDEO GENERATION FROM FORM**

    **NEW WORKFLOW:**
    1. ✅ Validates form data and service name
    2. ✅ Generates AI-enhanced slides (quick, ~5 seconds)
    3. ✅ Returns unique video_id IMMEDIATELY
    4. 🔄 Video generation runs in background (~20 minutes)
    5. 📹 User polls /get_completed_videos to retrieve finished video

    **Returns immediately with:**
    - `video_id`: Unique identifier to track your video
    - `status`: "pending" or "processing"
    - `estimated_time_minutes`: Expected completion time (~20 mins)

    **To get your video:**
    - Poll: `GET /bsk_portal/get_completed_videos/`
    - Check specific: `GET /bsk_portal/video_status/{video_id}`

    **Benefits:**
    - ⚡ No timeout issues
    - 🔄 Multiple videos can generate simultaneously
    - 📊 Track progress via status endpoint
    - 🎯 Retrieve all completed videos at once

    **Required Fields:**
    - service_name
    - service_description
    - how_to_apply
    - eligibility_criteria
    - required_documents
    """

    logger.info("=" * 80)
    logger.info("🚀 ASYNC VIDEO GENERATION - FORM SUBMISSION")
    logger.info("=" * 80)

    try:
        # ================================================================
        # STEP 1: Build and validate form content
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

        logger.info("🔍 Validating form content...")
        is_valid, validation_msg = validate_form_content(service_content)

        if not is_valid:
            logger.error(f"❌ Validation failed: {validation_msg}")
            raise HTTPException(status_code=400, detail=validation_msg)

        logger.info("✅ Form validation passed")

        # ================================================================
        # STEP 2: Validate service name
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
        service_content["service_name"] = official_service_name

        # ================================================================
        # STEP 3: Generate AI-enhanced slides (QUICK - runs synchronously)
        # ================================================================
        logger.info("🤖 Generating AI-enhanced slides...")
        try:
            slide_data = generate_slides_from_form(service_content)
            slides = slide_data.get("slides", [])

            if not slides:
                raise ValueError("No slides generated by AI")

            logger.info(f"✅ AI generated {len(slides)} professional slides")

        except Exception as e:
            logger.error(f"❌ AI enhancement failed: {str(e)}")
            raise HTTPException(
                status_code=500, detail=f"AI content enhancement failed: {str(e)}"
            )

        # ================================================================
        # STEP 4: Create video generation request in queue
        # ================================================================
        logger.info("📝 Creating video generation request...")
        video_id = queue_manager.create_video_request(
            db=db,
            service_id=matched_service_id,
            service_name=official_service_name,
            source_type="form_ai_enhanced",
            request_data=service_content,  # Store for reference
        )

        logger.info(f"✅ Video request created: {video_id}")

        # ================================================================
        # STEP 5: Start background video generation
        # ================================================================
        logger.info("🎬 Starting background video generation...")
        background_tasks.add_task(
            background_video_generation,
            video_id=video_id,
            slides=slides,
            service_name=official_service_name,
            service_id=matched_service_id,
            db=db,
        )

        # ================================================================
        # STEP 6: Return immediately with video_id
        # ================================================================
        logger.info("=" * 80)
        logger.info("✅ REQUEST ACCEPTED - VIDEO GENERATION IN PROGRESS")
        logger.info("=" * 80)

        return {
            "success": True,
            "message": "Video generation started in background",
            "video_id": video_id,
            "service_id": matched_service_id,
            "service_name": official_service_name,
            "status": "pending",
            "total_slides": len(slides),
            "estimated_time_minutes": len(slides) * 2,  # Rough estimate
            "next_steps": {
                "check_status": f"GET /bsk_portal/video_status/{video_id}",
                "get_completed": "GET /bsk_portal/get_completed_videos/",
                "poll_interval": "Check every 2-3 minutes",
            },
            "note": "Video generation takes ~20 minutes. Use the endpoints above to check progress and retrieve your video.",
        }

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"❌ Unexpected error: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


# ============================================================================
# 2. GENERATE VIDEO FROM PDF (ASYNC)
# ============================================================================
@app.post("/bsk_portal/generate_video_from_pdf/", tags=["Training video Generation"])
async def generate_video_from_pdf_async(
    background_tasks: BackgroundTasks,
    pdf_file: UploadFile = File(...),
    service_name: str = Form(
        ..., description="Service name (must match service_master)"
    ),
    use_openai: bool = Form(False),
    db: Session = Depends(get_db),
):
    """
    🎥 **ASYNC VIDEO GENERATION FROM PDF**

    **NEW WORKFLOW:**
    1. ✅ Uploads and validates PDF
    2. ✅ Extracts and validates content
    3. ✅ Generates AI-enhanced slides (quick, ~10 seconds)
    4. ✅ Returns unique video_id IMMEDIATELY
    5. 🔄 Video generation runs in background (~20 minutes)
    6. 📹 User polls /get_completed_videos to retrieve finished video

    **Returns immediately with:**
    - `video_id`: Unique identifier to track your video
    - `status`: "pending" or "processing"
    - `estimated_time_minutes`: Expected completion time

    **To get your video:**
    - Poll: `GET /bsk_portal/get_completed_videos/`
    - Check specific: `GET /bsk_portal/video_status/{video_id}`
    """

    logger.info("=" * 80)
    logger.info("🚀 ASYNC VIDEO GENERATION - PDF UPLOAD")
    logger.info("=" * 80)

    try:
        # ================================================================
        # STEP 1: Validate service name
        # ================================================================
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

        # ================================================================
        # STEP 2: Save and process PDF
        # ================================================================
        logger.info("📄 Processing PDF...")
        pdf_path = f"uploads/{pdf_file.filename}"
        os.makedirs("uploads", exist_ok=True)

        with open(pdf_path, "wb") as f:
            f.write(await pdf_file.read())

        # Extract content
        logger.info("📄 Extracting PDF content...")
        raw_pages = extract_raw_content(str(pdf_path))
        raw_text = "\n\n".join(["\n".join(page["lines"]) for page in raw_pages])

        # Validate content
        logger.info("🔍 Validating PDF content...")
        is_valid, validation_message = validate_pdf_content(raw_pages)

        if not is_valid:
            raise HTTPException(
                status_code=400,
                detail={
                    "error": "Invalid PDF Content",
                    "message": validation_message,
                    "hint": "Please upload a government service document.",
                },
            )

        logger.info(f"✅ {validation_message}")

        # ================================================================
        # STEP 3: Generate AI-enhanced slides (QUICK)
        # ================================================================
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

        # ================================================================
        # STEP 4: Create video generation request
        # ================================================================
        logger.info("📝 Creating video generation request...")
        video_id = queue_manager.create_video_request(
            db=db,
            service_id=matched_service_id,
            service_name=official_service_name,
            source_type="pdf_ai_enhanced",
            request_data={"filename": pdf_file.filename},
        )

        logger.info(f"✅ Video request created: {video_id}")

        # ================================================================
        # STEP 5: Start background video generation
        # ================================================================
        logger.info("🎬 Starting background video generation...")
        background_tasks.add_task(
            background_video_generation,
            video_id=video_id,
            slides=slides,
            service_name=official_service_name,
            service_id=matched_service_id,
            db=db,
        )

        # ================================================================
        # STEP 6: Return immediately
        # ================================================================
        logger.info("=" * 80)
        logger.info("✅ REQUEST ACCEPTED - VIDEO GENERATION IN PROGRESS")
        logger.info("=" * 80)

        return {
            "success": True,
            "message": "Video generation started in background",
            "video_id": video_id,
            "service_id": matched_service_id,
            "service_name": official_service_name,
            "status": "pending",
            "total_slides": len(slides),
            "estimated_time_minutes": len(slides) * 2,
            "next_steps": {
                "check_status": f"GET /bsk_portal/video_status/{video_id}",
                "get_completed": "GET /bsk_portal/get_completed_videos/",
                "poll_interval": "Check every 2-3 minutes",
            },
            "note": "Video generation takes ~20 minutes. Use the endpoints above to check progress and retrieve your video.",
        }

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"❌ Unexpected error: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


# ============================================================================
# 3. GET ALL Completed QUEUE
# ============================================================================
@app.get("/bsk_portal/get_completed_videos/", tags=["Training video Generation"])
def get_completed_videos(
    db: Session = Depends(get_db),
):
    """
    📹 **RETRIEVE ALL COMPLETED VIDEOS**

    **This is the main endpoint to check for finished videos.**

    Returns:
    - All videos that have completed generation
    - Videos are marked as "retrieved" after this call
    - Won't appear again in future calls (already downloaded)

    **Usage:**
    Poll this endpoint every 2-3 minutes to check for completed videos.

    **Response Format:**
    ```json
    {
        "success": true,
        "completed_videos": [
            {
                "video_id": "abc-123-def",
                "service_name": "Birth Certificate",
                "video_url": "/api/videos/Birth_Certificate/1",
                "file_size_mb": 45.2,
                "duration_seconds": 180,
                "total_slides": 8,
                "completed_at": "2024-02-10T14:30:00",
                "processing_time_seconds": 1200
            }
        ],
        "total_count": 1
    }
    ```

    **Important:**
    - Videos are removed from the queue after retrieval
    - Download/save videos immediately after retrieval
    - Videos older than 7 days may be auto-cleaned
    """

    try:
        logger.info("📋 Retrieving completed videos...")

        # ✅ Fetch completed but not yet retrieved videos
        completed_videos = (
            db.query(models.VideoGenerationQueue)
            .filter(
                models.VideoGenerationQueue.status == "completed",
                # models.VideoGenerationQueue.is_retrieved == False
            )
            .order_by(desc(models.VideoGenerationQueue.completed_at))
            .all()
        )
        # print(completed_videos)
        # Convert to response format
        response_data = []
        for video in completed_videos:
            response_data.append(
                {
                    "video_id": video.video_id,
                    "service_name": video.service_name,
                    "video_url": video.video_url,
                    "file_size_mb": video.file_size_mb,
                    "duration_seconds": video.duration_seconds,
                    "total_slides": video.total_slides,
                    "completed_at": video.completed_at,
                    # "processing_time_seconds": video.processing_time_seconds,
                }
            )

            # ✅ Mark as retrieved
            # video.is_retrieved = True

        # Commit changes
        # db.commit()

        logger.info(f"✅ Found {len(response_data)} completed videos")

        return {
            "success": True,
            "completed_videos": response_data,
            "total_count": len(response_data),
            "message": (
                (f"{len(response_data)} completed video(s) currently is queue. ")
                if response_data
                else "No completed videos available at this time."
            ),
        }

    except Exception as e:
        db.rollback()
        logger.error(f"❌ Error retrieving completed videos: {str(e)}")
        raise HTTPException(
            status_code=500, detail=f"Failed to retrieve completed videos: {str(e)}"
        )


# ============================================================================
# 4. CHECK SPECIFIC VIDEO STATUS
# ============================================================================
@app.get("/bsk_portal/video_status/{video_id}", tags=["Training video Generation"])
def check_video_status(
    video_id: str,
    db: Session = Depends(get_db),
):
    """
    🔍 **CHECK STATUS OF SPECIFIC VIDEO**

    Track the progress of a specific video generation request.

    **Possible Statuses:**
    - `pending`: Waiting to start
    - `processing`: Currently generating (~20 mins)
    - `completed`: Ready for download
    - `retrieved`: Already downloaded by user
    - `failed`: Generation failed (see error_message)

    **Response when completed:**
    ```json
    {
        "video_id": "abc-123",
        "status": "completed",
        "service_name": "Birth Certificate",
        "video_url": "/api/videos/Birth_Certificate/1",
        "file_size_mb": 45.2,
        "created_at": "2024-02-10T14:00:00",
        "completed_at": "2024-02-10T14:20:00"
    }
    ```

    **Usage:**
    ```bash
    GET /bsk_portal/video_status/abc-123-def-456
    ```
    """

    try:
        logger.info(f"🔍 Checking status for video: {video_id}")

        # Get status from queue
        status_info = queue_manager.get_request_status(db, video_id)

        if not status_info:
            raise HTTPException(
                status_code=404, detail=f"Video request not found: {video_id}"
            )

        logger.info(f"📊 Status: {status_info['status']}")

        return {
            "success": True,
            **status_info,
        }

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"❌ Error checking video status: {str(e)}")
        raise HTTPException(
            status_code=500, detail=f"Failed to check video status: {str(e)}"
        )


# ============================================================================
# 5. GET PENDING QUEUE (ADMIN/DEBUG)
# ============================================================================
@app.get("/bsk_portal/pending_videos/", tags=["Training video Generation"])
def get_pending_videos(
    limit: int = 50,
    db: Session = Depends(get_db),
):
    """
    📊 **VIEW PENDING/PROCESSING VIDEOS** (Admin/Debug)

    See all videos currently in the generation queue.

    **Use cases:**
    - Monitor queue length
    - Check system load
    - Debug stuck videos
    - Estimate wait times

    **Response:**
    ```json
    {
        "pending_videos": [
            {
                "video_id": "abc-123",
                "service_name": "Ration Card",
                "status": "processing",
                "created_at": "2024-02-10T14:00:00",
                "started_at": "2024-02-10T14:05:00"
            }
        ],
        "total_count": 1,
        "queue_health": "normal"
    }
    ```
    """

    try:
        pending = queue_manager.get_pending_requests(db, limit=limit)

        # Determine queue health
        if len(pending) == 0:
            queue_health = "empty"
        elif len(pending) < 5:
            queue_health = "normal"
        elif len(pending) < 20:
            queue_health = "busy"
        else:
            queue_health = "very_busy"

        return {
            "success": True,
            "pending_videos": pending,
            "total_count": len(pending),
            "queue_health": queue_health,
            "note": "Videos typically process in ~20 minutes each.",
        }

    except Exception as e:
        logger.error(f"❌ Error retrieving pending videos: {str(e)}")
        raise HTTPException(
            status_code=500, detail=f"Failed to retrieve pending videos: {str(e)}"
        )


# ============================================================================
# 6. Delete/Acknowledge Video (Called by BSK after successful retrieval)
# ============================================================================
@app.delete(
    "/bsk_portal/acknowledge_video/{video_id}",
    tags=["Training video Generation"],
)
def acknowledge_video(
    video_id: str,
    db: Session = Depends(get_db),
):
    """
    🧹 **ACKNOWLEDGE VIDEO — QUEUE CLEANUP**
    Called by the external BSK server **after** it has safely received and stored
    the video result that was pushed to it.
    **What it does:**
    - Permanently deletes the queue record for this `video_id`
    - Frees up space in the `VideoGenerationQueue` table
    - Safe to call for both completed and failed videos

    **When to call:**
    After the BSK server receives the POST to `/aiapi/push_completed_videos`
    and confirms it has stored the payload, it calls this endpoint with the
    same `video_id` to tell the AI server it can forget about this job.

    **Example:**
    ```
    DELETE /bsk_portal/acknowledge_video/abc-123-def
    ```

    **Response:**
    ```json
    {
        "success": true,
        "video_id": "abc-123-def",
        "message": "Queue entry deleted successfully"
    }
    ```

    **Idempotent behaviour:**
    If the `video_id` is not found (already deleted), a 404 is returned.
    """
    logger.info(f"🧹 Acknowledge request received for video_id: {video_id}")

    deleted = queue_manager.acknowledge_and_delete(db=db, video_id=video_id)

    if not deleted:
        raise HTTPException(
            status_code=404,
            detail=f"video_id '{video_id}' not found in queue. "
            "It may have already been deleted.",
        )

    logger.info(f"✅ Queue entry deleted for: {video_id}")

    return {
        "success": True,
        "video_id": video_id,
        "message": "Queue entry deleted successfully. Video generation job has been cleaned up.",
    }


# ============================================================================
# 7. GET VIDEO BY CLEAN URL
# ============================================================================
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
# SYNC API Endpoints
# ============================================================================


@app.get("/sync/status", tags=["Sync"])
def get_status(table_name: str = None, limit: int = 10, db: Session = Depends(get_db)):
    """
    Check sync status

    GET /sync/status
    GET /sync/status?table_name=provision
    GET /sync/status?limit=50
    """
    query = db.query(models.SyncCheckpoint).order_by(
        desc(models.SyncCheckpoint.last_sync_date)
    )

    if table_name:
        query = query.filter(models.SyncCheckpoint.table_name == table_name)

    logs = query.limit(limit).all()

    return {
        "logs": [
            {
                "table": log.table_name,
                # "type": log.sync_type,
                "status": log.sync_status,
                "started": log.last_sync_date.isoformat(),
                "duration": log.last_sync_duration_seconds,
                "fetched": log.total_records_synced,
                "inserted": log.total_sync_runs,
                "failed": log.total_failures,
                "error": log.error_message,
            }
            for log in logs
        ]
    }


@app.get("/sync", tags=["Sync"])
def sync_table(
    background_tasks: BackgroundTasks,
    table: str = Query(
        ...,
        description="Table name: bsk_master, deo_master, service_master, provision, or 'all'",
        example="provision",
    ),
    start_date: Optional[str] = Query(
        None,
        description="[PROVISION ONLY] Custom start date (YYYY-MM-DD). If not provided, uses checkpoint.",
        regex=r"^\d{4}-\d{2}-\d{2}$",
        example="2023-01-01",
    ),
    end_date: Optional[str] = Query(
        None,
        description="[PROVISION ONLY] Custom end date (YYYY-MM-DD). If not provided, uses today.",
        regex=r"^\d{4}-\d{2}-\d{2}$",
        example="2023-12-31",
    ),
    db: Session = Depends(get_db),
):
    """
    🎯 UNIFIED SYNC ENDPOINT - Sync any table or all tables with one API call

    **Supported Tables:**
    - `bsk_master` - Drop & reload strategy
    - `deo_master` - Drop & reload strategy
    - `service_master` - Drop & reload strategy
    - `provision` - Incremental sync with optional custom date range
    - `all` - Sync all tables in sequence

    **Usage Examples:**

    1️⃣ **Sync BSK Master** (drop & reload):
    ```bash
    GET /sync?table=bsk_master
    ```

    2️⃣ **Sync Provision** (normal incremental - uses checkpoint):
    ```bash
    GET /sync?table=provision
    ```

    3️⃣ **Sync Provision with custom date range** (first-time backfill):
    ```bash
    GET /sync?table=provision&start_date=2023-01-01&end_date=2023-12-31
    ```

    4️⃣ **Sync ALL tables**:
    ```bash
    GET /sync?table=all
    ```

    5️⃣ **Sync ALL tables with custom provision dates**:
    ```bash
    GET /sync?table=all&start_date=2023-01-01&end_date=2023-12-31
    ```

    **For Your Scheduler:**
    ```python
    # Daily sync - just call this
    requests.get("http://api/sync?table=all")

    # Or sync specific tables
    requests.get("http://api/sync?table=bsk_master")
    requests.get("http://api/sync?table=provision")
    ```

    **Validation:**
    - Table name must be one of: bsk_master, deo_master, service_master, provision, all
    - Dates only work with provision or all tables
    - Dates must be in YYYY-MM-DD format
    - start_date must be before end_date
    """
    try:
        # Validate table name
        valid_tables = [
            "bsk_master",
            "deo_master",
            "service_master",
            "provision",
            "all",
        ]
        if table not in valid_tables:
            raise HTTPException(
                status_code=400,
                detail=f"Invalid table name. Must be one of: {', '.join(valid_tables)}",
            )

        # Validate date range if both provided
        if start_date and end_date:
            start = datetime.strptime(start_date, "%Y-%m-%d")
            end = datetime.strptime(end_date, "%Y-%m-%d")

            if start > end:
                raise HTTPException(
                    status_code=400, detail="start_date must be before end_date"
                )

        # Warn if dates provided for non-provision tables
        if (start_date or end_date) and table not in ["provision", "all"]:
            raise HTTPException(
                status_code=400,
                detail=f"Date parameters only work with 'provision' or 'all' tables, not '{table}'",
            )

        service = SyncService(db)

        # Handle "all tables" request
        if table == "all":
            # Sync master tables first (drop & reload)
            background_tasks.add_task(service.sync_master_table, "bsk_master")
            background_tasks.add_task(service.sync_master_table, "deo_master")
            background_tasks.add_task(service.sync_master_table, "service_master")

            # Then sync provision (incremental)
            background_tasks.add_task(service.sync_provisions, start_date, end_date)

            return {
                "status": "started",
                "message": "All table syncs started in background",
                "tables": {
                    "bsk_master": "full_drop_reload",
                    "deo_master": "full_drop_reload",
                    "service_master": "full_drop_reload",
                    "provision": "incremental",
                },
                "provision_dates": {
                    "start_date": start_date or "from checkpoint",
                    "end_date": end_date or "today",
                },
                "check_status": f"GET /sync/status",
            }

        # Handle individual table sync
        if table == "provision":
            background_tasks.add_task(service.sync_provisions, start_date, end_date)
            sync_type = "incremental"
            strategy = f"Incremental sync from {start_date or 'checkpoint'} to {end_date or 'today'}"

        elif table in ["bsk_master", "deo_master", "service_master"]:
            background_tasks.add_task(service.sync_master_table, table)
            sync_type = "full_drop_reload"
            strategy = "Drop & reload (TRUNCATE + INSERT)"

        return {
            "status": "started",
            "message": f"{table} sync started in background",
            "table": table,
            "sync_type": sync_type,
            "strategy": strategy,
            "dates": {
                "start_date": start_date
                or ("from checkpoint" if table == "provision" else "N/A"),
                "end_date": end_date or ("today" if table == "provision" else "N/A"),
            },
            "check_status": f"GET /sync/status?table={table}",
        }

    except ValueError as ve:
        raise HTTPException(status_code=400, detail=f"Invalid date format: {ve}")
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to start sync: {str(e)}")


# ============================================================================
# For Precomputation AutoSchedular
# ============================================================================
@app.post("/precompute/training-recommendations", tags=["Training Analytics"])
def trigger_training_precompute(
    background_tasks: BackgroundTasks,
    db: Session = Depends(get_db),
):
    """
    Manually trigger training recommendations precomputation.
    
    **🔄 This endpoint COMPUTES and CACHES data (takes 10-30 seconds)**
    - Runs the heavy analytics computation
    - Analyzes last 365 days of provisions (~1.6M records)
    - Caches results for fast retrieval via GET endpoint
    
    **🚀 AUTOMATIC OPTIMIZATION:**
    - Uses 365-day sliding window (fixed, no configuration needed)
    - Keeps computation time constant (10-30s) regardless of database age

    **What it does:**
    1. Analyzes provision data from the last 365 days
    2. Identifies underperforming BSKs compared to nearby centers
    3. Generates personalized training recommendations
    4. Caches results in TrainingRecommendationCache table
    5. Takes 10-30 seconds to complete (runs in background)

    **When to use:**
    - After major data imports or updates
    - When you need fresh recommendations immediately
    - Before important reporting periods
    - For testing the precompute system
    
    **You DON'T need this if:**
    - Just viewing recommendations (use GET endpoint)
    - It's already Sunday (automatic update at 3:00 AM)
    - Cache is fresh (< 1 week old)

    **After running this:**
    - Data is cached and available instantly via GET /service_training_recommendation/
    - Cache stays fresh for ~1 week
    - Next automatic update: Sunday at 3:00 AM

    **Example Usage:**
    ```bash
    # Trigger precompute (runs in background)
    curl -X POST http://localhost:8000/precompute/training-recommendations

    # Check if it's completed
    curl http://localhost:8000/precompute/status

    # Get the fresh results (fast, from cache)
    curl http://localhost:8000/service_training_recommendation/
    ```
    """
    logger.info("🚀 Manual training precompute triggered (365-day sliding window)")

    # Check if cache is fresh (optional - prevent unnecessary recomputes)
    from datetime import datetime, timezone, timedelta
    
    latest_cache = db.query(models.TrainingRecommendationCache)\
        .order_by(desc(models.TrainingRecommendationCache.timestamp))\
        .first()
    
    if latest_cache and latest_cache.timestamp:
        age = datetime.now(timezone.utc) - latest_cache.timestamp.replace(tzinfo=timezone.utc)
        if age < timedelta(hours=1):
            return {
                "success": False,
                "message": "Cache was updated less than 1 hour ago. Please wait before triggering again.",
                "cache_age_minutes": int(age.total_seconds() / 60),
                "recommendation": "Use GET /service_training_recommendation/ to fetch current data",
            }

    # Define background task with FIXED 365-day window
    def run_precompute():
        compute_and_cache_recommendations(
            db=db,
            n_neighbors=10,
            top_n_services=10,
            min_provision_threshold=5,
            lookback_days=365,  # ✅ FIXED: Always 365 days
        )

    # Run in background to avoid timeout
    background_tasks.add_task(run_precompute)

    return {
        "success": True,
        "message": "Training recommendations precompute started in background",
        "status": "running",
        "optimization": {
            "sliding_window": "365 days (automatic)",
            "estimated_provisions": "~1.6M (constant)",
            "estimated_time": "10-30 seconds",
            "note": "Processing only last 365 days instead of ALL historical data",
        },
        "schedule": {
            "automatic_weekly": "Every Sunday at 3:00 AM",
            "manual_trigger": "POST /precompute/training-recommendations (this endpoint)",
        },
        "next_steps": [
            "Wait 10-30 seconds for completion",
            "Check status: GET /precompute/status",
            "Get results: GET /service_training_recommendation/",
        ],
    }