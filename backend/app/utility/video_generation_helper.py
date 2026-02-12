"""
Modified Video Generation Helper - Async Background Processing

This is the updated version that supports async video generation.
Key changes from original:
1. Added background_video_generation() function for async processing
2. Queue manager integration for status tracking
3. Error handling with queue status updates
"""

import os
import uuid
import logging
from typing import List, Optional, Tuple
from fastapi import HTTPException
from io import BytesIO
from pathlib import Path
from sqlalchemy import desc, func
from sqlalchemy.orm import Session
from datetime import datetime
# Import your existing functions
from services.unsplash_service import fetch_and_save_photo
from utils.audio_utils import text_to_speech
from utils.video_utils import create_slide, combine_slides_and_audio
from utils.image_utils import prepare_slide_image, create_fallback_image
from app.models import models

# Import queue manager
from app.utility.video_queue_manager import queue_manager, VideoGenerationStatus

logger = logging.getLogger(__name__)

# Directories (same as before)
# UPLOAD_DIR = Path("uploads")
TEMP_DIR = Path("temp")
OUTPUT_DIR = Path("output_videos")
VIDEO_BASE_DIR = Path("videos")

# UPLOAD_DIR.mkdir(exist_ok=True)
TEMP_DIR.mkdir(exist_ok=True)
OUTPUT_DIR.mkdir(exist_ok=True)
VIDEO_BASE_DIR.mkdir(exist_ok=True)


# ============================================================================
# BACKGROUND VIDEO GENERATION (NEW!)
# ============================================================================

async def background_video_generation(
    video_id: str,
    slides: List[dict],
    service_name: str,
    service_id: int,
    db: Session,
):
    """
    Background task for video generation - runs asynchronously
    
    This function:
    1. Updates status to PROCESSING
    2. Generates video (20+ minutes)
    3. Saves to filesystem
    4. Creates database record
    5. Updates queue with COMPLETED status and video URL
    6. Handles errors by updating queue with FAILED status
    
    Args:
        video_id: Unique ID from queue
        slides: List of slide data
        service_name: Service name
        service_id: Service ID
        db: Database session
    """
    try:
        logger.info(f"🚀 Starting background video generation: {video_id}")
        
        # ================================================================
        # STEP 1: Update status to PROCESSING
        # ================================================================
        queue_manager.update_status(db, video_id, VideoGenerationStatus.PROCESSING)
        
        # ================================================================
        # STEP 2: Generate video (existing function)
        # ================================================================
        logger.info(f"🎬 Generating video for {service_name}...")
        result = await generate_video_from_slides(slides, service_name)
        video_bytes = result["video_bytes"]
        
        logger.info(f"✅ Video generated successfully")
        logger.info(f"   📊 Size: {result['file_size_mb']} MB")
        logger.info(f"   ⏱️  Duration: ~{result['duration_estimate']} seconds")
        logger.info(f"   🎞️  Slides: {result['total_slides']}")
        
        # ================================================================
        # STEP 3: Calculate next version
        # ================================================================
        next_version = get_next_version(service_id, service_name, db)
        logger.info(f"📌 Video version: v{next_version}")
        
        # ================================================================
        # STEP 4: Save video to filesystem
        # ================================================================
        logger.info("💾 Saving video to filesystem...")
        video_info = save_video_to_filesystem(
            video_bytes, service_name, next_version
        )
        logger.info(f"✅ Video saved: {video_info['video_path']}")
        
        # ================================================================
        # STEP 5: Create database record in service_videos
        # ================================================================
        logger.info("📝 Creating database record...")
        video_record = models.ServiceVideo(
            service_id=service_id,
            service_name_metadata=service_name,
            video_version=next_version,
            source_type="async_generated",
            video_path=video_info["video_path"],
            video_url=f"/api/videos/{service_name.replace(' ', '_')}/{next_version}",
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
            models.ServiceVideo.service_id == service_id,
            models.ServiceVideo.video_version != next_version,
        ).update({"is_new": False}, synchronize_session=False)
        db.commit()
        
        logger.info(f"✅ Database record created (ID: {video_record.video_id})")
        
        # ================================================================
        # STEP 6: Update queue with completed video details
        # ================================================================
        video_url = f"/api/videos/{service_name.replace(' ', '_')}/{next_version}"
        
        queue_manager.link_completed_video(
            db=db,
            video_id=video_id,
            video_record_id=video_record.video_id,
            video_url=video_url,
            video_path=video_info["video_path"],
            file_size_mb=result["file_size_mb"],
            duration_seconds=result["duration_estimate"],
            total_slides=result["total_slides"],
        )
        
        logger.info(f"🎉 Video generation complete: {video_id}")
        logger.info(f"   📹 URL: {video_url}")

        # ================================================================
        # STEP 7: Push completion result to external BSK API
        # ================================================================
        logger.info(f"📡 Pushing completion result to BSK API for {video_id}...")
        push_ok = queue_manager.push_completion_to_external_api(
            db=db,
            video_id=video_id,
        )
        if push_ok:
            logger.info(f"✅ BSK API notified successfully for {video_id}")
        else:
            # Non-fatal — video is still saved and accessible locally
            logger.warning(
                f"⚠️  BSK API push failed for {video_id}. "
                "Video is saved locally; BSK server was not notified."
            )
        
    except Exception as e:
        # ================================================================
        # ERROR HANDLING: Update queue with FAILED status
        # ================================================================
        error_msg = str(e)
        logger.error(f"❌ Video generation failed for {video_id}: {error_msg}")
        
        queue_manager.update_status(
            db=db,
            video_id=video_id,
            status=VideoGenerationStatus.FAILED,
            error_message=error_msg,
        )

        # Notify BSK API about the failure so it can handle it on their end
        logger.info(f"📡 Pushing FAILED status to BSK API for {video_id}...")
        try:
            queue_manager.push_completion_to_external_api(
                db=db,
                video_id=video_id,
            )
        except Exception as push_err:
            logger.error(f"❌ Also failed to push error status to BSK API: {push_err}")
        
        # Re-raise to ensure the background task shows as failed
        raise


# ============================================================================
# EXISTING FUNCTIONS (UNCHANGED)
# ============================================================================

async def generate_video_from_slides(slides: List[dict], service_name: str) -> dict:
    """
    Generate video from structured slide data.
    Returns video as BytesIO (in-memory) instead of saving to disk.
    
    [Same implementation as before]
    """
    try:
        if not slides:
            raise ValueError("No slides provided for video generation")

        logger.info(f"🎬 Generating video with {len(slides)} slides (IN-MEMORY)")

        video_clips = []
        audio_paths = []

        for i, slide in enumerate(slides, 1):
            logger.info(
                f"🎥 Processing slide {i}/{len(slides)}: {slide.get('title', 'Untitled')}"
            )

            # Get or create image
            try:
                image_keyword = slide.get("image_keyword", "government training")
                image_path = fetch_and_save_photo(image_keyword)
                processed_image = prepare_slide_image(image_path)
            except Exception as e:
                logger.warning(f"⚠️ Image fetch failed, using fallback: {e}")
                processed_image = create_fallback_image(
                    output_path=str(TEMP_DIR / f"fallback_{i}.jpg")
                )

            # Generate narration
            title = slide.get("title", "")
            bullets = slide.get("bullets", [])
            narration_text = f"{title}. " + " ".join(bullets)

            # Generate audio
            logger.info(f"Generating audio for slide {i}...")
            audio_path = await text_to_speech(narration_text)
            audio_paths.append(audio_path)

            # Create video slide
            logger.info(f"🎞️ Creating video clip for slide {i}...")
            video_clip = create_slide(
                title=title,
                points=bullets,
                image_path=processed_image,
                audio_file=audio_path,
            )
            video_clips.append(video_clip)

        # Combine all slides - save to TEMP first
        logger.info("🎬 Combining slides into temporary video...")
        temp_filename = f"temp_{uuid.uuid4().hex[:8]}.mp4"
        temp_video_path = str(TEMP_DIR / temp_filename)

        # Use existing combine function but to temp location
        final_video_path = combine_slides_and_audio(
            video_clips=video_clips,
            audio_paths=audio_paths,
            service_name=temp_filename.replace(".mp4", ""),
        )

        # Read video into memory
        logger.info("📦 Loading video into memory...")
        with open(final_video_path, "rb") as f:
            video_bytes = BytesIO(f.read())

        # Get file size
        file_size_mb = os.path.getsize(final_video_path) / (1024 * 1024)

        # Cleanup temp video file
        os.remove(final_video_path)

        # Cleanup temp audio files
        logger.info("🧹 Cleaning up temporary files...")
        for audio_path in audio_paths:
            try:
                if os.path.exists(audio_path):
                    os.remove(audio_path)
            except Exception as e:
                logger.warning(f"Failed to cleanup {audio_path}: {e}")

        logger.info(f"✅ Video generation complete (IN-MEMORY)")

        return {
            "success": True,
            "video_bytes": video_bytes,
            "file_size_mb": round(file_size_mb, 2),
            "total_slides": len(slides),
            "duration_estimate": sum([len(s.get("bullets", [])) * 3 for s in slides]),
        }

    except Exception as e:
        logger.error(f"❌ Video generation failed: {str(e)}")
        raise


def validate_and_match_service(
    service_name: str, db: Session
) -> Tuple[Optional[int], str]:
    """
    Validates service name and tries to match with existing services.
    [Same implementation as before]
    """
    clean_name = service_name.strip()
    if not clean_name:
        raise HTTPException(
            status_code=400, 
            detail="Service name cannot be empty"
        )

    existing_service = (
        db.query(models.ServiceMaster)
        .filter(func.lower(models.ServiceMaster.service_name) == func.lower(clean_name))
        .first()
    )

    if existing_service:
        logger.info(
            f"✅ MATCHED existing service: '{existing_service.service_name}' "
            f"(ID: {existing_service.service_id})"
        )
        return (existing_service.service_id, existing_service.service_name)
    else:
        logger.info(f"✨ NEW service detected: '{clean_name}'")
        return (None, clean_name)


def get_next_version(service_id: Optional[int], service_name: str, db: Session) -> int:
    """
    Calculate next version number for a service.
    [Same implementation as before]
    """
    latest = None
    
    if service_id:
        latest = (
            db.query(models.ServiceVideo)
            .filter(models.ServiceVideo.service_id == service_id)
            .order_by(desc(models.ServiceVideo.video_version))
            .first()
        )
    
    if not latest:
        latest = (
            db.query(models.ServiceVideo)
            .filter(models.ServiceVideo.service_name_metadata == service_name)
            .order_by(desc(models.ServiceVideo.video_version))
            .first()
        )

    next_version = latest.video_version + 1 if latest else 1
    
    logger.info(
        f"📌 Version calculated: v{next_version} "
        f"(previous: {latest.video_version if latest else 'none'})"
    )
    
    return next_version


def save_video_to_filesystem(
    video_bytes: BytesIO, 
    service_name: str, 
    version: int,
    base_dir: Path = None
) -> dict:
    """
    Save video to filesystem with structure: videos/<service_name>/<version>.mp4
    [Same implementation as before]
    """
    if not video_bytes or video_bytes.getbuffer().nbytes == 0:
        raise ValueError("video_bytes is empty - cannot save empty video")

    if base_dir is None:
        base_dir = VIDEO_BASE_DIR
    
    base_dir = Path(base_dir)

    safe_service_name = (
        service_name
        .replace(" ", "_")
        .replace("/", "_")
        .replace("\\", "_")
        .replace(":", "_")
        .replace("*", "_")
        .replace("?", "_")
        .replace('"', "_")
        .replace("<", "_")
        .replace(">", "_")
        .replace("|", "_")
    )

    service_dir = base_dir / safe_service_name
    
    try:
        service_dir.mkdir(parents=True, exist_ok=True)
        logger.info(f"📁 Service directory: {service_dir}")
    except Exception as e:
        raise OSError(f"Failed to create directory {service_dir}: {e}")

    filename = f"{version}.mp4"
    video_path = service_dir / filename

    if video_path.exists():
        logger.warning(
            f"⚠️  File {video_path} already exists - will be overwritten"
        )

    try:
        video_bytes.seek(0)
        with open(video_path, "wb") as f:
            bytes_written = f.write(video_bytes.read())
        
        logger.info(f"💾 Video saved: {video_path} ({bytes_written:,} bytes)")
        
        if not video_path.exists():
            raise OSError(f"File {video_path} was not created")
        
        file_size = os.path.getsize(video_path)
        if file_size == 0:
            raise OSError(f"File {video_path} is empty after write")
            
        logger.info(f"✅ Verified: {file_size:,} bytes written")
        
    except Exception as e:
        if video_path.exists():
            try:
                os.remove(video_path)
                logger.info(f"🧹 Cleaned up failed write: {video_path}")
            except:
                pass
        raise OSError(f"Failed to write video file: {e}")

    try:
        relative_path = str(video_path.relative_to(base_dir))
    except ValueError:
        relative_path = f"{safe_service_name}/{filename}"

    return {
        "video_path": str(video_path),
        "relative_path": relative_path,
        "filename": filename,
        "service_folder": safe_service_name,
        "file_size_bytes": os.path.getsize(video_path),
    }