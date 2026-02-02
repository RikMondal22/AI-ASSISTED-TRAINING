import os
import uuid
import logging
from typing import List,Optional,Tuple
from fastapi import HTTPException
from io import BytesIO

from sqlalchemy import desc, func
from services.unsplash_service import fetch_and_save_photo
from utils.audio_utils import text_to_speech
from utils.video_utils import create_slide, combine_slides_and_audio
from utils.image_utils import prepare_slide_image, create_fallback_image
from pathlib import Path
from app.models import models
from sqlalchemy.orm import Session
logger = logging.getLogger(__name__)
UPLOAD_DIR = Path("uploads")
TEMP_DIR = Path("temp")
OUTPUT_DIR = Path("output_videos")

# # Create directories if they don't exist
UPLOAD_DIR.mkdir(exist_ok=True)
TEMP_DIR.mkdir(exist_ok=True)
OUTPUT_DIR.mkdir(exist_ok=True)
async def generate_video_from_slides(slides: List[dict], service_name: str) -> dict:
    """
    Generate video from structured slide data.
    Returns video as BytesIO (in-memory) instead of saving to disk.

    Args:
        slides: List of slide dictionaries with title, bullets, image_keyword
        service_name: Name for video metadata

    Returns:
        dict with video_bytes (BytesIO), total_slides, duration_estimate
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
            "video_bytes": video_bytes,  # BytesIO object
            "file_size_mb": round(file_size_mb, 2),
            "total_slides": len(slides),
            "duration_estimate": sum([len(s.get("bullets", [])) * 3 for s in slides]),
        }

    except Exception as e:
        logger.error(f"❌ Video generation failed: {str(e)}")
        raise


# Helper functions for video generation and service matching
# ============================================================================
# 1. SERVICE VALIDATION
# ============================================================================

def validate_and_match_service(
    service_name: str, db: Session
) -> Tuple[Optional[int], str]:
    """
    Validates service name and tries to match with existing services.

    Args:
        service_name: Service name from form input
        db: Database session

    Returns:
        tuple: (service_id or None, official_service_name)
        
    Raises:
        HTTPException: If service name is empty or invalid
    """
    # Clean and validate input
    clean_name = service_name.strip()
    if not clean_name:
        raise HTTPException(
            status_code=400, 
            detail="Service name cannot be empty"
        )

    # ✅ GOOD: Try exact match (case-insensitive)
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


# ============================================================================
# 2. VERSION MANAGEMENT
# ============================================================================

def get_next_version(service_id: Optional[int], service_name: str, db: Session) -> int:
    """
    Calculate next version number for a service.

    Args:
        service_id: ID from service_master (if exists)
        service_name: Service name (used as fallback)
        db: Database session

    Returns:
        int: Next version number (1 for new services)
    """
    latest = None
    
    # Try to find latest version by service_id first (more reliable)
    if service_id:
        latest = (
            db.query(models.ServiceVideo)
            .filter(models.ServiceVideo.service_id == service_id)
            .order_by(desc(models.ServiceVideo.video_version))
            .first()
        )
    
    # ⚠️ ISSUE FOUND: If service_id is None, fallback to name matching
    # But this could cause issues if service_name changes slightly
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


# ============================================================================
# 3. VIDEO FILE STORAGE
# ============================================================================

def save_video_to_filesystem(
    video_bytes: BytesIO, 
    service_name: str, 
    version: int,
    base_dir: Path = None
) -> dict:
    """
    Save video to filesystem with structure: videos/<service_name>/<version>.mp4

    Args:
        video_bytes: BytesIO object containing video data
        service_name: Service name (used for folder name)
        version: Version number (used for filename)
        base_dir: Base directory for videos (defaults to VIDEO_BASE_DIR from config)

    Returns:
        dict: {
            "video_path": str (absolute path),
            "relative_path": str (relative to base_dir),
            "filename": str,
            "service_folder": str
        }

    Raises:
        ValueError: If video_bytes is empty or invalid
        OSError: If file write fails
    """
    # ✅ VALIDATION: Check if video_bytes has content
    if not video_bytes or video_bytes.getbuffer().nbytes == 0:
        raise ValueError("video_bytes is empty - cannot save empty video")

    # Use provided base_dir or fall back to config
    if base_dir is None:
        base_dir = VIDEO_BASE_DIR
    
    base_dir = Path(base_dir)

    # ✅ IMPROVEMENT: Better sanitization of service name for folder
    # Replace spaces, slashes, and other problematic characters
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

    # Create service-specific directory
    service_dir = base_dir / safe_service_name
    
    try:
        service_dir.mkdir(parents=True, exist_ok=True)
        logger.info(f"📁 Service directory: {service_dir}")
    except Exception as e:
        raise OSError(f"Failed to create directory {service_dir}: {e}")

    # Create video filename
    filename = f"{version}.mp4"
    video_path = service_dir / filename

    # ✅ IMPROVEMENT: Check if file already exists
    if video_path.exists():
        logger.warning(
            f"⚠️  File {video_path} already exists - will be overwritten"
        )

    # Save video to file
    try:
        video_bytes.seek(0)  # Reset to beginning
        with open(video_path, "wb") as f:
            bytes_written = f.write(video_bytes.read())
        
        logger.info(f"💾 Video saved: {video_path} ({bytes_written:,} bytes)")
        
        # ✅ VALIDATION: Verify file was written
        if not video_path.exists():
            raise OSError(f"File {video_path} was not created")
        
        file_size = os.path.getsize(video_path)
        if file_size == 0:
            raise OSError(f"File {video_path} is empty after write")
            
        logger.info(f"✅ Verified: {file_size:,} bytes written")
        
    except Exception as e:
        # Cleanup on failure
        if video_path.exists():
            try:
                os.remove(video_path)
                logger.info(f"🧹 Cleaned up failed write: {video_path}")
            except:
                pass
        raise OSError(f"Failed to write video file: {e}")

    # Get relative path for URL generation
    try:
        relative_path = str(video_path.relative_to(base_dir))
    except ValueError:
        # Fallback if paths are on different drives
        relative_path = f"{safe_service_name}/{filename}"

    return {
        "video_path": str(video_path),
        "relative_path": relative_path,
        "filename": filename,
        "service_folder": safe_service_name,
        "file_size_bytes": os.path.getsize(video_path),
    }


# ============================================================================
# 4. ADDITIONAL HELPER: DELETE OLD VERSIONS (OPTIONAL)
# ============================================================================

def cleanup_old_versions(
    service_name: str, 
    keep_latest_n: int = 3,
    base_dir: Path = None
) -> dict:
    """
    Optional: Clean up old video versions, keeping only the latest N versions.

    Args:
        service_name: Service name
        keep_latest_n: Number of latest versions to keep
        base_dir: Base video directory

    Returns:
        dict: {
            "deleted_count": int,
            "deleted_files": list,
            "kept_versions": list
        }
    """
    if base_dir is None:
        base_dir = VIDEO_BASE_DIR
    
    base_dir = Path(base_dir)
    
    safe_service_name = service_name.replace(" ", "_").replace("/", "_")
    service_dir = base_dir / safe_service_name

    if not service_dir.exists():
        return {
            "deleted_count": 0,
            "deleted_files": [],
            "kept_versions": []
        }

    # Get all .mp4 files
    video_files = sorted(
        service_dir.glob("*.mp4"),
        key=lambda p: int(p.stem),  # Sort by version number
        reverse=True  # Newest first
    )

    kept_files = video_files[:keep_latest_n]
    files_to_delete = video_files[keep_latest_n:]

    deleted_files = []
    for file_path in files_to_delete:
        try:
            os.remove(file_path)
            deleted_files.append(str(file_path))
            logger.info(f"🗑️  Deleted old version: {file_path}")
        except Exception as e:
            logger.error(f"❌ Failed to delete {file_path}: {e}")

    return {
        "deleted_count": len(deleted_files),
        "deleted_files": deleted_files,
        "kept_versions": [int(f.stem) for f in kept_files]
    }


# ============================================================================
# 5. ADDITIONAL HELPER: GET VIDEO FILE PATH
# ============================================================================

def get_video_file_path(
    service_name: str,
    version: int,
    base_dir: Path = None
) -> Optional[Path]:
    """
    Get the file path for a specific video version.

    Args:
        service_name: Service name
        version: Version number
        base_dir: Base video directory

    Returns:
        Path object if file exists, None otherwise
    """
    if base_dir is None:
        base_dir = VIDEO_BASE_DIR
    
    base_dir = Path(base_dir)
    
    safe_service_name = service_name.replace(" ", "_").replace("/", "_")
    video_path = base_dir / safe_service_name / f"{version}.mp4"

    return video_path if video_path.exists() else None


