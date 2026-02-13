"""
Video Queue Manager - Asynchronous Video Generation System

This module handles async video generation with a queue-based approach:
1. User submits request → Gets unique video_id immediately
2. Video generation runs in background (~20 mins)
3. User polls /get_completed_videos endpoint to retrieve finished videos
4. Completed videos are removed from queue after retrieval
"""

import os
import uuid
import logging
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.ssl_ import create_urllib3_context
from datetime import datetime, timezone
from typing import Dict, List, Optional
from enum import Enum
from sqlalchemy.orm import Session
from sqlalchemy import desc
from app.models import models
from dotenv import load_dotenv
load_dotenv()
# ============================================================================
# External push-back config  (values come from environment variables)
# ============================================================================
BSK_API_BASE_URL  = os.getenv("BSK_API_VIDEO_BASE_URL",  "https://bsk.wb.gov.in/aiapi")
BSK_API_USERNAME  = os.getenv("BSK_API_VIDEO_USERNAME")
BSK_API_PASSWORD  = os.getenv("BSK_API_VIDEO_PASSWORD")
BSK_PUSH_URL      = f"{BSK_API_BASE_URL}/push_completed_videos"
BSK_AUTH_URL      = f"{BSK_API_BASE_URL}/generate_token"


print(BSK_API_BASE_URL)
print(BSK_API_USERNAME)
print(BSK_API_PASSWORD)

# ---------------------------------------------------------------------------
# SSL adapter — same pattern as SyncService (legacy govt server)
# ---------------------------------------------------------------------------
class _SSLAdapter(HTTPAdapter):
    def init_poolmanager(self, *args, **kwargs):
        ctx = create_urllib3_context()
        ctx.load_default_certs()
        ctx.options |= 0x4  # OP_LEGACY_SERVER_CONNECT
        kwargs["ssl_context"] = ctx
        return super().init_poolmanager(*args, **kwargs)


def _make_bsk_session() -> requests.Session:
    """Return a requests.Session with SSL adapter + fresh JWT from BSK auth."""
    session = requests.Session()
    adapter = _SSLAdapter()
    session.mount("https://", adapter)
    session.mount("http://",  adapter)

    if not BSK_API_USERNAME or not BSK_API_PASSWORD:
        raise RuntimeError(
            "BSK_API_USERNAME / BSK_API_PASSWORD env vars are not set. "
            "Cannot authenticate with BSK API."
        )

    resp = session.post(
        BSK_AUTH_URL,
        json={"username": BSK_API_USERNAME, "password": BSK_API_PASSWORD},
        timeout=30,
    )
    resp.raise_for_status()
    data = resp.json()

    token = data.get("token") or data.get("access_token") or data.get("jwt")
    if not token:
        raise RuntimeError(f"No JWT received from BSK auth API. Response: {data}")

    session.headers.update({
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json",
    })
    logger.info("✅ BSK API authenticated (fresh token acquired)")
    return session

logger = logging.getLogger(__name__)


class VideoGenerationStatus(str, Enum):
    """Status of video generation task"""
    PENDING = "pending"       # Just submitted, waiting to start
    PROCESSING = "processing" # Currently generating
    COMPLETED = "completed"   # Video ready, not yet retrieved
    RETRIEVED = "retrieved"   # User has downloaded/retrieved
    FAILED = "failed"        # Generation failed


class VideoQueueManager:
    """
    Manages the video generation queue and completion tracking
    """
    
    def __init__(self):
        """Initialize the queue manager"""
        self.logger = logging.getLogger(__name__)
    
    # ========================================================================
    # 1. CREATE VIDEO GENERATION REQUEST
    # ========================================================================
    
    def create_video_request(
        self,
        db: Session,
        service_id: int,
        service_name: str,
        source_type: str,
        request_data: dict,
    ) -> str:
        """
        Create a new video generation request and return unique video_id
        
        Args:
            db: Database session
            service_id: ID from service_master
            service_name: Official service name
            source_type: 'form_ai_enhanced' or 'pdf_ai_enhanced'
            request_data: Original request data (for debugging/tracking)
        
        Returns:
            str: Unique video_id (UUID)
        """
        # Generate unique ID
        video_id = str(uuid.uuid4())
        
        # Create database record with PENDING status
        video_request = models.VideoGenerationQueue(
            video_id=video_id,
            service_id=service_id,
            service_name=service_name,
            source_type=source_type,
            status=VideoGenerationStatus.PENDING,
            request_data=request_data,  # Store original request for reference
            created_at=datetime.now(timezone.utc),
            updated_at=datetime.now(timezone.utc),
        )
        
        db.add(video_request)
        db.commit()
        db.refresh(video_request)
        
        self.logger.info(
            f"📝 Created video request: {video_id} for service '{service_name}'"
        )
        
        return video_id
    
    # ========================================================================
    # 2. UPDATE REQUEST STATUS
    # ========================================================================
    
    def update_status(
        self,
        db: Session,
        video_id: str,
        status: VideoGenerationStatus,
        error_message: Optional[str] = None,
    ):
        """
        Update the status of a video generation request
        
        Args:
            db: Database session
            video_id: Unique video ID
            status: New status
            error_message: Error details if status is FAILED
        """
        video_request = (
            db.query(models.VideoGenerationQueue)
            .filter(models.VideoGenerationQueue.video_id == video_id)
            .first()
        )
        
        if not video_request:
            self.logger.error(f"❌ Video request not found: {video_id}")
            return
        
        video_request.status = status
        video_request.updated_at = datetime.now(timezone.utc)
        
        if status == VideoGenerationStatus.PROCESSING:
            video_request.started_at = datetime.now(timezone.utc)
        elif status == VideoGenerationStatus.COMPLETED:
            video_request.completed_at = datetime.now(timezone.utc)
        elif status == VideoGenerationStatus.FAILED:
            video_request.error_message = error_message
            video_request.failed_at = datetime.now(timezone.utc)
        
        db.commit()
        
        self.logger.info(f"📊 Updated video {video_id} status: {status}")
    
    # ========================================================================
    # 3. LINK COMPLETED VIDEO
    # ========================================================================
    
    def link_completed_video(
        self,
        db: Session,
        video_id: str,
        video_record_id: int,
        video_url: str,
        video_path: str,
        file_size_mb: float,
        duration_seconds: int,
        total_slides: int,
    ):
        """
        Link a completed video to the queue request
        
        Args:
            db: Database session
            video_id: Unique video ID from queue
            video_record_id: ID from service_videos table
            video_url: Download URL for the video
            video_path: Filesystem path
            file_size_mb: File size
            duration_seconds: Video duration
            total_slides: Number of slides
        """
        video_request = (
            db.query(models.VideoGenerationQueue)
            .filter(models.VideoGenerationQueue.video_id == video_id)
            .first()
        )
        
        if not video_request:
            self.logger.error(f"❌ Video request not found: {video_id}")
            return
        
        # Update with video details
        video_request.video_record_id = video_record_id
        video_request.video_url = video_url
        video_request.video_path = video_path
        video_request.file_size_mb = file_size_mb
        video_request.duration_seconds = duration_seconds
        video_request.total_slides = total_slides
        video_request.status = VideoGenerationStatus.COMPLETED
        video_request.completed_at = datetime.now(timezone.utc)
        video_request.updated_at = datetime.now(timezone.utc)
        
        db.commit()
        
        self.logger.info(
            f"✅ Linked completed video: {video_id} → {video_url}"
        )
    
    # ========================================================================
    # 4. GET COMPLETED VIDEOS (USER RETRIEVAL)
    # ========================================================================
    
    def get_completed_videos(
        self,
        db: Session,
        mark_as_retrieved: bool = True,
    ) -> List[Dict]:
        """
        Get all completed videos that haven't been retrieved yet
        
        This is the main endpoint users call to check if their videos are ready.
        Once retrieved, videos are marked as RETRIEVED and won't appear again.
        
        Args:
            db: Database session
            mark_as_retrieved: If True, mark videos as retrieved after fetching
        
        Returns:
            List of completed video dictionaries with URLs and metadata
        """
        # Query completed videos
        
        completed_videos = (
            db.query(models.VideoGenerationQueue)
            .filter(models.VideoGenerationQueue.status == VideoGenerationStatus.COMPLETED)
            .order_by(desc(models.VideoGenerationQueue.completed_at))
            .all()
        )
        
        if not completed_videos:
            return []
        
        # Build response list
        result = []
        for video in completed_videos:
            result.append({
                "video_id": video.video_id,
                "service_id": video.service_id,
                "service_name": video.service_name,
                "video_url": video.video_url,
                "video_path": video.video_path,
                "file_size_mb": video.file_size_mb,
                "duration_seconds": video.duration_seconds,
                "total_slides": video.total_slides,
                "source_type": video.source_type,
                "created_at": video.created_at.isoformat(),
                "completed_at": video.completed_at.isoformat(),
                "processing_time_seconds": (
                    (video.completed_at - video.started_at).total_seconds()
                    if video.started_at and video.completed_at
                    else None
                ),
            })
        
            self.logger.info(
                f"✅ Marked {len(completed_videos)} videos as retrieved"
            )
        
        return result
    
    # ========================================================================
    # 5. GET REQUEST STATUS
    # ========================================================================
    
    def get_request_status(
        self,
        db: Session,
        video_id: str,
    ) -> Optional[Dict]:
        """
        Get the status of a specific video generation request
        
        Args:
            db: Database session
            video_id: Unique video ID
        
        Returns:
            Dictionary with status details or None if not found
        """
        video_request = (
            db.query(models.VideoGenerationQueue)
            .filter(models.VideoGenerationQueue.video_id == video_id)
            .first()
        )
        
        if not video_request:
            return None
        
        return {
            "video_id": video_request.video_id,
            "service_name": video_request.service_name,
            "status": video_request.status,
            "source_type": video_request.source_type,
            "created_at": video_request.created_at.isoformat(),
            "started_at": video_request.started_at.isoformat() if video_request.started_at else None,
            "completed_at": video_request.completed_at.isoformat() if video_request.completed_at else None,
            "retrieved_at": video_request.retrieved_at.isoformat() if video_request.retrieved_at else None,
            "error_message": video_request.error_message,
            "video_url": video_request.video_url if video_request.status == VideoGenerationStatus.COMPLETED else None,
            "file_size_mb": video_request.file_size_mb,
            "duration_seconds": video_request.duration_seconds,
            "total_slides": video_request.total_slides,
        }
    
    # ========================================================================
    # 6. GET PENDING QUEUE
    # ========================================================================
    
    def get_pending_requests(
        self,
        db: Session,
        limit: int = 50,
    ) -> List[Dict]:
        """
        Get all pending/processing requests
        
        Args:
            db: Database session
            limit: Maximum number of requests to return
        
        Returns:
            List of pending request dictionaries
        """
        pending = (
            db.query(models.VideoGenerationQueue)
            .filter(
                models.VideoGenerationQueue.status.in_([
                    VideoGenerationStatus.PENDING,
                    VideoGenerationStatus.PROCESSING,
                ])
            )
            .order_by(models.VideoGenerationQueue.created_at)
            .limit(limit)
            .all()
        )
        
        return [
            {
                "video_id": req.video_id,
                "service_name": req.service_name,
                "status": req.status,
                "created_at": req.created_at.isoformat(),
                "started_at": req.started_at.isoformat() if req.started_at else None,
            }
            for req in pending
        ]
    
    # ========================================================================
    # 7. CLEANUP OLD RETRIEVED VIDEOS
    # ========================================================================
    
    # def cleanup_retrieved_videos(
    #     self,
    #     db: Session,
    #     days_old: int = 7,
    # ) -> int:
    #     """
    #     Clean up old retrieved videos from the queue
        
    #     Args:
    #         db: Database session
    #         days_old: Delete records older than this many days
        
    #     Returns:
    #         Number of records deleted
    #     """
    #     from datetime import timedelta
        
    #     cutoff_date = datetime.now(timezone.utc) - timedelta(days=days_old)
        
    #     deleted_count = (
    #         db.query(models.VideoGenerationQueue)
    #         .filter(
    #             models.VideoGenerationQueue.status == VideoGenerationStatus.RETRIEVED,
    #             models.VideoGenerationQueue.retrieved_at < cutoff_date,
    #         )
    #         .delete(synchronize_session=False)
    #     )
        
    #     db.commit()
        
    #     if deleted_count > 0:
    #         self.logger.info(
    #             f"🧹 Cleaned up {deleted_count} old retrieved video records"
    #         )
        
    #     return deleted_count

    # ========================================================================
    # 8. PUSH COMPLETION RESULT TO EXTERNAL BSK API
    # ========================================================================

    def push_completion_to_external_api(
        self,
        db: Session,
        video_id: str,
    ) -> bool:
        """
        Push video completion (success or failure) to the external BSK server.

        On SUCCESS  -> sends full video metadata payload
        On FAILURE  -> sends error payload so the server knows generation failed

        Args:
            db: Database session
            video_id: Unique video ID

        Returns:
            bool: True if the external API accepted the push, False otherwise
        """
        video_request = (
            db.query(models.VideoGenerationQueue)
            .filter(models.VideoGenerationQueue.video_id == video_id)
            .first()
        )

        if not video_request:
            self.logger.error(f"Cannot push - video request not found: {video_id}")
            return False

        # Build payload depending on outcome
        if video_request.status == VideoGenerationStatus.COMPLETED:
            payload = {
                "video_id": video_request.video_id,
                "service_name": video_request.service_name,
                "video_url": video_request.video_url,
                "file_size_mb": video_request.file_size_mb,
                "duration_seconds": video_request.duration_seconds,
                "total_slides": video_request.total_slides,
                "completed_at": (
                    video_request.completed_at.isoformat()
                    if video_request.completed_at
                    else None
                ),
                "processing_time_seconds": (
                    int((video_request.completed_at - video_request.started_at).total_seconds())
                    if video_request.started_at and video_request.completed_at
                    else None
                ),
                "status": "completed",
            }
        else:  # FAILED
            payload = {
                "video_id": video_request.video_id,
                "service_name": video_request.service_name,
                "status": "failed",
                "error_message": video_request.error_message,
                "failed_at": (
                    video_request.failed_at.isoformat()
                    if video_request.failed_at
                    else None
                ),
            }

        try:
            # Authenticate and get a fresh session (same pattern as SyncService)
            bsk_session = _make_bsk_session()

            response = bsk_session.post(BSK_PUSH_URL, json=payload, timeout=30)

            if response.status_code in (200, 201, 202):
                self.logger.info(
                    f"Pushed {payload['status']} result for {video_id} "
                    f"to BSK API (HTTP {response.status_code})"
                )
                # Record that we have pushed to avoid accidental double-push
                video_request.pushed_at = datetime.now(timezone.utc)
                video_request.updated_at = datetime.now(timezone.utc)
                db.commit()
                return True
            else:
                self.logger.error(
                    f"BSK API rejected push for {video_id}: "
                    f"HTTP {response.status_code} - {response.text[:1000]}"
                )
                return False

        except requests.RequestException as exc:
            self.logger.error(
                f"Network error pushing {video_id} to BSK API: {exc}"
            )
            return False

    # ========================================================================
    # 9. ACKNOWLEDGE AND DELETE
    # ========================================================================

    def acknowledge_and_delete(
        self,
        db: Session,
        video_id: str,
    ) -> bool:
        """
        Permanently remove a video queue record once the external server
        confirms it has processed/stored the result.

        Called via DELETE /bsk_portal/acknowledge_video/{video_id}

        Args:
            db: Database session
            video_id: Unique video ID

        Returns:
            bool: True if deleted, False if not found
        """
        video_request = (
            db.query(models.VideoGenerationQueue)
            .filter(models.VideoGenerationQueue.video_id == video_id)
            .first()
        )

        if not video_request:
            self.logger.warning(f"Acknowledge failed - video_id not found: {video_id}")
            return False

        db.delete(video_request)
        db.commit()

        self.logger.info(f"Queue entry permanently deleted: {video_id}")
        return True


# Global instance
queue_manager = VideoQueueManager()