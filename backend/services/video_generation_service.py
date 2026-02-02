"""
Video Generation Service

Orchestrates the complete video generation pipeline from content extraction
to final video assembly and storage.
"""

import logging
import asyncio
from pathlib import Path
from typing import Dict, Any, Optional, Tuple
from datetime import datetime, timezone
import json

from sqlalchemy.orm import Session
from app.models import models
from video_storage_service import VideoStorageService, get_video_url

# Import your existing video generation utilities
from utils.pdf_extractor import extract_raw_content
from services.gemini_service import generate_slides_from_raw
from services.unsplash_service import fetch_and_save_photo
from utils.audio_utils import text_to_speech
from utils.video_utils import create_slide, combine_slides_and_audio
from utils.image_utils import prepare_slide_image, create_fallback_image

logger = logging.getLogger(__name__)


class VideoGenerationService:
    """
    Main service for generating training videos from PDFs or form data.
    """
    
    def __init__(self, db: Session):
        """
        Initialize video generation service.
        
        Args:
            db: Database session
        """
        self.db = db
        self.storage_service = VideoStorageService()
        self.temp_dir = Path("temp_videos")
        self.temp_dir.mkdir(parents=True, exist_ok=True)
    
    async def generate_from_pdf(
        self,
        service_id: int,
        service_name: str,
        pdf_path: Path
    ) -> Dict[str, Any]:
        """
        Generate video from PDF document.
        
        Args:
            service_id: Service ID from database
            service_name: Name of the service
            pdf_path: Path to uploaded PDF file
            
        Returns:
            Dictionary with generation results and metadata
        """
        logger.info(f"🎬 Starting video generation from PDF for: {service_name}")
        
        # Get next version number
        version = self.storage_service.get_next_version_number(service_name)
        
        # Create video record in database
        video_record = self._create_video_record(
            service_id=service_id,
            service_name=service_name,
            version=version,
            source_type="pdf",
            pdf_filename=pdf_path.name
        )
        
        try:
            # Step 1: Extract content from PDF
            logger.info("📄 Step 1/5: Extracting content from PDF...")
            self._log_generation_step(video_record.video_id, "pdf_extraction", "started")
            
            raw_content = await asyncio.to_thread(extract_raw_content, str(pdf_path))
            
            self._log_generation_step(
                video_record.video_id, 
                "pdf_extraction", 
                "completed",
                details={"content_length": len(raw_content)}
            )
            
            # Step 2: Generate slides content using AI
            logger.info("🤖 Step 2/5: Generating slides with AI...")
            self._log_generation_step(video_record.video_id, "content_generation", "started")
            
            slides_data = await asyncio.to_thread(
                generate_slides_from_raw, 
                raw_content, 
                service_name
            )
            
            self._log_generation_step(
                video_record.video_id,
                "content_generation",
                "completed",
                details={"total_slides": len(slides_data)}
            )
            
            # Update total slides in video record
            video_record.total_slides = len(slides_data)
            self.db.commit()
            
            # Step 3: Fetch images for slides
            logger.info("🖼️ Step 3/5: Fetching images...")
            self._log_generation_step(video_record.video_id, "image_search", "started")
            
            slides_with_images = await self._fetch_slide_images(slides_data, service_name)
            
            self._log_generation_step(
                video_record.video_id,
                "image_search",
                "completed"
            )
            
            # Step 4: Generate audio from text
            logger.info("🔊 Step 4/5: Generating audio...")
            self._log_generation_step(video_record.video_id, "audio_generation", "started")
            
            audio_files = await self._generate_audio_for_slides(slides_with_images)
            
            self._log_generation_step(
                video_record.video_id,
                "audio_generation",
                "completed",
                details={"audio_files_count": len(audio_files)}
            )
            
            # Step 5: Create final video
            logger.info("🎥 Step 5/5: Assembling final video...")
            self._log_generation_step(video_record.video_id, "video_assembly", "started")
            
            temp_video_path = await self._assemble_video(
                slides_with_images,
                audio_files,
                service_name,
                version
            )
            
            # Calculate video duration
            duration = await self._get_video_duration(temp_video_path)
            
            # Save to permanent storage
            final_path, file_size = self.storage_service.save_video_file(
                temp_video_path,
                service_name,
                version
            )
            
            self._log_generation_step(
                video_record.video_id,
                "video_assembly",
                "completed",
                details={
                    "file_size_mb": file_size,
                    "duration_seconds": duration
                }
            )
            
            # Update video record with final metadata
            video_record.generation_status = "completed"
            video_record.completed_at = datetime.now(timezone.utc)
            video_record.video_file_size_mb = file_size
            video_record.duration_seconds = duration
            video_record.video_file_path = self.storage_service.get_relative_video_path(
                service_name, version
            )
            
            # Mark as latest version
            self._mark_as_latest_version(service_id, video_record.video_id)
            
            self.db.commit()
            
            logger.info(f"✅ Video generation completed: {service_name} v{version}")
            
            return {
                "success": True,
                "video_id": video_record.video_id,
                "version": version,
                "file_size_mb": file_size,
                "duration_seconds": duration,
                "total_slides": len(slides_data),
                "video_path": str(final_path)
            }
            
        except Exception as e:
            logger.error(f"❌ Video generation failed: {e}", exc_info=True)
            
            # Update record with error
            video_record.generation_status = "failed"
            video_record.error_message = str(e)
            video_record.completed_at = datetime.now(timezone.utc)
            self.db.commit()
            
            raise
    
    async def generate_from_form(
        self,
        service_id: int,
        service_name: str,
        form_data: Dict[str, Any]
    ) -> Dict[str, Any]:
        """
        Generate video from form input data.
        
        Args:
            service_id: Service ID from database
            service_name: Name of the service
            form_data: Dictionary with form fields
            
        Returns:
            Dictionary with generation results and metadata
        """
        logger.info(f"🎬 Starting video generation from form for: {service_name}")
        
        # Get next version number
        version = self.storage_service.get_next_version_number(service_name)
        
        # Create video record in database
        video_record = self._create_video_record(
            service_id=service_id,
            service_name=service_name,
            version=version,
            source_type="form",
            form_data=form_data
        )
        
        try:
            # Step 1: Convert form data to structured content
            logger.info("📝 Step 1/5: Processing form data...")
            self._log_generation_step(video_record.video_id, "content_generation", "started")
            
            structured_content = self._format_form_data(form_data)
            
            # Step 2: Generate slides from structured content
            logger.info("🤖 Step 2/5: Generating slides with AI...")
            
            slides_data = await asyncio.to_thread(
                generate_slides_from_raw,
                structured_content,
                service_name
            )
            
            self._log_generation_step(
                video_record.video_id,
                "content_generation",
                "completed",
                details={"total_slides": len(slides_data)}
            )
            
            # Update total slides
            video_record.total_slides = len(slides_data)
            self.db.commit()
            
            # Step 3: Fetch images
            logger.info("🖼️ Step 3/5: Fetching images...")
            self._log_generation_step(video_record.video_id, "image_search", "started")
            
            slides_with_images = await self._fetch_slide_images(slides_data, service_name)
            
            self._log_generation_step(
                video_record.video_id,
                "image_search",
                "completed"
            )
            
            # Step 4: Generate audio
            logger.info("🔊 Step 4/5: Generating audio...")
            self._log_generation_step(video_record.video_id, "audio_generation", "started")
            
            audio_files = await self._generate_audio_for_slides(slides_with_images)
            
            self._log_generation_step(
                video_record.video_id,
                "audio_generation",
                "completed",
                details={"audio_files_count": len(audio_files)}
            )
            
            # Step 5: Assemble video
            logger.info("🎥 Step 5/5: Assembling final video...")
            self._log_generation_step(video_record.video_id, "video_assembly", "started")
            
            temp_video_path = await self._assemble_video(
                slides_with_images,
                audio_files,
                service_name,
                version
            )
            
            # Get duration and save
            duration = await self._get_video_duration(temp_video_path)
            
            final_path, file_size = self.storage_service.save_video_file(
                temp_video_path,
                service_name,
                version
            )
            
            self._log_generation_step(
                video_record.video_id,
                "video_assembly",
                "completed",
                details={
                    "file_size_mb": file_size,
                    "duration_seconds": duration
                }
            )
            
            # Update video record
            video_record.generation_status = "completed"
            video_record.completed_at = datetime.now(timezone.utc)
            video_record.video_file_size_mb = file_size
            video_record.duration_seconds = duration
            video_record.video_file_path = self.storage_service.get_relative_video_path(
                service_name, version
            )
            
            # Mark as latest
            self._mark_as_latest_version(service_id, video_record.video_id)
            
            self.db.commit()
            
            logger.info(f"✅ Video generation completed: {service_name} v{version}")
            
            return {
                "success": True,
                "video_id": video_record.video_id,
                "version": version,
                "file_size_mb": file_size,
                "duration_seconds": duration,
                "total_slides": len(slides_data),
                "video_path": str(final_path)
            }
            
        except Exception as e:
            logger.error(f"❌ Video generation failed: {e}", exc_info=True)
            
            video_record.generation_status = "failed"
            video_record.error_message = str(e)
            video_record.completed_at = datetime.now(timezone.utc)
            self.db.commit()
            
            raise
    
    # ========================================================================
    # PRIVATE HELPER METHODS
    # ========================================================================
    
    def _create_video_record(
        self,
        service_id: int,
        service_name: str,
        version: int,
        source_type: str,
        pdf_filename: Optional[str] = None,
        form_data: Optional[Dict] = None
    ) -> models.ServiceVideo:
        """Create initial video record in database."""
        
        video_record = models.ServiceVideo(
            service_id=service_id,
            service_name=service_name,
            video_version=version,
            source_type=source_type,
            pdf_file_name=pdf_filename,
            form_data=form_data,
            generation_status="processing",
            is_active=True,
            is_latest=False,  # Will be set after successful completion
            video_file_path="",  # Will be set after video is saved
        )
        
        self.db.add(video_record)
        self.db.commit()
        self.db.refresh(video_record)
        
        logger.info(f"📝 Created video record: ID={video_record.video_id}, Version={version}")
        
        return video_record
    
    def _log_generation_step(
        self,
        video_id: int,
        step_name: str,
        step_status: str,
        details: Optional[Dict] = None
    ):
        """Log a generation step to the database."""
        
        step_log = models.VideoGenerationLog(
            video_id=video_id,
            step_name=step_name,
            step_status=step_status,
            step_details=details or {},
        )
        
        if step_status == "completed":
            step_log.completed_at = datetime.now(timezone.utc)
        
        self.db.add(step_log)
        self.db.commit()
    
    def _format_form_data(self, form_data: Dict[str, Any]) -> str:
        """Format form data into structured text for AI processing."""
        
        sections = []
        
        if form_data.get("service_description"):
            sections.append(f"## Service Description\n{form_data['service_description']}")
        
        if form_data.get("how_to_apply"):
            sections.append(f"## How to Apply\n{form_data['how_to_apply']}")
        
        if form_data.get("eligibility_criteria"):
            sections.append(f"## Eligibility Criteria\n{form_data['eligibility_criteria']}")
        
        if form_data.get("required_documents"):
            sections.append(f"## Required Documents\n{form_data['required_documents']}")
        
        if form_data.get("fees_charges"):
            sections.append(f"## Fees and Charges\n{form_data['fees_charges']}")
        
        if form_data.get("processing_time"):
            sections.append(f"## Processing Time\n{form_data['processing_time']}")
        
        if form_data.get("additional_info"):
            sections.append(f"## Additional Information\n{form_data['additional_info']}")
        
        return "\n\n".join(sections)
    
    async def _fetch_slide_images(
        self,
        slides_data: list,
        service_name: str
    ) -> list:
        """Fetch images for each slide."""
        
        slides_with_images = []
        
        for i, slide in enumerate(slides_data):
            try:
                # Fetch image based on slide content/keywords
                image_path = await asyncio.to_thread(
                    fetch_and_save_photo,
                    slide.get("keywords", service_name),
                    f"slide_{i}"
                )
                
                slide["image_path"] = image_path
                
            except Exception as e:
                logger.warning(f"⚠️ Image fetch failed for slide {i}: {e}")
                # Create fallback image
                slide["image_path"] = create_fallback_image(
                    slide.get("title", "Slide"),
                    f"slide_{i}_fallback"
                )
            
            slides_with_images.append(slide)
        
        return slides_with_images
    
    async def _generate_audio_for_slides(self, slides: list) -> list:
        """Generate audio narration for each slide."""
        
        audio_files = []
        
        for i, slide in enumerate(slides):
            try:
                narration_text = slide.get("narration", slide.get("content", ""))
                
                audio_path = await asyncio.to_thread(
                    text_to_speech,
                    narration_text,
                    f"audio_{i}"
                )
                
                audio_files.append(audio_path)
                
            except Exception as e:
                logger.error(f"❌ Audio generation failed for slide {i}: {e}")
                raise
        
        return audio_files
    
    async def _assemble_video(
        self,
        slides: list,
        audio_files: list,
        service_name: str,
        version: int
    ) -> Path:
        """Assemble final video from slides and audio."""
        
        # Create slide videos
        slide_videos = []
        
        for i, (slide, audio_file) in enumerate(zip(slides, audio_files)):
            slide_video = await asyncio.to_thread(
                create_slide,
                slide["image_path"],
                audio_file,
                f"slide_video_{i}"
            )
            slide_videos.append(slide_video)
        
        # Combine all slides into final video
        output_filename = f"{service_name}_v{version}_temp.mp4"
        temp_video_path = self.temp_dir / output_filename
        
        await asyncio.to_thread(
            combine_slides_and_audio,
            slide_videos,
            str(temp_video_path)
        )
        
        return temp_video_path
    
    async def _get_video_duration(self, video_path: Path) -> float:
        """Get video duration in seconds using ffprobe or similar."""
        # This is a placeholder - implement with actual video library
        # e.g., using ffprobe or moviepy
        try:
            from moviepy.editor import VideoFileClip
            clip = VideoFileClip(str(video_path))
            duration = clip.duration
            clip.close()
            return duration
        except:
            return 0.0
    
    def _mark_as_latest_version(self, service_id: int, video_id: int):
        """Mark a video as the latest version for a service."""
        
        # Set all other versions as not latest
        self.db.query(models.ServiceVideo).filter(
            models.ServiceVideo.service_id == service_id,
            models.ServiceVideo.video_id != video_id
        ).update({"is_latest": False})
        
        # Mark current version as latest
        self.db.query(models.ServiceVideo).filter(
            models.ServiceVideo.video_id == video_id
        ).update({"is_latest": True})
        
        self.db.commit()