"""
Video Storage Service

Handles file storage, directory management, and video file operations.
Implements the folder structure: videos/<service_name>/v<version>.mp4
"""

import os
import shutil
from pathlib import Path
from typing import Optional, Tuple
import logging
from datetime import datetime
import re

logger = logging.getLogger(__name__)


class VideoStorageService:
    """
    Manages video file storage with organized directory structure.
    
    Directory structure:
    videos/
    ├── Aadhaar_Card_Application/
    │   ├── v1.mp4
    │   ├── v2.mp4
    │   └── v3.mp4
    ├── Driving_License/
    │   ├── v1.mp4
    │   └── v2.mp4
    """
    
    def __init__(self, base_video_dir: str = "videos"):
        """
        Initialize video storage service.
        
        Args:
            base_video_dir: Base directory for storing all videos (default: 'videos')
        """
        self.base_video_dir = Path(base_video_dir)
        self._ensure_base_directory()
    
    def _ensure_base_directory(self):
        """Create base videos directory if it doesn't exist."""
        self.base_video_dir.mkdir(parents=True, exist_ok=True)
        logger.info(f"✅ Video storage initialized at: {self.base_video_dir.absolute()}")
    
    @staticmethod
    def sanitize_service_name(service_name: str) -> str:
        """
        Sanitize service name for use as directory name.
        
        Converts: "Aadhaar Card - Application" → "Aadhaar_Card_Application"
        
        Args:
            service_name: Original service name
            
        Returns:
            Sanitized name safe for filesystem
        """
        # Remove special characters and replace spaces with underscores
        sanitized = re.sub(r'[^\w\s-]', '', service_name)
        sanitized = re.sub(r'[-\s]+', '_', sanitized)
        sanitized = sanitized.strip('_')
        
        # Limit length to avoid filesystem issues
        if len(sanitized) > 100:
            sanitized = sanitized[:100]
        
        return sanitized
    
    def get_service_directory(self, service_name: str) -> Path:
        """
        Get the directory path for a specific service.
        
        Args:
            service_name: Name of the service
            
        Returns:
            Path object pointing to service directory
        """
        sanitized_name = self.sanitize_service_name(service_name)
        return self.base_video_dir / sanitized_name
    
    def create_service_directory(self, service_name: str) -> Path:
        """
        Create directory for a service if it doesn't exist.
        
        Args:
            service_name: Name of the service
            
        Returns:
            Path object pointing to created directory
        """
        service_dir = self.get_service_directory(service_name)
        service_dir.mkdir(parents=True, exist_ok=True)
        logger.info(f"📁 Service directory created: {service_dir}")
        return service_dir
    
    def get_video_filename(self, version: int) -> str:
        """
        Generate video filename for a specific version.
        
        Args:
            version: Video version number
            
        Returns:
            Filename in format: v<version>.mp4
        """
        return f"v{version}.mp4"
    
    def get_video_path(self, service_name: str, version: int) -> Path:
        """
        Get full path for a video file.
        
        Args:
            service_name: Name of the service
            version: Video version number
            
        Returns:
            Full path to video file
        """
        service_dir = self.get_service_directory(service_name)
        filename = self.get_video_filename(version)
        return service_dir / filename
    
    def get_relative_video_path(self, service_name: str, version: int) -> str:
        """
        Get relative path for storing in database.
        
        Args:
            service_name: Name of the service
            version: Video version number
            
        Returns:
            Relative path string: videos/<service_name>/v<version>.mp4
        """
        sanitized_name = self.sanitize_service_name(service_name)
        filename = self.get_video_filename(version)
        return f"videos/{sanitized_name}/{filename}"
    
    def get_next_version_number(self, service_name: str) -> int:
        """
        Determine the next version number for a service.
        
        Args:
            service_name: Name of the service
            
        Returns:
            Next available version number (starts from 1)
        """
        service_dir = self.get_service_directory(service_name)
        
        if not service_dir.exists():
            return 1
        
        # Find all version files
        existing_versions = []
        for file in service_dir.glob("v*.mp4"):
            try:
                version_num = int(file.stem[1:])  # Extract number from 'v123'
                existing_versions.append(version_num)
            except ValueError:
                continue
        
        if not existing_versions:
            return 1
        
        return max(existing_versions) + 1
    
    def save_video_file(
        self, 
        source_path: Path, 
        service_name: str, 
        version: int
    ) -> Tuple[Path, float]:
        """
        Save video file to storage with proper naming and location.
        
        Args:
            source_path: Path to the temporary video file
            service_name: Name of the service
            version: Video version number
            
        Returns:
            Tuple of (destination_path, file_size_mb)
        """
        # Create service directory if needed
        service_dir = self.create_service_directory(service_name)
        
        # Get destination path
        dest_path = self.get_video_path(service_name, version)
        
        # Copy file to destination
        shutil.copy2(source_path, dest_path)
        
        # Calculate file size in MB
        file_size_mb = dest_path.stat().st_size / (1024 * 1024)
        
        logger.info(
            f"💾 Video saved: {dest_path.name} "
            f"({file_size_mb:.2f} MB) in {service_dir.name}/"
        )
        
        return dest_path, file_size_mb
    
    def delete_video(self, service_name: str, version: int) -> bool:
        """
        Delete a specific video version.
        
        Args:
            service_name: Name of the service
            version: Video version number
            
        Returns:
            True if deleted successfully, False otherwise
        """
        video_path = self.get_video_path(service_name, version)
        
        if video_path.exists():
            video_path.unlink()
            logger.info(f"🗑️ Deleted video: {video_path}")
            
            # Clean up empty directory
            service_dir = self.get_service_directory(service_name)
            if service_dir.exists() and not any(service_dir.iterdir()):
                service_dir.rmdir()
                logger.info(f"🗑️ Removed empty directory: {service_dir}")
            
            return True
        
        logger.warning(f"⚠️ Video not found for deletion: {video_path}")
        return False
    
    def video_exists(self, service_name: str, version: int) -> bool:
        """
        Check if a video file exists.
        
        Args:
            service_name: Name of the service
            version: Video version number
            
        Returns:
            True if video file exists, False otherwise
        """
        video_path = self.get_video_path(service_name, version)
        return video_path.exists()
    
    def get_video_size_mb(self, service_name: str, version: int) -> Optional[float]:
        """
        Get video file size in megabytes.
        
        Args:
            service_name: Name of the service
            version: Video version number
            
        Returns:
            File size in MB, or None if file doesn't exist
        """
        video_path = self.get_video_path(service_name, version)
        
        if video_path.exists():
            return video_path.stat().st_size / (1024 * 1024)
        
        return None
    
    def list_service_videos(self, service_name: str) -> list:
        """
        List all video versions for a service.
        
        Args:
            service_name: Name of the service
            
        Returns:
            List of tuples: [(version, file_size_mb, created_date), ...]
        """
        service_dir = self.get_service_directory(service_name)
        
        if not service_dir.exists():
            return []
        
        videos = []
        for file in service_dir.glob("v*.mp4"):
            try:
                version_num = int(file.stem[1:])
                file_size_mb = file.stat().st_size / (1024 * 1024)
                created_date = datetime.fromtimestamp(file.stat().st_ctime)
                
                videos.append((version_num, file_size_mb, created_date))
            except (ValueError, OSError) as e:
                logger.warning(f"⚠️ Error processing video file {file}: {e}")
                continue
        
        # Sort by version number
        videos.sort(key=lambda x: x[0])
        
        return videos
    
    def get_total_storage_used(self) -> float:
        """
        Calculate total storage used by all videos.
        
        Returns:
            Total size in GB
        """
        total_bytes = 0
        
        for service_dir in self.base_video_dir.iterdir():
            if service_dir.is_dir():
                for video_file in service_dir.glob("*.mp4"):
                    total_bytes += video_file.stat().st_size
        
        total_gb = total_bytes / (1024 * 1024 * 1024)
        
        return total_gb
    
    def cleanup_old_versions(
        self, 
        service_name: str, 
        keep_latest: int = 3
    ) -> int:
        """
        Clean up old video versions, keeping only the latest N versions.
        
        Args:
            service_name: Name of the service
            keep_latest: Number of latest versions to keep (default: 3)
            
        Returns:
            Number of videos deleted
        """
        videos = self.list_service_videos(service_name)
        
        if len(videos) <= keep_latest:
            return 0
        
        # Sort by version number (descending) and get versions to delete
        videos_sorted = sorted(videos, key=lambda x: x[0], reverse=True)
        versions_to_delete = [v[0] for v in videos_sorted[keep_latest:]]
        
        deleted_count = 0
        for version in versions_to_delete:
            if self.delete_video(service_name, version):
                deleted_count += 1
        
        logger.info(
            f"🧹 Cleanup complete for '{service_name}': "
            f"Deleted {deleted_count} old versions, kept {keep_latest} latest"
        )
        
        return deleted_count


# ============================================================================
# UTILITY FUNCTIONS
# ============================================================================


def get_video_url(service_name: str, version: int, base_url: str) -> str:
    """
    Generate public URL for accessing a video.
    
    Args:
        service_name: Name of the service
        version: Video version number
        base_url: Base URL of the API (e.g., 'http://localhost:8000')
        
    Returns:
        Full URL to access the video
    """
    sanitized_name = VideoStorageService.sanitize_service_name(service_name)
    return f"{base_url}/api/videos/watch/{sanitized_name}/v{version}"