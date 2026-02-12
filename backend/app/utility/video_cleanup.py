"""
Video Storage Cleanup Utility
==============================

This module handles automatic cleanup of old training videos to save storage space.

Features:
- Scheduled weekly cleanup
- Configurable retention period
- Safe deletion with database sync
- Preserves latest versions
- Detailed logging
- Dry-run mode for testing

Storage Structure:
videos/
  ├── service_name_1/
  │   ├── v1.mp4
  │   ├── v2.mp4
  │   └── v3.mp4 (latest - KEEP)
  └── service_name_2/
      └── v1.mp4 (latest - KEEP)
"""

import os
import shutil
from pathlib import Path
from datetime import datetime, timedelta
from typing import List, Dict, Optional
import logging
from sqlalchemy.orm import Session

from app.models.database import SessionLocal
from app.models import models

# ============================================================================
# CONFIGURATION
# ============================================================================

# Video storage base directory
VIDEO_BASE_DIR = Path("videos")

# Cleanup configuration
DEFAULT_RETENTION_DAYS = 15  # Keep videos for 30 days
KEEP_LATEST_N_VERSIONS = 2  # Always keep latest 2 versions per service
MIN_FREE_SPACE_GB = 10  # Minimum free space to maintain (GB)

# Setup logging
logger = logging.getLogger(__name__)

# ============================================================================
# STORAGE ANALYSIS
# ============================================================================


def get_directory_size(path: Path) -> int:
    """
    Calculate total size of directory in bytes

    Args:
        path: Directory path

    Returns:
        Size in bytes
    """
    total_size = 0
    try:
        for item in path.rglob("*"):
            if item.is_file():
                total_size += item.stat().st_size
    except Exception as e:
        logger.error(f"Error calculating directory size: {e}")

    return total_size


def get_free_space_gb(path: Path) -> float:
    """
    Get free disk space in GB (cross-platform: Windows + Unix/Linux)

    Args:
        path: Path to check

    Returns:
        Free space in GB
    """
    try:
        import platform

        if platform.system() == "Windows":
            import ctypes

            free_bytes = ctypes.c_ulonglong(0)
            ctypes.windll.kernel32.GetDiskFreeSpaceExW(
                ctypes.c_wchar_p(str(path)), None, None, ctypes.pointer(free_bytes)
            )
            return free_bytes.value / (1024**3)  # Convert to GB
        else:
            # Unix/Linux
            stat = os.statvfs(path)
            free_bytes = stat.f_bavail * stat.f_frsize
            return free_bytes / (1024**3)  # Convert to GB
    except Exception as e:
        logger.error(f"Error getting free space: {e}")
        return 0


def analyze_video_storage() -> Dict:
    """
    Analyze current video storage usage

    Returns:
        Dictionary with storage statistics
    """
    if not VIDEO_BASE_DIR.exists():
        return {
            "total_size_mb": 0,
            "total_videos": 0,
            "services_count": 0,
            "free_space_gb": 0,
            "oldest_video_date": None,
            "newest_video_date": None,
        }

    total_size = get_directory_size(VIDEO_BASE_DIR)
    total_videos = sum(1 for _ in VIDEO_BASE_DIR.rglob("*.mp4"))
    services_count = sum(1 for item in VIDEO_BASE_DIR.iterdir() if item.is_dir())
    free_space = get_free_space_gb(VIDEO_BASE_DIR)

    # Find oldest and newest videos
    video_files = list(VIDEO_BASE_DIR.rglob("*.mp4"))
    oldest_date = None
    newest_date = None

    if video_files:
        oldest_file = min(video_files, key=lambda f: f.stat().st_mtime)
        newest_file = max(video_files, key=lambda f: f.stat().st_mtime)
        oldest_date = datetime.fromtimestamp(oldest_file.stat().st_mtime)
        newest_date = datetime.fromtimestamp(newest_file.stat().st_mtime)

    return {
        "total_size_mb": total_size / (1024 * 1024),
        "total_videos": total_videos,
        "services_count": services_count,
        "free_space_gb": free_space,
        "oldest_video_date": oldest_date,
        "newest_video_date": newest_date,
    }


# ============================================================================
# VIDEO IDENTIFICATION
# ============================================================================


def get_service_videos(service_dir: Path) -> List[Dict]:
    """
    Get all videos for a service with metadata

    Args:
        service_dir: Service directory path

    Returns:
        List of video info dictionaries
    """
    videos = []

    if not service_dir.exists() or not service_dir.is_dir():
        return videos

    for video_file in service_dir.glob("*.mp4"):
        try:
            stat = video_file.stat()

            videos.append(
                {
                    "path": video_file,
                    "name": video_file.name,
                    "size_mb": stat.st_size / (1024 * 1024),
                    "created_date": datetime.fromtimestamp(stat.st_ctime),
                    "modified_date": datetime.fromtimestamp(stat.st_mtime),
                    "age_days": (
                        datetime.now() - datetime.fromtimestamp(stat.st_mtime)
                    ).days,
                    "version": video_file.stem,  # e.g., "v1", "v2"
                }
            )
        except Exception as e:
            logger.error(f"Error processing video {video_file}: {e}")

    # Sort by modified date (newest first)
    videos.sort(key=lambda x: x["modified_date"], reverse=True)

    return videos


def identify_deletable_videos(
    retention_days: int = DEFAULT_RETENTION_DAYS,
    keep_latest_n: int = KEEP_LATEST_N_VERSIONS,
    db: Optional[Session] = None,
) -> List[Dict]:
    """
    Identify videos that can be safely deleted

    Deletion criteria:
    1. Videos older than retention_days
    2. NOT in the latest N versions
    3. Optionally: Not referenced in database as active

    Args:
        retention_days: Keep videos newer than this
        keep_latest_n: Always keep this many latest versions
        db: Database session for checking references

    Returns:
        List of deletable video info
    """
    deletable = []

    if not VIDEO_BASE_DIR.exists():
        return deletable

    cutoff_date = datetime.now() - timedelta(days=retention_days)

    # Process each service directory
    for service_dir in VIDEO_BASE_DIR.iterdir():
        if not service_dir.is_dir():
            continue

        service_name = service_dir.name
        videos = get_service_videos(service_dir)

        logger.debug(f"📁 Service '{service_name}': {len(videos)} videos found")

        # Always keep the latest N versions
        protected_videos = videos[:keep_latest_n]
        candidate_videos = videos[keep_latest_n:]

        for video in candidate_videos:
            # Check age
            if video["modified_date"] < cutoff_date:
                # Check database reference (optional)
                is_referenced = False

                if db:
                    is_referenced = check_video_in_database(
                        db, service_name, video["version"]
                    )

                if not is_referenced:
                    deletable.append(
                        {
                            **video,
                            "service_name": service_name,
                            "reason": f"Older than {retention_days} days",
                            "can_delete": True,
                        }
                    )
                else:
                    logger.info(
                        f"⚠️  Video {service_name}/{video['name']} is old but "
                        f"still referenced in database - keeping"
                    )

    # Sort by age (oldest first)
    deletable.sort(key=lambda x: x["modified_date"])

    return deletable


def check_video_in_database(db: Session, service_name: str, version: str) -> bool:
    """
    Check if video is referenced in database as active

    Args:
        db: Database session
        service_name: Service name
        version: Version string (e.g., "v1")

    Returns:
        True if video is referenced and should be kept
    """
    try:
        # Check in training_video_logs table
        video_log = (
            db.query(models.TrainingVideoLog)
            .filter(
                models.TrainingVideoLog.service_name == service_name,
                models.TrainingVideoLog.version == version,
            )
            .first()
        )

        # If exists and is recent (within last 7 days), consider it active
        if video_log:
            if video_log.generated_at:
                age_days = (datetime.now() - video_log.generated_at).days
                if age_days <= 7:
                    return True

        return False

    except Exception as e:
        logger.error(f"Error checking database: {e}")
        # If database check fails, err on the side of caution
        return True


# ============================================================================
# SAFE DELETION
# ============================================================================


def delete_video_safely(video_info: Dict, dry_run: bool = False) -> bool:
    """
    Safely delete a video file with logging

    Args:
        video_info: Video information dictionary
        dry_run: If True, only log what would be deleted

    Returns:
        True if deletion was successful (or would be in dry_run)
    """
    video_path = video_info["path"]

    if not video_path.exists():
        logger.warning(f"⚠️  Video already deleted: {video_path}")
        return False

    try:
        if dry_run:
            logger.info(
                f"[DRY RUN] Would delete: {video_path} "
                f"({video_info['size_mb']:.2f} MB, "
                f"{video_info['age_days']} days old)"
            )
            return True
        else:
            # Actually delete the file
            video_path.unlink()

            logger.info(
                f"🗑️  Deleted: {video_path} "
                f"({video_info['size_mb']:.2f} MB freed, "
                f"was {video_info['age_days']} days old)"
            )

            return True

    except Exception as e:
        logger.error(f"❌ Failed to delete {video_path}: {e}")
        return False


def delete_empty_service_dirs():
    """
    Delete service directories that are now empty after cleanup
    """
    if not VIDEO_BASE_DIR.exists():
        return

    for service_dir in VIDEO_BASE_DIR.iterdir():
        if service_dir.is_dir():
            # Check if directory is empty
            if not any(service_dir.iterdir()):
                try:
                    service_dir.rmdir()
                    logger.info(f"🗑️  Removed empty directory: {service_dir.name}")
                except Exception as e:
                    logger.error(f"Failed to remove directory {service_dir}: {e}")


# ============================================================================
# CLEANUP EXECUTION
# ============================================================================


def cleanup_old_videos(
    retention_days: int = DEFAULT_RETENTION_DAYS,
    keep_latest_n: int = KEEP_LATEST_N_VERSIONS,
    dry_run: bool = False,
    max_deletions: Optional[int] = None,
) -> Dict:
    """
    Main cleanup function - identify and delete old videos

    Args:
        retention_days: Delete videos older than this (days)
        keep_latest_n: Always keep N latest versions per service
        dry_run: If True, only log what would be deleted
        max_deletions: Maximum number of videos to delete (safety limit)

    Returns:
        Cleanup statistics dictionary
    """
    logger.info("🧹 Starting video cleanup process...")

    if dry_run:
        logger.info("⚠️  DRY RUN MODE - No files will be deleted")

    # Step 1: Analyze current storage
    before_stats = analyze_video_storage()
    logger.info(
        f"📊 Storage before cleanup: "
        f"{before_stats['total_videos']} videos, "
        f"{before_stats['total_size_mb']:.2f} MB, "
        f"{before_stats['free_space_gb']:.2f} GB free"
    )

    # Step 2: Identify deletable videos
    db = SessionLocal()
    try:
        deletable_videos = identify_deletable_videos(
            retention_days=retention_days, keep_latest_n=keep_latest_n, db=db
        )

        logger.info(f"🔍 Found {len(deletable_videos)} videos eligible for deletion")

        if max_deletions:
            deletable_videos = deletable_videos[:max_deletions]
            logger.info(f"⚠️  Limited to {max_deletions} deletions (safety limit)")

        # Step 3: Delete videos
        deleted_count = 0
        failed_count = 0
        total_space_freed_mb = 0

        for video in deletable_videos:
            success = delete_video_safely(video, dry_run=dry_run)
            if success:
                deleted_count += 1
                total_space_freed_mb += video["size_mb"]
            else:
                failed_count += 1

        # Step 4: Clean up empty directories
        if not dry_run:
            delete_empty_service_dirs()

        # Step 5: Analyze after cleanup
        after_stats = analyze_video_storage()

        # Calculate additional stats
        retained_count = before_stats["total_videos"] - deleted_count
        services_processed = (
            len(set(v.get("service_name") for v in deletable_videos))
            if deletable_videos
            else 0
        )

        stats = {
            "started_at": datetime.now().isoformat(),
            "dry_run": dry_run,
            "retention_days": retention_days,
            "keep_latest_n": keep_latest_n,
            "before": {
                "total_videos": before_stats["total_videos"],
                "total_size_mb": before_stats["total_size_mb"],
                "free_space_gb": before_stats["free_space_gb"],
            },
            "after": {
                "total_videos": after_stats["total_videos"],
                "total_size_mb": after_stats["total_size_mb"],
                "free_space_gb": after_stats["free_space_gb"],
            },
            "cleanup": {
                "eligible_for_deletion": len(deletable_videos),
                "deleted_count": deleted_count,
                "failed_count": failed_count,
                "space_freed_mb": total_space_freed_mb,
                "retained_count": retained_count,
                "services_processed": services_processed,
            },
        }

        # Log summary
        logger.info("=" * 70)
        logger.info("🧹 VIDEO CLEANUP SUMMARY")
        logger.info("=" * 70)
        logger.info(f"Mode: {'DRY RUN' if dry_run else 'ACTUAL DELETION'}")
        logger.info(f"Retention period: {retention_days} days")
        logger.info(f"Keep latest: {keep_latest_n} versions per service")
        logger.info(f"")
        logger.info(f"Videos deleted: {deleted_count}/{len(deletable_videos)}")
        logger.info(f"Failed deletions: {failed_count}")
        logger.info(f"Space freed: {total_space_freed_mb:.2f} MB")
        logger.info(f"")
        logger.info(f"Storage before: {before_stats['total_size_mb']:.2f} MB")
        logger.info(f"Storage after: {after_stats['total_size_mb']:.2f} MB")
        logger.info(f"Free space: {after_stats['free_space_gb']:.2f} GB")
        logger.info("=" * 70)

        return stats

    except Exception as e:
        logger.error(f"❌ Cleanup failed: {e}")
        raise
    finally:
        db.close()


# ============================================================================
# EMERGENCY CLEANUP
# ============================================================================


def emergency_cleanup(target_free_gb: float = MIN_FREE_SPACE_GB) -> Dict:
    """
    Emergency cleanup when disk space is critically low

    Deletes oldest videos first until target free space is reached

    Args:
        target_free_gb: Target free space in GB

    Returns:
        Cleanup statistics
    """
    logger.warning(f"🚨 EMERGENCY CLEANUP: Target {target_free_gb} GB free space")

    current_free = get_free_space_gb(VIDEO_BASE_DIR)

    if current_free >= target_free_gb:
        logger.info(f"✅ Sufficient space available ({current_free:.2f} GB)")
        return {"emergency": False, "message": "No emergency cleanup needed"}

    logger.warning(
        f"⚠️  Low disk space: {current_free:.2f} GB " f"(need {target_free_gb} GB)"
    )

    # Get ALL videos, sorted by age (oldest first)
    all_videos = []

    for service_dir in VIDEO_BASE_DIR.iterdir():
        if service_dir.is_dir():
            videos = get_service_videos(service_dir)
            # Keep only latest version safe
            deletable = videos[1:]  # Skip the newest
            for video in deletable:
                all_videos.append({**video, "service_name": service_dir.name})

    # Sort by modified date (oldest first)
    all_videos.sort(key=lambda x: x["modified_date"])

    deleted_count = 0
    space_freed_mb = 0

    for video in all_videos:
        # Check if we've reached target
        current_free = get_free_space_gb(VIDEO_BASE_DIR)
        if current_free >= target_free_gb:
            logger.info(f"✅ Target free space reached: {current_free:.2f} GB")
            break

        # Delete video
        if delete_video_safely(video, dry_run=False):
            deleted_count += 1
            space_freed_mb += video["size_mb"]

    final_free = get_free_space_gb(VIDEO_BASE_DIR)

    stats = {
        "emergency": True,
        "target_free_gb": target_free_gb,
        "initial_free_gb": current_free,
        "final_free_gb": final_free,
        "deleted_count": deleted_count,
        "space_freed_mb": space_freed_mb,
        "target_reached": final_free >= target_free_gb,
    }

    logger.warning(
        f"🚨 Emergency cleanup complete: "
        f"Deleted {deleted_count} videos, "
        f"freed {space_freed_mb:.2f} MB, "
        f"free space: {final_free:.2f} GB"
    )

    return stats


# ============================================================================
# CLI / TESTING
# ============================================================================

if __name__ == "__main__":
    import sys

    # Setup logging for CLI
    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
    )

    print("\n" + "=" * 70)
    print("🧹 VIDEO CLEANUP UTILITY")
    print("=" * 70)

    if len(sys.argv) > 1 and sys.argv[1] == "--emergency":
        # Emergency cleanup
        emergency_cleanup()
    elif len(sys.argv) > 1 and sys.argv[1] == "--analyze":
        # Just analyze, don't delete
        stats = analyze_video_storage()
        print("\n📊 STORAGE ANALYSIS:")
        print(f"Total videos: {stats['total_videos']}")
        print(f"Total size: {stats['total_size_mb']:.2f} MB")
        print(f"Services: {stats['services_count']}")
        print(f"Free space: {stats['free_space_gb']:.2f} GB")
        if stats["oldest_video_date"]:
            print(f"Oldest video: {stats['oldest_video_date']}")
            print(f"Newest video: {stats['newest_video_date']}")
    else:
        # Normal cleanup (dry run by default)
        dry_run = "--execute" not in sys.argv

        cleanup_old_videos(
            retention_days=DEFAULT_RETENTION_DAYS,
            keep_latest_n=KEEP_LATEST_N_VERSIONS,
            dry_run=dry_run,
        )

        if dry_run:
            print("\n💡 Run with --execute to actually delete files")
            print("   python video_cleanup.py --execute")
