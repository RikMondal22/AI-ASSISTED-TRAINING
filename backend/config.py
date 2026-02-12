"""
Configuration management for BSK Training Video Generator
Docker-optimized version with auto-detection
"""

import os
import platform
import shutil
from pathlib import Path
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# ============================================================
# PROJECT PATHS (Works with backend/ structure)
# ============================================================
# Handle both running from root and from backend/
PROJECT_ROOT = Path(__file__).parent.resolve()

# Check if we're in backend/ folder or root
if PROJECT_ROOT.name == "backend":
    # Running from backend folder
    BACKEND_DIR = PROJECT_ROOT
    ROOT_DIR = PROJECT_ROOT.parent
else:
    # Running from root (Docker scenario)
    ROOT_DIR = PROJECT_ROOT
    BACKEND_DIR = PROJECT_ROOT / "backend"

# Asset directories
ASSETS_DIR = ROOT_DIR / "assets"

# Working directories (in backend folder)
IMAGES_DIR = BACKEND_DIR / "images"
OUTPUT_VIDEOS_DIR = BACKEND_DIR / "output_videos"
GENERATED_PDFS_DIR = BACKEND_DIR / "generated_pdfs"
TEMP_DIR = BACKEND_DIR / "temp"
UPLOADS_DIR = BACKEND_DIR / "uploads"

# Ensure directories exist
for directory in [IMAGES_DIR, OUTPUT_VIDEOS_DIR, GENERATED_PDFS_DIR, TEMP_DIR, UPLOADS_DIR]:
    directory.mkdir(exist_ok=True, parents=True)

# ============================================================
# API KEYS (from environment)
# ============================================================
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")
GOOGLE_API_KEY = os.getenv("GOOGLE_API_KEY")
UNSPLASH_ACCESS_KEY = os.getenv("UNSPLASH_ACCESS_KEY")

# Validation
if not GOOGLE_API_KEY:
    print("⚠️  WARNING: GOOGLE_API_KEY not set in environment")
if not UNSPLASH_ACCESS_KEY:
    print("⚠️  WARNING: UNSPLASH_ACCESS_KEY not set in environment")

# ============================================================
# API ENDPOINTS
# ============================================================
UNSPLASH_API_URL = "https://api.unsplash.com/search/photos"

# ============================================================
# MODEL CONFIGURATION
# ============================================================
OPENAI_MODEL = "gpt-4o-mini"
GEMINI_MODEL = "gemini-2.5-flash-lite"

# ============================================================
# VOICE CONFIGURATION
# ============================================================
VOICES = {
    "en-IN-NeerjaNeural": "Neerja (Female, Indian English)",
    "en-IN-PrabhatNeural": "Prabhat (Male, Indian English)",
    "en-US-AriaNeural": "Aria (Female, US English)",
    "en-US-GuyNeural": "Guy (Male, US English)",
    "en-GB-SoniaNeural": "Sonia (Female, British English)",
    "en-AU-NatashaNeural": "Natasha (Female, Australian English)",
}

DEFAULT_VOICE = "en-IN-NeerjaNeural"
DEFAULT_VOICE_RATE = "+5%"
DEFAULT_VOICE_PITCH = "+0Hz"

# ============================================================
# VIDEO CONFIGURATION
# ============================================================
VIDEO_WIDTH = 1280
VIDEO_HEIGHT = 720
VIDEO_FPS = 30
VIDEO_CODEC = "libx264"
AUDIO_CODEC = "aac"
VIDEO_BITRATE = "2000k"

# ============================================================
# IMAGE CONFIGURATION
# ============================================================
IMAGE_CACHE_DIR = IMAGES_DIR
FALLBACK_IMAGE = ASSETS_DIR / "default_background.jpg"
AVATAR_IMAGE = ASSETS_DIR / "avatar" / "avatar.png"
AVATAR_HEIGHT = 220

# ============================================================
# OCR CONFIGURATION (Docker & Platform-agnostic)
# ============================================================
def detect_tesseract():
    """
    Auto-detect Tesseract installation across platforms
    Priority: Docker env var > PATH > Platform-specific paths
    """
    # 1. Check Docker environment variable first
    docker_tesseract = os.getenv("TESSERACT_CMD")
    if docker_tesseract and os.path.exists(docker_tesseract):
        return docker_tesseract
    
    # 2. Check if tesseract is in PATH
    tesseract_path = shutil.which("tesseract")
    if tesseract_path:
        return tesseract_path
    
    # 3. Platform-specific paths (for local development)
    if platform.system() == "Windows":
        windows_paths = [
            r"C:\Program Files\Tesseract-OCR\tesseract.exe",
            r"C:\Program Files (x86)\Tesseract-OCR\tesseract.exe",
        ]
        for path in windows_paths:
            if os.path.exists(path):
                return path
    elif platform.system() == "Linux":
        linux_paths = [
            "/usr/bin/tesseract",
            "/usr/local/bin/tesseract",
        ]
        for path in linux_paths:
            if os.path.exists(path):
                return path
    
    return None

TESSERACT_CMD = detect_tesseract()
OCR_AVAILABLE = TESSERACT_CMD is not None
OCR_DPI = 300

# ============================================================
# IMAGEMAGICK CONFIGURATION (Docker & Platform-agnostic)
# ============================================================
def detect_imagemagick():
    """
    Auto-detect ImageMagick installation across platforms
    Priority: Docker env var > PATH > Platform-specific paths
    """
    # 1. Check Docker environment variable first
    docker_magick = os.getenv("IMAGEMAGICK_BINARY")
    if docker_magick and os.path.exists(docker_magick):
        return docker_magick
    
    # 2. Check if magick or convert is in PATH
    for cmd in ["magick", "convert"]:
        magick_path = shutil.which(cmd)
        if magick_path:
            return magick_path
    
    # 3. Platform-specific paths (for local development)
    if platform.system() == "Windows":
        windows_paths = [
            r"C:\Program Files\ImageMagick-7.1.2-Q16-HDRI\magick.exe",
            r"C:\Program Files\ImageMagick\magick.exe",
            r"C:\Program Files (x86)\ImageMagick\magick.exe",
        ]
        for path in windows_paths:
            if os.path.exists(path):
                return path
    elif platform.system() == "Linux":
        linux_paths = [
            "/usr/bin/convert",
            "/usr/bin/magick",
            "/usr/local/bin/convert",
            "/usr/local/bin/magick",
        ]
        for path in linux_paths:
            if os.path.exists(path):
                return path
    
    return None

IMAGEMAGICK_BINARY = detect_imagemagick()

# ============================================================
# VALIDATION & STARTUP INFO
# ============================================================
def print_startup_info():
    """Print configuration info on startup"""
    print("=" * 70)
    print("TrainAI - BSK Training Video Generator")
    print("=" * 70)
    print(f"📁 Root Directory: {ROOT_DIR}")
    print(f"📁 Backend Directory: {BACKEND_DIR}")
    print(f"🖼️  Images: {IMAGES_DIR}")
    print(f"🎬 Videos: {OUTPUT_VIDEOS_DIR}")
    print(f"📄 PDFs: {GENERATED_PDFS_DIR}")
    print("")
    print("🔧 External Tools:")
    print(f"  • Tesseract: {TESSERACT_CMD or '❌ Not found'}")
    print(f"  • ImageMagick: {IMAGEMAGICK_BINARY or '❌ Not found'}")
    print(f"  • Platform: {platform.system()}")
    print("")
    print("🔑 API Keys:")
    print(f"  • Google API: {'✅ Set' if GOOGLE_API_KEY else '❌ Missing'}")
    print(f"  • Unsplash: {'✅ Set' if UNSPLASH_ACCESS_KEY else '❌ Missing'}")
    print(f"  • OpenAI: {'✅ Set' if OPENAI_API_KEY else '⚠️  Not set (optional)'}")
    print("=" * 70)

# Print info when config is imported
if __name__ != "__main__":  # Don't print during imports
    pass
else:
    print_startup_info()

# ============================================================
# CONFIGURATION FOR MoviePy (ImageMagick)
# ============================================================
# This will be used by video_utils.py
MOVIEPY_IMAGEMAGICK_BINARY = IMAGEMAGICK_BINARY

if __name__ == "__main__":
    print_startup_info()