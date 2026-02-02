"""
Configuration management for BSK Training Video Generator
Loads all settings from environment variables with proper fallbacks
"""

import os
from pathlib import Path
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# ============================================================
# PROJECT PATHS (Cross-platform)
# ============================================================
PROJECT_ROOT = Path(__file__).parent.resolve()
ASSETS_DIR = PROJECT_ROOT / "assets"
IMAGES_DIR = PROJECT_ROOT / "images"
OUTPUT_VIDEOS_DIR = PROJECT_ROOT / "output_videos"
GENERATED_PDFS_DIR = PROJECT_ROOT / "generated_pdfs"

# Ensure directories exist
IMAGES_DIR.mkdir(exist_ok=True)
OUTPUT_VIDEOS_DIR.mkdir(exist_ok=True)
GENERATED_PDFS_DIR.mkdir(exist_ok=True)

# ============================================================
# API KEYS (from environment)
# ============================================================
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")
GOOGLE_API_KEY = os.getenv("GOOGLE_API_KEY")
UNSPLASH_ACCESS_KEY = os.getenv("UNSPLASH_ACCESS_KEY")

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
# OCR CONFIGURATION (Platform-specific)
# ============================================================
import platform
import shutil

# Auto-detect Tesseract installation
if platform.system() == "Windows":
    TESSERACT_PATHS = [
        r"C:\Program Files\Tesseract-OCR\tesseract.exe",
        r"C:\Program Files (x86)\Tesseract-OCR\tesseract.exe",
    ]
    TESSERACT_CMD = next((p for p in TESSERACT_PATHS if os.path.exists(p)), None)
else:
    TESSERACT_CMD = shutil.which("tesseract")

OCR_AVAILABLE = TESSERACT_CMD is not None
OCR_DPI = 300

# ============================================================
# IMAGEMAGICK CONFIGURATION (Platform-specific)
# ============================================================
if platform.system() == "Windows":
    IMAGEMAGICK_PATHS = [
        r"C:\Program Files\ImageMagick-7.1.2-Q16-HDRI\magick.exe",
        r"C:\Program Files\ImageMagick\magick.exe",
        r"C:\Program Files (x86)\ImageMagick\magick.exe",
    ]
    IMAGEMAGICK_BINARY = next((p for p in IMAGEMAGICK_PATHS if os.path.exists(p)), None)
else:
    IMAGEMAGICK_BINARY = shutil.which("magick") or shutil.which("convert")


# ============================================================
# VALIDATION
# ============================================================
def validate_config():
    """Validate that all required configuration is present"""
    errors = []

    # Check API keys
    if not GOOGLE_API_KEY:
        errors.append("GOOGLE_API_KEY not set in environment")
    if not UNSPLASH_ACCESS_KEY:
        errors.append("UNSPLASH_ACCESS_KEY not set in environment")

    # Check external tools
    if not TESSERACT_CMD:
        errors.append(
            "Tesseract OCR not found. Install from https://github.com/tesseract-ocr/tesseract"
        )
    if not IMAGEMAGICK_BINARY:
        errors.append("ImageMagick not found. Install from https://imagemagick.org/")

    # Check required assets
    if not FALLBACK_IMAGE.exists():
        errors.append(f"Fallback image not found: {FALLBACK_IMAGE}")

    return errors


# ============================================================
# STARTUP CHECK
# ============================================================
if __name__ == "__main__":
    print("=" * 60)
    print("BSK Training Video Generator - Configuration Check")
    print("=" * 60)

    errors = validate_config()

    if errors:
        print("\n⚠️  Configuration Issues Found:")
        for error in errors:
            print(f"  ❌ {error}")
        print("\nPlease fix these issues before running the application.")
    else:
        print("\n✅ All configuration checks passed!")
        print(f"\n📁 Project Root: {PROJECT_ROOT}")
        print(f"🖼️  Images Dir: {IMAGES_DIR}")
        print(f"🎬 Videos Dir: {OUTPUT_VIDEOS_DIR}")
        print(f"📄 PDFs Dir: {GENERATED_PDFS_DIR}")
        print(f"\n🔧 Tesseract: {TESSERACT_CMD or 'Not found'}")
        print(f"🎨 ImageMagick: {IMAGEMAGICK_BINARY or 'Not found'}")
