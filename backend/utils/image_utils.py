"""
Image utilities for training video generation
Goals:
- Always produce 16:9 images
- Preserve subject focus
- Avoid distortion
- Ensure professional visual consistency
"""

import os
from PIL import Image, ImageEnhance

# -------------------------------------------------
# CONFIG
# -------------------------------------------------
TARGET_WIDTH = 1280
TARGET_HEIGHT = 720
ASPECT_RATIO = TARGET_WIDTH / TARGET_HEIGHT


# -------------------------------------------------
# CORE IMAGE PROCESSOR
# -------------------------------------------------
def prepare_slide_image(image_path):
    """
    Prepare an image for video slide usage:
    - Center crop to 16:9
    - Resize to 1280x720
    - Enhance contrast slightly
    """

    if not os.path.exists(image_path):
        raise FileNotFoundError(f"Image not found: {image_path}")

    with Image.open(image_path).convert("RGB") as img:
        img_width, img_height = img.size
        img_ratio = img_width / img_height

        # -----------------------------
        # CENTER CROP TO 16:9
        # -----------------------------
        if img_ratio > ASPECT_RATIO:
            # Image is wider than 16:9 → crop sides
            new_width = int(img_height * ASPECT_RATIO)
            left = (img_width - new_width) // 2
            img = img.crop((left, 0, left + new_width, img_height))
        else:
            # Image is taller than 16:9 → crop top/bottom
            new_height = int(img_width / ASPECT_RATIO)
            top = (img_height - new_height) // 2
            img = img.crop((0, top, img_width, top + new_height))

        # -----------------------------
        # RESIZE FOR VIDEO
        # -----------------------------
        img = img.resize((TARGET_WIDTH, TARGET_HEIGHT), Image.LANCZOS)

        # -----------------------------
        # LIGHT ENHANCEMENT (SAFE)
        # -----------------------------
        img = ImageEnhance.Contrast(img).enhance(1.05)
        img = ImageEnhance.Sharpness(img).enhance(1.05)

        # -----------------------------
        # SAVE PROCESSED IMAGE
        # -----------------------------
        base, _ = os.path.splitext(image_path)
        processed_path = f"{base}_video.jpg"
        img.save(processed_path, "JPEG", quality=92, subsampling=0)

        return processed_path


# -------------------------------------------------
# FALLBACK IMAGE GENERATOR
# -------------------------------------------------


def create_fallback_image(output_path="images/fallback_video.jpg"):
    """
    Create a clean fallback background
    when Unsplash image is missing or fails.
    """
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    img = Image.new("RGB", (TARGET_WIDTH, TARGET_HEIGHT), (30, 30, 40))
    img.save(output_path, "JPEG", quality=90)

    return output_path
