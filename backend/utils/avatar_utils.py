"""
Avatar utilities for training video generation

Design goals:
- Calm, professional avatar
- Subtle motion (no distraction)
- Syncs with audio duration
- Easily replaceable with real lip-sync later
"""

import os
from moviepy.editor import ImageClip, CompositeVideoClip
import numpy as np

# -------------------------------------------------
# CONFIG
# -------------------------------------------------
DEFAULT_AVATAR_PATH = "assets/avatar/avatar.png"  # Provide a clean PNG avatar
AVATAR_HEIGHT = 220  # Professional size (not too big)


# -------------------------------------------------
# AVATAR CLIP GENERATOR
# -------------------------------------------------
def create_avatar_clip(duration, position=("left", "bottom")):
    """
    Create an animated avatar clip for a slide

    Animation:
    - Gentle breathing (scale)
    - Subtle side sway
    """

    if not os.path.exists(DEFAULT_AVATAR_PATH):
        return None

    avatar = ImageClip(DEFAULT_AVATAR_PATH).resize(height=AVATAR_HEIGHT)
    avatar = avatar.set_duration(duration)

    # -----------------------------
    # SUBTLE BREATHING EFFECT
    # -----------------------------
    avatar = avatar.resize(lambda t: 1 + 0.015 * np.sin(2 * np.pi * t / 4))

    # -----------------------------
    # SUBTLE HEAD SWAY
    # -----------------------------
    def avatar_position(t):
        sway = 4 * np.sin(2 * np.pi * t / 6)
        return (60 + sway, 720 - AVATAR_HEIGHT - 40)

    avatar = avatar.set_position(avatar_position)

    return avatar


# -------------------------------------------------
# AVATAR OVERLAY HELPER
# -------------------------------------------------
def add_avatar_to_slide(slide_clip, audio_duration):
    """
    Overlay avatar on an existing slide clip
    """
    avatar_clip = create_avatar_clip(audio_duration)
    if avatar_clip is None:
        return slide_clip

    return CompositeVideoClip([slide_clip, avatar_clip])
