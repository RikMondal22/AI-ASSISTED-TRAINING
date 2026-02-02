"""
Audio utilities for BSK training video generation

Goals:
- Clear, slow, professional narration
- Natural pauses between bullet points
- Predictable duration for video sync
"""

import tempfile
import edge_tts
import asyncio
import re
import os

# -------------------------------------------------
# DEFAULT VOICE SETTINGS (TRAINING OPTIMIZED)
# -------------------------------------------------
DEFAULT_VOICE = "en-IN-NeerjaNeural"  # Indian English, calm & professional
DEFAULT_RATE = "+5%"  # Slightly slower than normal
DEFAULT_PITCH = "+0Hz"


# -------------------------------------------------
# TEXT PRE-PROCESSING (VERY IMPORTANT)
# -------------------------------------------------
def prepare_narration_text(text: str) -> str:
    """
    Convert bullet-style text into narration-friendly speech.
    Adds pauses and removes visual-only symbols.
    """

    # Remove bullet symbols if present
    text = re.sub(r"[•▪◦]", "", text)

    # Ensure proper spacing after sentences
    text = re.sub(r"\.\s*", ". ", text)

    # Add slight pause markers after each sentence
    # Edge TTS respects commas and sentence breaks well
    text = text.replace(".", ". ")

    return text.strip()


# -------------------------------------------------
# TEXT TO SPEECH (ASYNC)
# -------------------------------------------------
async def text_to_speech(
    text: str,
    voice: str = DEFAULT_VOICE,
    rate: str = DEFAULT_RATE,
    pitch: str = DEFAULT_PITCH,
):
    """
    Generate narration audio for one slide.

    Input:
    - text: narration text (usually slide bullets joined)
    Output:
    - path to generated .mp3 file
    """

    narration_text = prepare_narration_text(text)

    communicate = edge_tts.Communicate(
        text=narration_text, voice=voice, rate=rate, pitch=pitch
    )

    with tempfile.NamedTemporaryFile(delete=False, suffix=".mp3") as audio_file:
        output_path = audio_file.name

    await communicate.save(output_path)

    # -------- HARD VALIDATION --------
    if not os.path.exists(output_path) or os.path.getsize(output_path) < 1024:
        raise RuntimeError("TTS failed: empty or invalid audio file generated")

    return output_path


# -------------------------------------------------
# SYNC HELPER (OPTIONAL BUT USEFUL)
# -------------------------------------------------
def estimate_audio_duration(text: str) -> float:
    """
    Rough duration estimation (seconds),
    useful for debugging or future timing logic.
    """
    words = len(text.split())
    avg_wpm = 130  # training-friendly speed
    return (words / avg_wpm) * 60
