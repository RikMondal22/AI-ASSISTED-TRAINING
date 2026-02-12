import os
import shutil
from moviepy.config import change_settings

# =============================================================================
# ImageMagick Configuration - COMPLETE FIX
# =============================================================================

def setup_imagemagick():
    """
    Auto-detect and configure ImageMagick for MoviePy
    Works in Docker and Windows
    """
    
    # Force Docker path first if in container
    docker_path = "/usr/bin/convert"
    if os.path.exists(docker_path):
        os.environ["IMAGEMAGICK_BINARY"] = docker_path
        change_settings({"IMAGEMAGICK_BINARY": docker_path})
        print(f"✅ ImageMagick configured: {docker_path}")
        return docker_path
    
    # Check environment variable
    env_path = os.getenv("IMAGEMAGICK_BINARY")
    if env_path and os.path.exists(env_path):
        change_settings({"IMAGEMAGICK_BINARY": env_path})
        print(f"✅ ImageMagick configured from env: {env_path}")
        return env_path
    
    # Check system PATH
    for cmd in ["convert", "magick"]:
        path = shutil.which(cmd)
        if path:
            os.environ["IMAGEMAGICK_BINARY"] = path
            change_settings({"IMAGEMAGICK_BINARY": path})
            print(f"✅ ImageMagick configured from PATH: {path}")
            return path
    
    # Windows fallback
    windows_paths = [
        r"C:\Program Files\ImageMagick-7.1.2-Q16-HDRI\magick.exe",
        r"C:\Program Files\ImageMagick\magick.exe",
        r"C:\Program Files (x86)\ImageMagick\magick.exe",
    ]
    for path in windows_paths:
        if os.path.exists(path):
            os.environ["IMAGEMAGICK_BINARY"] = path
            change_settings({"IMAGEMAGICK_BINARY": path})
            print(f"✅ ImageMagick configured: {path}")
            return path
    
    print("⚠️  WARNING: ImageMagick not found!")
    return None

# Initialize ImageMagick BEFORE any other imports that use it
setup_imagemagick()
from moviepy.editor import (
    ImageClip,
    concatenate_videoclips,
    CompositeVideoClip,
    AudioFileClip,
    concatenate_audioclips,
    TextClip,
    ColorClip,
    vfx,
)


# Now import avatar utils (which uses ImageMagick)
from utils.avatar_utils import add_avatar_to_slide

# Video configuration
VIDEO_W, VIDEO_H = 1280, 720
TOP_TEXT_HEIGHT = int(VIDEO_H * 0.6)
BOTTOM_IMAGE_HEIGHT = VIDEO_H - TOP_TEXT_HEIGHT


# =============================================================================
# Slide Creation
# =============================================================================

def create_slide(title, points, image_path, audio_file):
    """Create a single video slide with title, bullets, image, and audio"""
    
    if not os.path.exists(audio_file) or os.path.getsize(audio_file) < 1024:
        raise RuntimeError(f"Invalid audio file: {audio_file}")
    
    audio_clip = AudioFileClip(audio_file)
    duration = audio_clip.duration + 0.4  # small buffer

    # Background
    bg = (
        ColorClip(size=(VIDEO_W, VIDEO_H), color=(20, 22, 32))
        .set_duration(duration)
        .fx(vfx.fadein, 0.4)
        .fx(vfx.fadeout, 0.4)
    )

    overlay = (
        ColorClip(size=(VIDEO_W, VIDEO_H), color=(0, 0, 0))
        .set_opacity(0.35)
        .set_duration(duration)
    )

    # Title
    title_clip = (
        TextClip(
            title,
            fontsize=48,
            color="white",
            font="DejaVu-Sans-Bold",  # Use font that exists in Docker
            size=(VIDEO_W - 120, None),
            method="caption",
        )
        .set_position(("center", 50))
        .set_start(0.2)
        .set_duration(duration)
        .fadein(0.6)
    )

    title_shadow = (
        TextClip(
            title,
            fontsize=48,
            color="black",
            font="DejaVu-Sans-Bold",
            size=(VIDEO_W - 120, None),
            method="caption",
        )
        .set_position(("center", 52))
        .set_start(0.2)
        .set_duration(duration)
        .set_opacity(0.6)
    )

    # Bullets
    bullet_clips = []
    start_y = 140
    line_gap = 44

    for i, point in enumerate(points[:5]):
        appear_time = 0.8 + i * 0.5
        text = f"• {point.strip()}"

        bullet = (
            TextClip(
                text,
                fontsize=32,
                color="white",
                font="DejaVu-Sans",
                size=(VIDEO_W - 200, None),
                method="caption",
            )
            .set_position((100, start_y + i * line_gap))
            .set_start(appear_time)
            .set_duration(duration - appear_time)
            .fadein(0.4)
        )

        shadow = (
            TextClip(
                text,
                fontsize=32,
                color="black",
                font="DejaVu-Sans",
                size=(VIDEO_W - 200, None),
                method="caption",
            )
            .set_position((102, start_y + i * line_gap + 2))
            .set_start(appear_time)
            .set_duration(duration - appear_time)
            .set_opacity(0.5)
        )

        bullet_clips.extend([shadow, bullet])

    # Image
    image_clips = []
    if os.path.exists(image_path):
        img = (
            ImageClip(image_path)
            .resize(height=220)
            .set_position((VIDEO_W - 260, VIDEO_H - 260))
            .set_duration(duration)
        )
        image_clips.append(img)

    # Footer
    footer = (
        TextClip(
            "Bangla Sahayta Kendra • Government of West Bengal",
            fontsize=18,
            color="lightgray",
            font="DejaVu-Sans",
            size=(VIDEO_W - 80, None),
            method="caption",
        )
        .set_position(("center", VIDEO_H - 40))
        .set_duration(duration)
    )

    slide = CompositeVideoClip(
        [bg, overlay, title_shadow, title_clip] + bullet_clips + image_clips + [footer],
        size=(VIDEO_W, VIDEO_H),
    ).set_duration(duration)

    slide = slide.set_audio(audio_clip)

    # Add avatar
    slide = add_avatar_to_slide(slide, audio_clip.duration)

    return slide.crossfadein(0.4).crossfadeout(0.4)


def combine_slides_and_audio(video_clips, audio_paths, service_name=None):
    """Combine multiple slides into final video"""
    
    # Smooth overlap between slides
    final_video = concatenate_videoclips(video_clips, method="compose", padding=-0.4)

    audio_clips = [AudioFileClip(p) for p in audio_paths]
    final_audio = concatenate_audioclips(audio_clips)

    final_video = final_video.set_audio(final_audio)

    os.makedirs("output_videos", exist_ok=True)

    filename = "bsk_training_video.mp4"
    if service_name:
        safe = service_name.replace(" ", "_")
        filename = f"BSK_Training_{safe}.mp4"

    output_path = os.path.join("output_videos", filename)

    final_video.write_videofile(
        output_path,
        codec="libx264",
        audio_codec="aac",
        fps=30,
        preset="medium",
        bitrate="2000k",
        threads=4,
    )

    return output_path