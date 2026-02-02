"""
Unsplash image service for training video generation

Goals:
- Stable & predictable images
- Caching to avoid API overuse
- Safe fallback if API fails
"""

import os
import requests
import hashlib
from urllib.parse import quote_plus
from config import (
    UNSPLASH_ACCESS_KEY,
    UNSPLASH_API_URL,
    IMAGE_CACHE_DIR,
    FALLBACK_IMAGE,
)

# -------------------------------------------------
# CONFIG VALIDATION
# -------------------------------------------------
if not UNSPLASH_ACCESS_KEY:
    raise ValueError(
        "UNSPLASH_ACCESS_KEY not found in environment. "
        "Please set it in your .env file or environment variables."
    )


# -------------------------------------------------
# INTERNAL HELPERS
# -------------------------------------------------
def normalize_query(query: str) -> str:
    """
    Normalize image search keyword for better Unsplash results
    """
    query = query.lower().strip()
    query = query.replace("&", "and")
    return query


def cached_image_path(query: str) -> str:
    """
    Generate deterministic cache filename from query
    """
    hash_key = hashlib.md5(query.encode("utf-8")).hexdigest()
    return os.path.join(IMAGE_CACHE_DIR, f"{hash_key}.jpg")


# -------------------------------------------------
# UNSPLASH FETCH
# -------------------------------------------------
def fetch_photo_from_unsplash(query: str):
    """
    Fetch a single Unsplash image metadata
    """
    headers = {"Authorization": f"Client-ID {UNSPLASH_ACCESS_KEY}"}

    params = {"query": quote_plus(query), "per_page": 5, "orientation": "landscape"}

    response = requests.get(
        UNSPLASH_API_URL, headers=headers, params=params, timeout=10
    )
    response.raise_for_status()

    results = response.json().get("results", [])
    if not results:
        raise ValueError("No images found")

    # Pick the most relevant image (first result)
    return results[0]


# -------------------------------------------------
# PUBLIC API
# -------------------------------------------------
def fetch_and_save_photo(query: str) -> str:
    """
    Fetch an image from Unsplash and cache it locally.
    Always returns a local image path.
    """

    if not query or not query.strip():
        query = "government training presentation"

    query = normalize_query(query)

    # -----------------------------
    # CACHE CHECK
    # -----------------------------
    image_path = cached_image_path(query)
    if os.path.exists(image_path):
        return image_path

    # -----------------------------
    # FETCH FROM UNSPLASH
    # -----------------------------
    try:
        photo = fetch_photo_from_unsplash(query)
        image_url = photo["urls"]["regular"]

        image_data = requests.get(image_url, timeout=10).content
        with open(image_path, "wb") as f:
            f.write(image_data)

        return image_path

    except Exception as e:
        print(f"[Unsplash] Fallback used for query '{query}': {e}")
        return str(FALLBACK_IMAGE)
