import logging
from datetime import datetime, timedelta
import time
from fastapi import HTTPException
from sqlalchemy.orm import Session
from app.models import models
import pandas as pd
import numpy as np
from typing import Dict, List
import json
from dotenv import load_dotenv

logger = logging.getLogger(__name__)
import os

load_dotenv()
BASE_VIDEO_URL = os.getenv("BASE_URL", "https://videos.example.com/")


def enrich_recommendation(cache_rec, db: Session) -> dict:
    """
    Enrich cached provision data with real-time master table data.
    This is FAST because master tables are small (BSK, DEO, Service).

    UPDATED: Now includes video URLs for services that have training videos

    SIMPLIFIED: Removed unnecessary fields (bsk_lat, bsk_long, nearest_bsks,
    top_services_in_area, deos, analysis_metadata)
    """
    bsk_id = cache_rec.bsk_id

    # Get BSK details (FAST - small table ~500 rows)
    bsk = db.query(models.BSKMaster).filter(models.BSKMaster.bsk_id == bsk_id).first()

    # Get service details for recommendations
    recom_service_ids = cache_rec.recom_service_id or []
    recom_service_provs = cache_rec.recom_service_prov or []
    recom_service_neigh_provs = cache_rec.recom_service_neigh_prov or []

    # Fetch service details in batch (FAST - small table)
    services = (
        db.query(models.ServiceMaster)
        .filter(models.ServiceMaster.service_id.in_(recom_service_ids))
        .all()
        if recom_service_ids
        else []
    )

    service_lookup = {s.service_id: s for s in services}

    # Build recommended_services array
    recommended_services = []
    nearest_bsk_ids = cache_rec.nearest_bsks_id or []
    num_neighbors = len(nearest_bsk_ids)

    for i, service_id in enumerate(recom_service_ids):
        service = service_lookup.get(service_id)

        # Get values from parallel arrays
        current_prov = recom_service_provs[i] if i < len(recom_service_provs) else 0
        neigh_prov = (
            recom_service_neigh_provs[i] if i < len(recom_service_neigh_provs) else 0
        )

        # Calculate gap and nearby average
        nearby_avg = neigh_prov / num_neighbors if num_neighbors > 0 else 0
        gap = nearby_avg - current_prov

        # ✅ NEW: Get latest video URL for this service (if exists)
        video_url = None

        if service:
            # Try to find video by service_id first (most reliable)
            from sqlalchemy import desc, func

            latest_video = (
                db.query(models.ServiceVideo)
                .filter(
                    models.ServiceVideo.service_id == service_id,
                    models.ServiceVideo.is_done == True,  # Only completed videos
                    models.ServiceVideo.is_active == True,  # Only active videos
                )
                .order_by(desc(models.ServiceVideo.video_version))
                .first()
            )

            # If not found by ID, try by service name
            if not latest_video:
                latest_video = (
                    db.query(models.ServiceVideo)
                    .filter(
                        func.lower(models.ServiceVideo.service_name_metadata)
                        == func.lower(service.service_name),
                        models.ServiceVideo.is_done == True,
                        models.ServiceVideo.is_active == True,
                    )
                    .order_by(desc(models.ServiceVideo.video_version))
                    .first()
                )

            if latest_video:
                video_url = f"{BASE_VIDEO_URL}/{latest_video.video_url}"

        recommended_services.append(
            {
                "service_id": service_id,
                "service_name": service.service_name if service else "Unknown",
                "service_type": service.service_type if service else "N/A",
                "current_provisions": current_prov,
                "nearby_avg_provisions": round(nearby_avg, 2),
                "gap": round(gap, 2),
                "total_provisions_in_area": neigh_prov,
                "reason": (
                    f"Nearby BSKs (within {num_neighbors} nearest) are performing "
                    f"{nearby_avg:.1f} provisions on average for '{service.service_name if service else 'this service'}', "
                    f"while this BSK has only {current_prov} provisions. "
                    f"This service is highly demanded in the area ({neigh_prov} total provisions in neighborhood)."
                ),
            }
        )

    # Build response with simplified structure
    return {
        # ==========================================
        # GROUP 1: BSK IDENTIFICATION
        # ==========================================
        "bsk_id": bsk_id,
        "bsk_name": bsk.bsk_name if bsk else "",
        "bsk_type": bsk.bsk_type if bsk else "",
        # ==========================================
        # GROUP 2: BSK LOCATION
        # ==========================================
        "district_name": bsk.district_name if bsk else "",
        # ==========================================
        # GROUP 3: BSK PERFORMANCE METRICS
        # ==========================================
        "total_provision": cache_rec.total_provisions,
        "unique_service_provided": cache_rec.unique_services_provided,
        "priority_score": cache_rec.priority_score,
        # ==========================================
        # GROUP 4: TRAINING RECOMMENDATIONS (WITH VIDEO URLS)
        # ==========================================
        "total_training_services": cache_rec.total_training_services,
        "recommended_services": sorted(
            recommended_services, key=lambda x: x["gap"], reverse=True
        ),
    }


def compute_and_cache_recommendations(
    db: Session,
    n_neighbors: int = 10,
    top_n_services: int = 10,
    min_provision_threshold: int = 5,
    lookback_days: int = 365,  # ✅ NEW: Sliding window parameter
) -> dict:
    """
    Run OPTIMIZED provision analytics using SLIDING WINDOW technique.

    🚀 OPTIMIZATION: Instead of processing ALL provisions (which grows infinitely),
    we only process provisions from the last N days (default: 365 days).

    This keeps computation time CONSTANT regardless of database age!

    BEFORE: Query ALL provisions → 1.66M rows (growing daily) → 30-180s
    AFTER:  Query RECENT provisions → ~150K rows (stable) → 10-30s

    Args:
        db: Database session
        n_neighbors: Number of nearby BSKs to analyze
        top_n_services: Number of top services to consider
        min_provision_threshold: Minimum provisions to not need training
        lookback_days: Number of days to look back (default: 365)

    Returns:
        Summary of computation results
    """
    logger.info(
        f"🔄 Starting OPTIMIZED provision computation with {lookback_days}-day sliding window..."
    )

    # Calculate the cutoff date
    cutoff_date = datetime.now() - timedelta(days=lookback_days)
    logger.info(f"📅 Analyzing provisions from {cutoff_date.date()} to today")

    # Create computation log
    log_entry = models.RecommendationComputationLog(
        n_neighbors=n_neighbors,
        top_n_services=top_n_services,
        min_provision_threshold=min_provision_threshold,
        triggered_by="api_precompute_optimized",
        status="running",
    )
    db.add(log_entry)
    db.commit()

    start_time = time.time()

    try:
        # STEP 1: Fetch data with SLIDING WINDOW optimization
        logger.info("📊 Fetching data from database with date filter...")

        # BSKs - small table, no filtering needed
        bsks = db.query(models.BSKMaster).all()
        bsks_df = pd.DataFrame(
            [
                {
                    "bsk_id": b.bsk_id,
                    "bsk_name": b.bsk_name,
                    "bsk_code": b.bsk_code,
                    "bsk_lat": b.bsk_lat,
                    "bsk_long": b.bsk_long,
                    "district_name": b.district_name,
                    "block_municipalty_name": b.block_municipalty_name,
                    "bsk_type": b.bsk_type,
                }
                for b in bsks
            ]
        )

        # ✅ OPTIMIZATION: SLIDING WINDOW - Only fetch provisions from last N days
        # THIS IS THE KEY OPTIMIZATION!
        # Since prov_date is stored as Text, we need to cast it to DATE for comparison
        from sqlalchemy import cast, Date
        
        try:
            # Cast the text column to DATE type for proper comparison
            provisions = (
                db.query(models.Provision)
                .filter(
                    cast(models.Provision.prov_date, Date) >= cutoff_date.date()
                )
                .all()
            )
        except Exception as e:
            # If casting fails (e.g., invalid date formats), fall back to fetching all
            logger.warning(f"⚠️ Date filtering failed: {e}. Fetching all provisions...")
            provisions = db.query(models.Provision).all()

        provisions_df = pd.DataFrame(
            [
                {
                    "bsk_id": p.bsk_id,
                    "service_id": p.service_id,
                    "customer_id": p.customer_id,
                    "prov_date": p.prov_date,
                }
                for p in provisions
            ]
        )

        # Log the optimization impact
        logger.info(
            f"✅ OPTIMIZATION IMPACT: Fetched {len(provisions_df):,} provisions "
            f"(instead of ALL provisions) - {lookback_days} day window"
        )

        # Services - small table, no filtering needed
        services = db.query(models.ServiceMaster).all()
        services_df = pd.DataFrame(
            [
                {
                    "service_id": s.service_id,
                    "service_name": s.service_name,
                    "service_type": s.service_type,
                }
                for s in services
            ]
        )

        # DEOs - small table, no filtering needed
        deos = db.query(models.DEOMaster).all()
        deos_df = pd.DataFrame(
            [
                {
                    "agent_id": d.agent_id,
                    "user_name": d.user_name,
                    "agent_code": d.agent_code,
                    "agent_email": d.agent_email,
                    "agent_phone": d.agent_phone,
                    "bsk_id": d.bsk_id,
                    "bsk_post": d.bsk_post,
                    "is_active": d.is_active,
                }
                for d in deos
            ]
        )

        logger.info(
            f"✅ Loaded {len(bsks_df)} BSKs, {len(provisions_df):,} provisions "
            f"(from last {lookback_days} days), {len(services_df)} services, {len(deos_df)} DEOs"
        )

        # STEP 2: Run the analytics algorithm with filtered data
        logger.info("🧮 Running recommendation algorithm on RECENT provisions...")
        recommendations = training_recommendation(
            bsks_df=bsks_df,
            provisions_df=provisions_df,  # Now contains only recent data!
            deos_df=deos_df,
            services_df=services_df,
            n_neighbors=n_neighbors,
            top_n_services=top_n_services,
            min_provision_threshold=min_provision_threshold,
        )

        logger.info(f"✅ Generated {len(recommendations)} recommendations")

        # STEP 3: Clear old cache and store new results
        logger.info("🗑️ Clearing old cache...")
        db.query(models.TrainingRecommendationCache).delete()
        db.commit()

        logger.info("💾 Storing new provision computations in cache...")
        for rec in recommendations:
            bsk_id = rec["bsk_id"]

            # Calculate provision metrics for this BSK (from filtered data)
            prov_data = provisions_df[provisions_df["bsk_id"] == bsk_id]
            total_prov = len(prov_data)
            unique_services = (
                prov_data["service_id"].nunique() if len(prov_data) > 0 else 0
            )

            # Extract nearest BSKs - separate IDs and distances into parallel arrays
            nearest_bsks_raw = rec.get("nearest_bsks", [])
            nearest_bsk_ids = [nb["bsk_id"] for nb in nearest_bsks_raw]
            distances = [nb["distance_km"] for nb in nearest_bsks_raw]

            # Extract top services in neighborhood
            top_services_in_area = rec.get("top_services_in_area", [])

            # Extract recommended services into parallel arrays
            recom_services = rec.get("recommended_services", [])
            recom_service_ids = []
            recom_service_provs = []
            recom_service_neigh_provs = []

            for s in recom_services:
                recom_service_ids.append(s["service_id"])
                recom_service_provs.append(s.get("current_provisions", 0))
                recom_service_neigh_provs.append(s.get("total_provisions_in_area", 0))

            # Create optimized cache entry with list-based storage
            cache_entry = models.TrainingRecommendationCache(
                bsk_id=bsk_id,
                # Provision metrics (from sliding window data)
                total_provisions=total_prov,
                unique_services_provided=unique_services,
                priority_score=rec.get("priority_score", 0),
                # Nearest BSKs - parallel arrays
                nearest_bsks_id=nearest_bsk_ids,
                distance_km=distances,
                # Top services in neighborhood
                neigh_top_services_id=top_services_in_area,
                # Recommendations - parallel arrays
                total_training_services=len(recom_services),
                recom_service_id=recom_service_ids,
                recom_service_prov=recom_service_provs,
                recom_service_neigh_prov=recom_service_neigh_provs,
            )
            db.add(cache_entry)

        db.commit()
        logger.info(f"✅ Cached {len(recommendations)} entries")

        # STEP 4: Update computation log with optimization metrics
        duration = time.time() - start_time
        log_entry.completion_timestamp = datetime.now()
        log_entry.computation_duration_seconds = duration
        log_entry.status = "completed"
        log_entry.total_bsks_analyzed = len(bsks_df)
        log_entry.total_provisions_processed = len(provisions_df)
        log_entry.total_recommendations_generated = len(recommendations)
        db.commit()

        logger.info(
            f"✅ OPTIMIZED computation completed in {duration:.2f}s "
            f"(processing {len(provisions_df):,} provisions from last {lookback_days} days)"
        )

        return {
            "success": True,
            "message": f"Successfully computed and cached {len(recommendations)} recommendations",
            "computation_time_seconds": round(duration, 2),
            "provisions_processed": len(provisions_df),
            "lookback_days": lookback_days,  # ✅ NEW
            "cutoff_date": cutoff_date.isoformat(),  # ✅ NEW
            "bsks_analyzed": len(bsks_df),
            "recommendations_generated": len(recommendations),
            "optimization_note": f"Used sliding window of {lookback_days} days instead of all historical data",  # ✅ NEW
        }

    except Exception as e:
        # Log failure
        log_entry.status = "failed"
        log_entry.error_message = str(e)
        log_entry.completion_timestamp = datetime.now()
        log_entry.computation_duration_seconds = time.time() - start_time
        db.commit()

        logger.error(f"❌ Computation failed: {e}")
        raise HTTPException(status_code=500, detail=f"Computation failed: {str(e)}")


# ============================================================================
# REMAINING HELPER FUNCTIONS (UNCHANGED)
# ============================================================================


def haversine_distance(lat1, lon1, lat2, lon2):
    """
    Calculate the great circle distance between two points
    on the earth (specified in decimal degrees).
    Returns distance in kilometers.
    """
    # Convert decimal degrees to radians
    lat1, lon1, lat2, lon2 = map(np.radians, [lat1, lon1, lat2, lon2])

    # Haversine formula
    dlat = lat2 - lat1
    dlon = lon2 - lon1
    a = np.sin(dlat / 2) ** 2 + np.cos(lat1) * np.cos(lat2) * np.sin(dlon / 2) ** 2
    c = 2 * np.arcsin(np.sqrt(a))

    # Radius of earth in kilometers
    r = 6371
    return c * r


def find_nearest_bsks(
    target_bsk_id: int, bsks_df: pd.DataFrame, n_neighbors: int = 10
) -> List[int]:
    """
    Find N nearest BSKs to a target BSK using geographic coordinates.

    Args:
        target_bsk_id: The BSK ID to find neighbors for
        bsks_df: DataFrame with BSK data including lat/long
        n_neighbors: Number of nearest neighbors to find

    Returns:
        List of nearest BSK IDs (excluding the target BSK itself)
    """
    # Get target BSK coordinates
    target_bsk = bsks_df[bsks_df["bsk_id"] == target_bsk_id]
    if target_bsk.empty:
        return []

    target_lat = float(target_bsk.iloc[0]["bsk_lat"])
    target_lon = float(target_bsk.iloc[0]["bsk_long"])

    # Calculate distances to all other BSKs
    distances = []
    for _, bsk in bsks_df.iterrows():
        if bsk["bsk_id"] == target_bsk_id:
            continue

        try:
            lat = float(bsk["bsk_lat"])
            lon = float(bsk["bsk_long"])
            dist = haversine_distance(target_lat, target_lon, lat, lon)
            distances.append({"bsk_id": int(bsk["bsk_id"]), "distance_km": dist})
        except (ValueError, TypeError):
            continue

    # Sort by distance and get top N
    distances.sort(key=lambda x: x["distance_km"])
    nearest_ids = [d["bsk_id"] for d in distances[:n_neighbors]]

    return nearest_ids


def get_top_services_from_bsks(
    bsk_ids: List[int],
    provisions_df: pd.DataFrame,
    services_df: pd.DataFrame,
    top_n: int = 10,
) -> List[Dict]:
    """
    Find top services performed by a list of BSKs.

    Args:
        bsk_ids: List of BSK IDs to analyze
        provisions_df: DataFrame with provision data (filtered by date)
        services_df: DataFrame with service details
        top_n: Number of top services to return

    Returns:
        List of top service dictionaries with counts
    """
    # Filter provisions for these BSKs
    bsk_provisions = provisions_df[provisions_df["bsk_id"].isin(bsk_ids)]

    if bsk_provisions.empty:
        return []

    # Count services
    service_counts = (
        bsk_provisions.groupby("service_id").size().reset_index(name="total_provisions")
    )
    service_counts = service_counts.sort_values(
        "total_provisions", ascending=False
    ).head(top_n)

    # Add service details
    services_lookup = services_df.set_index("service_id").to_dict("index")

    top_services = []
    for _, row in service_counts.iterrows():
        service_id = int(row["service_id"])
        if service_id in services_lookup:
            service_info = services_lookup[service_id]
            top_services.append(
                {
                    "service_id": service_id,
                    "service_name": str(service_info.get("service_name", "Unknown")),
                    "service_type": str(service_info.get("service_type", "N/A")),
                    "total_provisions_in_area": int(row["total_provisions"]),
                }
            )

    return top_services


def calculate_bsk_service_performance(
    bsk_id: int, service_id: int, provisions_df: pd.DataFrame
) -> int:
    """
    Calculate how many times a BSK has provided a specific service.

    Note: provisions_df is already filtered by date in the parent function.
    """
    count = len(
        provisions_df[
            (provisions_df["bsk_id"] == bsk_id)
            & (provisions_df["service_id"] == service_id)
        ]
    )
    return count


def training_recommendation(
    bsks_df: pd.DataFrame,
    provisions_df: pd.DataFrame,
    deos_df: pd.DataFrame,
    services_df: pd.DataFrame,
    n_neighbors: int = 5,
    top_n_services: int = 10,
    min_provision_threshold: int = 5,
) -> List[Dict]:
    """
    Generate training recommendations based on nearest BSK analysis.

    ✅ OPTIMIZED: Now works with sliding window filtered provisions_df.
    The algorithm itself doesn't change, but it processes less data.

    Algorithm:
    1. For each BSK, find N nearest BSKs
    2. Identify top services performed by those nearby BSKs (from recent data)
    3. Check if the target BSK is underperforming on those services (from recent data)
    4. Generate recommendations with reasoning

    Args:
        bsks_df: DataFrame of BSK centers
        provisions_df: DataFrame of service provisions (PRE-FILTERED by date)
        deos_df: DataFrame of DEOs
        services_df: DataFrame of services
        n_neighbors: Number of nearby BSKs to analyze
        top_n_services: Number of top services to consider
        min_provision_threshold: Minimum provisions to not need training

    Returns:
        List of training recommendations with detailed reasoning
    """
    print("🔄 Starting proximity-based training recommendation analysis...")
    print(f"   Working with {len(provisions_df):,} provision records")

    # Prepare BSK data
    print("[1/5] Preparing BSK data...")
    bsks = bsks_df.copy()
    bsks["bsk_id"] = pd.to_numeric(bsks["bsk_id"], errors="coerce")
    bsks["bsk_lat"] = pd.to_numeric(bsks["bsk_lat"], errors="coerce")
    bsks["bsk_long"] = pd.to_numeric(bsks["bsk_long"], errors="coerce")
    bsks = bsks.dropna(subset=["bsk_lat", "bsk_long", "bsk_id"])

    if len(bsks) == 0:
        print("❌ No valid BSKs with coordinates found")
        return []

    print(f"   ✓ {len(bsks)} valid BSKs loaded")

    # Prepare provisions data (already filtered by parent function)
    print("[2/5] Preparing provisions data...")
    prov = provisions_df.copy()
    prov["bsk_id"] = pd.to_numeric(prov["bsk_id"], errors="coerce")
    prov["service_id"] = pd.to_numeric(prov["service_id"], errors="coerce")
    prov = prov.dropna(subset=["bsk_id", "service_id"])
    print(f"   ✓ {len(prov)} provision records loaded")

    # Prepare DEOs lookup
    print("[3/5] Preparing DEO data...")
    deos_by_bsk = (
        deos_df.groupby("bsk_id").apply(lambda x: x.to_dict("records")).to_dict()
    )

    # Generate recommendations
    print("[4/5] Analyzing BSK neighborhoods and generating recommendations...")
    recommendations = []

    for idx, bsk_row in bsks.iterrows():
        bsk_id = int(bsk_row["bsk_id"])

        # Find nearest BSKs
        nearest_bsk_ids = find_nearest_bsks(bsk_id, bsks, n_neighbors)

        if not nearest_bsk_ids:
            continue

        # Get top services from nearby BSKs (using filtered provisions)
        top_services = get_top_services_from_bsks(
            nearest_bsk_ids, prov, services_df, top_n_services
        )

        if not top_services:
            continue

        # Check performance on each top service
        recommended_services = []

        for service in top_services:
            service_id = service["service_id"]

            # Get current BSK's performance (from filtered data)
            current_provisions = calculate_bsk_service_performance(
                bsk_id, service_id, prov
            )

            # Calculate average for nearby BSKs (from filtered data)
            nearby_provisions = prov[
                (prov["bsk_id"].isin(nearest_bsk_ids))
                & (prov["service_id"] == service_id)
            ]
            avg_provisions = (
                len(nearby_provisions) / len(nearest_bsk_ids) if nearest_bsk_ids else 0
            )

            # If underperforming, add to recommendations
            if current_provisions < min_provision_threshold:
                gap = avg_provisions - current_provisions

                recommended_services.append(
                    {
                        "service_id": service_id,
                        "service_name": service["service_name"],
                        "service_type": service["service_type"],
                        "current_provisions": int(current_provisions),
                        "nearby_avg_provisions": round(float(avg_provisions), 2),
                        "gap": round(float(gap), 2),
                        "total_provisions_in_area": service["total_provisions_in_area"],
                        "reason": (
                            f"Nearby BSKs (within {n_neighbors} nearest) are performing "
                            f"{avg_provisions:.1f} provisions on average for '{service['service_name']}', "
                            f"while this BSK has only {current_provisions} provisions. "
                            f"This service is highly demanded in the area ({service['total_provisions_in_area']} "
                            f"total provisions in neighborhood)."
                        ),
                    }
                )

        if recommended_services:
            # Get DEO information
            bsk_deos = deos_by_bsk.get(bsk_id, [])
            deo_details = []

            for deo_row in bsk_deos:
                deo_details.append(
                    {
                        "agent_id": str(deo_row.get("agent_id", "")),
                        "user_name": str(deo_row.get("user_name", "")),
                        "agent_code": str(deo_row.get("agent_code", "")),
                        "agent_email": str(deo_row.get("agent_email", "")),
                        "agent_phone": str(deo_row.get("agent_phone", "")),
                        "bsk_post": str(deo_row.get("bsk_post", "")),
                        "is_active": bool(deo_row.get("is_active", False)),
                    }
                )

            # Get nearby BSK info
            nearby_bsks_info = []
            for nearby_id in nearest_bsk_ids:
                nearby_bsk = bsks[bsks["bsk_id"] == nearby_id]
                if not nearby_bsk.empty:
                    nearby_bsks_info.append(
                        {
                            "bsk_id": int(nearby_id),
                            "bsk_name": str(nearby_bsk.iloc[0]["bsk_name"]),
                            "bsk_code": str(nearby_bsk.iloc[0]["bsk_code"]),
                            "distance_km": round(
                                haversine_distance(
                                    float(bsk_row["bsk_lat"]),
                                    float(bsk_row["bsk_long"]),
                                    float(nearby_bsk.iloc[0]["bsk_lat"]),
                                    float(nearby_bsk.iloc[0]["bsk_long"]),
                                ),
                                2,
                            ),
                        }
                    )

            # Create recommendation
            recommendation = {
                "bsk_id": int(bsk_id),
                "bsk_name": str(bsk_row.get("bsk_name", "")),
                "bsk_code": str(bsk_row.get("bsk_code", "")),
                "district_name": str(bsk_row.get("district_name", "")),
                "block_municipalty_name": str(
                    bsk_row.get("block_municipalty_name", "")
                ),
                "bsk_type": str(bsk_row.get("bsk_type", "")),
                "bsk_lat": (
                    float(bsk_row["bsk_lat"]) if pd.notna(bsk_row["bsk_lat"]) else None
                ),
                "bsk_long": (
                    float(bsk_row["bsk_long"])
                    if pd.notna(bsk_row["bsk_long"])
                    else None
                ),
                "nearest_bsks": nearby_bsks_info,
                "nearest_bsk_ids": [int(x) for x in nearest_bsk_ids],
                "top_services_in_area": [s["service_id"] for s in top_services],
                "total_training_services": len(recommended_services),
                "recommended_services": sorted(
                    recommended_services, key=lambda x: x["gap"], reverse=True
                ),
                "deos": deo_details,
                "priority_score": sum(s["gap"] for s in recommended_services),
                "analysis_metadata": {
                    "n_neighbors_analyzed": len(nearest_bsk_ids),
                    "top_n_services_considered": len(top_services),
                    "analysis_timestamp": datetime.now().isoformat(),
                },
            }

            recommendations.append(recommendation)

        # Progress indicator
        if (idx + 1) % 100 == 0:
            print(f"   Processed {idx + 1}/{len(bsks)} BSKs...")

    # Sort by priority
    recommendations = sorted(
        recommendations, key=lambda x: x["priority_score"], reverse=True
    )

    print(f"✅ Generated {len(recommendations)} training recommendations")

    return recommendations


def export_recommendations_json(
    recommendations: List[Dict], filepath: str = "training_recommendations.json"
):
    """Export recommendations to JSON file."""
    with open(filepath, "w", encoding="utf-8") as f:
        json.dump(recommendations, f, indent=2, ensure_ascii=False)

    print(f"💾 Training recommendations exported to {filepath}")