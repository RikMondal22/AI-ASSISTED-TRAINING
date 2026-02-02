import pandas as pd
import numpy as np
from sklearn.cluster import KMeans
from collections import Counter
from typing import Optional, Tuple


def find_underperforming_bsks(
    bsks_df,
    provisions_df,
    deos_df,
    services_df,
    period_start=None,
    period_end=None,
    delta_state=0,
    delta_dist=0,
    delta_cluster=0,
    n_clusters=None,
):
    """
    Three Level Benchmark Approach for underperforming BSKs with recommendations.
    Args:
        bsks_df: DataFrame of BSKs
        provisions_df: DataFrame of provisions (transactions)
        deos_df: DataFrame of DEOs
        services_df: DataFrame of services
        period_start, period_end: filter provisions to this period (inclusive)
        delta_state, delta_dist, delta_cluster: buffer values (absolute or as fraction)
        n_clusters: number of clusters for KMeans (default: sqrt(N))
    Returns:
        DataFrame with BSK info, total_services, recommended_services, and DEO details
    """
    # --- Ensure provisions_df has district_id for district-level logic ---
    provisions_df = provisions_df.merge(
        bsks_df[["bsk_id", "district_id"]], on="bsk_id", how="left"
    )
    # 1. Filter provisions by period if specified
    if period_start or period_end:
        provisions_df = provisions_df.copy()
        provisions_df["prov_date"] = pd.to_datetime(
            provisions_df["prov_date"], errors="coerce"
        )
        if period_start:
            provisions_df = provisions_df[
                provisions_df["prov_date"] >= pd.to_datetime(period_start)
            ]
        if period_end:
            provisions_df = provisions_df[
                provisions_df["prov_date"] <= pd.to_datetime(period_end)
            ]

    # 2. Compute total services per BSK
    service_counts = (
        provisions_df.groupby("bsk_id").size().reset_index(name="total_services")
    )
    bsks_df = bsks_df.copy()
    bsks_df["bsk_id"] = pd.to_numeric(bsks_df["bsk_id"], errors="coerce")
    merged = bsks_df.merge(service_counts, on="bsk_id", how="left")
    merged["total_services"] = merged["total_services"].fillna(0)
    # Debug: Print total_services for all BSKs
    print("--- total_services breakdown ---")
    print(merged[["bsk_id", "bsk_name", "total_services"]])

    # 3. Compute State_Avg
    state_avg = merged["total_services"].mean()
    if 0 < delta_state < 1:
        state_buffer = state_avg * delta_state
    else:
        state_buffer = delta_state
    state_threshold = state_avg - state_buffer

    # 4. Compute Dist_Avg per district
    dist_avgs = merged.groupby("district_id")["total_services"].mean()
    dist_buffers = dist_avgs * delta_dist if 0 < delta_dist < 1 else delta_dist
    dist_thresholds = dist_avgs - dist_buffers

    # 5. Cluster BSKs (KMeans on lat/long)
    coords = merged[["bsk_lat", "bsk_long"]].astype(float)
    if n_clusters is None:
        n_clusters = int(np.sqrt(len(merged))) or 1
    kmeans = KMeans(n_clusters=n_clusters, random_state=42, n_init=10)
    merged["cluster_id"] = kmeans.fit_predict(coords)
    clust_avgs = merged.groupby("cluster_id")["total_services"].mean()
    clust_buffers = (
        clust_avgs * delta_cluster if 0 < delta_cluster < 1 else delta_cluster
    )
    clust_thresholds = clust_avgs - clust_buffers

    # 6. Flag underperformers and attach reason
    def is_under(row):
        dist_thr = dist_thresholds.get(row["district_id"], state_threshold)
        clust_thr = clust_thresholds.get(row["cluster_id"], state_threshold)
        reasons = []
        if row["total_services"] < state_threshold:
            reasons.append(
                f'Below state threshold ({row["total_services"]:.1f} < {state_threshold:.1f})'
            )
        if row["total_services"] < dist_thr:
            reasons.append(
                f'Below district threshold ({row["total_services"]:.1f} < {dist_thr:.1f})'
            )
        if row["total_services"] < clust_thr:
            reasons.append(
                f'Below cluster threshold ({row["total_services"]:.1f} < {clust_thr:.1f})'
            )
        under = (
            row["total_services"] < state_threshold
            and row["total_services"] < dist_thr
            and row["total_services"] < clust_thr
        )
        return under, ", ".join(reasons)

    merged[["underperforming", "reason"]] = merged.apply(
        lambda row: pd.Series(is_under(row)), axis=1
    )
    under_bsks = merged[merged["underperforming"]].copy()

    # 7. For each underperforming BSK, recommend Top 10 services in its district not delivered by this BSK
    recommendations = []
    for idx, row in under_bsks.iterrows():
        bsk_id = row["bsk_id"]
        district_id = row["district_id"]
        cluster_id = row["cluster_id"]
        # Services delivered by this BSK
        bsk_services = set(
            provisions_df[provisions_df["bsk_id"] == bsk_id]["service_id"]
        )
        # Top services in district
        district_services = provisions_df[provisions_df["district_id"] == district_id][
            "service_id"
        ]
        top_services = [sid for sid, _ in Counter(district_services).most_common(10)]
        # Recommend those not already delivered
        recommended = [sid for sid in top_services if sid not in bsk_services]
        # Get service names
        rec_names = [
            services_df.loc[services_df["service_id"] == sid, "service_name"].values[0]
            for sid in recommended
            if sid in services_df["service_id"].values
        ]
        recommendations.append(rec_names)
    under_bsks["recommended_services"] = recommendations

    # 8. Attach DEO details
    deos_df = deos_df.copy()
    deos_df["bsk_id"] = pd.to_numeric(deos_df["bsk_id"], errors="coerce")
    under_bsks = under_bsks.merge(
        deos_df, on="bsk_id", how="left", suffixes=("", "_deo")
    )

    # Compute a normalized score for each BSK (min-max of total_services)
    min_services = merged["total_services"].min()
    max_services = merged["total_services"].max()
    print(f"min_services: {min_services}, max_services: {max_services}")
    if max_services > min_services:
        merged["score"] = (merged["total_services"] - min_services) / (
            max_services - min_services
        )
    else:
        merged["score"] = 0.0
    print("--- score breakdown ---")
    print(merged[["bsk_id", "bsk_name", "total_services", "score"]])
    under_bsks = under_bsks.merge(
        merged[["bsk_id", "score"]], on="bsk_id", how="left", suffixes=("", "_score")
    )
    # 9. Select relevant columns for output
    output_cols = [
        "bsk_id",
        "bsk_code",
        "bsk_name",
        "district_id",
        "district_name",
        "block_municipalty_name",
        "bsk_lat",
        "bsk_long",
        "cluster_id",
        "total_services",
        "score",
        "underperforming",
        "reason",
        "recommended_services",
        "agent_id",
        "user_name",
        "agent_code",
        "agent_email",
        "agent_phone",
        "date_of_engagement",
        "bsk_post",
    ]
    output = (
        under_bsks[output_cols]
        if all(col in under_bsks.columns for col in output_cols)
        else under_bsks
    )
    # --- Fix: Replace NaN/inf with None for JSON serialization ---
    output = output.replace([np.nan, np.inf, -np.inf], None)
    return output


def analyze_bsk_performance_trends(
    bsks_df, provisions_df, services_df, time_period="monthly"
) -> pd.DataFrame:
    """
    Analyze BSK performance trends over time.

    Args:
        bsks_df: DataFrame of BSKs
        provisions_df: DataFrame of provisions
        services_df: DataFrame of services
        time_period: 'daily', 'weekly', 'monthly', 'quarterly'

    Returns:
        DataFrame with BSK performance metrics over time
    """
    provisions_df = provisions_df.copy()
    provisions_df["prov_date"] = pd.to_datetime(
        provisions_df["prov_date"], errors="coerce"
    )

    # Group by time period
    freq_map = {"daily": "D", "weekly": "W", "monthly": "M", "quarterly": "Q"}

    freq = freq_map.get(time_period, "M")

    # Group by BSK and time period
    time_groups = (
        provisions_df.groupby(["bsk_id", pd.Grouper(key="prov_date", freq=freq)])
        .agg({"service_id": "count", "customer_id": "nunique"})
        .reset_index()
    )

    time_groups.columns = ["bsk_id", "period", "total_services", "unique_customers"]

    # Merge with BSK details
    result = time_groups.merge(
        bsks_df[["bsk_id", "bsk_name", "district_name", "block_municipalty_name"]],
        on="bsk_id",
        how="left",
    )

    return result


def get_top_performing_bsks(
    bsks_df, provisions_df, services_df, top_n=20, metric="total_services"
) -> pd.DataFrame:
    """
    Get top performing BSKs based on various metrics.

    Args:
        bsks_df: DataFrame of BSKs
        provisions_df: DataFrame of provisions
        services_df: DataFrame of services
        top_n: Number of top BSKs to return
        metric: 'total_services', 'unique_customers', 'service_diversity'

    Returns:
        DataFrame with top performing BSKs
    """
    # Calculate performance metrics
    performance = (
        provisions_df.groupby("bsk_id")
        .agg({"service_id": ["count", "nunique"], "customer_id": "nunique"})
        .reset_index()
    )

    performance.columns = [
        "bsk_id",
        "total_services",
        "service_diversity",
        "unique_customers",
    ]

    # Merge with BSK details
    result = performance.merge(bsks_df, on="bsk_id", how="left")

    # Sort by chosen metric
    if metric in result.columns:
        result = result.sort_values(metric, ascending=False).head(top_n)

    return result


def calculate_district_benchmarks(bsks_df, provisions_df, services_df) -> pd.DataFrame:
    """
    Calculate performance benchmarks by district.

    Args:
        bsks_df: DataFrame of BSKs
        provisions_df: DataFrame of provisions
        services_df: DataFrame of services

    Returns:
        DataFrame with district-level benchmarks
    """
    # Merge provisions with BSK district info
    provisions_with_district = provisions_df.merge(
        bsks_df[["bsk_id", "district_id", "district_name"]], on="bsk_id", how="left"
    )

    # Calculate BSK-level metrics
    bsk_metrics = (
        provisions_with_district.groupby(["bsk_id", "district_id", "district_name"])
        .agg({"service_id": "count", "customer_id": "nunique"})
        .reset_index()
    )

    bsk_metrics.columns = [
        "bsk_id",
        "district_id",
        "district_name",
        "total_services",
        "unique_customers",
    ]

    # Calculate district-level benchmarks
    district_benchmarks = (
        bsk_metrics.groupby(["district_id", "district_name"])
        .agg(
            {
                "total_services": ["mean", "median", "std", "min", "max"],
                "unique_customers": ["mean", "median", "std", "min", "max"],
                "bsk_id": "count",
            }
        )
        .reset_index()
    )

    # Flatten column names
    district_benchmarks.columns = [
        "district_id",
        "district_name",
        "avg_services",
        "median_services",
        "std_services",
        "min_services",
        "max_services",
        "avg_customers",
        "median_customers",
        "std_customers",
        "min_customers",
        "max_customers",
        "total_bsks",
    ]

    return district_benchmarks


def identify_service_gaps(
    bsks_df, provisions_df, services_df, target_district_id=None
) -> pd.DataFrame:
    """
    Identify service gaps - services that are popular in some BSKs but missing in others.

    Args:
        bsks_df: DataFrame of BSKs
        provisions_df: DataFrame of provisions
        services_df: DataFrame of services
        target_district_id: Optional district to focus analysis on

    Returns:
        DataFrame with service gap analysis
    """
    # Filter by district if specified
    if target_district_id:
        target_bsks = bsks_df[bsks_df["district_id"] == target_district_id]["bsk_id"]
        provisions_filtered = provisions_df[provisions_df["bsk_id"].isin(target_bsks)]
    else:
        provisions_filtered = provisions_df

    # Calculate service popularity across BSKs
    service_popularity = (
        provisions_filtered.groupby("service_id")
        .agg({"bsk_id": "nunique", "customer_id": "count"})
        .reset_index()
    )

    service_popularity.columns = ["service_id", "bsk_count", "total_provisions"]

    # Merge with service details
    service_gaps = service_popularity.merge(
        services_df[["service_id", "service_name", "service_type"]],
        on="service_id",
        how="left",
    )

    # Calculate total BSKs for percentage
    total_bsks = (
        bsks_df["bsk_id"].nunique() if not target_district_id else len(target_bsks)
    )
    service_gaps["bsk_penetration"] = (service_gaps["bsk_count"] / total_bsks) * 100

    # Sort by popularity but low penetration (good candidates for expansion)
    service_gaps["gap_score"] = service_gaps["total_provisions"] * (
        100 - service_gaps["bsk_penetration"]
    )
    service_gaps = service_gaps.sort_values("gap_score", ascending=False)

    return service_gaps


########################## Trial new Underperforming #############################################
# import pandas as pd
# import numpy as np
# from sklearn.cluster import KMeans
# from datetime import timedelta

# def find_underperforming_bsks(
#     bsks_df: pd.DataFrame,
#     provisions_df: pd.DataFrame,
#     services_df: pd.DataFrame,
#     recent_days: int = 90
# ):
#     """
#     FINAL VERSION – Auto KMeans + Cluster + Rural/Urban benchmarking
#     """

#     # ----------------------------
#     # 1. PREPARE BSK DATA
#     # ----------------------------
#     bsks = bsks_df.copy()
#     bsks['bsk_lat'] = pd.to_numeric(bsks['bsk_lat'], errors='coerce')
#     bsks['bsk_long'] = pd.to_numeric(bsks['bsk_long'], errors='coerce')
#     bsks = bsks.dropna(subset=['bsk_lat', 'bsk_long'])

#     # ----------------------------
#     # 2. AUTO KMEANS (ML)
#     # ----------------------------
#     n_clusters = int(np.sqrt(len(bsks)))
#     kmeans = KMeans(n_clusters=n_clusters, random_state=42, n_init=10)
#     bsks['cluster_id'] = kmeans.fit_predict(bsks[['bsk_lat', 'bsk_long']])

#     # ----------------------------
#     # 3. PREPARE PROVISIONS
#     # ----------------------------
#     prov = provisions_df.copy()
#     prov['prov_date'] = pd.to_datetime(prov['prov_date'], errors='coerce')
#     prov = prov.merge(
#         bsks[['bsk_id', 'cluster_id', 'bsk_type']],
#         on='bsk_id',
#         how='left'
#     )

#     # ----------------------------
#     # 4. BASIC METRICS
#     # ----------------------------
#     metrics = bsks[['bsk_id', 'cluster_id', 'bsk_type']].copy()

#     metrics['total_services'] = prov.groupby('bsk_id').size()
#     metrics['unique_services'] = prov.groupby('bsk_id')['service_id'].nunique()

#     metrics[['total_services', 'unique_services']] = (
#         metrics[['total_services', 'unique_services']].fillna(0)
#     )

#     # ----------------------------
#     # 5. TREND (90 DAYS)
#     # ----------------------------
#     latest_date = prov['prov_date'].max()
#     recent_cutoff = latest_date - timedelta(days=recent_days)
#     prev_cutoff = recent_cutoff - timedelta(days=recent_days)

#     recent = prov[prov['prov_date'] >= recent_cutoff]
#     previous = prov[(prov['prov_date'] < recent_cutoff) & (prov['prov_date'] >= prev_cutoff)]

#     metrics['recent_services'] = metrics['bsk_id'].map(recent.groupby('bsk_id').size()).fillna(0)
#     metrics['previous_services'] = metrics['bsk_id'].map(previous.groupby('bsk_id').size()).fillna(0)

#     metrics['trend_ratio'] = np.where(
#         metrics['previous_services'] > 0,
#         metrics['recent_services'] / metrics['previous_services'],
#         1.0
#     )

#     metrics['trend_score'] = metrics['trend_ratio'].clip(0.5, 1.5) / 1.5

#     # ----------------------------
#     # 6. SERVICE SATURATION
#     # ----------------------------
#     service_counts = prov.groupby(['bsk_id', 'service_id']).size().reset_index(name='cnt')

#     top_service = (
#         service_counts.sort_values(['bsk_id', 'cnt'], ascending=[True, False])
#         .groupby('bsk_id')
#         .first()
#         .reset_index()
#     )

#     top_service = top_service.merge(
#         metrics[['bsk_id', 'total_services']], on='bsk_id', how='left'
#     )

#     top_service['top_service_share'] = np.where(
#         top_service['total_services'] > 0,
#         top_service['cnt'] / top_service['total_services'],
#         0
#     )

#     metrics = metrics.merge(
#         top_service[['bsk_id', 'top_service_share']],
#         on='bsk_id',
#         how='left'
#     ).fillna({'top_service_share': 0})

#     metrics['saturation_score'] = 1 - metrics['top_service_share']

#     # ----------------------------
#     # 7. CLUSTER + RURAL/URBAN MEDIANS
#     # ----------------------------
#     peer_medians = metrics.groupby(
#         ['cluster_id', 'bsk_type']
#     ).agg({
#         'total_services': 'median',
#         'unique_services': 'median'
#     }).reset_index()

#     metrics = metrics.merge(
#         peer_medians,
#         on=['cluster_id', 'bsk_type'],
#         suffixes=('', '_peer_median')
#     )

#     metrics['relative_output_score'] = (
#         metrics['total_services'] / metrics['total_services_peer_median']
#     ).clip(0, 1)

#     metrics['service_diversity_score'] = (
#         metrics['unique_services'] / metrics['unique_services_peer_median']
#     ).clip(0, 1)

#     # ----------------------------
#     # 8. FINAL SCORE
#     # ----------------------------
#     metrics['final_score'] = (
#         0.30 * metrics['relative_output_score'] +
#         0.25 * metrics['service_diversity_score'] +
#         0.25 * metrics['trend_score'] +
#         0.20 * metrics['saturation_score']
#     )

#     # ----------------------------
#     # 9. UNDERPERFORMING FLAG
#     # ----------------------------
#     metrics['underperforming'] = (
#         (metrics['final_score'] < 0.45) |
#         (
#             (metrics['relative_output_score'] < 0.75).astype(int) +
#             (metrics['service_diversity_score'] < 0.75).astype(int) +
#             (metrics['trend_score'] < 0.6).astype(int) +
#             (metrics['top_service_share'] > 0.65).astype(int)
#         ) >= 2
#     )

#     # ----------------------------
#     # 10. DESCRIPTIVE REASONS
#     # ----------------------------
#     def build_reasons(row):
#         codes, desc = [], []

#         if row['relative_output_score'] < 0.75:
#             codes.append("LOW_CLUSTER_RELATIVE_OUTPUT")
#             desc.append("Service delivery is lower than nearby similar BSKs")

#         if row['service_diversity_score'] < 0.75:
#             codes.append("LOW_SERVICE_DIVERSITY")
#             desc.append("Limited range of services compared to nearby BSKs")

#         if row['trend_score'] < 0.6:
#             codes.append("DECLINING_90_DAY_TREND")
#             desc.append("Service volume has declined in the last 90 days")

#         if row['top_service_share'] > 0.65:
#             codes.append("OVER_DEPENDENCE_ON_SINGLE_SERVICE")
#             desc.append("Majority of services are concentrated in one service")

#         return pd.Series([codes, desc])

#     metrics[['reason_codes', 'reason_description']] = metrics.apply(build_reasons, axis=1)

#     # ----------------------------
#     # 11. FINAL OUTPUT (LIKE BEFORE)
#     # ----------------------------
#     final_df = bsks.merge(metrics, on=['bsk_id', 'cluster_id', 'bsk_type'], how='left')
#     final_df = final_df.replace([np.inf, -np.inf, np.nan], None)

#     return final_df
