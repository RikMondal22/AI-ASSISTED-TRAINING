# import pandas as pd
# import numpy as np
# from sentence_transformers import SentenceTransformer, util
# from sklearn.feature_extraction.text import TfidfVectorizer
# from sklearn.metrics.pairwise import cosine_similarity
# from geopy.distance import geodesic
# import nltk
# from nltk.corpus import stopwords
# import math
# from math import radians, sin, cos, sqrt, atan2
# from sklearn.cluster import KMeans
# from collections import Counter
# import chromadb
# from chromadb.config import Settings
# import os
# import hashlib
# import json
# from typing import List, Dict, Optional, Tuple

# nltk.download("stopwords", quiet=True)
# STOPWORDS = set(stopwords.words("english"))


# # Haversine function at top level
# def haversine(lat1, lon1, lat2, lon2):
#     R = 6371
#     lat1, lon1, lat2, lon2 = map(radians, [lat1, lon1, lat2, lon2])
#     dlat = lat2 - lat1
#     dlon = lon2 - lon1
#     a = sin(dlat / 2) ** 2 + cos(lat1) * cos(lat2) * sin(dlon / 2) ** 2
#     c = 2 * atan2(sqrt(a), sqrt(1 - a))
#     return R * c


# _model = None


# def get_model():
#     global _model
#     if _model is None:
#         _model = SentenceTransformer("all-MiniLM-L6-v2")
#     return _model


# class ServiceEmbeddingManager:
#     """Manages service embeddings using ChromaDB for efficient storage and retrieval."""

#     def __init__(
#         self, db_path: str = "./chroma_db", collection_name: str = "service_embeddings"
#     ):
#         self.db_path = db_path
#         self.collection_name = collection_name
#         self.model = get_model()
#         self._client = None
#         self._collection = None
#         self._initialize_db()

#     def _initialize_db(self):
#         """Initialize ChromaDB client and collection."""
#         try:
#             self._client = chromadb.PersistentClient(path=self.db_path)
#             self._collection = self._client.get_or_create_collection(
#                 name=self.collection_name, metadata={"hnsw:space": "cosine"}
#             )
#         except Exception as e:
#             print(f"Error initializing ChromaDB: {e}")
#             raise

#     def _generate_service_hash(self, service_data: Dict) -> str:
#         """Generate a hash for service data to detect changes."""
#         content = f"{service_data.get('service_name', '')}{service_data.get('service_type', '')}{service_data.get('service_desc', '')}"
#         return hashlib.md5(content.encode()).hexdigest()

#     def _create_full_description(self, service_data: Dict) -> str:
#         """Create full description from service data."""
#         return f"{service_data.get('service_name', '')} {service_data.get('service_type', '')} {service_data.get('service_desc', '')}"

#     def build_embeddings_from_dataframe(
#         self, services_df: pd.DataFrame, force_rebuild: bool = False
#     ):
#         """Build and store embeddings for all services in the dataframe."""
#         print("Building service embeddings...")

#         if force_rebuild:
#             # Clear existing collection
#             self._client.delete_collection(self.collection_name)
#             self._collection = self._client.create_collection(
#                 name=self.collection_name, metadata={"hnsw:space": "cosine"}
#             )

#         existing_ids = set()
#         try:
#             existing_records = self._collection.get()
#             existing_ids = set(existing_records["ids"])
#         except:
#             pass

#         services_to_process = []
#         service_ids = []
#         service_descriptions = []
#         metadatas = []

#         for idx, row in services_df.iterrows():
#             service_id = str(row["service_id"])
#             service_hash = self._generate_service_hash(row.to_dict())

#             # Check if service already exists and hasn't changed
#             if not force_rebuild and service_id in existing_ids:
#                 try:
#                     existing_record = self._collection.get(ids=[service_id])
#                     if (
#                         existing_record["metadatas"]
#                         and len(existing_record["metadatas"]) > 0
#                     ):
#                         existing_hash = existing_record["metadatas"][0].get("hash", "")
#                         if existing_hash == service_hash:
#                             continue  # Skip if unchanged
#                 except:
#                     pass

#             full_desc = self._create_full_description(row.to_dict())
#             services_to_process.append(row.to_dict())
#             service_ids.append(service_id)
#             service_descriptions.append(full_desc)
#             metadatas.append(
#                 {
#                     "service_name": str(row.get("service_name", "")),
#                     "service_type": str(row.get("service_type", "")),
#                     "service_desc": str(row.get("service_desc", "")),
#                     "hash": service_hash,
#                 }
#             )

#         if not services_to_process:
#             print("All service embeddings are up to date.")
#             return

#         print(f"Processing {len(services_to_process)} services...")

#         # Generate embeddings in batches
#         batch_size = 100
#         for i in range(0, len(service_descriptions), batch_size):
#             batch_descriptions = service_descriptions[i : i + batch_size]
#             batch_ids = service_ids[i : i + batch_size]
#             batch_metadatas = metadatas[i : i + batch_size]

#             # Generate embeddings
#             embeddings = self.model.encode(batch_descriptions, convert_to_tensor=False)

#             # Store in ChromaDB
#             self._collection.upsert(
#                 embeddings=embeddings.tolist(),
#                 documents=batch_descriptions,
#                 metadatas=batch_metadatas,
#                 ids=batch_ids,
#             )

#             print(
#                 f"Processed batch {i//batch_size + 1}/{(len(service_descriptions) - 1)//batch_size + 1}"
#             )

#         print("Service embeddings built successfully!")

#     def get_similar_services(
#         self, new_service: Dict, top_k: int = None
#     ) -> Tuple[List[str], List[float]]:
#         """Find similar services using stored embeddings."""
#         if top_k is None:
#             # Get all services
#             all_records = self._collection.get()
#             top_k = len(all_records["ids"]) if all_records["ids"] else 10

#         new_service_desc = self._create_full_description(new_service)
#         print(f"\n[ServiceRecommendation] New service description: {new_service_desc}")
#         new_embedding = self.model.encode(new_service_desc, convert_to_tensor=False)
#         print(
#             f"[ServiceRecommendation] New service embedding (first 5 values): {new_embedding[:5]}"
#         )

#         # Query ChromaDB for similar services
#         results = self._collection.query(
#             query_embeddings=[new_embedding.tolist()],
#             n_results=min(top_k, self._collection.count()),
#         )
#         print(f"[ServiceRecommendation] Query results: {results}")

#         service_ids = results["ids"][0] if results["ids"] else []
#         similarities = (
#             [1 - distance for distance in results["distances"][0]]
#             if results["distances"]
#             else []
#         )

#         print("[ServiceRecommendation] Similar services found:")
#         for sid, sim in zip(service_ids, similarities):
#             print(f"  Service ID: {sid}, Similarity: {sim:.4f}")

#         return service_ids, similarities

#     def get_all_service_embeddings(self) -> Tuple[List[str], np.ndarray]:
#         """Get all stored service embeddings."""
#         results = self._collection.get(include=["embeddings"])
#         service_ids = results["ids"]
#         embeddings = (
#             np.array(results["embeddings"]) if results["embeddings"] else np.array([])
#         )
#         return service_ids, embeddings

#     def get_service_count(self) -> int:
#         """Get the number of stored services."""
#         return self._collection.count()


# # Global embedding manager instance
# _embedding_manager = None


# def get_embedding_manager() -> ServiceEmbeddingManager:
#     """Get or create the global embedding manager instance."""
#     global _embedding_manager
#     if _embedding_manager is None:
#         _embedding_manager = ServiceEmbeddingManager()
#     return _embedding_manager


# def extract_keywords(text, top_n=5):
#     vectorizer = TfidfVectorizer(stop_words="english")
#     tfidf = vectorizer.fit_transform([text])
#     scores = zip(vectorizer.get_feature_names_out(), tfidf.toarray()[0])
#     sorted_scores = sorted(scores, key=lambda x: x[1], reverse=True)
#     return [word for word, score in sorted_scores[:top_n]]


# def haversine_distance(lat1, lon1, lat2, lon2):
#     """Calculate the Haversine distance between two points on the Earth."""
#     R = 6371  # Earth radius in kilometers
#     lat1, lon1, lat2, lon2 = map(math.radians, [lat1, lon1, lat2, lon2])
#     dlat = lat2 - lat1
#     dlon = lon2 - lon1
#     a = (
#         math.sin(dlat / 2) ** 2
#         + math.cos(lat1) * math.cos(lat2) * math.sin(dlon / 2) ** 2
#     )
#     c = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))
#     return R * c


# def recommend_bsk_for_service(
#     new_service,
#     services_df,
#     provisions_df,
#     bsk_df,
#     top_n=10,
#     target_location=None,
#     use_precomputed_embeddings=True,
# ):
#     """
#     Recommend BSKs for a new service using precomputed embeddings for better performance.

#     Args:
#         new_service: Dict with service details
#         services_df: DataFrame of existing services
#         provisions_df: DataFrame of provisions
#         bsk_df: DataFrame of BSKs
#         top_n: Number of top BSKs to return
#         target_location: Optional tuple (lat, lon) for location-based scoring
#         use_precomputed_embeddings: Whether to use ChromaDB for embeddings (default: True)
#     """
#     if use_precomputed_embeddings:
#         embedding_manager = get_embedding_manager()

#         # Ensure embeddings are built for current services
#         if embedding_manager.get_service_count() == 0:
#             print("No precomputed embeddings found. Building embeddings...")
#             embedding_manager.build_embeddings_from_dataframe(services_df)

#         # Get similar services using precomputed embeddings
#         similar_service_ids, embedding_similarities = (
#             embedding_manager.get_similar_services(new_service, top_k=len(services_df))
#         )

#         # Create a mapping from service_id to embedding similarity
#         embedding_similarity_map = dict(
#             zip([int(sid) for sid in similar_service_ids], embedding_similarities)
#         )

#         # Add embedding similarities to services_df
#         services_df = services_df.copy()
#         services_df["embedding_similarity"] = services_df["service_id"].map(
#             lambda x: embedding_similarity_map.get(x, 0.0)
#         )
#     else:
#         # Fallback to original embedding computation
#         model = get_model()
#         services_df = services_df.copy()
#         services_df["full_desc"] = (
#             services_df["service_name"].astype(str)
#             + " "
#             + services_df.get("service_type", "").astype(str)
#             + " "
#             + services_df.get("service_desc", "").astype(str)
#         )

#         # Embedding similarity
#         service_embeddings = model.encode(
#             services_df["full_desc"].tolist(), convert_to_tensor=True
#         )
#         new_service_desc = (
#             new_service["service_name"]
#             + " "
#             + new_service["service_type"]
#             + " "
#             + new_service["service_desc"]
#         )
#         new_service_embedding = model.encode(new_service_desc, convert_to_tensor=True)
#         similarities = (
#             util.cos_sim(new_service_embedding, service_embeddings)[0].cpu().numpy()
#         )
#         services_df["embedding_similarity"] = similarities

#     # Ensure full_desc exists for TF-IDF and keyword analysis
#     if "full_desc" not in services_df.columns:
#         services_df["full_desc"] = (
#             services_df["service_name"].astype(str)
#             + " "
#             + services_df.get("service_type", "").astype(str)
#             + " "
#             + services_df.get("service_desc", "").astype(str)
#         )

#     # TF-IDF similarity (still computed on-demand as it's fast)
#     new_service_desc = (
#         new_service["service_name"]
#         + " "
#         + new_service["service_type"]
#         + " "
#         + new_service["service_desc"]
#     )
#     tfidf_vectorizer = TfidfVectorizer(stop_words="english")
#     tfidf_matrix = tfidf_vectorizer.fit_transform(
#         services_df["full_desc"].tolist() + [new_service_desc]
#     )
#     tfidf_similarities = cosine_similarity(tfidf_matrix[-1], tfidf_matrix[:-1])[0]
#     services_df["tfidf_similarity"] = tfidf_similarities

#     # Keyword overlap
#     new_keywords = set(extract_keywords(new_service_desc))
#     services_df["keyword_overlap"] = services_df["full_desc"].apply(
#         lambda desc: len(new_keywords & set(extract_keywords(desc)))
#     )

#     # Combine similarities
#     max_kw = max(services_df["keyword_overlap"].max(), 1)
#     services_df["total_similarity"] = (
#         0.5 * services_df["embedding_similarity"]
#         + 0.3 * services_df["tfidf_similarity"]
#         + 0.2 * services_df["keyword_overlap"] / max_kw
#     )

#     similar_services = services_df[services_df["total_similarity"] > 0.4]
#     similar_service_ids = similar_services["service_id"].tolist()

#     # Usage analysis
#     relevant_provisions = provisions_df[
#         provisions_df["service_id"].isin(similar_service_ids)
#     ]
#     bsk_counts = relevant_provisions["bsk_id"].value_counts().reset_index()
#     bsk_counts.columns = ["bsk_id", "usage_count"]

#     # Geospatial analysis
#     bsk_df["bsk_lat"] = pd.to_numeric(bsk_df["bsk_lat"], errors="coerce")
#     bsk_df["bsk_long"] = pd.to_numeric(bsk_df["bsk_long"], errors="coerce")
#     if target_location and "bsk_lat" in bsk_df.columns and "bsk_long" in bsk_df.columns:
#         bsk_df["distance_km"] = bsk_df.apply(
#             lambda row: (
#                 haversine_distance(
#                     target_location[0],
#                     target_location[1],
#                     row["bsk_lat"],
#                     row["bsk_long"],
#                 )
#                 if not np.isnan(row["bsk_lat"]) and not np.isnan(row["bsk_long"])
#                 else np.nan
#             ),
#             axis=1,
#         )
#     else:
#         bsk_df["distance_km"] = np.nan

#     recommended_bsk = bsk_counts.merge(bsk_df, on="bsk_id", how="left")

#     # Usage score (min-max normalization) - this is the final score
#     usage_min = recommended_bsk["usage_count"].min()
#     usage_max = recommended_bsk["usage_count"].max()
#     recommended_bsk["score"] = (
#         (recommended_bsk["usage_count"] - usage_min) / (usage_max - usage_min)
#         if usage_max > usage_min
#         else 1.0
#     )
#     recommended_bsk = recommended_bsk.sort_values("score", ascending=False)

#     # Reason text based on usage score
#     def reason_text(row):
#         # Usage
#         if row["score"] >= 0.7:
#             return "High usage of similar services"
#         elif row["score"] >= 0.4:
#             return "Moderate usage of similar services"
#         elif row["score"] > 0:
#             return "Some usage of similar services"
#         else:
#             return "Low usage of similar services"

#     recommended_bsk["reason"] = recommended_bsk.apply(reason_text, axis=1)
#     # Return both recommended BSKs and top similar services (as DataFrame)
#     top_similar_services = similar_services.sort_values(
#         "total_similarity", ascending=False
#     ).head(5)
#     return recommended_bsk.head(top_n), top_similar_services


# # BSK analytics functions moved to bsk_analytics.py


# def initialize_service_embeddings(
#     services_df: pd.DataFrame, force_rebuild: bool = False
# ):
#     """
#     Initialize or rebuild service embeddings in ChromaDB.

#     Args:
#         services_df: DataFrame containing service data
#         force_rebuild: If True, rebuild all embeddings even if they exist
#     """
#     embedding_manager = get_embedding_manager()
#     embedding_manager.build_embeddings_from_dataframe(
#         services_df, force_rebuild=force_rebuild
#     )
#     print(
#         f"Service embeddings initialized. Total services: {embedding_manager.get_service_count()}"
#     )


# def update_service_embeddings(services_df: pd.DataFrame):
#     """
#     Update service embeddings with new or changed services.
#     This is more efficient than force rebuilding as it only processes changed services.

#     Args:
#         services_df: DataFrame containing service data
#     """
#     embedding_manager = get_embedding_manager()
#     embedding_manager.build_embeddings_from_dataframe(services_df, force_rebuild=False)
#     print(
#         f"Service embeddings updated. Total services: {embedding_manager.get_service_count()}"
#     )


# def get_embedding_stats():
#     """Get statistics about stored embeddings."""
#     embedding_manager = get_embedding_manager()
#     return {
#         "total_services": embedding_manager.get_service_count(),
#         "db_path": embedding_manager.db_path,
#         "collection_name": embedding_manager.collection_name,
#     }


# # Database integration functions
# def get_recommendation_data_from_db(include_inactive: bool = False):
#     """
#     Fetch all data needed for recommendations from the database.

#     Args:
#         include_inactive: Whether to include inactive records

#     Returns:
#         Tuple of (services_df, bsk_df, deos_df, provisions_df) or None if error
#     """
#     try:
#         from .database_service import fetch_all_data_for_recommendations

#         data = fetch_all_data_for_recommendations(include_inactive)

#         # Validate that we got all required data
#         required_data = ["services_df", "bsk_df", "deos_df", "provisions_df"]
#         missing_data = [key for key in required_data if data.get(key) is None]

#         if missing_data:
#             print(f"Warning: Failed to load the following data: {missing_data}")

#         return (
#             data["services_df"],
#             data["bsk_df"],
#             data["deos_df"],
#             data["provisions_df"],
#         )

#     except ImportError:
#         print(
#             "Database service not available. Please use CSV files or install database dependencies."
#         )
#         return None, None, None, None
#     except Exception as e:
#         print(f"Error fetching data from database: {e}")
#         return None, None, None, None


# def recommend_bsk_for_service_from_db(
#     new_service,
#     top_n=10,
#     target_location=None,
#     use_precomputed_embeddings=True,
#     include_inactive=False,
# ):
#     """
#     Recommend BSKs for a new service using data directly from the database.

#     Args:
#         new_service: Dict with service details
#         top_n: Number of top BSKs to return
#         target_location: Optional tuple (lat, lon) for location-based scoring
#         use_precomputed_embeddings: Whether to use ChromaDB for embeddings
#         include_inactive: Whether to include inactive records

#     Returns:
#         DataFrame with recommended BSKs or None if error
#     """
#     # Fetch data from database
#     services_df, bsk_df, deos_df, provisions_df = get_recommendation_data_from_db(
#         include_inactive
#     )

#     if any(df is None for df in [services_df, bsk_df, provisions_df]):
#         print("Error: Could not fetch required data from database")
#         return None

#     # Use the main recommendation function
#     result = recommend_bsk_for_service(
#         new_service=new_service,
#         services_df=services_df,
#         provisions_df=provisions_df,
#         bsk_df=bsk_df,
#         top_n=top_n,
#         target_location=target_location,
#         use_precomputed_embeddings=use_precomputed_embeddings,
#     )
#     # If the result is a tuple (recommended_bsk, similar_services), return as dict for frontend compatibility
#     if isinstance(result, tuple) and len(result) == 2:
#         recommended_bsk, similar_services = result
#         return {
#             "recommendations": recommended_bsk,
#             "similar_services": similar_services,
#         }
#     else:
#         return result


# def initialize_embeddings_from_db(
#     force_rebuild: bool = False, include_inactive: bool = False
# ):
#     """
#     Initialize service embeddings using data from the database.

#     Args:
#         force_rebuild: Whether to rebuild all embeddings
#         include_inactive: Whether to include inactive services
#     """
#     try:
#         from .database_service import fetch_services_from_db

#         services_df = fetch_services_from_db(include_inactive)
#         if services_df is None:
#             print("Error: Could not fetch services from database")
#             return False

#         initialize_service_embeddings(services_df, force_rebuild=force_rebuild)
#         return True

#     except ImportError:
#         print("Database service not available. Please install database dependencies.")
#         return False
#     except Exception as e:
#         print(f"Error initializing embeddings from database: {e}")
#         return False
