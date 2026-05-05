from __future__ import annotations

from collections import defaultdict
from typing import Dict, List, Optional, Set, Tuple
import os

import numpy as np
import pandas as pd
from sklearn.preprocessing import StandardScaler


try:
    from openai import OpenAI
    OPENAI_AVAILABLE = True
except Exception:
    OPENAI_AVAILABLE = False

try:
    from sentence_transformers import SentenceTransformer
    SENTENCE_TRANSFORMERS_AVAILABLE = True
except Exception:
    SENTENCE_TRANSFORMERS_AVAILABLE = False


class _TextEmbedder:
    def __init__(
        self,
        is_use_openai: bool,
        openai_model: str,
        sentence_model_name: str,
    ) -> None:
        self.is_use_openai = is_use_openai
        self.openai_model = openai_model
        self.sentence_model_name = sentence_model_name
        self._client = None
        self._sentence_model = None

        if self.is_use_openai and OPENAI_AVAILABLE and os.getenv("OPENAI_API_KEY"):
            self._client = OpenAI()
        elif SENTENCE_TRANSFORMERS_AVAILABLE:
            self._sentence_model = SentenceTransformer(self.sentence_model_name)

    def _openai_dim(self) -> int:
        if "large" in self.openai_model:
            return 3072
        return 1536

    def get_dim(self) -> int:
        if self._sentence_model is not None:
            return int(self._sentence_model.get_sentence_embedding_dimension())
        if self._client is not None:
            return self._openai_dim()
        return 384

    def encode(self, texts: List[str]) -> np.ndarray:
        if self._client is not None:
            return self._encode_openai(texts)
        if self._sentence_model is not None:
            return self._sentence_model.encode(
                texts,
                convert_to_numpy=True,
                show_progress_bar=False,
                batch_size=32,
            )
        return np.zeros((len(texts), self.get_dim()), dtype=np.float32)

    def _encode_openai(self, texts: List[str]) -> np.ndarray:
        safe_texts = [t.strip() if t and str(t).strip() else "[EMPTY]" for t in texts]
        resp = self._client.embeddings.create(model=self.openai_model, input=safe_texts)
        arr = np.array([d.embedding for d in resp.data], dtype=np.float32)
        norms = np.linalg.norm(arr, axis=1, keepdims=True) + 1e-12
        return arr / norms


class UserFeatureBuilder:
    def __init__(
        self,
        sentence_model_name: str = "all-MiniLM-L6-v2",
        device: str = None,
        is_use_openai: bool = True,
        openai_model: str = "text-embedding-3-small",
    ) -> None:
        self.sentence_model_name = sentence_model_name
        self.device = device or "cpu"
        self.is_use_openai = is_use_openai

        self.embedder = _TextEmbedder(
            is_use_openai=is_use_openai,
            openai_model=openai_model,
            sentence_model_name=sentence_model_name,
        )

        self.global_industry_freq: Dict[str, float] = {}
        self.global_service_freq: Dict[str, float] = {}
        self.scaler = StandardScaler()

    def fit(self, df_history: pd.DataFrame) -> "UserFeatureBuilder":
        industry_counts = df_history["industry"].value_counts()
        total = len(df_history)
        self.global_industry_freq = {ind: count / total for ind, count in industry_counts.items()}

        all_services = []
        for services_str in df_history["services"].dropna():
            services = [s.strip() for s in str(services_str).split(",")]
            all_services.extend(services)

        from collections import Counter

        service_counts = Counter(all_services)
        total_services = sum(service_counts.values()) or 1
        self.global_service_freq = {
            svc: count / total_services for svc, count in service_counts.items()
        }

        numerical_features = []
        for user_id in df_history["linkedin_company_outsource"].unique():
            user_hist = df_history[df_history["linkedin_company_outsource"] == user_id]
            n_interactions = len(user_hist)
            if "client_size_mid" in user_hist.columns:
                avg_client_size = user_hist["client_size_mid"].mean()
            else:
                avg_client_size = 0.0
            numerical_features.append([n_interactions, avg_client_size])

        if numerical_features:
            self.scaler.fit(numerical_features)

        return self

    def build_user_features(
        self,
        user_id: str,
        df_history: pd.DataFrame,
        user_info: Optional[pd.Series] = None,
    ) -> Dict[str, np.ndarray]:
        features = {}
        user_history = df_history[df_history["linkedin_company_outsource"] == user_id]

        if not user_history.empty:
            features["industry_preferences"] = self._build_industry_vector(user_history)
            features["service_usage"] = self._build_service_vector(user_history)
            features["size_statistics"] = self._build_size_statistics(user_history)
            features["location_preferences"] = self._build_location_vector(user_history)
            features["numerical_stats"] = self._build_numerical_stats(user_history)
        else:
            features["industry_preferences"] = np.zeros(len(self.global_industry_freq))
            features["service_usage"] = np.zeros(len(self.global_service_freq))
            features["size_statistics"] = np.zeros(5)
            features["location_preferences"] = np.zeros(10)
            features["numerical_stats"] = np.zeros(2)

        embed_dim = self.embedder.get_dim()
        if user_info is not None:
            desc = user_info.get("description_company_outsource")
            svc = user_info.get("services_company_outsource")

            if pd.notna(desc) and str(desc).strip():
                desc_emb = self.embedder.encode([str(desc)])[0]
            else:
                desc_emb = np.zeros(embed_dim)

            if pd.notna(svc) and str(svc).strip():
                svc_emb = self.embedder.encode([str(svc)])[0]
            else:
                svc_emb = np.zeros(embed_dim)
        else:
            desc_emb = np.zeros(embed_dim)
            svc_emb = np.zeros(embed_dim)

        features["company_description"] = desc_emb
        features["company_services"] = svc_emb
        return features

    def _build_industry_vector(self, user_history: pd.DataFrame) -> np.ndarray:
        industry_counts = user_history["industry"].value_counts()
        vector = np.zeros(len(self.global_industry_freq))
        industry_to_idx = {ind: i for i, ind in enumerate(self.global_industry_freq.keys())}
        total_user_interactions = len(user_history)

        for industry, count in industry_counts.items():
            if industry in industry_to_idx:
                tf = count / total_user_interactions
                idf = np.log(1.0 / (self.global_industry_freq[industry] + 1e-6))
                vector[industry_to_idx[industry]] = tf * idf

        norm = np.linalg.norm(vector)
        if norm > 0:
            vector = vector / norm
        return vector

    def _build_service_vector(self, user_history: pd.DataFrame) -> np.ndarray:
        service_counts = defaultdict(int)
        for services_str in user_history["services"].dropna():
            services = [s.strip() for s in str(services_str).split(",")]
            for svc in services:
                if svc:
                    service_counts[svc] += 1

        vector = np.zeros(len(self.global_service_freq))
        service_to_idx = {svc: i for i, svc in enumerate(self.global_service_freq.keys())}
        total = sum(service_counts.values()) or 1

        for svc, count in service_counts.items():
            if svc in service_to_idx:
                vector[service_to_idx[svc]] = count / total

        norm = np.linalg.norm(vector)
        if norm > 0:
            vector = vector / norm
        return vector

    def _build_size_statistics(self, user_history: pd.DataFrame) -> np.ndarray:
        size_buckets = ["micro", "small", "medium", "large", "enterprise"]
        size_counts = np.zeros(len(size_buckets))

        for _, row in user_history.iterrows():
            client_min = row.get("client_min")
            client_max = row.get("client_max")
            if pd.notna(client_min) and pd.notna(client_max):
                mid = (client_min + client_max) / 2
                if mid <= 10:
                    size_counts[0] += 1
                elif mid <= 50:
                    size_counts[1] += 1
                elif mid <= 200:
                    size_counts[2] += 1
                elif mid <= 1000:
                    size_counts[3] += 1
                else:
                    size_counts[4] += 1

        total = size_counts.sum()
        if total > 0:
            size_counts = size_counts / total
        return size_counts

    def _build_location_vector(self, user_history: pd.DataFrame) -> np.ndarray:
        location_counts = user_history["location"].value_counts()
        top_locations = location_counts.head(10)
        vector = np.zeros(10)
        total = len(user_history)
        for i, (loc, count) in enumerate(top_locations.items()):
            vector[i] = count / total
        return vector

    def _build_numerical_stats(self, user_history: pd.DataFrame) -> np.ndarray:
        n_interactions = len(user_history)
        if "client_size_mid" in user_history.columns:
            avg_client_size = user_history["client_size_mid"].mean()
            if pd.isna(avg_client_size):
                avg_client_size = 0.0
        else:
            avg_client_size = 0.0

        stats = np.array([n_interactions, avg_client_size])
        try:
            stats = self.scaler.transform([stats])[0]
        except Exception:
            pass
        return stats

    def get_combined_feature_vector(self, features: Dict[str, np.ndarray]) -> np.ndarray:
        vectors = [
            features.get("industry_preferences", np.array([])),
            features.get("service_usage", np.array([])),
            features.get("size_statistics", np.array([])),
            features.get("location_preferences", np.array([])),
            features.get("numerical_stats", np.array([])),
            features.get("company_description", np.array([])),
            features.get("company_services", np.array([])),
        ]
        vectors = [v for v in vectors if len(v) > 0]
        if not vectors:
            return np.array([])

        combined = np.concatenate(vectors)
        norm = np.linalg.norm(combined)
        if norm > 0:
            combined = combined / norm
        return combined


class UserBasedCollaborativeRecommender:
    def __init__(
        self,
        min_similarity: float = 0.1,
        top_k_similar_users: int = 20,
        sentence_model_name: str = "all-MiniLM-L6-v2",
        is_use_openai: bool = True,
        openai_model: str = "text-embedding-3-small",
    ) -> None:
        self.min_similarity = min_similarity
        self.top_k_similar_users = top_k_similar_users
        self.feature_builder = UserFeatureBuilder(
            sentence_model_name=sentence_model_name,
            is_use_openai=is_use_openai,
            openai_model=openai_model,
        )

        self.user_features: Dict[str, np.ndarray] = {}
        self.user_triplets: Dict[str, Set[str]] = defaultdict(set)
        self.triplet_popularity: Dict[str, int] = defaultdict(int)

    def fit(
        self,
        df_history: pd.DataFrame,
        df_user_info: Optional[pd.DataFrame] = None,
    ) -> "UserBasedCollaborativeRecommender":
        self.feature_builder.fit(df_history)
        unique_users = df_history["linkedin_company_outsource"].unique()

        for user_id in unique_users:
            user_info = None
            if df_user_info is not None:
                user_rows = df_user_info[df_user_info["linkedin_company_outsource"] == user_id]
                if not user_rows.empty:
                    user_info = user_rows.iloc[0]

            features = self.feature_builder.build_user_features(
                user_id, df_history, user_info
            )
            combined = self.feature_builder.get_combined_feature_vector(features)
            self.user_features[user_id] = combined

            user_history = df_history[df_history["linkedin_company_outsource"] == user_id]
            if "triplet" in user_history.columns:
                self.user_triplets[user_id] = set(user_history["triplet"].dropna().unique())
                for triplet in self.user_triplets[user_id]:
                    self.triplet_popularity[triplet] += 1

        return self

    def find_similar_users(self, target_user_id: str, k: Optional[int] = None) -> List[Tuple[str, float]]:
        if k is None:
            k = self.top_k_similar_users
        if target_user_id not in self.user_features:
            return []

        target_vector = self.user_features[target_user_id]
        similarities = []
        for user_id, user_vector in self.user_features.items():
            if user_id == target_user_id:
                continue
            sim = np.dot(target_vector, user_vector)
            if sim >= self.min_similarity:
                similarities.append((user_id, float(sim)))

        similarities.sort(key=lambda x: x[1], reverse=True)
        return similarities[:k]

    def recommend_triplets(self, user_id: str, top_k: int = 10, exclude_seen: bool = True) -> pd.DataFrame:
        similar_users = self.find_similar_users(user_id)
        if not similar_users:
            return self._recommend_by_popularity(top_k)

        triplet_scores = defaultdict(float)
        seen_triplets = self.user_triplets.get(user_id, set())

        for similar_user, similarity in similar_users:
            similar_user_triplets = self.user_triplets.get(similar_user, set())
            for triplet in similar_user_triplets:
                if exclude_seen and triplet in seen_triplets:
                    continue
                triplet_scores[triplet] += similarity

        if not triplet_scores:
            return self._recommend_by_popularity(top_k)

        sorted_triplets = sorted(triplet_scores.items(), key=lambda x: x[1], reverse=True)[:top_k]
        return pd.DataFrame([
            {"triplet": triplet, "score": score} for triplet, score in sorted_triplets
        ])

    def _recommend_by_popularity(self, top_k: int) -> pd.DataFrame:
        sorted_triplets = sorted(
            self.triplet_popularity.items(),
            key=lambda x: x[1],
            reverse=True,
        )[:top_k]
        max_count = sorted_triplets[0][1] if sorted_triplets else 1
        return pd.DataFrame([
            {"triplet": triplet, "score": count / max_count} for triplet, count in sorted_triplets
        ])


class CollaborativeHandler:
    def __init__(
        self,
        min_similarity: float = 0.1,
        top_k_similar_users: int = 20,
        is_use_openai: bool = True,
        openai_model: str = "text-embedding-3-small",
    ) -> None:
        self.min_similarity = min_similarity
        self.top_k_similar_users = top_k_similar_users
        self.is_use_openai = is_use_openai
        self.openai_model = openai_model
        self._recommender: Optional[UserBasedCollaborativeRecommender] = None

    def fit(
        self,
        df_train: pd.DataFrame,
        df_user_info: Optional[pd.DataFrame] = None,
    ) -> "CollaborativeHandler":
        self._recommender = UserBasedCollaborativeRecommender(
            min_similarity=self.min_similarity,
            top_k_similar_users=self.top_k_similar_users,
            is_use_openai=self.is_use_openai,
            openai_model=self.openai_model,
        )
        self._recommender.fit(df_history=df_train, df_user_info=df_user_info)
        return self

    def recommend(
        self,
        user_id: str,
        top_k: int = 10,
        exclude_seen: bool = True,
    ) -> pd.DataFrame:
        if self._recommender is None:
            raise RuntimeError("CollaborativeHandler is not fitted. Call fit() first.")
        return self._recommender.recommend_triplets(
            user_id,
            top_k=top_k,
            exclude_seen=exclude_seen,
        )
