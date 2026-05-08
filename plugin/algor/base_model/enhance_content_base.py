from __future__ import annotations

from typing import Dict, Optional, List, Set

import numpy as np
import pandas as pd
from sklearn.decomposition import PCA
from sklearn.preprocessing import StandardScaler

from plugin.algor.base_model.content_base_handler import _TextEmbedder


class EnhancedTripletEmbedder:
    def __init__(
        self,
        triplet_manager,
        sentence_model_name: str = "all-MiniLM-L6-v2",
        embedding_dim: int = 384,
        fusion_method: str = "concat",
        is_use_openai: bool = True,
        openai_model: str = "text-embedding-3-small",
        fusion_weights: Optional[Dict[str, float]] = None,
    ) -> None:
        self.triplet_manager = triplet_manager
        self.embedding_dim = embedding_dim
        self.fusion_method = fusion_method
        self.is_use_openai = is_use_openai
        self.openai_model = openai_model
        self.fusion_weights = fusion_weights or {
            "text": 0.3,
            "triplet": 0.4,
            "categorical": 0.2,
            "numerical": 0.1,
        }

        self.embedder = _TextEmbedder(
            is_use_openai=is_use_openai,
            openai_model=openai_model,
            sentence_model_name=sentence_model_name,
        )
        self.text_dim = int(self.embedder.get_dim())

        self.industry_embeddings: Dict[str, np.ndarray] = {}
        self.size_embeddings: Dict[str, np.ndarray] = {}
        self.service_embeddings: Dict[str, np.ndarray] = {}
        self.location_embeddings: Dict[str, np.ndarray] = {}

        self.scaler = StandardScaler()
        self.pca: Optional[PCA] = None

    def _create_size_embeddings(self) -> None:
        size_buckets = ["micro", "small", "medium", "large", "enterprise", "unknown"]
        size_descriptions = [
            "micro company with 1-10 employees",
            "small company with 11-50 employees",
            "medium company with 51-200 employees",
            "large company with 201-1000 employees",
            "enterprise company with over 1000 employees",
            "unknown company size",
        ]
        size_embs = self.embedder.encode(size_descriptions)
        for bucket, emb in zip(size_buckets, size_embs):
            self.size_embeddings[bucket] = emb

    def _create_service_embeddings(self, all_services: List[str]) -> None:
        if not all_services:
            return
        service_embs = self.embedder.encode(all_services)
        for service, emb in zip(all_services, service_embs):
            self.service_embeddings[service] = emb

    def _encode_text_fields(self, df: pd.DataFrame) -> np.ndarray:
        combined_texts = []
        for _, row in df.iterrows():
            services = str(row.get("services", "")).strip()
            background = str(row.get("background", "")).strip()
            industry = str(row.get("industry", "")).strip()

            text_parts = []
            if industry and industry.lower() != "nan":
                text_parts.append(f"Industry: {industry}")
            if services and services.lower() != "nan":
                text_parts.append(f"Services: {services}")
            if background and background.lower() != "nan":
                bg = background[:800] + "..." if len(background) > 800 else background
                text_parts.append(f"Background: {bg}")

            combined_texts.append(" | ".join(text_parts) if text_parts else "No description")

        return self.embedder.encode(combined_texts)

    def _encode_triplet_components(self, df: pd.DataFrame) -> np.ndarray:
        if "triplet" not in df.columns:
            return np.zeros((len(df), self.text_dim * 3), dtype=np.float32)

        triplet_embeddings = []
        for _, row in df.iterrows():
            triplet = row.get("triplet", "")
            if pd.isna(triplet) or not triplet:
                triplet_embeddings.append(np.zeros(self.text_dim * 3, dtype=np.float32))
                continue

            industry, size, services_str = self.triplet_manager.parse_triplet(triplet)

            if industry in self.industry_embeddings:
                ind_emb = self.industry_embeddings[industry]
            else:
                ind_emb = self.embedder.encode([industry])[0]
                self.industry_embeddings[industry] = ind_emb

            if size in self.size_embeddings:
                size_emb = self.size_embeddings[size]
            else:
                size_emb = self.size_embeddings.get("unknown", np.zeros(self.text_dim))

            services = [
                s.strip() for s in services_str.split(",") if s.strip() and s != "unknown"
            ]
            if services:
                service_embs = []
                for svc in services:
                    if svc in self.service_embeddings:
                        service_embs.append(self.service_embeddings[svc])
                    else:
                        svc_emb = self.embedder.encode([svc])[0]
                        self.service_embeddings[svc] = svc_emb
                        service_embs.append(svc_emb)
                svc_emb = np.mean(service_embs, axis=0)
            else:
                svc_emb = np.zeros(self.text_dim)

            triplet_embeddings.append(np.concatenate([ind_emb, size_emb, svc_emb]))

        return np.array(triplet_embeddings, dtype=np.float32)

    def _encode_categorical_fields(self, df: pd.DataFrame) -> np.ndarray:
        if "location" not in df.columns:
            return np.zeros((len(df), 32), dtype=np.float32)

        location_vecs = []
        for loc in df["location"].fillna("Unknown"):
            if loc in self.location_embeddings:
                location_vecs.append(self.location_embeddings[loc])
            else:
                np.random.seed(hash(str(loc)) % 2**31)
                emb = np.random.normal(0, 0.1, 32)
                self.location_embeddings[loc] = emb
                location_vecs.append(emb)

        return np.array(location_vecs, dtype=np.float32)

    def _encode_numerical_fields(self, df: pd.DataFrame, fit: bool = False) -> np.ndarray:
        if "client_min" not in df.columns or "client_max" not in df.columns:
            return np.zeros((len(df), 1), dtype=np.float32)

        client_mid = df[["client_min", "client_max"]].mean(axis=1, skipna=True).fillna(0)
        numerical_data = client_mid.values.reshape(-1, 1).astype(np.float32)

        if fit:
            return self.scaler.fit_transform(numerical_data)
        return self.scaler.transform(numerical_data)

    def _fuse_embeddings(
        self,
        text_emb: np.ndarray,
        triplet_emb: np.ndarray,
        cat_emb: np.ndarray,
        num_emb: np.ndarray,
    ) -> np.ndarray:
        weights = self.fusion_weights

        combined = np.hstack(
            [
                text_emb * weights["text"],
                triplet_emb * weights["triplet"],
                cat_emb * weights["categorical"],
                num_emb * weights["numerical"],
            ]
        )

        if self.fusion_method == "concat" and combined.shape[1] > self.embedding_dim * 2:
            if self.pca is None:
                n_components = min(self.embedding_dim, combined.shape[1] - 1, combined.shape[0] - 1)
                self.pca = PCA(n_components=n_components, random_state=42)
                combined = self.pca.fit_transform(combined)
            else:
                combined = self.pca.transform(combined)

        return combined

    def fit(self, df: pd.DataFrame) -> "EnhancedTripletEmbedder":
        self._create_size_embeddings()

        all_services = set()
        for svc_str in df.get("services", pd.Series(dtype=str)).dropna():
            services = [s.strip() for s in str(svc_str).split(",")]
            all_services.update([s for s in services if s])
        self._create_service_embeddings(list(all_services))

        if "location" in df.columns:
            for loc in df["location"].dropna().unique().tolist()[:100]:
                np.random.seed(hash(str(loc)) % 2**31)
                self.location_embeddings[loc] = np.random.normal(0, 0.1, 32)

        self._encode_numerical_fields(df, fit=True)
        return self

    def transform(self, df: pd.DataFrame) -> np.ndarray:
        text_emb = self._encode_text_fields(df)
        triplet_emb = self._encode_triplet_components(df)
        cat_emb = self._encode_categorical_fields(df)
        num_emb = self._encode_numerical_fields(df, fit=False)

        final_embeddings = self._fuse_embeddings(text_emb, triplet_emb, cat_emb, num_emb)
        norms = np.linalg.norm(final_embeddings, axis=1, keepdims=True) + 1e-12
        final_embeddings = final_embeddings / norms
        return final_embeddings.astype(np.float32)


class EnhancedTripletContentRecommender:
    def __init__(
        self,
        df_history: pd.DataFrame,
        df_test: pd.DataFrame,
        triplet_manager,
        embedding_config: Optional[Dict] = None,
        is_use_openai: bool = True,
        openai_model: str = "text-embedding-3-small",
        val_user_ids: Optional[Set[str]] = None,
    ) -> None:
        if val_user_ids and "linkedin_company_outsource" in df_history.columns:
            is_val = df_history["linkedin_company_outsource"].isin(val_user_ids)
            data_val = df_history[is_val].copy()
            data_train = df_history[~is_val].copy()
            if data_train.empty:
                data_train = df_history.copy()
                data_val = df_history.iloc[0:0].copy()
        else:
            data_train = df_history.copy()
            data_val = df_history.iloc[0:0].copy()

        self.data_train = data_train
        self.data_val = data_val
        self.df_test = df_test.copy()
        self.triplet_manager = triplet_manager

        embedding_config = embedding_config or {}
        self.embedder = EnhancedTripletEmbedder(
            triplet_manager=triplet_manager,
            sentence_model_name=embedding_config.get("sentence_model_name", "all-MiniLM-L6-v2"),
            embedding_dim=embedding_config.get("embedding_dim", 384),
            fusion_method=embedding_config.get("fusion_method", "concat"),
            fusion_weights=embedding_config.get("fusion_weights"),
            is_use_openai=is_use_openai,
            openai_model=openai_model,
        )

        self.embedder.fit(self.data_train)
        self.X_train = self.embedder.transform(self.data_train)
        self.X_val = self.embedder.transform(self.data_val) if not self.data_val.empty else np.zeros((0, 1))
        self.X_test = self.embedder.transform(self.df_test)

    def build_user_profile(self, user_id: str) -> Optional[np.ndarray]:
        user_history = self.data_train[self.data_train["linkedin_company_outsource"] == user_id]
        if user_history.empty:
            return None
        user_embeddings = self.embedder.transform(user_history)
        return np.mean(user_embeddings, axis=0, keepdims=True)

    def recommend_triplets(
        self,
        user_id: str,
        top_k: int = 10,
        mode: str = "test",
    ) -> pd.DataFrame:
        user_profile = self.build_user_profile(user_id)
        if user_profile is None:
            return pd.DataFrame(columns=["triplet", "score"])

        if mode == "val" and not self.data_val.empty:
            X_candidates = self.X_val
            df_candidates = self.data_val
        else:
            X_candidates = self.X_test
            df_candidates = self.df_test

        if X_candidates.size == 0 or df_candidates.empty:
            return pd.DataFrame(columns=["triplet", "score"])

        similarities = np.dot(X_candidates, user_profile.T).ravel()
        df_scored = df_candidates.copy()
        df_scored["score"] = similarities

        if "triplet" not in df_scored.columns:
            return pd.DataFrame(columns=["triplet", "score"])

        triplet_results = (
            df_scored.groupby("triplet")
            .agg(
                {
                    "score": "max",
                    "industry": "first",
                    "location": "first",
                    "services": "first",
                }
            )
            .reset_index()
            .sort_values("score", ascending=False)
            .head(top_k)
        )

        return triplet_results


class EnhancedContentBaseHandler:
    def __init__(
        self,
        triplet_manager,
        embedding_weights: Optional[Dict[str, float]] = None,
        is_use_openai: bool = True,
        openai_model: str = "text-embedding-3-small",
        validation_split: float = 0.2,
    ) -> None:
        self.triplet_manager = triplet_manager
        self.is_use_openai = is_use_openai
        self.openai_model = openai_model
        self.validation_split = validation_split
        self.embedding_weights = embedding_weights or {
            "triplet_structure": 0.2,
            "background_text": 0.35,
            "services_text": 0.25,
            "location": 0.1,
            "numerical": 0.1,
        }
        self._recommender: Optional[EnhancedTripletContentRecommender] = None

    def fit(
        self,
        df_train: pd.DataFrame,
        df_test: pd.DataFrame,
        val_user_ids: Optional[Set[str]] = None,
    ) -> "EnhancedContentBaseHandler":
        fusion_weights = {
            "text": self.embedding_weights.get("background_text", 0.0)
            + self.embedding_weights.get("services_text", 0.0),
            "triplet": self.embedding_weights.get("triplet_structure", 0.0),
            "categorical": self.embedding_weights.get("location", 0.0),
            "numerical": self.embedding_weights.get("numerical", 0.0),
        }
        self._recommender = EnhancedTripletContentRecommender(
            df_history=df_train,
            df_test=df_test,
            triplet_manager=self.triplet_manager,
            is_use_openai=self.is_use_openai,
            openai_model=self.openai_model,
            val_user_ids=val_user_ids,
            embedding_config={
                "sentence_model_name": "all-MiniLM-L6-v2",
                "embedding_dim": 384,
                "fusion_method": "concat",
                "fusion_weights": fusion_weights,
            },
        )
        return self

    def recommend(self, user_id: str, top_k: int = 10, mode: str = "test") -> pd.DataFrame:
        if self._recommender is None:
            raise RuntimeError("EnhancedContentBaseHandler is not fitted. Call fit() first.")
        return self._recommender.recommend_triplets(user_id, top_k=top_k, mode=mode)
