from __future__ import annotations

from typing import Dict, Optional, Literal, List
import os

import numpy as np
import pandas as pd
from scipy import sparse
from sklearn.preprocessing import StandardScaler
from sklearn.metrics.pairwise import cosine_similarity
from sklearn.model_selection import train_test_split


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


class TripletContentRecommender:
    def __init__(
        self,
        df_history: pd.DataFrame,
        df_test: pd.DataFrame,
        triplet_manager,
        sentence_model_name: str = "all-MiniLM-L6-v2",
        embedding_weights: Optional[Dict[str, float]] = None,
        is_use_openai: bool = True,
        openai_model: str = "text-embedding-3-small",
        validation_split: float = 0.2,
    ) -> None:
        if validation_split > 0 and len(df_history) > 1:
            data_train, data_val = train_test_split(
                df_history,
                test_size=validation_split,
                random_state=42,
            )
        else:
            data_train = df_history.copy()
            data_val = df_history.iloc[0:0].copy()

        self.data_train = data_train.copy()
        self.data_val = data_val.copy()
        self.df_test = df_test.copy()
        self.triplet_manager = triplet_manager
        self.is_use_openai = is_use_openai

        self.embedding_weights = embedding_weights or {
            "triplet_structure": 0.3,
            "background_text": 0.3,
            "services_text": 0.2,
            "location": 0.1,
            "numerical": 0.1,
        }

        self.embedder = _TextEmbedder(
            is_use_openai=is_use_openai,
            openai_model=openai_model,
            sentence_model_name=sentence_model_name,
        )
        self.scaler = StandardScaler()

        self.industry_vocab: Optional[List[str]] = None
        self.size_vocab: Optional[List[str]] = None
        self.service_vocab: Optional[List[str]] = None

        self._build_features()

    def _build_features(self) -> None:
        self._build_vocabularies(self.data_train)
        self.X_train = self._transform_to_features(self.data_train, fit=True)
        self.X_val = self._transform_to_features(self.data_val, fit=False)
        self.X_test = self._transform_to_features(self.df_test, fit=False)

    def _build_vocabularies(self, df: pd.DataFrame) -> None:
        if "triplet" not in df.columns:
            raise ValueError("DataFrame must have 'triplet' column")

        parsed = df["triplet"].apply(self.triplet_manager.parse_triplet)
        industries = [p[0] for p in parsed]
        services_list = [p[2] for p in parsed]

        self.industry_vocab = sorted(set(industries))
        self.size_vocab = ["micro", "small", "medium", "large", "enterprise", "unknown"]

        all_services = set()
        for svc_str in services_list:
            if svc_str != "unknown":
                services = [s.strip() for s in svc_str.split(",") if s.strip()]
                all_services.update(services)
        self.service_vocab = sorted(all_services)

    def _embed_triplet_structure(self, df: pd.DataFrame) -> np.ndarray:
        if "triplet" not in df.columns:
            raise ValueError("DataFrame must have 'triplet' column")
        if self.industry_vocab is None or self.size_vocab is None or self.service_vocab is None:
            raise ValueError("Vocabularies not built. Call _build_vocabularies first.")

        parsed = df["triplet"].apply(self.triplet_manager.parse_triplet)
        industries = [p[0] for p in parsed]
        sizes = [p[1] for p in parsed]
        services_list = [p[2] for p in parsed]

        industry_map = {ind: i for i, ind in enumerate(self.industry_vocab)}
        industry_matrix = np.zeros((len(df), len(self.industry_vocab)))
        for i, ind in enumerate(industries):
            if ind in industry_map:
                industry_matrix[i, industry_map[ind]] = 1.0

        size_map = {s: i for i, s in enumerate(self.size_vocab)}
        size_matrix = np.zeros((len(df), len(self.size_vocab)))
        for i, size in enumerate(sizes):
            if size in size_map:
                size_matrix[i, size_map[size]] = 1.0

        service_map = {svc: i for i, svc in enumerate(self.service_vocab)}
        service_matrix = np.zeros((len(df), len(self.service_vocab)))
        for i, svc_str in enumerate(services_list):
            if svc_str != "unknown":
                services = [s.strip() for s in svc_str.split(",") if s.strip()]
                for svc in services:
                    if svc in service_map:
                        service_matrix[i, service_map[svc]] = 1.0

        return np.hstack([industry_matrix, size_matrix, service_matrix])

    def _embed_text_fields(self, df: pd.DataFrame) -> Dict[str, np.ndarray]:
        background_texts = []
        services_texts = []
        for _, row in df.iterrows():
            bg = str(row.get("background", "")).strip()
            if not bg or bg.lower() == "nan":
                bg = "No background information"
            background_texts.append(bg[:1000])

            svc = str(row.get("services", "")).strip()
            if not svc or svc.lower() == "nan":
                svc = "No services specified"
            services_texts.append(svc)

        return {
            "background": self.embedder.encode(background_texts),
            "services": self.embedder.encode(services_texts),
        }

    def _embed_location(self, df: pd.DataFrame) -> np.ndarray:
        locations = df["location"].fillna("Unknown")
        if hasattr(self, "top_locations"):
            top_locs = self.top_locations
        else:
            top_locs = self.data_train["location"].value_counts().head(20).index.tolist()
            self.top_locations = top_locs

        location_matrix = np.zeros((len(df), len(top_locs)))
        for i, loc in enumerate(locations):
            if loc in top_locs:
                idx = top_locs.index(loc)
                location_matrix[i, idx] = 1.0

        return location_matrix

    def _embed_numerical(self, df: pd.DataFrame, fit: bool = False) -> np.ndarray:
        if df.empty:
            return np.zeros((0, 1), dtype=np.float32)

        numerical_features = []
        for _, row in df.iterrows():
            client_min = row.get("client_min")
            client_max = row.get("client_max")
            if pd.notna(client_min) and pd.notna(client_max):
                client_mid = (client_min + client_max) / 2
            else:
                client_mid = 0
            numerical_features.append([client_mid])

        numerical_features = np.array(numerical_features, dtype=np.float32)
        if fit:
            numerical_features = self.scaler.fit_transform(numerical_features)
        else:
            numerical_features = self.scaler.transform(numerical_features)
        return numerical_features

    def _transform_to_features(self, df: pd.DataFrame, fit: bool = False) -> np.ndarray:
        feature_blocks = []
        weights = []

        triplet_features = self._embed_triplet_structure(df)
        feature_blocks.append(triplet_features)
        weights.append(self.embedding_weights["triplet_structure"])

        text_embeddings = self._embed_text_fields(df)
        feature_blocks.append(text_embeddings["background"])
        weights.append(self.embedding_weights["background_text"])

        feature_blocks.append(text_embeddings["services"])
        weights.append(self.embedding_weights["services_text"])

        location_features = self._embed_location(df)
        feature_blocks.append(location_features)
        weights.append(self.embedding_weights["location"])

        numerical_features = self._embed_numerical(df, fit=fit)
        feature_blocks.append(numerical_features)
        weights.append(self.embedding_weights["numerical"])

        weighted_blocks = [block * weight for block, weight in zip(feature_blocks, weights)]
        combined = np.hstack(weighted_blocks)
        norms = np.linalg.norm(combined, axis=1, keepdims=True) + 1e-12
        combined = combined / norms

        return combined.astype(np.float32)

    def build_user_profile(self, user_id: str) -> Optional[np.ndarray]:
        user_history = self.data_train[self.data_train["linkedin_company_outsource"] == user_id]
        if user_history.empty:
            return None
        user_features = self._transform_to_features(user_history, fit=False)
        user_profile = np.mean(user_features, axis=0, keepdims=True)
        return user_profile

    def recommend_triplets(
        self,
        user_id: str,
        top_k: int = 10,
        mode: Literal["val", "test"] = "test",
    ) -> pd.DataFrame:
        user_profile = self.build_user_profile(user_id)
        if user_profile is None:
            return self._recommend_by_popularity(top_k, mode=mode)

        if mode == "val":
            X_candidates = self.X_val
            df_candidates = self.data_val
        else:
            X_candidates = self.X_test
            df_candidates = self.df_test

        similarities = cosine_similarity(X_candidates, user_profile).ravel()
        df_scored = df_candidates.copy()
        df_scored["score"] = similarities

        if "triplet" in df_scored.columns:
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

            triplet_results["triplet_industry"] = triplet_results["triplet"].apply(
                lambda x: self.triplet_manager.parse_triplet(x)[0]
            )
            triplet_results["triplet_client_size"] = triplet_results["triplet"].apply(
                lambda x: self.triplet_manager.parse_triplet(x)[1]
            )
            triplet_results["triplet_services"] = triplet_results["triplet"].apply(
                lambda x: self.triplet_manager.parse_triplet(x)[2]
            )

            return triplet_results

        return pd.DataFrame(columns=["triplet", "score", "industry", "client_size", "services"])

    def _recommend_by_popularity(
        self,
        top_k: int,
        mode: Literal["val", "test"] = "test",
    ) -> pd.DataFrame:
        if mode == "val":
            df_candidates = self.data_val
        else:
            df_candidates = self.df_test

        if df_candidates.empty and not self.data_train.empty:
            df_candidates = self.data_train

        if "triplet" not in df_candidates.columns or df_candidates.empty:
            return pd.DataFrame(columns=["triplet", "score"])

        triplet_counts = df_candidates["triplet"].value_counts()
        if triplet_counts.empty:
            return pd.DataFrame(columns=["triplet", "score"])

        sorted_counts = triplet_counts.head(top_k)
        max_count = sorted_counts.iloc[0]
        return pd.DataFrame(
            [{"triplet": t, "score": c / max_count} for t, c in sorted_counts.items()]
        )


class ContentBaseHandler:
    def __init__(
        self,
        triplet_manager,
        is_use_openai: bool = True,
        openai_model: str = "text-embedding-3-small",
        validation_split: float = 0.2,
    ) -> None:
        self.triplet_manager = triplet_manager
        self.is_use_openai = is_use_openai
        self.openai_model = openai_model
        self.validation_split = validation_split
        self._recommender: Optional[TripletContentRecommender] = None

    def fit(self, df_train: pd.DataFrame, df_test: pd.DataFrame) -> "ContentBaseHandler":
        self._recommender = TripletContentRecommender(
            df_history=df_train,
            df_test=df_test,
            triplet_manager=self.triplet_manager,
            is_use_openai=self.is_use_openai,
            openai_model=self.openai_model,
            validation_split=self.validation_split,
        )
        return self

    def recommend(
        self,
        user_id: str,
        top_k: int = 10,
        mode: str = "test",
    ) -> pd.DataFrame:
        if self._recommender is None:
            raise RuntimeError("ContentBaseHandler is not fitted. Call fit() first.")
        return self._recommender.recommend_triplets(user_id, top_k=top_k, mode=mode)
