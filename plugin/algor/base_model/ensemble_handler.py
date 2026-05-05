from __future__ import annotations

from collections import defaultdict
from typing import Dict, List, Optional, Literal, Tuple

import numpy as np
import pandas as pd
from sklearn.ensemble import GradientBoostingRegressor
from sklearn.preprocessing import StandardScaler

from plugin.algor.base_model.content_base_handler import TripletContentRecommender
from plugin.algor.base_model.enhance_content_base import EnhancedContentBaseHandler
from plugin.algor.base_model.collaborative_handler import UserBasedCollaborativeRecommender


class TripletEnsembleRecommender:
    def __init__(
        self,
        triplet_manager,
        n_estimators: int = 100,
        learning_rate: float = 0.1,
        max_depth: int = 5,
        random_state: int = 42,
        is_use_openai: bool = True,
        openai_model: str = "text-embedding-3-small",
    ) -> None:
        self.triplet_manager = triplet_manager
        self.n_estimators = n_estimators
        self.learning_rate = learning_rate
        self.max_depth = max_depth
        self.random_state = random_state
        self.is_use_openai = is_use_openai
        self.openai_model = openai_model

        self.meta_learner = GradientBoostingRegressor(
            n_estimators=n_estimators,
            learning_rate=learning_rate,
            max_depth=max_depth,
            random_state=random_state,
            subsample=0.8,
            min_samples_leaf=10,
        )
        self.scaler = StandardScaler()

        self.content_model: Optional[TripletContentRecommender] = None
        self.enhanced_content_model: Optional[TripletContentRecommender] = None
        self.user_collab_model: Optional[UserBasedCollaborativeRecommender] = None

        self.df_train: Optional[pd.DataFrame] = None
        self.df_test: Optional[pd.DataFrame] = None
        self.feature_stats: Dict[str, float] = {}
        self.is_fitted = False

    def fit(
        self,
        df_train: pd.DataFrame,
        df_test: pd.DataFrame,
        ground_truth: Dict[str, List[str]],
        validation_split: float = 0.2,
    ) -> "TripletEnsembleRecommender":
        self.df_train = df_train.copy()
        self.df_test = df_test.copy()

        train_users = list(ground_truth.keys())
        np.random.seed(self.random_state)
        np.random.shuffle(train_users)

        n_val = int(len(train_users) * validation_split)
        val_users = set(train_users[:n_val])

        self._fit_base_models(df_train, df_test)

        X_meta, y_meta = self._prepare_meta_training_data(val_users, ground_truth)
        if len(X_meta) == 0:
            self.is_fitted = False
            return self

        X_meta_scaled = self.scaler.fit_transform(X_meta)
        self.meta_learner.fit(X_meta_scaled, y_meta)
        self.is_fitted = True
        return self

    def _fit_base_models(self, df_train: pd.DataFrame, df_test: pd.DataFrame) -> None:
        self.content_model = TripletContentRecommender(
            df_history=df_train,
            df_test=df_test,
            triplet_manager=self.triplet_manager,
            is_use_openai=self.is_use_openai,
            openai_model=self.openai_model,
        )
        enhanced_handler = EnhancedContentBaseHandler(
            triplet_manager=self.triplet_manager,
            is_use_openai=self.is_use_openai,
            openai_model=self.openai_model,
        )
        enhanced_handler.fit(df_train, df_test)
        self.enhanced_content_model = enhanced_handler._recommender

        self.user_collab_model = UserBasedCollaborativeRecommender(
            min_similarity=0.1,
            top_k_similar_users=20,
            is_use_openai=self.is_use_openai,
            openai_model=self.openai_model,
        )
        self.user_collab_model.fit(df_history=df_train, df_user_info=df_train)

        self._calculate_feature_stats(df_train)

    def _calculate_feature_stats(self, df_train: pd.DataFrame) -> None:
        user_counts = df_train.groupby("linkedin_company_outsource").size()
        self.feature_stats["user_count_mean"] = user_counts.mean()
        self.feature_stats["user_count_std"] = user_counts.std() + 1e-6

        if "triplet" in df_train.columns:
            triplet_counts = df_train.groupby("triplet").size()
            self.feature_stats["triplet_pop_mean"] = triplet_counts.mean()
            self.feature_stats["triplet_pop_std"] = triplet_counts.std() + 1e-6

    def _prepare_meta_training_data(
        self,
        val_users: set,
        ground_truth: Dict[str, List[str]],
    ) -> Tuple[np.ndarray, np.ndarray]:
        X_meta = []
        y_meta = []

        for user_id in val_users:
            if user_id not in ground_truth:
                continue

            gt_triplets = set(ground_truth[user_id])
            base_preds = self._get_base_predictions(user_id, top_k=50, mode="val")
            if not base_preds:
                continue

            all_triplets = set()
            for model_preds in base_preds.values():
                all_triplets.update(model_preds.keys())
            all_triplets.update(gt_triplets)

            user_history = self.df_train[self.df_train["linkedin_company_outsource"] == user_id]
            user_history_count = len(user_history)

            for triplet in all_triplets:
                features = self._create_meta_features(
                    user_id, triplet, base_preds, user_history_count
                )
                label = 1.0 if triplet in gt_triplets else 0.0
                X_meta.append(features)
                y_meta.append(label)

        return np.array(X_meta), np.array(y_meta)

    def _get_base_predictions(
        self,
        user_id: str,
        top_k: int = 50,
        mode: Literal["val", "test"] = "test",
    ) -> Dict[str, Dict[str, float]]:
        predictions: Dict[str, Dict[str, float]] = {}

        try:
            content_recs = self.content_model.recommend_triplets(user_id, top_k=top_k, mode=mode)
            predictions["content"] = dict(zip(content_recs["triplet"], content_recs["score"]))
        except Exception:
            predictions["content"] = {}

        if self.enhanced_content_model is not None:
            try:
                enhanced_recs = self.enhanced_content_model.recommend_triplets(
                    user_id, top_k=top_k, mode=mode
                )
                predictions["enhanced_content"] = dict(
                    zip(enhanced_recs["triplet"], enhanced_recs["score"])
                )
            except Exception:
                predictions["enhanced_content"] = {}

        try:
            collab_recs = self.user_collab_model.recommend_triplets(user_id, top_k=top_k)
            predictions["user_collab"] = dict(zip(collab_recs["triplet"], collab_recs["score"]))
        except Exception:
            predictions["user_collab"] = {}

        return predictions

    def _create_meta_features(
        self,
        user_id: str,
        triplet: str,
        base_preds: Dict[str, Dict[str, float]],
        user_history_count: int,
    ) -> np.ndarray:
        content_score = base_preds.get("content", {}).get(triplet, 0.0)
        enhanced_content_score = base_preds.get("enhanced_content", {}).get(triplet, 0.0)
        collab_score = base_preds.get("user_collab", {}).get(triplet, 0.0)
        enhanced_collab_score = 0.0

        scores = [content_score, enhanced_content_score, collab_score, enhanced_collab_score]
        non_zero_scores = [s for s in scores if s > 0]

        score_variance = np.var(scores)
        max_min_spread = max(scores) - min(scores)

        user_count_norm = (user_history_count - self.feature_stats.get("user_count_mean", 0)) / (
            self.feature_stats.get("user_count_std", 1)
        )

        triplet_pop = self.user_collab_model.triplet_popularity.get(triplet, 0) if self.user_collab_model else 0
        triplet_pop_norm = (triplet_pop - self.feature_stats.get("triplet_pop_mean", 0)) / (
            self.feature_stats.get("triplet_pop_std", 1)
        )

        industry_match, size_match, service_overlap = self._calculate_triplet_user_match(
            user_id, triplet
        )

        n_models_recommending = len(non_zero_scores)

        return np.array(
            [
                content_score,
                collab_score,
                score_variance,
                max_min_spread,
                user_count_norm,
                triplet_pop_norm,
                industry_match,
                size_match,
                service_overlap,
                n_models_recommending / 2.0,
            ]
        )

    def _calculate_triplet_user_match(
        self,
        user_id: str,
        triplet: str,
    ) -> Tuple[float, float, float]:
        industry, size, services = self.triplet_manager.parse_triplet(triplet)
        user_history = self.df_train[self.df_train["linkedin_company_outsource"] == user_id]
        if user_history.empty:
            return 0.0, 0.0, 0.0

        industry_counts = user_history["industry"].value_counts()
        total = len(user_history)
        industry_match = industry_counts.get(industry, 0) / total

        size_match = 0.0
        size_counts = defaultdict(int)
        for _, row in user_history.iterrows():
            client_min = row.get("client_min")
            client_max = row.get("client_max")
            if pd.notna(client_min) and pd.notna(client_max):
                mid = (client_min + client_max) / 2
                if mid <= 10:
                    bucket = "micro"
                elif mid <= 50:
                    bucket = "small"
                elif mid <= 200:
                    bucket = "medium"
                elif mid <= 1000:
                    bucket = "large"
                else:
                    bucket = "enterprise"
                size_counts[bucket] += 1
        if size_counts:
            size_match = size_counts.get(size, 0) / sum(size_counts.values())

        triplet_services = set(
            s.strip() for s in services.split(",") if s.strip() and s != "unknown"
        )
        user_services = set()
        for svc_str in user_history["services"].dropna():
            for s in str(svc_str).split(","):
                if s.strip():
                    user_services.add(s.strip())

        if triplet_services and user_services:
            intersection = len(triplet_services & user_services)
            union = len(triplet_services | user_services)
            service_overlap = intersection / union if union > 0 else 0.0
        else:
            service_overlap = 0.0

        return industry_match, size_match, service_overlap

    def recommend_triplets(
        self,
        user_id: str,
        top_k: int = 10,
        mode: Literal["val", "test"] = "test",
    ) -> pd.DataFrame:
        base_preds = self._get_base_predictions(user_id, top_k=top_k * 5, mode=mode)
        if not any(base_preds.values()):
            return self._recommend_by_popularity(top_k)

        all_triplets = set()
        for model_preds in base_preds.values():
            all_triplets.update(model_preds.keys())
        if not all_triplets:
            return self._recommend_by_popularity(top_k)

        user_history = self.df_train[self.df_train["linkedin_company_outsource"] == user_id]
        user_history_count = len(user_history)

        triplet_scores = {}
        for triplet in all_triplets:
            if self.is_fitted:
                features = self._create_meta_features(
                    user_id, triplet, base_preds, user_history_count
                )
                features_scaled = self.scaler.transform([features])
                score = self.meta_learner.predict(features_scaled)[0]
            else:
                content_score = base_preds.get("content", {}).get(triplet, 0.0)
                enhanced_content_score = base_preds.get("enhanced_content", {}).get(triplet, 0.0)
                collab_score = base_preds.get("user_collab", {}).get(triplet, 0.0)
                score = 0.4 * content_score + 0.35 * enhanced_content_score + 0.25 * collab_score
            triplet_scores[triplet] = score

        sorted_triplets = sorted(triplet_scores.items(), key=lambda x: x[1], reverse=True)[:top_k]
        return pd.DataFrame(
            [{"triplet": triplet, "score": score} for triplet, score in sorted_triplets]
        )

    def _recommend_by_popularity(self, top_k: int) -> pd.DataFrame:
        if self.user_collab_model:
            sorted_triplets = sorted(
                self.user_collab_model.triplet_popularity.items(),
                key=lambda x: x[1],
                reverse=True,
            )[:top_k]
            if sorted_triplets:
                max_count = sorted_triplets[0][1]
                return pd.DataFrame(
                    [{"triplet": t, "score": c / max_count} for t, c in sorted_triplets]
                )
        return pd.DataFrame(columns=["triplet", "score"])


class EnsembleHandler:
    def __init__(
        self,
        triplet_manager,
        n_estimators: int = 100,
        learning_rate: float = 0.1,
        max_depth: int = 5,
        random_state: int = 42,
        is_use_openai: bool = True,
        openai_model: str = "text-embedding-3-small",
    ) -> None:
        self.triplet_manager = triplet_manager
        self.is_use_openai = is_use_openai
        self.openai_model = openai_model
        self.n_estimators = n_estimators
        self.learning_rate = learning_rate
        self.max_depth = max_depth
        self.random_state = random_state
        self._recommender: Optional[TripletEnsembleRecommender] = None

    def fit(
        self,
        df_train: pd.DataFrame,
        df_test: pd.DataFrame,
        ground_truth: Dict[str, List[str]],
    ) -> "EnsembleHandler":
        self._recommender = TripletEnsembleRecommender(
            triplet_manager=self.triplet_manager,
            n_estimators=self.n_estimators,
            learning_rate=self.learning_rate,
            max_depth=self.max_depth,
            random_state=self.random_state,
            is_use_openai=self.is_use_openai,
            openai_model=self.openai_model,
        )
        self._recommender.fit(df_train=df_train, df_test=df_test, ground_truth=ground_truth)
        return self

    def recommend(self, user_id: str, top_k: int = 10, mode: str = "test") -> pd.DataFrame:
        if self._recommender is None:
            raise RuntimeError("EnsembleHandler is not fitted. Call fit() first.")
        return self._recommender.recommend_triplets(user_id, top_k=top_k, mode=mode)
