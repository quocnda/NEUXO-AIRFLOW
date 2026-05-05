from __future__ import annotations

from typing import Dict, Optional

import pandas as pd

from plugin.algor.base_model.content_base_handler import TripletContentRecommender


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
        self._recommender: Optional[TripletContentRecommender] = None

    def fit(self, df_train: pd.DataFrame, df_test: pd.DataFrame) -> "EnhancedContentBaseHandler":
        self._recommender = TripletContentRecommender(
            df_history=df_train,
            df_test=df_test,
            triplet_manager=self.triplet_manager,
            embedding_weights=self.embedding_weights,
            is_use_openai=self.is_use_openai,
            openai_model=self.openai_model,
            validation_split=self.validation_split,
        )
        return self

    def recommend(self, user_id: str, top_k: int = 10, mode: str = "test") -> pd.DataFrame:
        if self._recommender is None:
            raise RuntimeError("EnhancedContentBaseHandler is not fitted. Call fit() first.")
        return self._recommender.recommend_triplets(user_id, top_k=top_k, mode=mode)
