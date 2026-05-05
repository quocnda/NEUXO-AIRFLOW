from __future__ import annotations

from typing import Dict, List, Optional, Tuple
import re

import numpy as np
import pandas as pd
from sqlalchemy.orm import Session

from hook.sqlalchemyHook import SQLAlchemyHook
from model.model import ClutchReview, LinkedinCompany


def _clean_text(value: Optional[object]) -> str:
	if value is None or (isinstance(value, float) and np.isnan(value)):
		return ""
	return str(value).strip()


def _parse_client_size(value: Optional[str]) -> Tuple[Optional[float], Optional[float]]:
	if value is None or (isinstance(value, float) and np.isnan(value)):
		return None, None
	text = str(value).strip()
	if not text or text.lower() in {"unknown", "n/a", "nan"}:
		return None, None
	numbers = [float(n) for n in re.findall(r"\d+", text.replace(",", ""))]
	if len(numbers) >= 2:
		return numbers[0], numbers[1]
	if len(numbers) == 1:
		return numbers[0], numbers[0]
	return None, None


SIZE_BUCKETS: Dict[str, Tuple[int, int]] = {
	"micro": (0, 10),
	"small": (11, 50),
	"medium": (51, 200),
	"large": (201, 1000),
	"enterprise": (1001, 10**9),
}


class TripletManager:
	def __init__(self, service_separator: str = ",", triplet_separator: str = "|||") -> None:
		self.service_separator = service_separator
		self.triplet_separator = triplet_separator

	def normalize_client_size(self, row: pd.Series) -> str:
		client_min = row.get("client_min")
		client_max = row.get("client_max")
		if pd.notna(client_min) and pd.notna(client_max):
			mid = (client_min + client_max) / 2
			return self._midpoint_to_bucket(mid)
		return "unknown"

	def _midpoint_to_bucket(self, mid: float) -> str:
		for bucket_name, (low, high) in SIZE_BUCKETS.items():
			if low <= mid <= high:
				return bucket_name
		return "unknown"

	def normalize_services(self, services_str: str) -> str:
		if pd.isna(services_str) or not str(services_str).strip():
			return "unknown"
		services = [s.strip() for s in str(services_str).split(self.service_separator)]
		services = [s for s in services if s and s.lower() != "nan"]
		if not services:
			return "unknown"
		return self.service_separator.join(services)

	def create_triplet(self, row: pd.Series) -> str:
		industry = str(row.get("industry", "unknown")).strip()
		if not industry or industry.lower() == "nan":
			industry = "unknown"
		client_size = self.normalize_client_size(row)
		services = self.normalize_services(row.get("services", ""))
		return f"{industry}{self.triplet_separator}{client_size}{self.triplet_separator}{services}"

	def parse_triplet(self, triplet_str: str) -> Tuple[str, str, str]:
		parts = triplet_str.split(self.triplet_separator)
		if len(parts) == 3:
			return parts[0], parts[1], parts[2]
		return "unknown", "unknown", "unknown"


def add_triplet_column(
	df: pd.DataFrame,
	triplet_manager: TripletManager,
	column_name: str = "triplet",
) -> pd.DataFrame:
	df = df.copy()
	df[column_name] = df.apply(triplet_manager.create_triplet, axis=1)
	return df


class DataHandler:
	def __init__(self, conn_id: str = "neuxo_connections", session: Optional[Session] = None) -> None:
		self.conn_id = conn_id
		self._session = session

		self.triplet_manager_class = TripletManager
		self.add_triplet_column = add_triplet_column

	def _get_session(self) -> Session:
		if self._session is not None:
			return self._session
		hook = SQLAlchemyHook(conn_id=self.conn_id)
		self._session = hook.get_session()
		return self._session

	def load_reviews(self, limit: Optional[int] = None) -> pd.DataFrame:
		session = self._get_session()
		query = (
			session.query(ClutchReview, LinkedinCompany)
			.outerjoin(LinkedinCompany, ClutchReview.company_id == LinkedinCompany.id)
		)
		if limit:
			query = query.limit(limit)

		rows = []
		for review, company in query.all():
			services_company_outsource = review.services_company_outsource
			if not services_company_outsource and company is not None and company.labels:
				services_company_outsource = ", ".join([str(s).strip() for s in company.labels if str(s).strip()])

			description_company_outsource = review.description_company_outsource
			if not description_company_outsource and company is not None:
				description_company_outsource = company.description

			linkedin_company_outsource = ""
			if company is not None:
				linkedin_company_outsource = company.linkedin_url or company.website or company.id

			rows.append(
				{
					"linkedin_company_outsource": linkedin_company_outsource,
					"company_lead_name": review.reviewer_company,
					"industry": review.industry,
					"location": review.location,
					"services": review.services,
					"project_description": review.project_description,
					"background": review.background,
					"website_outsource_url": review.website_url or (company.website if company is not None else None),
					"client_size": review.client_size,
					"services_company_outsource": services_company_outsource,
					"description_company_outsource": description_company_outsource,
				}
			)

		df = pd.DataFrame(rows)
		return df

	def preprocess(self, df: pd.DataFrame) -> pd.DataFrame:
		if df.empty:
			return df

		df = df.copy()

		for col in [
			"linkedin_company_outsource",
			"company_lead_name",
			"industry",
			"location",
			"services",
			"project_description",
			"background",
			"website_outsource_url",
			"services_company_outsource",
			"description_company_outsource",
		]:
			if col not in df.columns:
				df[col] = ""
			df[col] = df[col].apply(_clean_text)

		client_min = []
		client_max = []
		client_mid = []
		for value in df.get("client_size", pd.Series([None] * len(df))):
			min_val, max_val = _parse_client_size(value)
			client_min.append(min_val)
			client_max.append(max_val)
			if min_val is not None and max_val is not None:
				client_mid.append((min_val + max_val) / 2)
			else:
				client_mid.append(None)

		df["client_min"] = client_min
		df["client_max"] = client_max
		df["client_size_mid"] = client_mid

		return df

	def split_train_test(
		self,
		df: pd.DataFrame,
		test_size: float = 0.2,
		random_state: int = 42,
	) -> Tuple[pd.DataFrame, pd.DataFrame]:
		if df.empty:
			return df.copy(), df.copy()

		if "linkedin_company_outsource" not in df.columns:
			df = df.sample(frac=1.0, random_state=random_state).reset_index(drop=True)
			n_test = max(1, int(len(df) * test_size))
			df_test = df.iloc[:n_test].copy()
			df_train = df.iloc[n_test:].copy()
			return df_train, df_test

		rng = np.random.RandomState(random_state)
		train_ratio = 1.0 - test_size
		train_parts = []
		test_parts = []
		for _, group in df.groupby("linkedin_company_outsource", dropna=False):
			indices = group.index.to_numpy(copy=True)
			rng.shuffle(indices)
			group = group.loc[indices]
			split_index = int(len(group) * train_ratio)
			if len(group) >= 2:
				split_index = max(1, min(split_index, len(group) - 1))
			train_parts.append(group.iloc[:split_index])
			test_parts.append(group.iloc[split_index:])

		df_train = pd.concat(train_parts, ignore_index=True) if train_parts else df.copy()
		df_test = pd.concat(test_parts, ignore_index=True) if test_parts else df.copy()
		return df_train, df_test

	def add_triplets(
		self,
		df_train: pd.DataFrame,
		df_test: pd.DataFrame,
	) -> Tuple[pd.DataFrame, pd.DataFrame, object]:
		triplet_manager = self.triplet_manager_class()
		df_train = self.add_triplet_column(df_train, triplet_manager, column_name="triplet")
		df_test = self.add_triplet_column(df_test, triplet_manager, column_name="triplet")
		return df_train, df_test, triplet_manager

	def build_ground_truth(self, df_test: pd.DataFrame) -> Dict[str, List[str]]:
		ground_truth: Dict[str, List[str]] = {}
		if df_test.empty:
			return ground_truth

		for _, row in df_test.iterrows():
			user_id = row.get("linkedin_company_outsource")
			triplet = row.get("triplet")
			if not user_id or not triplet:
				continue
			ground_truth.setdefault(user_id, []).append(triplet)
		return ground_truth

	def prepare_dataset(
		self,
		limit: Optional[int] = None,
		test_size: float = 0.2,
		random_state: int = 42,
	) -> Tuple[pd.DataFrame, pd.DataFrame, object, Dict[str, List[str]]]:
		df_raw = self.load_reviews(limit=limit)
		df = self.preprocess(df_raw)
		df_train, df_test = self.split_train_test(df, test_size=test_size, random_state=random_state)
		df_train, df_test, triplet_manager = self.add_triplets(df_train, df_test)
		ground_truth = self.build_ground_truth(df_test)
		return df_train, df_test, triplet_manager, ground_truth
