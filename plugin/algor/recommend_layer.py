from __future__ import annotations

from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple
import re

import pytz

import pandas as pd
from sqlalchemy import Integer, cast, func, or_

from plugin.algor.data_handler import DataHandler
from plugin.algor.base_model.content_base_handler import ContentBaseHandler
from plugin.algor.base_model.enhance_content_base import EnhancedContentBaseHandler
from plugin.algor.base_model.collaborative_handler import CollaborativeHandler
from plugin.algor.base_model.ensemble_handler import EnsembleHandler
from plugin.algor import INDUSTRY_MAPPING, SIZE_MAPPING
from model.model import (
	CompanyFunding,
	EventsList,
	GuestList,
	LinkedinCompany,
	LinkedinJob,
	MasterCompanies,
	NewsInformation,
)


def _unique_users(df: pd.DataFrame, limit_users: Optional[int] = None) -> List[str]:
	users = [u for u in df.get("linkedin_company_outsource", pd.Series(dtype=str)).unique() if u]
	if limit_users is not None:
		return users[:limit_users]
	return users


def _collect_recommendations(
	handler,
	users: List[str],
	top_k: int,
	mode: Optional[str] = None,
	exclude_seen: Optional[bool] = None,
) -> pd.DataFrame:
	results = []
	for user_id in users:
		if mode is not None:
			recs = handler.recommend(user_id, top_k=top_k, mode=mode)
		elif exclude_seen is not None:
			recs = handler.recommend(user_id, top_k=top_k, exclude_seen=exclude_seen)
		else:
			recs = handler.recommend(user_id, top_k=top_k)

		for _, rec in recs.iterrows():
			results.append(
				{
					"linkedin_company_outsource": user_id,
					"triplet": rec.get("triplet"),
					"score": float(rec.get("score", 0)),
				}
			)
	return pd.DataFrame(results)


def _parse_triplet(triplet: Optional[str]) -> Tuple[str, str, str]:
	if not triplet:
		return "unknown", "unknown", "unknown"
	parts = [p.strip() for p in str(triplet).split("|||")]
	while len(parts) < 3:
		parts.append("unknown")
	return parts[0] or "unknown", parts[1] or "unknown", parts[2] or "unknown"


def _normalize_token(value: Optional[str]) -> str:
	if value is None:
		return ""
	return str(value).strip().lower()



def _industry_matches(company_industry: str, industry: str) -> bool:
	industry_norm = _normalize_token(industry)
	if not industry_norm or industry_norm == "unknown":
		return True
	mapped = INDUSTRY_MAPPING.get(industry, [])
	if mapped:
		company_norm = _normalize_token(company_industry)
		mapped_norm = {_normalize_token(value) for value in mapped}
		return company_norm in mapped_norm

	return _normalize_token(company_industry) == industry_norm


def _industry_filter_values(industry: str) -> List[str]:
	if not industry or industry == "unknown":
		return []
	mapped = INDUSTRY_MAPPING.get(industry, [])
	if mapped:
		return mapped
	return [industry]

def _size_filter_values(size: str) -> List[str]:
	if not size or size == "unknown":
		return []
	mapped = SIZE_MAPPING.get(size, [])
	if mapped:
		return mapped
	return [size]


def _get_master_field_names() -> set:
	if MasterCompanies is None or not hasattr(MasterCompanies, "__table__"):
		return set()
	return set(MasterCompanies.__table__.columns.keys())


def _safe_defaults(defaults: Dict[str, object]) -> Dict[str, object]:
	field_names = _get_master_field_names()
	if not field_names:
		return {}
	return {key: value for key, value in defaults.items() if key in field_names}


def _resolve_session(session):
	if session is not None:
		return session
	data_handler = DataHandler(session=session)
	return data_handler._get_session()


def run_recommendation(
	top_k: int = 352,
	limit_rows: Optional[int] = None,
	limit_users: Optional[int] = None,
	is_use_openai: bool = False,
	openai_model: str = "text-embedding-3-small",
	session = None
) -> Dict[str, pd.DataFrame]:
	data_handler = DataHandler(session=session)
	df_train, df_test, triplet_manager, ground_truth = data_handler.prepare_dataset(
		limit=limit_rows
	)

	if df_train.empty or df_test.empty:
		return {"error": pd.DataFrame([{"message": "No data available for recommendation."}])}

	users = _unique_users(df_test, limit_users=limit_users)
	if not users:
		return {"error": pd.DataFrame([{"message": "No users found in test data."}])}
	print('=========== DF Train ===========')
	print(df_train.info())
	print(df_train.iloc[0])
 
	print('=========== DF Test ===========')
	print(df_test.info())
	print(df_test.iloc[0])	
	content_handler = ContentBaseHandler(
		triplet_manager=triplet_manager,
		is_use_openai=is_use_openai,
		openai_model=openai_model,
	).fit(df_train, df_test)
	print('=========== Content Handler fitted ===========')
	enhanced_handler = EnhancedContentBaseHandler(
		triplet_manager=triplet_manager,
		is_use_openai=is_use_openai,
		openai_model=openai_model,
	).fit(df_train, df_test)
	print('=========== Enhanced Content Handler fitted ===========')
	collab_handler = CollaborativeHandler(
		is_use_openai=is_use_openai,
		openai_model=openai_model,
	).fit(df_train, df_train)
	print('=========== Collaborative Handler fitted ===========')
	ensemble_handler = EnsembleHandler(
		triplet_manager=triplet_manager,
		is_use_openai=is_use_openai,
		openai_model=openai_model,
	).fit(df_train, df_test, ground_truth)

	results = {
		"content": _collect_recommendations(content_handler, users, top_k, mode="test"),
		"enhanced_content": _collect_recommendations(enhanced_handler, users, top_k, mode="test"),
		"collaborative": _collect_recommendations(collab_handler, users, top_k, exclude_seen=True),
		"ensemble": _collect_recommendations(ensemble_handler, users, top_k, mode="test"),
	}

	return results


def run_online_recommendation(
	user_records: List[Dict[str, object]],
	top_k: int = 50,
	user_id: str = "online_user",
	is_use_openai: bool = False,
	openai_model: str = "text-embedding-3-small",
	print_output: bool = True,
	session = None
) -> Dict[str, pd.DataFrame]:
	data_handler = DataHandler(session=session)
	df_all_raw = data_handler.load_reviews()
	df_all = data_handler.preprocess(df_all_raw)

	if df_all.empty:
		return {"error": pd.DataFrame([{"message": "No data available for recommendation."}])}

	if not user_records:
		return {"error": pd.DataFrame([{"message": "No user records provided."}])}

	triplet_manager = data_handler.triplet_manager_class()
	df_all = data_handler.add_triplet_column(df_all, triplet_manager, column_name="triplet")

	df_online = pd.DataFrame(user_records)
	if user_id:
		df_online["linkedin_company_outsource"] = user_id
	elif "linkedin_company_outsource" in df_online.columns and not df_online.empty:
		user_id = str(df_online["linkedin_company_outsource"].iloc[0])
	else:
		return {"error": pd.DataFrame([{"message": "Missing user identifier."}])}

	df_online = data_handler.preprocess(df_online)
	df_online = data_handler.add_triplet_column(df_online, triplet_manager, column_name="triplet")

	df_history = pd.concat([df_all, df_online], ignore_index=True)
	ground_truth = data_handler.build_ground_truth(df_all)

	print('=========== DF History ===========')
	print(df_history.info())
	print(df_history.iloc[0])
	print('=========== DF All ===========')
	print(df_all.info())
	print(df_all.iloc[0])
	print("==============================")
 
	# content_handler = ContentBaseHandler(
	# 	triplet_manager=triplet_manager,
	# 	is_use_openai=is_use_openai,
	# 	openai_model=openai_model,
	# 	validation_split=0.0,
	# ).fit(df_history, df_all)
	# print('=========== Content Handler fitted ===========')
	# enhanced_handler = EnhancedContentBaseHandler(
	# 	triplet_manager=triplet_manager,
	# 	is_use_openai=is_use_openai,
	# 	openai_model=openai_model,
	# 	validation_split=0.0,
	# ).fit(df_history, df_all)
	# print('=========== Enhanced Content Handler fitted ===========')
	# collab_handler = CollaborativeHandler(
	# 	is_use_openai=is_use_openai,
	# 	openai_model=openai_model,
	# ).fit(df_history, df_history)
	# print('=========== Collaborative Handler fitted ===========')

	ensemble_handler = EnsembleHandler(
		triplet_manager=triplet_manager,
		is_use_openai=is_use_openai,
		openai_model=openai_model,
	).fit(df_history, df_all, ground_truth)
	print('=========== Ensemble Handler fitted ===========')
	results = {
		"ensemble": _collect_recommendations(ensemble_handler, [user_id], top_k, mode="test"),
	}

	if print_output:
		for name, df in results.items():
			print(f"[{name}]")
			if df.empty:
				print("No recommendations.")
			else:
				print(df.head(top_k).to_string(index=False))
			print("-" * 40)

	return results


def run_online_recommendation_with_scoring(
	user_records: List[Dict[str, object]],
	top_k: int = 350,
	user_id: str = "online_user",
	is_use_openai: bool = False,
	openai_model: str = "text-embedding-3-small",
	session = None,
) -> Dict[str, pd.DataFrame]:
	print("[recommendation_with_scoring] start")
	try:
		results = run_online_recommendation(
			user_records=user_records,
			top_k=top_k,
			user_id=user_id,
			is_use_openai=is_use_openai,
			openai_model=openai_model,
			print_output=False,
			session=session,
		)
		if "error" in results:
			print("[recommendation_with_scoring] recommendation error")
			error_df = results.get("error", pd.DataFrame())
			return {
				"recommendation_results": error_df,
				"matched_companies": pd.DataFrame(),
				"master_companies_updated": pd.DataFrame(),
				"summary": {
					"total_matched": 0,
					"total_with_triggers": 0,
					"avg_total_score": 0.0,
				},
			}

		df_recommendation = results.get("ensemble", pd.DataFrame())
		print(
			f"[recommendation_with_scoring] recommendation rows={len(df_recommendation)}"
		)
		print(df_recommendation.head(10))
		matched_companies: Dict[str, Dict[str, object]] = {}
		count = 0
		for _, row in df_recommendation.iterrows():
			if count == 5: break
			count += 1
			triplet = row.get("triplet")
			industry, size, specialization = _parse_triplet(triplet)
			rec_score = float(row.get("score", 0.0))
			if industry == "Other industries":
				continue
			

			query = session.query(LinkedinCompany)
			industry_values = _industry_filter_values(industry)
			if industry_values:
				query = query.filter(LinkedinCompany.industry.in_(industry_values))
			size_values = _size_filter_values(size)
			if size_values:
				query = query.filter(LinkedinCompany.size.in_(size_values))
			companies = query.all()
			print(f"[recommendation_with_scoring] query matched companies={len(companies)}")
	
			companies = query.all()
			for company in companies:
				company_id = str(company.id)
				current = matched_companies.get(company_id)
				if current is None or rec_score > float(current["recommendation_score"]):
					matched_companies[company_id] = {
						"company": company,
						"linkedin_company_outsource": getattr(company, "linkedin_url", ""),
						"industry": industry,
						"size": size,
						"specialization": specialization,
						"recommendation_score": rec_score,
					}

			if companies:
				print(
					"[recommendation_with_scoring] matched triplet",
					triplet,
					"companies=",
					len(companies),
				)

		matched_rows = []
		master_rows = []
		master_fields = _get_master_field_names()
		now = datetime.now(pytz.UTC)

		for match in matched_companies.values():
			company = match["company"]
			jobs_count = 0
			events_count = 0
			news_count = 0
			funding_count = 0

			jobs_count = (
				session.query(LinkedinJob)
				.filter(
					LinkedinJob.company_id == company.id,
					LinkedinJob.created_at >= now - timedelta(days=100),
				)
				.count()
			)
			events_count = (
				session.query(GuestList)
				.join(EventsList, GuestList.event_id == EventsList.id)
				.filter(
					GuestList.company_id == company.id,
					EventsList.start_date >= now - timedelta(days=100),
				)
				.count()
			)
			news_count = (
				session.query(NewsInformation)
				.filter(
					NewsInformation.company_id == company.id,
					NewsInformation.time_post >= now - timedelta(days=100),
				)
				.count()
			)
			funding_count = (
				session.query(CompanyFunding)
				.filter(CompanyFunding.company_id == company.id)
				.count()
			)

			triggers = []
			if events_count > 0:
				triggers.append("event")
			if jobs_count > 0:
				triggers.append("hiring")
			if news_count > 0:
				triggers.append("news")
			if funding_count > 0:
				triggers.append("funding")

			trigger_score = (
				jobs_count * 0.3
				+ events_count * 0.3
				+ funding_count * 0.3
				+ news_count * 0.1
			)
			recommendation_score = float(match["recommendation_score"])
			total_score = recommendation_score + trigger_score

			source = "recommendation_with_trigger" if triggers else "recommendation"
			defaults = {
				"company_id": company.id,
				"recommendation_score": recommendation_score,
				"trigger_score": trigger_score,
				"total_score": total_score,
				"trigger": triggers,
				"updated_at": now,
			}
			if "total_score" not in master_fields and "score" in master_fields:
				defaults["score"] = total_score
			defaults = _safe_defaults(defaults)
			if defaults:
				master_record = (
					session.query(MasterCompanies)
					.filter(MasterCompanies.company_id == company.id)
					.first()
				)
				if master_record is None:
					master_record = MasterCompanies(**defaults)
					session.add(master_record)
				else:
					for key, value in defaults.items():
						setattr(master_record, key, value)
			else:
				print(
					"[recommendation_with_scoring] skip update, missing fields for",
					str(company.id),
				)

			# print(
			# 	"[recommendation_with_scoring] company=",
			# 	str(company.id),
			# 	"rec=",
			# 	recommendation_score,
			# 	"trigger=",
			# 	trigger_score,
			# 	"total=",
			# 	total_score,
			# 	"triggers=",
			# 	triggers,
			# )

			row_payload = {
				"linkedin_company_outsource": match.get("linkedin_company_outsource", ""),
				"industry": match.get("industry", ""),
				"size": match.get("size", ""),
				"specialization": match.get("specialization", ""),
				"recommendation_score": recommendation_score,
				"trigger_score": trigger_score,
				"total_score": total_score,
				"triggers": triggers,
				"updated_at": now,
				"source": source,
			}
			matched_rows.append(row_payload)
			master_rows.append(row_payload)

		df_matched = pd.DataFrame(matched_rows)
		df_master = pd.DataFrame(master_rows)

		total_matched = len(df_matched)
		total_with_triggers = int(df_matched["triggers"].apply(bool).sum()) if total_matched else 0
		avg_total_score = float(df_matched["total_score"].mean()) if total_matched else 0.0

		# session.flush()
		# session.commit()
		return {
			"recommendation_results": df_recommendation,
			"matched_companies": df_matched,
			"master_companies_updated": df_master,
			"summary": {
				"total_matched": total_matched,
				"total_with_triggers": total_with_triggers,
				"avg_total_score": avg_total_score,
			},
		}
	except Exception as exc:
		print("[recommendation_with_scoring] error:", str(exc))
		return {
			"recommendation_results": pd.DataFrame(),
			"matched_companies": pd.DataFrame(),
			"master_companies_updated": pd.DataFrame(),
			"summary": {
				"total_matched": 0,
				"total_with_triggers": 0,
				"avg_total_score": 0.0,
			},
		}
