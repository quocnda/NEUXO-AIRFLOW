from __future__ import annotations

from typing import Any

from sqlalchemy import or_

from model.model import GuestList, LinkedinCompany, LinkedinPersonalEmail, PersonalExperience

from .BaseApify import BaseApify
from plugin.Linkedin import getCompanyInfor


class LinkedinEmailAndProfileScraper(BaseApify):
	DEFAULT_RUN_INPUT = {
		"usernames": [],
		"includeEmail": True,
	}

	def __init__(self, session):
		actor_name = "LINKEDIN_GET_EMAIL_AND_PROFILE"
		super().__init__(actor_name=actor_name)
		self.session = session

	def run(self, usernames: list[str]) -> int:
		run_input = self._default_run_input()
		run_input["usernames"] = usernames

		data = self.run_actor(run_input=run_input)
		print("Data from Apify: ", len(data))
		return self.upsert_people_and_update_guest_email(data)

	@classmethod
	def _normalize_linkedin_url(cls, value: str | None) -> str | None:
		if not value:
			return None
		return cls._safe_str(value).rstrip("/") if cls._safe_str(value) else None

	def _find_company_by_linkedin_url(self, linkedin_url: str | None) -> LinkedinCompany | None:
		normalized_url = self._normalize_linkedin_url(linkedin_url)
		if not normalized_url:
			return None

		return (
			self.session.query(LinkedinCompany)
			.filter(
				or_(
					LinkedinCompany.linkedin_url == normalized_url,
					LinkedinCompany.linkedin_url == f"{normalized_url}/",
				)
			)
			.first()
		)

	def _get_or_create_company(
		self,
		current_company_url: str | None,
		company_name: str | None = None,
	) -> LinkedinCompany | None:
		normalized_url = self._normalize_linkedin_url(current_company_url)
		if not normalized_url:
			return None

		company = self._find_company_by_linkedin_url(normalized_url)
		if company is not None:
			return company

		company_info = getCompanyInfor(normalized_url)
		company = LinkedinCompany(
			name=company_name or company_info.get("name") or normalized_url,
			linkedin_url=normalized_url,
			website=company_info.get("website"),
			description=company_info.get("description"),
			industry=company_info.get("industry"),
			organization_type=company_info.get("organization_type"),
			headquarters=company_info.get("headquarters"),
			followers=company_info.get("followers"),
			size=company_info.get("size"),
			avatar_url=company_info.get("avatar_url"),
		)
		self.session.add(company)
		self.session.flush()
		return company


	@classmethod
	def _parse_year_month(cls, payload: Any) -> tuple[int | None, str | None]:
		if not isinstance(payload, dict):
			return None, None

		year = payload.get("year")
		try:
			year_value = int(year) if year is not None else None
		except (TypeError, ValueError):
			year_value = None

		month_value = cls._safe_str(payload.get("month"))
		return year_value, month_value

	def _find_person_by_linkedin_url(self, linkedin_url: str) -> LinkedinPersonalEmail | None:
		variants = (linkedin_url, linkedin_url.rstrip("/"))
		if not variants:
			return None

		return (
			self.session.query(LinkedinPersonalEmail)
			.filter(LinkedinPersonalEmail.linkedin_url.in_(variants))
			.first()
		)

	def _replace_personal_experience(
		self,
		personal_id: str,
		experiences: list[dict[str, Any]],
		source_profile_url: str | None,
	) -> None:
		self.session.query(PersonalExperience).filter(
			PersonalExperience.personal_id == personal_id
		).delete(synchronize_session=False)

		for experience in experiences:
			if not isinstance(experience, dict):
				continue

			start_year, start_month = self._parse_year_month(experience.get("start_date"))
			end_year, end_month = self._parse_year_month(experience.get("end_date"))

			self.session.add(
				PersonalExperience(
					linkedin_company_id=self._safe_str(experience.get("company_id")),
					linkedin_company_url=self._safe_str(experience.get("company_linkedin_url")),
					linkedin_company_logo=self._safe_str(experience.get("company_logo_url")),
					title=self._safe_str(experience.get("title")),
					company_name=self._safe_str(experience.get("company")),
					time_period=self._safe_str(experience.get("duration")),
					location=self._safe_str(experience.get("location")),
					employment_type=self._safe_str(experience.get("employment_type")),
					workplace_type=self._safe_str(experience.get("location_type")),
					duration=self._safe_str(experience.get("duration")),
					description=self._safe_str(experience.get("description")),
					start_date_text=None,
					start_month=start_month,
					start_year=start_year,
					end_date_text=None,
					end_month=end_month,
					end_year=end_year,
					is_current=bool(experience.get("is_current")),
					company_universal_name=None,
					experience_group_id=None,
					source_profile_url=source_profile_url,
					raw_data=experience,
					personal_id=personal_id,
				)
			)

	def _update_guest_email(self, linkedin_url: str | None, email: str | None) -> None:
		if not linkedin_url or not email:
			return
		linkedin_username = linkedin_url.split(".com")[-1]
		variants = [linkedin_username]
		if not variants:
			return None

		self.session.query(GuestList).filter(
			or_(
				GuestList.linkedin_url.in_(variants)
			)
		).update({"email": email}, synchronize_session=False)

	def upsert_people_and_update_guest_email(self, data: list[dict[str, Any]]) -> int:
		processed_count = 0

		try:
			for item in data:
				if not isinstance(item, dict):
					continue

				basic_info = item.get("basic_info") if isinstance(item.get("basic_info"), dict) else {}
				profile_url = self._normalize_linkedin_url(
					basic_info.get("profile_url") or item.get("profileUrl")
				)
				current_company_url = self._normalize_linkedin_url(
					basic_info.get("current_company_url")
				)
				current_company_name = self._safe_str(
					basic_info.get("current_company")
				)

				if not profile_url:
					continue

				email = self._safe_str(basic_info.get("email")) or ""
				person = self._find_person_by_linkedin_url(profile_url)
				company = self._get_or_create_company(current_company_url, current_company_name)
				print('------------->>>>   COMPANY ID :', company.id if company else None)
				if person is None:
					person = LinkedinPersonalEmail(
						email=email,
						first_name=self._safe_str(basic_info.get("first_name")),
						last_name=self._safe_str(basic_info.get("last_name")),
						linkedin_url=profile_url.rstrip("/") if profile_url else None,
						avatar_linkedin_url=self._safe_str(basic_info.get("profile_picture_url")),
						role=self._safe_str(basic_info.get("headline")),
						about=self._safe_str(basic_info.get("about")),
						education=item.get("education") if isinstance(item.get("education"), list) else [],
						urn=self._safe_str(basic_info.get("urn")),
						company=company,
					)
					self.session.add(person)
					self.session.flush()
				elif company is not None and person.company_id != company.id:
					person.company = company

				experiences = item.get("experience") if isinstance(item.get("experience"), list) else []
				self._replace_personal_experience(
					personal_id=person.id,
					experiences=experiences,
					source_profile_url=profile_url.rstrip("/") if profile_url else None,
				)

				self._update_guest_email(linkedin_url=profile_url, email=email)
				processed_count += 1

			self.session.commit()
			return processed_count
		except Exception:
			self.session.rollback()
			raise
		finally:
			self.session.close()
