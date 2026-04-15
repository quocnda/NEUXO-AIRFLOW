from __future__ import annotations

import uuid
from datetime import datetime
from sqlalchemy import (
    Column,
    String,
    Text,
    Integer,
    Float,
    Boolean,
    DateTime,
    Date,
    ForeignKey,
    Index,
    create_engine,
)

from sqlalchemy.dialects.mysql import JSON
from sqlalchemy.orm import declarative_base, relationship, sessionmaker
from sqlalchemy.sql import func

Base = declarative_base()


class TimestampMixin:
    """Standard created_at / updated_at pattern (DB-side timestamps)."""

    created_at = Column(DateTime, default=func.now(), nullable=True)
    updated_at = Column(DateTime, default=func.now(), onupdate=func.now(), nullable=True)



def uuid_str() -> str:
    """Return UUID as string for CHAR(36) PK."""
    return uuid.uuid4().hex 

class Users(Base):
    __tablename__ = "Users"  # TODO: change to real user table name

    id = Column(Integer, primary_key=True, autoincrement=True)  # TODO: change if needed

# =========================
# DEFAULT tables
# =========================

class LinkedinCategory(Base):
    __tablename__ = "DEFAULT_Linkedin_Category"

    id = Column(String(36), primary_key=True, default=uuid_str)
    name = Column(String(100), nullable=False)
    description = Column(Text, nullable=True)


class LinkedinJobLabels(Base):
    __tablename__ = "DEFAULT_Linkedin_JobLabels"

    id = Column(String(36), primary_key=True, default=uuid_str)
    name = Column(String(100), nullable=False)
    description = Column(Text, nullable=True)

    category_id = Column(String(36), ForeignKey("DEFAULT_Linkedin_Category.id"), nullable=True)
    category = relationship("LinkedinCategory", lazy="joined")

    is_pick_gen_ai = Column(Boolean, default=True, nullable=False)


class LinkedinLocation(Base):
    __tablename__ = "DEFAULT_Linkedin_Location"

    id = Column(String(36), primary_key=True, default=uuid_str)
    name = Column(String(100), nullable=False)


class LinkedinExcludeKey(Base):
    __tablename__ = "DEFAULT_Linkedin_ExcludeKey"

    id = Column(String(36), primary_key=True, default=uuid_str)
    key = Column(String(100), nullable=False)


class LinkedinExcludeCompany(Base):
    __tablename__ = "DEFAULT_Linkedin_ExcludeCompany"

    id = Column(String(36), primary_key=True, default=uuid_str)
    company = Column(String(100), nullable=False)
    
class LinkedinCompany(Base, TimestampMixin):
    __tablename__ = "Linkedin_Companies"

    id = Column(String(36), primary_key=True, default=uuid_str)

    name = Column(String(100), nullable=False)
    avatar_url = Column(String(1000), nullable=True)
    linkedin_url = Column(String(500), nullable=True)
    linkedin_uid = Column(String(100), nullable=True)
    website = Column(String(1000), nullable=True)
    size = Column(String(50), nullable=True)
    link_twitter = Column(String(100), nullable=True)

    linkedin_funding_amt = Column(String(100), nullable=True)
    linkedin_lasted_funding_date = Column(DateTime, nullable=True)

    description = Column(Text, nullable=True)
    industry = Column(String(256), nullable=True)
    organization_type = Column(String(500), nullable=True)
    headquarters = Column(String(500), nullable=True)

    followers = Column(Integer, nullable=True)  # Django PositiveIntegerField -> Integer
    country = Column(String(100), nullable=True)
    short_description = Column(Text, nullable=True)

    labels = Column(JSON, default=list, nullable=True)
    category = Column(String(100), nullable=True)

    note = Column(Text, nullable=True)
    note_of_user = Column(String(500), nullable=True)
    is_finding_company = Column(String(200), nullable=True)
    is_crawl = Column(String(50), nullable=True)

    is_blacklist = Column(Boolean, default=False, nullable=True)

    organization_revune_apollo = Column(String(255), default="", nullable=True)

    lst_email_contact = Column(JSON, default=list, nullable=True)
    user_reach_out = Column(String(255), nullable=True)
    


# Indexes equivalent to Django Meta.indexes
Index("idx_company_name", LinkedinCompany.name)
Index("idx_company_industry", LinkedinCompany.industry)
Index("idx_company_country", LinkedinCompany.country)
Index("idx_company_category", LinkedinCompany.category)
Index("idx_company_is_blacklist", LinkedinCompany.is_blacklist)
Index("idx_company_created_at", LinkedinCompany.created_at)
Index("idx_company_updated_at", LinkedinCompany.updated_at)

class LinkedinJob(Base, TimestampMixin):
    __tablename__ = "Linkedin_Jobs"

    id = Column(String(36), primary_key=True, default=uuid_str)
    title = Column(String(1000), nullable=False)

    category_id = Column(String(36), ForeignKey("DEFAULT_Linkedin_Category.id"), nullable=True)
    category = relationship("LinkedinCategory")

    location_id = Column(String(36), ForeignKey("DEFAULT_Linkedin_Location.id"), nullable=True)
    location = relationship("LinkedinLocation")

    company_id = Column(String(36), ForeignKey("Linkedin_Companies.id"), nullable=True)
    company = relationship("LinkedinCompany")

    linkedin_url = Column(String(200), nullable=True)
    description = Column(Text, nullable=True)

    label_id = Column(String(36), ForeignKey("DEFAULT_Linkedin_JobLabels.id"), nullable=True)
    label = relationship("LinkedinJobLabels")

    short_description = Column(Text, nullable=True)
    note = Column(Text, nullable=True)

    status = Column(Text, default="active", nullable=True)
    last_check = Column(DateTime, default=func.now(), nullable=True)


Index("job_created_at_idx", LinkedinJob.created_at)
Index("job_company_lastcheck_idx", LinkedinJob.company_id, LinkedinJob.last_check)
Index("job_linkedin_url_idx", LinkedinJob.linkedin_url)



# =========================
# NOTIFICATIONS
# =========================
class Notification(Base, TimestampMixin):
    __tablename__ = "Notification"

    id = Column(String(36), primary_key=True, default=uuid_str)

    # choices in app layer:
    # SUB_DOMAIN / LINKEDIN / TWITTER / EVENT / HIRING / NEWS / FUNDING / JOB_CHANGE
    type = Column(Text, nullable=False)

    reference_id = Column(Text, nullable=False)
    title = Column(Text, nullable=True)
    post_url = Column(Text, nullable=True)
    time_post = Column(DateTime, default=func.now(), nullable=True)

    company_id = Column(String(36), ForeignKey("Linkedin_Companies.id"), nullable=True)
    company = relationship("LinkedinCompany")

    guest_id = Column(Text, nullable=True)
    is_send = Column(Boolean, default=False, nullable=False)
    
# -------------------- EVENTS ----------------------------------------------

class MainEvents(Base, TimestampMixin):
    __tablename__ = "Events_MainEvents"
    id = Column(String(36), primary_key=True, default=uuid_str)
    api_id = Column(String(100), nullable=True)
    name = Column(String(500), nullable=True)
    description = Column(Text, nullable=True)
    event_url = Column(String(1000), nullable=True)
    cover_image_url = Column(String(1000), nullable=True)
    avatar_image_url = Column(String(1000), nullable=True)
    
    geo_city = Column(String(100), nullable=True)
    geo_country = Column(String(100), nullable=True)
    geo_region = Column(String(100), nullable=True)
    
    instagram_url = Column(String(1000), nullable=True)
    linkedin_url = Column(String(1000), nullable=True)
    twitter_url = Column(String(1000), nullable=True)
    website = Column(String(1000), nullable=True)
    youtube_url = Column(String(1000), nullable=True)
    verified_at = Column(DateTime, nullable=True)
    
    
class EventsList(Base, TimestampMixin):
    __tablename__ = "Events_List"
    
    id = Column(String(36), primary_key=True, default=uuid_str)
    name = Column(String(500), nullable=True)
    
    event_url = Column(String(200), nullable=True)
    start_date = Column(DateTime, nullable=True)
    ticket_key = Column(String(100), nullable=True)
    api_id = Column(String(100), nullable=True)
    
    location = Column(String(1000), nullable=True)
    country = Column(String(100), nullable=True)
    event_parent = Column(String(100), nullable=True)
    event_parent_path = Column(String(200), nullable=True)
    
    note = Column(Text, nullable=True)
    number_of_company = Column(Integer, nullable=True)
    number_of_guest = Column(Integer, nullable=True)
    account = Column(String(100), nullable=True)
    
    main_event_id = Column(String(36), ForeignKey("Events_MainEvents.id"), nullable=True)
    main_event = relationship("MainEvents", lazy="joined")
    
    event_image = Column(Text, nullable=True)
    approval_status = Column(String(100), nullable=True)
    guest_count = Column(Integer, nullable=True)
    start_at = Column(DateTime, nullable=True)
    end_at = Column(DateTime, nullable=True)


class GuestList(Base, TimestampMixin):
    __tablename__ = "Events_GuestList"
    
    id = Column(String(36), primary_key=True, default=uuid_str)
    name = Column(String(500), nullable=False)
    linkedin_url = Column(String(200), nullable=True)
    twitter_url = Column(String(200), nullable=True)
    website = Column(String(200), nullable=True)
    
    company_id = Column(String(36), ForeignKey("Linkedin_Companies.id"), nullable=True)
    company = relationship("LinkedinCompany", lazy="joined")
    
    event_id = Column(String(36), ForeignKey("Events_List.id"), nullable=True)
    event = relationship("EventsList", lazy="joined")
    
    email = Column(String(100), nullable=False)
    role = Column(String(300), nullable=True)
    check_company = Column(Boolean, default=False, nullable=True)
    
    email_status_emailinfor = Column(String(100), nullable=True)
    send_by_emailinfor = Column(String(100), nullable=True)
    last_activity_emailinfor = Column(DateTime, nullable=True)
    error_emailinfor = Column(Text, nullable=True)
    last_reply_emailinfor = Column(DateTime, nullable=True)
    note = Column(Text, nullable=True)
    email_input_from_user = Column(Boolean, default=False, nullable=True)
    
    time_zone = Column(Text, nullable=True)


Index("guestlist_created_at_idx", GuestList.created_at)
Index("guestlist_company_idx", GuestList.company_id)
Index("guestlist_email_status_idx", GuestList.email_status_emailinfor)
    
    
#-------------Funding------------------------------

class CompanyFunding(Base, TimestampMixin):
    __tablename__ = "Funding"

    id = Column(String(36), primary_key=True, default=uuid_str)
    name = Column(String(500), nullable=True)
    round = Column(String(100), nullable=True)
    date = Column(Date, nullable=True)
    amount = Column(String(100), nullable=True)
    category = Column(String(100), nullable=True)
    
    website = Column(String(1000), nullable=True)
    project_url = Column(String(1000), nullable=True)
    linkedin_url = Column(String(1000), nullable=True)
    linkedin_uid = Column(String(100), nullable=True)
    
    company_id = Column(String(36), ForeignKey("Linkedin_Companies.id"), nullable=True)
    logo_url = Column(String(1000), nullable=True)
    
    news_url = Column(String(1000), nullable=True)
    
    
#------------------Clutch review company-----------------------------

class ClutchReview(Base, TimestampMixin):
    __tablename__ = "Clutch_Review"

    id = Column(String(36), primary_key=True, default=uuid_str)
    
    company_id = Column(String(36), ForeignKey("Linkedin_Companies.id"), nullable=True)
    company = relationship("LinkedinCompany", lazy="joined")
    
    reviewer_name = Column(String(500), nullable=True)
    reviewer_role = Column(String(500), nullable=True)
    reviewer_company = Column(String(500), nullable=True)
    industry = Column(String(500), nullable=True)
    location = Column(String(500), nullable=True)
    client_size = Column(String(500), nullable=True)
    services = Column(String(500), nullable=True)
    project_size = Column(String(500), nullable=True)
    project_length = Column(String(500), nullable=True)
    project_description = Column(Text, nullable=True)
    background = Column(Text, nullable=True)
    website_url = Column(String(1000), nullable=True)
    linkedin_url = Column(String(500), nullable=True)
    description_company_outsource = Column(Text, nullable=True)
    services_company_outsource = Column(String(500), nullable=True)
    
    
#-------------Persons-----------------------------
class LinkedinPersonalEmail(Base, TimestampMixin):
    __tablename__ = "Linkedin_Peoples"

    id = Column(String(36), primary_key=True, default=uuid_str)
    email = Column(String(100), nullable=False)
    first_name = Column(String(100), nullable=True)
    last_name = Column(String(100), nullable=True)
    linkedin_url = Column(String(200), nullable=True)
    twitter_url = Column(String(200), nullable=True)
    avatar_linkedin_url = Column(Text, nullable=True)
    role = Column(String(200), nullable=True)
    
    company_id = Column(String(36), ForeignKey("Linkedin_Companies.id"), nullable=True)
    company = relationship("LinkedinCompany", lazy="joined")
    
    twitter_summary = Column(Text, nullable=True)
    about = Column(Text, nullable=True)
    education = Column(JSON, default=list, nullable=True)
    note = Column(Text, nullable=True)
    is_update = Column(Integer, default=0, nullable=True)
    urn = Column(Text, nullable=True)


class PersonalExperience(Base, TimestampMixin):
    __tablename__ = "Linkedin_Personal_Experience"

    id = Column(String(36), primary_key=True, default=uuid_str)

    linkedin_company_id = Column(String(50), nullable=True)
    linkedin_company_url = Column(Text, nullable=True)
    linkedin_company_logo = Column(Text, nullable=True)
    title = Column(Text, nullable=True)
    company_name = Column(Text, nullable=True)
    time_period = Column(Text, nullable=True)

    location = Column(Text, nullable=True)
    employment_type = Column(String(100), nullable=True)
    workplace_type = Column(String(100), nullable=True)
    duration = Column(String(100), nullable=True)
    description = Column(Text, nullable=True)

    start_date_text = Column(String(100), nullable=True)
    start_month = Column(String(20), nullable=True)
    start_year = Column(Integer, nullable=True)

    end_date_text = Column(String(100), nullable=True)
    end_month = Column(String(20), nullable=True)
    end_year = Column(Integer, nullable=True)

    is_current = Column(Boolean, default=False)

    company_universal_name = Column(String(255), nullable=True)
    experience_group_id = Column(String(255), nullable=True)
    source_profile_url = Column(String(500), nullable=True)

    raw_data = Column(JSON, nullable=True, default=dict)

    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)

    personal_id = Column(String(36), ForeignKey("Linkedin_Peoples.id"), nullable=False)
    personal = relationship("LinkedinPersonalEmail", lazy="joined")


class PersonalEmail(Base, TimestampMixin):
    __tablename__ = "Personal_Email"

    id = Column(String(36), primary_key=True, default=uuid_str)
    email = Column(String(100), nullable=False)
    
    personal_id = Column(String(36), ForeignKey("Linkedin_Peoples.id"), nullable=False)
    personal = relationship("LinkedinPersonalEmail", lazy="joined")

#------------------Company News -----------------------------
class NewsInformation(Base, TimestampMixin):
    __tablename__ = "News"
    id = Column(String(36), primary_key=True, default=uuid_str)
    name_company = Column(String(500), nullable=True)
    company_id = Column(String(36), ForeignKey("Linkedin_Companies.id"), nullable=True)
    company = relationship("LinkedinCompany", lazy="joined")
    link_news = Column(String(1000), nullable=True)
    content = Column(Text, nullable=True)
    category = Column(String(100), nullable=True)
    time_post = Column(DateTime, default=func.now(), nullable=True)
    title = Column(String(1000), nullable=True)
    
    
class ApifyToken(Base, TimestampMixin):
    __tablename__ = "Apify_Token"

    id = Column(String(36), primary_key=True, default=uuid_str)
    token = Column(Text, nullable=True)
    next_time_available = Column(DateTime, nullable=True)
    gmail = Column(String(255), nullable=True)
    pass_gmail = Column(String(255), nullable=True)
    
    status = Column(String(100), nullable=True)  # e.g., "available", "in_use", "cooldown"
    
    
class Mentions(Base, TimestampMixin):
    __tablename__ = "Mentions"

    id = Column(String(36), primary_key=True, default=uuid_str)
    company_id = Column(String(36), ForeignKey("Linkedin_Companies.id"), nullable=True)
    company = relationship("LinkedinCompany", lazy="joined")
    type = Column(String(100), nullable=False)  # e.g., "SUB_DOMAIN", "LINKEDIN", etc.
    guest_id = Column(String(36), nullable=True)  # For mentions related to guests (e.g., events)
    note = Column(Text, nullable=True)
    
class MentionsSubDomain(Base, TimestampMixin):
    __tablename__ = "Mentions_SubDomain"

    id = Column(String(36), primary_key=True, default=uuid_str)
    sub_domain = Column(Text, nullable=True)
    ip = Column(String(100), nullable=True)
    mentions_id = Column(String(36), ForeignKey("Mentions.id"), nullable=False)
    mentions = relationship("Mentions", lazy="joined")
    