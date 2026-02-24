import os
import random
import time
import threading
from queue import Queue, Empty
from datetime import datetime
from langchain_openai import ChatOpenAI
from langchain_core.output_parsers import StrOutputParser
from langchain_core.prompts import ChatPromptTemplate
from openai import OpenAI
import google.generativeai as genai
from contextlib import contextmanager

from hook.sqlalchemyHook import SQLAlchemyHook
from model.model import (
    EventsList,
    GuestList,
    LinkedinCategory,
    LinkedinCompany,
    LinkedinJobLabels,
    # TODO: Add these models to model.py if needed
    # CompanyFunding,
    # LinkedinPersonalEmail,
)



DEFAULT_LLM_TYPE = "OPENAI"

class AIManager:
    _instance = None
    _lock = threading.Lock()

    def __init__(self, llm_type=DEFAULT_LLM_TYPE):
        self.llm_type = llm_type.upper()
        key_env_map = {
            "GEMINI": "API_KEY_GEMINI",
            "OPENAI": "OPENAI_API_KEY",
        }

        env_key_name = key_env_map.get(self.llm_type)
        if not env_key_name:
            raise ValueError(f"[AIManager] Unsupported llm_type: {self.llm_type}")

        raw_keys = os.getenv(env_key_name)
        if not raw_keys:
            raise EnvironmentError(f"[AIManager] Missing environment variable: {env_key_name}")

        self.api_keys = raw_keys.split(",")
        self.model_pool = Queue(maxsize=5)  # Connection pool size
        self.initialize_pool()

    def initialize_pool(self):
        """Initialize the model pool with models using different API keys"""
        for _ in range(min(5, len(self.api_keys))):
            model = self._create_model()
            self.model_pool.put(model)

    def _create_model(self):
        """Create a new AI model instance with a random API key"""
        api_key = random.choice(self.api_keys)
        if self.llm_type == "GEMINI":
            genai.configure(api_key=api_key)
            return genai.GenerativeModel("gemini-2.0-flash")
        if self.llm_type == "OPENAI":
            return ChatOpenAI(model="gpt-4o-mini", temperature=0.3, api_key=api_key)

    @classmethod
    def get_instance(cls):
        """Get the singleton instance of AIManager"""
        if cls._instance is None:
            with cls._lock:
                if cls._instance is None:
                    cls._instance = cls()
        return cls._instance

    def get_model(self):
        """Get a model from the pool, wait if none available"""
        try:
            return self.model_pool.get(block=True, timeout=2)
        except Empty:
            # If pool is empty, create a new model
            return self._create_model()

    def release_model(self, model):
        """Return a model to the pool"""
        if self.model_pool.qsize() < 5:  # Keep pool at reasonable size
            self.model_pool.put(model)

    def execute_with_retry(self, func, *args, max_retries=3, **kwargs):
        """Execute a function with a model from the pool with retry logic"""
        retries = 0
        model = self.get_model()

        while retries < max_retries:
            try:
                result = func(model, *args, **kwargs)
                self.release_model(model)
                return result
            except Exception as e:
                retries += 1
                print(
                    f"Error executing API model call (attempt {retries}/{max_retries}): {e}"
                )
                if retries >= max_retries:
                    self.release_model(model)
                    raise
                # Create a new model for retry
                model = self._create_model()
                time.sleep(1)  # Backoff before retry
                
    def generate_content_basic(self, model, prompt):
        if self.llm_type == "GEMINI":
            response = model.generate_content(prompt)
            if not response or not hasattr(response, "text"):
                raise ValueError("Invalid response: No valid content returned.")
            return response.text.strip()
        elif self.llm_type == "OPENAI":
            prompt = ChatPromptTemplate.from_messages(
                [
                    (
                        "system",
                        prompt,
                    ),
                    ("human", ""),
                ]
            )
            chain = prompt | model | StrOutputParser()
            return chain.invoke({}).strip()

#########################################################################################################################
# BASE CLASS
#########################################################################################################################
class BaseAIUsage:
    """Base class for AI usage with common functionality."""

    def __init__(self,session = None, llm_type=DEFAULT_LLM_TYPE):
        self.session = session
        self.llm_type = llm_type.upper()
        self.ai_manager = AIManager.get_instance()

    def _invoke_with_openai(self, model, prompt_text):
        """Helper method to invoke OpenAI model with prompt."""
        prompt = ChatPromptTemplate.from_messages(
            [
                ("system", prompt_text),
                ("human", ""),
            ]
        )
        chain = prompt | model | StrOutputParser()
        return chain.invoke({}).strip()

    def _invoke_with_gemini(self, model, prompt_text):
        """Helper method to invoke Gemini model with prompt."""
        response = model.generate_content(prompt_text)
        if not response or not hasattr(response, "text"):
            raise ValueError("Invalid response: No valid content returned.")
        return response.text.strip()

    def _generate_content(self, model, prompt_text):
        """Generate content using the appropriate LLM."""
        if self.llm_type == "GEMINI":
            return self._invoke_with_gemini(model, prompt_text)
        elif self.llm_type == "OPENAI":
            return self._invoke_with_openai(model, prompt_text)
        raise ValueError(f"Unsupported LLM type: {self.llm_type}")

    def build_prompt(self, *args, **kwargs):
        """Build prompt - to be implemented by subclasses."""
        raise NotImplementedError("Subclasses must implement build_prompt()")

    def generate(self, *args, **kwargs):
        """Generate output - to be implemented by subclasses."""
        raise NotImplementedError("Subclasses must implement generate()")


#########################################################################################################################
# SUMMARY DESCRIPTION AI USAGE
#########################################################################################################################
class SummaryDescriptionAIUsage(BaseAIUsage):
    """AI usage for summarizing company/job descriptions."""

    def build_prompt(self, description, industry=None):
        industry = industry or "Unknown Industry"
        return f"""
        ### **Role:** 
        You are a business analyst assistant.

        ### **Company Description:**
        {description}

        ### **Industry:**
        {industry}

        ### **Your Task:**
        Summarize the company's main business activities in **one concise sentence of no more than 15 words**. Focus on the **core operations, products, or services** described.\
        The industry is provided for context but **should not override the primary description**."""

    def generate(self, description, industry=None):
        if not description or description == "Waiting":
            return "Not found description"

        prompt = self.build_prompt(description, industry)

        try:
            return self.ai_manager.execute_with_retry(
                self._generate_content, prompt
            )
        except Exception as e:
            print(f"        XXXXXXX Error shortening description: {e}")
            return None





#########################################################################################################################
# GEN LABEL AND CATEGORY AI USAGE
#########################################################################################################################
class GenLabelAndCategoryAIUsage(BaseAIUsage):
    """AI usage for generating labels and categories for companies."""

    def build_prompt(self, *args, **kwargs):
        """Not used directly - use build_prompt_category or build_prompt_label."""
        pass

    def build_prompt_category(self, description, categories):
        prompt = f"""Company Description: {description}

        Categories:
        """
        for i, category in enumerate(categories):
            prompt += f"{i+1}. {category}\n"

        prompt += """
        **Prompt:**

        Based on the provided company description, carefully analyze and identify the most suitable category from the given list.

        **If no matching category is found, return "General".**
        """
        return prompt

    def build_prompt_label(self, description, labels, industry):
        prompt = f"""Description: {description}

        Industry: {industry}

        Labels:
        """
        for i, label in enumerate(labels):
            prompt += f"{i+1}. {label}\n"

        prompt += """

        **Prompt:**

        Summarize the company description in one or two concise sentences that highlight the company's primary focus, nature, and activities.
        Based on the summarized description and industry (if any), identify the most suitable labels for the company from the list. Select only the top labels that best represent the company's primary focus, The returned result cannot exceed 5 labels and ensuring they are relevant and non-redundant.
        **If no suitable labels are found, respond with "General".**
        """
        return prompt

    def _generate_content_category(self, model, prompt, categories):
        """Generate category content with matching logic."""
        if self.llm_type == "GEMINI":
            response = model.generate_content(prompt)
            category_title = response.text.replace("**", "").strip().split("\n")[0]
        elif self.llm_type == "OPENAI":
            category_title = self._invoke_with_openai(model, prompt).replace("**", "").split("\n")[0]
        else:
            return "Other"

        categories_with_general = categories + ["General"]
        for category in categories_with_general:
            if category.lower().replace(" ", "") in category_title.lower().replace(" ", ""):
                return category
        return "Other"

    def _generate_content_label(self, model, prompt, label_list):
        """Generate label content with matching logic."""
        if self.llm_type == "GEMINI":
            response = model.generate_content(prompt)
            title = response.text.replace("**", "").replace("\n", " ").strip()
        elif self.llm_type == "OPENAI":
            title = self._invoke_with_openai(model, prompt).replace("**", "").replace("\n", " ")
        else:
            return ["General"]

        labels = [label for label in label_list if label.lower().replace(" ", "") in title.lower().replace(" ", "")]
        return labels if labels else ["General"]

    def generate_category(self, description):
        """Generate category for a company description."""
        categories = [
            row[0] for row in self.session.query(LinkedinCategory.name)
            .filter(LinkedinCategory.name.isnot(None))
            .all()
        ]
        prompt = self.build_prompt_category(description, categories)

        try:
            return self.ai_manager.execute_with_retry(
                self._generate_content_category, prompt, categories
            )
        except Exception as e:
            print(f"        XXXXXXX Error generating COMPANY category: {e}")
            return "General"

    def generate_label(self, description, label_list, industry):
        """Generate labels for a company description."""
        prompt = self.build_prompt_label(description, label_list, industry)

        try:
            return self.ai_manager.execute_with_retry(
                self._generate_content_label, prompt, label_list
            )
        except Exception as e:
            print(f"        XXXXXXX Error generating COMPANY label: {e}")
            return ["General"]

    def gen_label_and_category(self, company_description, industry):
        """Generate both label and category for a company."""
        category = self.generate_category(company_description)

        if category == "General":
            return ["General"], category
        if category is None:
            return [], None
        if category == "Other":
            return [], category

        labels = []
        list_labels = (
            self.session.query(LinkedinJobLabels)
            .join(LinkedinCategory, LinkedinJobLabels.category_id == LinkedinCategory.id)
            .filter(
                LinkedinCategory.name == category,                )
                .all()
            )
        for label in list_labels:
            if label is None:
                continue
            labels.append(label.name)

        matched_label = self.generate_label(company_description, labels, industry)
        return matched_label, category

    def generate(self, description, industry=None):
        """Main generate method for label and category."""
        if description is None or description == "Waiting":
            return [], None

        if industry is None:
            industry = "Unknown Industry"

        matched_label, category = self.gen_label_and_category(description, industry)
        if matched_label is None:
            matched_label = []
        if category is None:
            return ["General"], "General"
        if category == "Other":
            return [], "Other"

        return matched_label, category
