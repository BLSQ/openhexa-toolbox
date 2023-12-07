"""KoboToolbox API."""

from typing import List

import requests
import requests_cache


class Field:
    def __init__(self, meta: dict):
        """Initialize a Field object from json metadata."""
        self.meta = meta

    @property
    def uid(self) -> str:
        """Get field UID."""
        return self.meta.get("$kuid")

    @property
    def name(self) -> str:
        """Get field name."""
        return self.meta.get("name", "")

    @property
    def xpath(self) -> str:
        """Get field xpath."""
        return self.meta.get("$xpath")

    @property
    def label(self) -> str:
        """Get field full label."""
        if "label" in self.meta:
            return self.meta["label"][0]
        else:
            return None

    @property
    def type(self) -> str:
        """Get field data type."""
        return self.meta.get("type")

    @property
    def list_name(self) -> str:
        """Get the list of available choices for the field."""
        return self.meta.get("select_from_list_name")

    @property
    def condition(self) -> str:
        """Get the condition for which the question must be answered."""
        if self.meta.get("relevant"):
            return self.parse_condition(self.meta.get("relevant"))
        else:
            return None

    @staticmethod
    def parse_condition(expression: str) -> dict:
        """Transform the conditionnal expression string into a string that can be
        evaluated by Python.

        Assumes that the variable name that contain record data is named `record`.
        """
        expression = expression.replace("selected(${", "(record.get('")
        expression = expression.replace("${", "record.get('")
        expression = expression.replace("}", "')")
        expression = expression.replace(", ", " == ")
        return expression


class Survey:
    def __init__(self, meta: dict):
        """Initialize a Survey object from json metadata."""
        self.meta = meta
        if "content" in meta:
            self.fields = self.parse_fields()
            self.choices = self.parse_choices()

    def __repr__(self) -> str:
        return f'Survey("{self.name}")'

    @property
    def uid(self) -> str:
        return self.meta["uid"]

    @property
    def name(self) -> str:
        return self.meta.get("name")

    @property
    def description(self) -> str:
        return self.meta["settings"].get("description")

    @property
    def country(self) -> str:
        return self.meta["settings"].get("country")

    def parse_fields(self) -> List[Field]:
        """Transform fields json metadata into Field objects."""
        return [Field(f) for f in self.meta["content"]["survey"]]

    def parse_choices(self) -> dict:
        """Get all choice lists."""
        choice_lists = {}
        if "choices" not in self.meta["content"]:
            return None
        for choice in self.meta["content"]["choices"]:
            if "list_name" in choice:
                list_name = choice["list_name"]
                if list_name not in choice_lists:
                    choice_lists[list_name] = []
                choice_lists[list_name].append(choice)
        return choice_lists

    def get_field_from_xpath(self, xpath: str) -> Field:
        """Get field object from its xpath."""
        for field in self.fields:
            if field.xpath == xpath:
                return field
        raise ValueError("Field not found")

    def get_label_from_xpath(self, xpath: str) -> str:
        """Get field label from its xpath."""
        for field in self.fields:
            xpath = field.meta.get("$xpath")
            if xpath == xpath:
                label = field.meta.get("label")
                if label:
                    return label[0]
                else:
                    raise ValueError("No label found for xpath")
        raise ValueError("Field not found")

    def get_field(self, name: str) -> Field:
        """Get survey field object based on its name."""
        return [f for f in self.fields if f.name.lower() == name.lower()][0]

    @property
    def labels(self) -> dict:
        """Get a mapping of fields xpaths and labels."""
        mapping = {}
        for field in self.fields:
            xpath = field.meta.get("$xpath")
            label = field.meta.get("label")
            if xpath and label:
                mapping[xpath] = " ".join(label[0].split())
        return mapping

    @property
    def value_from_choice(self, choice: str):
        """Get choice value from choice id."""
        choices = self.choices.get(choice)
        if not choices:
            raise ValueError("Choice list name not found")
        for c in choices:
            if c.get("name") == choice:
                label = c.get("label")
                if label:
                    return label[0]


class AuthenticationError(Exception):
    pass


class Api:
    def __init__(self, url: str, cache_dir: str = None):
        self.url = url.rstrip("/")
        if cache_dir:
            self.session = requests_cache.CachedSession(cache_dir)
        else:
            self.session = requests.Session()

    def authenticate(self, token: str):
        self.session.headers["Authorization"] = f"Token {token}"

    def check_authentication(self):
        if "Authorization" not in self.session.headers:
            raise AuthenticationError("Not authenticated")

    def list_surveys(self) -> List[dict]:
        """List UID and names of available surveys."""
        surveys = []
        r = self.session.get(f"{self.url}/assets.json")
        r.raise_for_status()
        assets = r.json()["results"]
        for asset in assets:
            if asset.get("asset_type") == "survey":
                surveys.append({"uid": asset.get("uid"), "name": asset.get("name")})
        return surveys

    def get_survey(self, uid: str) -> Survey:
        """Get survey from its UID."""
        r = self.session.get(f"{self.url}/assets/{uid}.json")
        r.raise_for_status()
        return Survey(r.json())

    def get_survey_data(self, survey: Survey) -> dict:
        """Get survey data."""
        r = self.session.get(survey.meta["data"])
        r.raise_for_status()
        return r.json().get("results")
