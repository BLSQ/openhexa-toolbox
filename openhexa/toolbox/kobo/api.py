"""KoboToolbox API."""

from __future__ import annotations

from functools import cached_property
from typing import List

import requests


class Field:
    def __init__(self, meta: dict):
        """Initialize a Field object from json metadata."""
        self.meta = meta

    def __repr__(self) -> str:
        return f'Field("{self.name}")'

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


class Survey:
    def __init__(self, client: Api, meta: dict):
        """Initialize a Survey object from json metadata."""
        self.client = client
        self.meta = meta

    def __repr__(self) -> str:
        return f'Survey("{self.name}")'

    @property
    def uid(self) -> str:
        return self.meta["uid"]

    @property
    def name(self) -> str:
        return self.meta.get("name")

    @cached_property
    def fields(self) -> List[Field]:
        """All available fields in survey."""
        fields = []
        for meta in self.meta["content"]["survey"]:
            fields.append(Field(meta))
        return fields

    def get_field(self, uid: str) -> Field:
        """Get field object from its UID."""
        for meta in self.meta["content"]["survey"]:
            if meta.get("$kuid") == uid:
                return Field(meta)

    def get_field_from_name(self, name: str) -> Field:
        """Get field object from its name."""
        for meta in self.meta["content"]["survey"]:
            if meta.get("name") == name:
                return Field(meta)

    @cached_property
    def choices(self) -> dict:
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

    def get_data(self) -> List[dict]:
        """Download survey data."""
        r = self.client.session.get(self.meta["data"])
        r.raise_for_status()
        return r.json().get("results")

    @cached_property
    def _xpaths_labels_mapping(self) -> dict:
        """Get a mapping of fields xpaths and labels."""
        mapping = {}
        for field in self.fields:
            xpath = field.meta.get("$xpath")
            label = field.meta.get("label")
            if xpath and label:
                mapping[xpath] = " ".join(label[0].split())
        return mapping

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
    def __init__(self, url: str):
        self.url = url.rstrip("/")
        self.session = requests.Session()

    def authenticate(self, token: str):
        self.session.headers["Authorization"] = f"Token {token}"

    def check_authentication(self):
        if "Authorization" not in self.session.headers:
            raise AuthenticationError("Not authenticated")

    def _get_assets(self) -> List[dict]:
        r = self.session.get(f"{self.url}/assets.json")
        r.raise_for_status()
        return r.json()["results"]

    @cached_property
    def surveys(self) -> List[dict]:
        """List UID and names of available surveys."""
        surveys = []
        assets = self._get_assets()
        for asset in assets:
            if asset.get("asset_type") == "survey":
                surveys.append({"uid": asset.get("uid"), "name": asset.get("name")})
        return surveys

    def get_survey(self, uid: str) -> Survey:
        """Get survey from its UID."""
        r = self.session.get(f"{self.url}/assets/{uid}.json")
        r.raise_for_status()
        return Survey(client=self, meta=r.json())

    def get_survey_data(self, survey: Survey) -> dict:
        """Get survey data."""
        r = self.session.get(survey.meta["data"])
        r.raise_for_status()
        return r.json().get("results")
