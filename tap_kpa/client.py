"""REST client handling, including KpaStream base class."""

from time import sleep
from typing import Any, Dict, Optional

import requests
from pendulum import parse
from singer_sdk import typing as th
from singer_sdk.exceptions import FatalAPIError, RetriableAPIError
from singer_sdk.streams import RESTStream


class KpaStream(RESTStream):
    """Kpa stream class."""

    url_base = "https://api.kpaehs.com/v1"
    records_jsonpath = "$[*]"
    next_page_token_jsonpath = "$.next_page"
    replication_field = None
    pagination = True

    @property
    def http_headers(self) -> dict:
        """Return the http headers needed."""
        headers = {}
        headers["Content-Type"] = "application/json"
        if "user_agent" in self.config:
            headers["User-Agent"] = self.config.get("user_agent")
        return headers

    def validate_response(self, response: requests.Response) -> None:
        error = f"Error status code: {response.status_code}, response: {response.text}, response url: {response.url}"
        if (
            response.status_code == 200
            and response.json().get("error") == "rate_limit_exceeded"
        ):
            self.logger.info("Rate limit exceeded, sleeping for 120 seconds...")
            sleep(120)
            raise RetriableAPIError(error, response)

        if (
            response.status_code in self.extra_retry_statuses
            or 500 <= response.status_code < 600
        ):
            raise RetriableAPIError(error, response)
        elif 400 <= response.status_code < 500 or (
            response.status_code == 200 and response.json().get("ok") == False
        ):
            raise FatalAPIError(error)

    def get_next_page_token(
        self, response: requests.Response, previous_token: Optional[Any]
    ) -> Optional[Any]:
        """Return a token for identifying next page or None if no more pages."""
        if self.pagination:
            previous_token = previous_token or 1
            next_page_token = previous_token + 1
            if response.json().get("paging", {}).get("last_page", 0) >= next_page_token:
                return next_page_token

    def get_starting_time(self, context):
        start_date = self.config.get("start_date")
        if start_date:
            start_date = parse(self.config.get("start_date"))
        rep_key = self.get_starting_timestamp(context)
        return rep_key or start_date

    def get_url_params(
        self, context: Optional[dict], next_page_token: Optional[Any]
    ) -> Dict[str, Any]:
        """Return a dictionary of values to be used in URL parameterization."""
        params: dict = {}
        if next_page_token:
            params["page"] = next_page_token

        start_date = self.get_starting_time(context)
        if self.replication_key and self.replication_field and start_date:
            params[self.replication_field] = start_date.timestamp()
        return params

    def prepare_request_payload(
        self, context: Optional[dict], next_page_token: Optional[Any]
    ) -> Optional[dict]:
        payload = {"token": self.config.get("access_token")}
        return payload

    def get_jsonschema_type(self, field):
        settings = field.get("settings", {})
        if settings.get("inputtype") == "checkbox":
            return th.BooleanType
        if settings.get("style") == "list" and settings.get("multiple"):
            return th.ArrayType(th.StringType)
        if field.get("type") == "datetime":
            return th.DateTimeType
        if field.get("type") == "counter":
            return th.IntegerType
        if field.get("type") in ["sketch", "attachments"]:
            return th.ArrayType(th.CustomType({"type": ["object", "string"]}))
        return th.StringType

    def get_schema(self, fields) -> dict:
        properties = []
        for field in fields:
            properties.append(
                th.Property(field.get("title"), self.get_jsonschema_type(field))
            )
        # Return the list as a JSON Schema dictionary object
        property_list = th.PropertiesList(*properties).to_dict()
        return property_list
