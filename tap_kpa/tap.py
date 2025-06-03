"""Kpa tap class."""

from typing import Generator, List
from time import sleep

import traceback
import re
import requests
import backoff
from singer_sdk import Stream, Tap
from singer_sdk import typing as th
from singer_sdk.exceptions import FatalAPIError, RetriableAPIError

from tap_kpa.streams import (
    FormsResponseDateStream,
    FormsResponseListStream,
    RolesListStream,
    UsersListStream, LinesOfBusinessListStream
)

STREAM_TYPES = [RolesListStream, UsersListStream, LinesOfBusinessListStream]


class TapKpa(Tap):
    """Kpa tap class."""

    name = "tap-kpa"

    config_jsonschema = th.PropertiesList(
        th.Property(
            "access_token",
            th.StringType,
            required=True,
            description="The token to authenticate against the API service",
        ),
        th.Property(
            "start_date",
            th.DateTimeType,
            description="The earliest record date to sync",
        ),
    ).to_dict()

    @backoff.on_exception(
        backoff.expo,
        (RetriableAPIError),
        max_tries=5,
        factor=2,
    )
    def _make_request(self, url: str, data: dict) -> requests.Response:
        """Make API request with backoff strategy."""
        
        response = requests.post(url, json=data)
        error = f"Error status code: {response.status_code}, response: {response.text}, response url: {response.url}"
        
        if (
            response.status_code == 200
            and response.json().get("error") == "rate_limit_exceeded"
        ):
            self.logger.info("Rate limit exceeded, sleeping for 120 seconds...")
            sleep(120)
            raise RetriableAPIError(error, response)
        if response.status_code in [429] or 500 <= response.status_code < 600:
            raise RetriableAPIError(error, response)
        elif 400 <= response.status_code < 500 or (
            response.status_code == 200 and response.json().get("ok") == False
        ):
            raise FatalAPIError(error)
        
        return response

    def discover_forms_streams(self) -> Generator[Stream, Stream, Exception]:
        """Return a list of discovered streams."""
        # create a stream per form
        forms_url = "https://api.kpaehs.com/v1/forms.list"
        data = {"token": self.config.get("access_token")}
        forms = self._make_request(forms_url, data)
        if forms.status_code == 200 and forms.json().get("ok"):
            forms = forms.json().get("forms", [])
            for form in forms:
                form_id = form.get("id")
                # Clean up form name, no spaces or non alphanumeric chars
                name = form.get("name").replace(" ", "_")
                pattern = re.compile("[^\w]+")
                name = pattern.sub("", name)

                # create parent stream
                parent_stream_name = f"{name}_responses_list"
                parent_stream = type(
                    parent_stream_name,
                    (FormsResponseListStream,),
                    {
                        "name": parent_stream_name,
                        "form_id": form_id,
                    },
                )
                yield parent_stream(tap=self)

                # create forms stream
                yield type(
                    name,
                    (FormsResponseDateStream,),
                    {
                        "name": name,
                        "form_id": form_id,
                        "parent_stream_type": parent_stream,
                    },
                )(tap=self)
        else:
            raise Exception(
                f"Request to get forms has failed with status code {forms.status_code} and response {forms.text}"
            )

    def discover_streams(self) -> List[Stream]:
        return [stream_class(tap=self) for stream_class in STREAM_TYPES] + [
            form for form in self.discover_forms_streams()
        ]

    @property
    def catalog(self):
        """Get the tap's working catalog.

        Returns:
            A Singer catalog object.
        """
        if self._catalog is None:
            self._catalog = self._singer_catalog

        return self._catalog

    @property
    def catalog_dict(self) -> dict:
        """Get catalog dictionary.
        Returns:
            The tap's catalog as a dict
        """
        catalog = super().catalog_dict
        trace_stack = traceback.format_stack()
        is_discover = "run_discovery" in str(trace_stack)
        if is_discover:
            # filter out all the parent streams, we want to hide from end user
            catalog = {
                "streams": [
                    x
                    for x in catalog["streams"]
                    if not x["stream"].endswith("_responses_list")
                ]
            }

        return catalog


if __name__ == "__main__":
    TapKpa.cli()
