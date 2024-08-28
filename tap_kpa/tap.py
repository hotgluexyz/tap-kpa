"""Kpa tap class."""

from typing import Generator, List

import traceback
import re
import requests
from singer_sdk import Stream, Tap
from singer_sdk import typing as th

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

    def discover_forms_streams(self) -> Generator[Stream, Stream, Exception]:
        """Return a list of discovered streams."""
        # create a stream per form
        forms_url = "https://api.kpaehs.com/v1/forms.list"
        data = {"token": self.config.get("access_token")}
        forms = requests.post(forms_url, json=data)
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
