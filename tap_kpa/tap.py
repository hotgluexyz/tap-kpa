"""Kpa tap class."""

from typing import List

import requests
from singer_sdk import Stream, Tap
from singer_sdk import typing as th

from tap_kpa.streams import FormsResponseDateStream, FormsResponseListStream

STREAM_TYPES = []


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

    def discover_streams(self) -> List[Stream]:
        """Return a list of discovered streams."""
        # create a stream per form
        forms_url = "https://api.kpaehs.com/v1/forms.list"
        data = {"token": self.config.get("access_token")}
        forms = requests.post(forms_url, json=data)
        if forms.status_code == 200 and forms.json().get("ok"):
            forms = forms.json().get("forms", [])
            for form in forms:
                form_id = form.get("id")
                name = form.get("name").replace(" ", "_")
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
                name = f"{name}_responses_data"
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


if __name__ == "__main__":
    TapKpa.cli()
