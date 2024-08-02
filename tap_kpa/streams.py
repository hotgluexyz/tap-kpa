"""Stream type classes for tap-kpa."""

import requests
from backports.cached_property import cached_property
from singer_sdk import typing as th

from tap_kpa.client import KpaStream
from datetime import datetime
import pytz

class FormsResponseListStream(KpaStream):
    """Define custom stream."""

    path = "/responses.list"
    replication_key = "updated"
    replication_field = "updated_after"
    records_jsonpath = "$.responses[*]"
    form_id = None
    rest_method = "POST"

    schema = th.PropertiesList(
        th.Property("id", th.IntegerType),
        th.Property("created", th.DateTimeType),
        th.Property("updated", th.DateTimeType),
    ).to_dict()

    def get_child_context(self, record, context):
        return {"response_id": record.get("id")}

    def prepare_request_payload(self, context, next_page_token):
        payload = super().prepare_request_payload(context, next_page_token)
        payload.update({"form_id": self.form_id})
        return payload


class FormsResponseDateStream(KpaStream):
    """Define custom stream."""

    path = "/responses.info"
    key_properties = []
    form_id = None
    records_jsonpath = "$.response"
    fields_dict = {}
    rest_method = "POST"
    pagination = False

    def get_fields(self):
        url = f"{self.url_base}/forms.info"
        data = super().prepare_request_payload({}, None)
        data.update({"form_id": self.form_id})
        fields = self.make_request("POST", url, json=data)
        if fields.status_code == 200 and fields.json().get("ok"):
            fields = fields.json().get("form", {}).get("latest", {}).get("fields", [])
            return fields
        raise Exception(
            f"Error while trying to fetch fields for form id {self.form_id}. Error: {fields.text}"
        )

    @cached_property
    def fields(self):
        return self.get_fields()

    @cached_property
    def schema(self) -> dict:
        fields = self.fields
        return self.get_schema(fields)

    def post_process(self, row, context) -> dict:
        if not self.fields_dict:
            self.fields_dict = {field["id"]: field["title"] for field in self.fields}
        processed_row = {}
        row = row.get("latest", {}).get("responses")
        for field_id, value in row.items():
            # Retrieve the corresponding field name
            field_name = self.fields_dict.get(field_id)
            field_type = (
                self.schema["properties"].get(field_name, {}).get("type", [""])[0]
            )

            if field_name is None:
                continue
            # Extract the first key's value from the field_info['value']
            value = value.get("value")
            if value:
                if (
                    field_type == "string"
                    and isinstance(value.get("values"), list)
                    and value["values"]
                ):
                    processed_row[field_name] = value["values"][0]
                elif value.get("attachments"):
                    processed_row[field_name] = value["attachments"]
                elif value.get("utc_time"):
                    processed_row[field_name] = datetime.fromtimestamp(value["utc_time"] / 1000.0, tz=pytz.utc)
                else:
                    first_key = next(iter(value))
                    processed_row[field_name] = value[first_key]
        return processed_row

    def prepare_request_payload(self, context, next_page_token):
        payload = super().prepare_request_payload(context, next_page_token)
        payload.update({"response_id": context.get("response_id")})
        return payload
