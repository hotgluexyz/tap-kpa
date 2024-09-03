"""Stream type classes for tap-kpa."""

import requests
from backports.cached_property import cached_property
from singer_sdk import typing as th

from tap_kpa.client import KpaStream
from datetime import datetime
import pytz


# https://api.kpaehs.com/docs/method/responses.list
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
        if next_page_token:
            payload["page"] = next_page_token
        return payload


class FormsResponseDateStream(KpaStream):
    """Define custom stream."""

    path = "/responses.info"
    key_properties = []
    ids = set()
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
            for field in self.fields:
                if field.get("title") not in self.fields_dict.values():
                    self.fields_dict[field["id"]] = field.get("title")
                else:
                    self.fields_dict[field["id"]] = f'{field["title"]}_{field["id"]}'
        processed_row = {}
        # Add id to set
        if row.get("id") in self.ids:
            return None
        self.ids.add(row.get("id"))

        processed_row["kpa_id"] = row.get("id")
        processed_row["kpa_created"] = datetime.fromtimestamp(
            int(row.get("created")) / 1000.0, tz=pytz.utc
        )
        processed_row["kpa_updated"] = datetime.fromtimestamp(
            int(row.get("updated")) / 1000.0, tz=pytz.utc
        )
        row = row.get("latest", {}).get("responses")
        for field_id, value in row.items():
            # Retrieve the corresponding field name
            field_name = self.fields_dict.get(field_id)
            if field_name is None:
                continue

            field_name = field_name.strip()
            field_type = (
                self.schema["properties"].get(field_name, {}).get("type", [""])[0]
            )

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
                    processed_row[field_name] = datetime.fromtimestamp(
                        value["utc_time"] / 1000.0, tz=pytz.utc
                    )
                else:
                    first_key = next(iter(value))
                    processed_row[field_name] = value[first_key]
        return processed_row

    def prepare_request_payload(self, context, next_page_token):
        payload = super().prepare_request_payload(context, next_page_token)
        payload.update({"response_id": context.get("response_id")})
        return payload


class RolesListStream(KpaStream):
    """Define custom stream."""

    name = "roles"
    path = "/roles.list"
    records_jsonpath = "$.roles[*]"
    rest_method = "POST"

    schema = th.PropertiesList(
        th.Property("id", th.StringType),
        th.Property("name", th.StringType),
    ).to_dict()


class UsersListStream(KpaStream):
    """Define custom stream."""

    name = "users"
    path = "/users.list"
    records_jsonpath = "$.users[*]"
    rest_method = "POST"

    schema = th.PropertiesList(
        th.Property("created", th.IntegerType),
        th.Property("registered_on", th.IntegerType),
        th.Property("supervisor_id", th.StringType),
        th.Property("mentor_id", th.StringType),
        th.Property("hse_id", th.StringType),
        th.Property("manager_id", th.StringType),
        th.Property("clients_id", th.ArrayType(th.StringType)),
        th.Property("firstname", th.StringType),
        th.Property("lastname", th.StringType),
        th.Property("employeeNumber", th.StringType),
        th.Property("email", th.EmailType),
        th.Property("username", th.StringType),
        th.Property("cellPhone", th.StringType),
        th.Property("hireDate", th.IntegerType),
        th.Property("sseDate", th.IntegerType),
        th.Property("terminationDate", th.IntegerType),
        th.Property("emergencyContact", th.StringType),
        th.Property("isDriver", th.BooleanType),
        th.Property("isRegulatedDriver", th.BooleanType),
        th.Property("role_id", th.StringType),
        th.Property("metavalues", th.ObjectType(additional_properties=th.CustomType({"type": ["object", "string"]}))),
        th.Property("creator_id", th.ObjectType(
                th.Property("firstname", th.StringType),
                th.Property("lastname", th.StringType),
                th.Property("id", th.StringType),
            )),
        th.Property("fieldOffice_id", th.ArrayType(th.StringType)),
        th.Property("lineOfBusiness_id", th.ArrayType(th.StringType)),
        th.Property("lastWebAccess", th.IntegerType),
        th.Property("lastMobileAccess", th.IntegerType),
        th.Property("id", th.StringType),
    ).to_dict()

class LinesOfBusinessListStream(KpaStream):
    """Define custom stream."""

    name = "lines_of_business"
    path = "/linesofbusiness.list"
    records_jsonpath = "$.linesofbusiness[*]"
    rest_method = "POST"

    schema = th.PropertiesList(
        th.Property("name", th.StringType),
        th.Property("code", th.StringType),
        th.Property("created", th.IntegerType),
        th.Property("id", th.StringType),
    ).to_dict()