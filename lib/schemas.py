# Copyright 2021 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the 'License');
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an 'AS IS' BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.


class SchemaMode:
  Backend = "BE"
  Frontend = "FE"


def make_type(typename, required):
  if required:
    return [typename]
  else:
    return ["null", typename]


def add_field(schema, name, field_type):
  # schema["fields"].append({"name": name, "type": field_type, "default": "NONE"})
  schema["fields"].append({"name": name, "type": field_type})


def add_string(schema, name, required):
  add_field(schema, name, make_type("string", required))


def add_array(schema, name, typename, required):
  add_field(schema, name, make_type({"type": "array", "items": typename}, required))


def add_int64_array(schema, name, required):
  add_array(schema, name, "long", required)


def add_float64_array(schema, name, required):
  add_array(schema, name, "double", required)


def add_string_array(schema, name, required):
  add_array(schema, name, "string", required)
  # add_field(schema, name, ["null", {"type": "array", "items": "string"}])


def add_bool(schema, name, required):
  add_field(schema, name, make_type("boolean", required))


def add_int64(schema, name, required):
  add_field(schema, name, make_type("long", required))


def add_timestamp(schema, name, required):
  add_field(schema, name, make_type("string", required))


def add_fields(schema, table_columns):
  debug = []
  for table_column in table_columns:
    ignore_field = False
    required = False
    if len(table_column) == 4:
      func, col, required, ignore_field = table_column
    elif len(table_column) == 3:
      func, col, required = table_column
    else:
      func, col = table_column[:2]
    debug.append((func, col, required, ignore_field))
    if not ignore_field:
      func(schema, col, required)


def oms_retail_schema(name, fields):
  schema = dict()
  schema["namespace"] = "google.retail.oms"
  schema["type"] = "record"
  schema["name"] = name
  schema["fields"] = list()
  add_fields(schema, fields)
  return schema


def get_product_schema(mode=SchemaMode.Backend):
  fields = [
      (add_string, "id", True, mode == SchemaMode.Frontend),  # NOFE
      (add_string, "sku", mode == SchemaMode.Frontend),
      (add_string, "title", mode == SchemaMode.Frontend),
      (add_string, "name", mode == SchemaMode.Frontend),
      (add_string, "description", mode == SchemaMode.Frontend),
      (add_string, "pdp_link", mode == SchemaMode.Frontend),
      (add_string, "main_image_link"),
      (add_string_array, "additional_images"),
      (add_int64, "gtin"),
      (add_string, "mpn"),
      (add_bool, "identifier_exists"),
      (add_float64_array, "features", False, mode == SchemaMode.Frontend),  # NOFE
      (add_float64_array, "memory", False, mode == SchemaMode.Frontend),  # NOFE
      (add_string_array, "filters"),
      (add_string_array, "item_groups", mode == SchemaMode.Frontend),
      (add_timestamp, "created_at"),
      (add_timestamp, "expires_at"),
      (add_timestamp, "last_updated"),
      (add_timestamp, "commit_timestamp", True, mode == SchemaMode.Frontend),  # NOFE
  ]

  return oms_retail_schema("product", fields)
