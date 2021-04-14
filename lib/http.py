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

from flask import jsonify
from fastavro.schema import parse_schema
from fastavro.schema import schema_name
from fastavro.validation import _validate
from fastavro.validation import validate
from fastavro.validation import ValidationError
from fastavro.validation import ValidationErrorData
import logging
from typing import Any, Dict, List, Optional, Tuple, TypeVar, Union

_validate: Any
_T0 = TypeVar('_T0')


def build_error_json(required_fields, found_fields) -> Tuple[Any, int]:
  required_set = set(required_fields)
  found_set = set(found_fields)
  if required_set == found_set:
    return None, 200
  errors = []
  missing = required_set.difference(found_set)
  for field in missing:
    errors.append("Missing required field '%s'" % field)
  return jsonify(dict(errors=errors)), 400


def parse_request_json(request, required_fields) -> Tuple[Any, Optional[int]]:
  request_json = request.get_json(silent=True)
  found_fields = []
  if request_json is None:
    return build_error_json(required_fields, found_fields)
  result = list()
  for field in required_fields:
    if field in request_json:
      found_fields.append(field)
      result.append(request_json.get(field))
  response, code = build_error_json(required_fields, found_fields)
  if response is None:
    return result, None
  return response, code
