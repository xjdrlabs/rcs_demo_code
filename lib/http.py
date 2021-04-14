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


class RequestParser(object):

  def __init__(self, schema) -> None:
    self.schema = schema

  def parse(self, request) -> None:
    '''
    returns (fields, error) where
      - fields is a list of values corresponding to the names found in schema
      - error is a tuple of (Response, code)
    '''
    pass


class JsonRequestParser(RequestParser):

  def parse(self, request) -> Any:
    return parse_request_json(request, self.schema)


class AvroRequestParser(RequestParser):

  def parse(self, request) -> None:
    pass


class DispatchingRequestParser(object):

  def __init__(self, json_parser, avro_parser) -> None:
    self.json_parser = json_parser
    self.avro_parser = avro_parser

  def parse(self, request) -> None:
    pass


def get_payload(request) -> Tuple[Any, Optional[List[str]]]:
  payload = None
  errors = None
  if request.content_type == 'application/json':
    payload = request.json
  else:
    errors = [f"unsupported content type '{request.content_type}'"]
  return payload, errors


def build_errors_response(errors: _T0) -> Dict[str, _T0]:
  return {'errors': errors}


def pretty_type(schema_type: _T0) -> Union[str, _T0]:
  if isinstance(schema_type, dict):
    if 'type' in schema_type and 'items' in schema_type:
      if schema_type['type'] == 'array':
        items = schema_type['items']
        return f'array of {items}'
    return str(schema_type)
  return schema_type


def pretty_avro_errors(errors) -> List[str]:
  result = []
  error_by_field = dict()
  for error in errors:
    logging.debug(f'pretty_avro_errors {error}')
    if not isinstance(error, ValidationErrorData):
      error_type = type(error)
      logging.debug(f'got some dum dum here: {error_type}')
      continue
    #logging.debug(f'storing this bad boy')
    field_errors = error_by_field.setdefault(error.field, [])
    field_errors.append(error)

  #logging.debug(f'accumulated errors: {error_by_field}')
  for key, values in error_by_field.items():
    field = key.split('.')[-1]
    value = values[0].datum
    logging.debug(f'pretty_avro_errors {key} {values}')
    typeset = set(map(lambda x: pretty_type(x.schema), values))
    types = ', '.join(typeset)
    if value is None and 'null' not in typeset:
      result.append(f'Missing required field "{field}"')
    else:
      result.append(
          f'The field "{field}" has a value of "{value}" which is not one of the valid types ({types})'
      )

  return result


def validate_record(datum, schema) -> Optional[List[str]]:
  errors = []
  named_schemas = dict()
  parent_ns = ""
  raise_errors = True
  parsed_schema = parse_schema(schema,
                               _force=True,
                               _named_schemas=named_schemas)
  _, namespace = schema_name(parsed_schema, parent_ns)
  # validate datum for known fields
  for field in parsed_schema['fields']:
    try:
      name = field['name']
      default = field.get('default')
      _validate(datum=datum.get(name, default),
                schema=field['type'],
                named_schemas=named_schemas,
                field=f'{namespace}.{name}',
                raise_errors=raise_errors)
    except ValidationError as e:
      errors.extend(e.errors)
  if len(errors) == 0:
    return None
  return pretty_avro_errors(errors)


def reject_unknown_fields(datum, avro_schema) -> Optional[List[str]]:
  errors = list()
  # validate datum for unknown fields
  known_fields = {field['name'] for field in avro_schema['fields']}
  for field_name in datum.keys():
    if field_name not in known_fields:
      errors.append(f'Forbidden field {field_name}')
  if len(errors) == 0:
    return None
  return errors


def validation_check(datum, avro_schema) -> Optional[list]:
  errors = []
  validation_errors = validate_record(datum, avro_schema)
  if validation_errors:
    errors.extend(validation_errors)
  reject_errors = reject_unknown_fields(datum, avro_schema)
  if reject_errors:
    errors.extend(reject_errors)
  if len(errors) == 0:
    return None
  return errors


def handle_request(request, avro_schema, do_work) -> Tuple[Any, int]:
  payload, errors = get_payload(request)
  if errors is None:
    errors = validation_check(payload, avro_schema)
  response = dict()
  if errors is None:
    response, errors = do_work(payload)
  if errors is None:
    code = 200
  else:
    response = build_errors_response(errors)
    code = 400
  return jsonify(response), code
