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

import base64
import logging
from lib.record import deserialize_record


def avro_handler(event, sink_table, func) -> None:
  attributes = event.get('attributes', {})
  table = attributes.get('spez.sink.table', None)
  if table is None:
    return
  if table != sink_table:
    return
  data = event.get('data')
  if data is None:
    return
  message = base64.b64decode(data)
  record = deserialize_record(message)
  func(record, message)


def table_filter(event, context, table, func) -> None:
  attributes = event.get('attributes', {})
  sink_table = attributes.get('spez.sink.table', None)
  if sink_table is None:
    logging.error(f"table_filter: sink_table is None attributes: {attributes}")
    return
  if table != sink_table:
    logging.debug(f"table_filter: sink_table {sink_table} is not equal to {table}")
    return
  func(event, context)


def avro_event(event, context, func) -> None:
  payload = event.get('data')
  if payload is None:
    logging.error("avro_event: payload is None")
    return
  message = base64.b64decode(payload)
  if message is None:
    logging.error("avro_event: message is None")
    return
  record = deserialize_record(message)
  if record is None:
    logging.error("avro_event: record is None")
    return
  func(record)
