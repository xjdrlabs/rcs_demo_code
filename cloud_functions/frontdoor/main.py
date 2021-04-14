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

import os
import uuid

from lib import spanner
from lib import http
from lib.timestamp import timestamp
from typing import Tuple, Union

table = os.environ.get("TABLE", "frontdoor")
database = spanner.get_database()

#TODO(xjdr): need to add customer_id so that we can track things by customer
columns = ("id", "event_info_json_string", "created_at", "commit_timestamp")


def add_to_cart(request) -> Union[int, Tuple[str, int]]:
  required_fields = ["event_info_json_string"]
  fields, error = http.parse_request_json(request, required_fields)
  if error is not None:
    return error
  event_info_json_string = fields

  values = []
  values.append((
      str(uuid.uuid4()),
      event_info_json_string,
      timestamp(),
      spanner.COMMIT_TIMESTAMP,
  ))
  with database.batch() as batch:
    batch.insert(table=table, columns=columns, values=values)
  return "OK", 200
