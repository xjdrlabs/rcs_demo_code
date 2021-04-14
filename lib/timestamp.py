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

import datetime
from typing import Any


def timestamp() -> str:
  return datetime.datetime.utcnow().replace(tzinfo=datetime.timezone.utc).isoformat('T').split('+')[0] + 'Z'

def parse_timestamp_str(dts) -> datetime.datetime:
  try:
    return datetime.datetime.fromisoformat(dts[:-1])
  except ValueError as e:
    raise e  #TODO(xjdr) what to do here?


def timestamp_now() -> datetime.datetime:
  return datetime.datetime.utcnow()


def now_plus_seconds(seconds) -> datetime.datetime:
  return timestamp_now() + datetime.timedelta(seconds=seconds)


def now_plus_minutes(minutes) -> datetime.datetime:
  return timestamp_now() + datetime.timedelta(minutes=minutes)


def now_plus_hours(hours) -> datetime.datetime:
  return timestamp_now() + datetime.timedelta(hours=hours)


def now_plus_days(days) -> datetime.datetime:
  return timestamp_now() + datetime.timedelta(days=days)


def now_plus_weeks(weeks) -> datetime.datetime:
  return timestamp_now() + datetime.timedelta(weeks=weeks)


def to_spanner_timestamp(dt) -> Any:
  return dt.replace(tzinfo=datetime.timezone.utc).isoformat('T').split('+')[0] + 'Z'
