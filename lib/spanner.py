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

from google.auth import credentials as gcreds
from google.auth import default as auth_default
from google.cloud import spanner as gcloud_spanner
from typing import Any

COMMIT_TIMESTAMP = gcloud_spanner.COMMIT_TIMESTAMP


def get_auth() -> Any:
  if os.environ.get("SPANNER_EMULATOR_HOST", None) is not None:
    return gcreds.AnonymousCredentials(), "rcs-demo-test"
  else:
    return auth_default()


def get_database():
  credentials, project = get_auth()
  client = gcloud_spanner.Client(project=project, credentials=credentials)
  instance = client.instance(os.environ.get("INSTANCE", "test-instance"))
  database = instance.database(os.environ.get("DATABASE", "test-database"))

  return database


def write_to_table(database, table, avro_schema, row) -> None:
  columns = [field["name"] for field in avro_schema["fields"]]
  row_value = [row[column] for column in columns]
  with database.batch() as batch:
    batch.insert(table=table, columns=columns, values=[row_value])
