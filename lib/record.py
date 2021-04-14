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

import io
import json
import avro

from avro.datafile import DataFileReader, DataFileWriter
from avro.io import DatumReader, DatumWriter
from typing import Any


def serialize_record(msg, schema) -> bytes:
  return serialize_records([msg], schema)


def serialize_records(msgs, schema) -> bytes:
  with io.BytesIO() as buf:
    writer = DataFileWriter(buf, DatumWriter(),
                            avro.schema.parse(json.dumps(schema)))
    for line_item in msgs:
      #print(f"SERRECORD {line_item}")
      writer.append(line_item)

    writer.flush()
    record = buf.getvalue()

    return record


def deserialize_record(record) -> Any:
  msgs = deserialize_records(record)
  return msgs[0] if len(msgs) else None


def deserialize_records(record) -> list:
  #print(f"DESRECORD {record}")
  with io.BytesIO(record) as buf:
    reader = DataFileReader(buf, DatumReader())
    msgs = [msg for msg in reader]
    #print(f'WHAT? {msgs}')
    reader.close()

    return msgs
