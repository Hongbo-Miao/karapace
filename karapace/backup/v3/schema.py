"""
karapace

Copyright (c) 2023 Aiven Ltd
See LICENSE for details
"""
from dataclasses_avroschema import AvroModel
from karapace.dataclasses import default_dataclass
from typing import Optional, Sequence
from typing_extensions import Annotated, TypeAlias

import datetime
import uuid

i32: TypeAlias = Annotated[int, "Int32"]
i64: TypeAlias = Annotated[int, "Int64"]


# Do we actually need this? Can be introduced with default once need arises for
# a different checksum than default?
# class ChecksumAlgorithm(enum.IntEnum):
#     xxhash3_64_be = 1


@default_dataclass
class Metadata(AvroModel):
    version: i32
    tool_name: str
    tool_version: str
    created_at: datetime.datetime
    # fixme: Consider introducing end-of-file metadata so that we can store the
    #        exact number of records instead.
    estimated_record_count: i64
    topic_name: str
    topic_id: Optional[uuid.UUID]
    partition_count: i32
    # checksum_algorithm: ChecksumAlgorithm


@default_dataclass
class Header(AvroModel):
    key: bytes
    value: bytes


@default_dataclass
class Record(AvroModel):
    key: Optional[bytes]
    value: Optional[bytes]
    headers: Sequence[Header]
    partition: i32
    offset: i64
    timestamp_ms: i64


@default_dataclass
class Envelope(AvroModel):
    record: bytes
    checksum: bytes
