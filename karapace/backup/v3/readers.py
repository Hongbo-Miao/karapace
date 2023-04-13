"""
Copyright (c) 2023 Aiven Ltd
See LICENSE for details
"""
from __future__ import annotations

from .errors import InvalidChecksum
from .schema import Envelope, Metadata, Record
from dataclasses_avroschema import AvroModel
from typing import Generator, IO, TypeVar
from xxhash import xxh64

import struct


def read_uint32(buffer: IO[bytes]) -> int:
    return struct.unpack(">I", buffer.read(4))[0]


def read_uint64(buffer: IO[bytes]) -> int:
    return struct.unpack(">Q", buffer.read(8))[0]


T = TypeVar("T", bound=AvroModel)


def read_sized(buffer: IO[bytes], type_: type[T]) -> T:
    size = read_uint32(buffer)
    return type_.deserialize(buffer.read(size))


def read_metadata(buffer: IO[bytes]) -> Metadata:
    return read_sized(buffer, Metadata)


def read_record(buffer: IO[bytes]) -> Record:
    """
    Decode a length-value encoded envelope from `buffer`, verify its checksum is
    intact and return the wrapped record decoded.

    :raises DecodeRecordError:
    :raises MessageNotProcessable: Backup contains a schema-valid message, but the
        message does not contain all data to make it processable. This is a consequence
        of protobuf treating everything as optional
    """
    envelope = read_sized(buffer, Envelope)

    # Verify record is intact.
    if envelope.checksum != xxh64(envelope.record).digest():
        raise InvalidChecksum

    return Record.deserialize(envelope.record)


def read_records(buffer: IO[bytes]) -> Generator[Record, None, None]:
    while True:
        # Attempt to peek into buffer, and break out of loop if it's exhausted.
        position = buffer.tell()
        if buffer.read(1) == b"":
            break
        buffer.seek(position)
        yield read_record(buffer)
