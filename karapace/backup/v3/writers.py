"""
Copyright (c) 2023 Aiven Ltd
See LICENSE for details
"""
from .errors import IntegerAboveBound, IntegerBelowBound
from .schema import Envelope, Metadata, Record
from dataclasses_avroschema import AvroModel
from typing import Final, IO, NoReturn, TypeVar
from xxhash import xxh64

import struct

UINT32_RANGE: Final = range(0, 2**32)
UINT64_RANGE: Final = range(0, 2**64)


def _reraise_for_bound(valid: range, value: int, exception: struct.error) -> NoReturn:
    if value < valid.start:
        raise IntegerBelowBound(f"Value is too small for valid {valid}") from exception
    if value >= valid.stop:
        raise IntegerAboveBound(f"Value is too large for valid {valid}") from exception
    raise exception


def write_uint32(buffer: IO[bytes], value: int) -> None:
    # Because try-blocks are cheap, zero-cost after Python 3.11 even, and
    # because we expect valid values to be much more common than invalid values,
    # we do not validate `value` up-front. Instead, we let struct.pack() raise
    # and figure out a reasonable error message after-wards.
    try:
        buffer.write(struct.pack(">I", value))
    except struct.error as exception:
        _reraise_for_bound(UINT32_RANGE, value, exception)


def write_uint64(buffer: IO[bytes], value: int) -> None:
    try:
        buffer.write(struct.pack(">Q", value))
    except struct.error as exception:
        _reraise_for_bound(UINT64_RANGE, value, exception)


T = TypeVar("T", bound=AvroModel)


def write_sized(buffer: IO[bytes], model: AvroModel) -> None:
    encoded = model.serialize()
    write_uint32(buffer, len(encoded))
    buffer.write(encoded)


def write_metadata(buffer: IO[bytes], metadata: Metadata) -> None:
    write_sized(buffer, metadata)


def write_record(buffer: IO[bytes], record: Record) -> None:
    """
    Encode Record, compute its checksum, wrap it in an Envelope which is written
    to `buffer`, preceded by its byte length.
    """
    # Encode the record, compute its checksum, and wrap in an Envelope.
    encoded_record = record.serialize()
    envelope = Envelope(
        record=encoded_record,
        checksum=xxh64(encoded_record).digest(),
    )
    write_sized(buffer, envelope)
