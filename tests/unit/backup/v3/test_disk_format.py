"""
Copyright (c) 2023 Aiven Ltd
See LICENSE for details
"""
from hypothesis import given
from hypothesis.strategies import integers
from karapace.backup.v3.errors import IntegerAboveBound, IntegerBelowBound
from karapace.backup.v3.readers import read_metadata, read_records, read_uint32, read_uint64
from karapace.backup.v3.schema import Header, Metadata, Record
from karapace.backup.v3.writers import UINT32_RANGE, UINT64_RANGE, write_backup, write_uint32, write_uint64
from tests.unit.backup.v3.conftest import setup_buffer
from typing import IO

import datetime
import pytest
import time


@pytest.mark.parametrize(
    ("byte_value", "expected_result"),
    (
        (b"\x00\x00\x00\x00", 0),
        (b"\xff\xff\xff\xff", 2**32 - 1),
        (b"\x00\x00\x00C", 67),
    ),
)
def test_read_uint32(
    buffer: IO[bytes],
    byte_value: bytes,
    expected_result: int,
) -> None:
    buffer.write(byte_value)
    buffer.seek(0)
    assert read_uint32(buffer) == expected_result


@pytest.mark.parametrize(
    ("byte_value", "expected_result"),
    (
        (b"\x00\x00\x00\x00\x00\x00\x00\x00", 0),
        (b"\xff\xff\xff\xff\xff\xff\xff\xff", 2**64 - 1),
        (b"\x00\x00\x00\x00\x00\x00\x00C", 67),
    ),
)
def test_read_uint64(
    buffer: IO[bytes],
    byte_value: bytes,
    expected_result: int,
) -> None:
    buffer.write(byte_value)
    buffer.seek(0)
    assert read_uint64(buffer) == expected_result


class TestWriteUint32:
    @pytest.mark.parametrize(
        ("value", "expected_bytes"),
        (
            (0, b"\x00\x00\x00\x00"),
            (2**32 - 1, b"\xff\xff\xff\xff"),
            (67, b"\x00\x00\x00C"),
        ),
    )
    def test_can_write_valid_value(
        self,
        buffer: IO[bytes],
        value: int,
        expected_bytes: bytes,
    ) -> None:
        write_uint32(buffer, value)
        buffer.seek(0)
        assert buffer.read(4) == expected_bytes

    def test_raises_integer_out_bound_for_too_small_value(
        self,
        buffer: IO[bytes],
    ) -> None:
        with pytest.raises(
            IntegerBelowBound,
            match=r"^Value is too small for valid range\(0, 4294967296\)$",
        ):
            write_uint32(buffer, -1)

    def test_raises_integer_out_of_bound_for_too_big_value(
        self,
        buffer: IO[bytes],
    ) -> None:
        with pytest.raises(
            IntegerAboveBound,
            match=r"^Value is too large for valid range\(0, 4294967296\)$",
        ):
            write_uint32(buffer, 2**32)


@given(
    integers(min_value=UINT32_RANGE.start, max_value=UINT32_RANGE.stop - 1),
)
def test_uint32_roundtrip(value: int) -> None:
    with setup_buffer() as buffer:
        write_uint32(buffer, value)
        buffer.seek(0)
        assert read_uint32(buffer) == value


class TestWriteUint64:
    @pytest.mark.parametrize(
        ("value", "expected_bytes"),
        (
            (0, b"\x00\x00\x00\x00\x00\x00\x00\x00"),
            (2**64 - 1, b"\xff\xff\xff\xff\xff\xff\xff\xff"),
            (67, b"\x00\x00\x00\x00\x00\x00\x00C"),
        ),
    )
    def test_write_uint64(
        self,
        buffer: IO[bytes],
        value: int,
        expected_bytes: bytes,
    ) -> None:
        write_uint64(buffer, value)
        buffer.seek(0)
        assert buffer.read(8) == expected_bytes

    def test_raises_integer_out_bound_for_too_small_value(
        self,
        buffer: IO[bytes],
    ) -> None:
        with pytest.raises(
            IntegerBelowBound,
            match=r"^Value is too small for valid range\(0, 18446744073709551616\)$",
        ):
            write_uint64(buffer, -1)

    def test_raises_integer_out_of_bound_for_too_big_value(
        self,
        buffer: IO[bytes],
    ) -> None:
        with pytest.raises(
            IntegerAboveBound,
            match=r"^Value is too large for valid range\(0, 18446744073709551616\)$",
        ):
            write_uint64(buffer, 2**64)


@given(
    integers(min_value=UINT64_RANGE.start, max_value=UINT64_RANGE.stop - 1),
)
def test_uint64_roundtrip(value: int) -> None:
    with setup_buffer() as buffer:
        write_uint64(buffer, value)
        buffer.seek(0)
        assert read_uint64(buffer) == value


def test_full_roundtrip(buffer: IO[bytes]) -> None:
    created_at = datetime.datetime.now(tz=datetime.timezone.utc).replace(microsecond=0)
    metadata = Metadata(
        version=0,
        tool_name="foo",
        tool_version="3.0.0",
        created_at=created_at,
        estimated_record_count=0,
        topic_name="a-topic",
        topic_id=None,
        partition_count=1,
    )
    records = (
        Record(
            key=None,
            value=b"123",
            headers=[],
            partition=0,
            offset=0,
            timestamp_ms=int(time.time() * 1000),
        ),
        Record(
            key=b"a key",
            value=b"a value",
            headers=[
                Header(key=b"header1", value=b"some value"),
                Header(key=b"header2", value=b"other value"),
            ],
            partition=1,
            offset=0,
            timestamp_ms=int(time.time() * 1000),
        ),
    )

    write_backup(buffer, metadata, records)

    buffer.seek(0)
    assert read_metadata(buffer) == metadata
    assert tuple(read_records(buffer)) == records
