"""
Copyright (c) 2023 Aiven Ltd
See LICENSE for details
"""
from __future__ import annotations

from .readers import read_metadata, read_records
from .schema import Header, Metadata, Record
from .writers import write_metadata, write_record
from kafka.consumer.fetcher import ConsumerRecord
from karapace.backup.backend import BaseBackupProducer, BaseBackupReader, ProducerSend
from karapace.version import __version__
from typing import IO, Iterator

import datetime
import uuid


class SchemaBackupV3Reader(BaseBackupReader[IO[bytes]]):
    def read(self, topic_name: str, buffer: IO[bytes]) -> Iterator[ProducerSend]:
        metadata = read_metadata(buffer)

        if metadata.partition_count != 1:
            raise RuntimeError("Cannot restore multi-partition topics")

        if metadata.topic_name != topic_name:
            raise RuntimeError("Metadata entry contains differing topic name")

        for record in read_records(buffer):
            yield ProducerSend(
                topic_name=topic_name,
                value=record.value,
                key=record.key,
                headers=tuple((header.key, header.value) for header in record.headers),
                partition=record.partition,
                timestamp_ms=record.timestamp_ms,
            )


class SchemaBackupV3Producer(BaseBackupProducer[IO[bytes]]):
    @classmethod
    def store_metadata(
        cls,
        buffer: IO[bytes],
        topic_name: str,
        topic_id: uuid.UUID | None,
        estimated_record_count: int,
        partition_count: int,
    ) -> None:
        if partition_count != 1:
            raise RuntimeError("Cannot backup multi-partition topics")

        write_metadata(
            buffer,
            metadata=Metadata(
                version=3,
                tool_name="karapace",
                tool_version=__version__,
                created_at=datetime.datetime.now(datetime.timezone.utc),
                estimated_record_count=estimated_record_count,
                topic_name=topic_name,
                topic_id=topic_id,
                partition_count=partition_count,
            ),
        )

    @classmethod
    def store_record(cls, buffer: IO[bytes], record: ConsumerRecord) -> None:
        write_record(
            buffer,
            record=Record(
                key=record.key,
                value=record.value,
                headers=tuple(Header(key=key, value=value) for key, value in record.headers),
                partition=record.partition,
                offset=record.offset,
                # fixme: I think this is wrong? timestamp here is probably resolved?
                timestamp_ms=record.timestamp,
            ),
        )
