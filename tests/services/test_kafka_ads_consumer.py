import logging
import uuid
from dataclasses import dataclass, field
from typing import Any

import pytest

from src.application.ports.usecases import IndexAdPort
from src.application.services.kafka_ads_consumer import KafkaAdsConsumer
from src.application.tracing import current_trace_id, reset_trace_id, set_trace_id
from src.application.usecases.index_ad import IndexAd
from src.application.usecases.remove_ad import RemoveAd
from src.infrastructure.logging_setup import TraceIdFilter
from tests.conftest import FakeAdSource, FakeUnitOfWork, make_snapshot


@dataclass
class FakeRecord:
    value: dict[str, Any]
    headers: tuple[tuple[str, bytes], ...] = ()


@dataclass
class FakeConsumer:
    records: list[FakeRecord]
    commits: int = 0
    trace_ids_at_commit: list[str | None] = field(default_factory=list)

    def __aiter__(self) -> "FakeConsumer":
        self._iter = iter(self.records)
        return self

    async def __anext__(self) -> FakeRecord:
        try:
            return next(self._iter)
        except StopIteration:
            raise StopAsyncIteration

    async def commit(self) -> None:
        self.commits += 1
        self.trace_ids_at_commit.append(current_trace_id())


class RaisingIndexAd(IndexAdPort):
    def __init__(self) -> None:
        self.trace_ids: list[str | None] = []

    async def execute(self, ad_id: int) -> None:
        self.trace_ids.append(current_trace_id())
        raise RuntimeError("ad-service is down")


def event(name: str, ad_id: int, trace_id: str | bytes | None = None) -> FakeRecord:
    if trace_id is None:
        return FakeRecord({"event": name, "payload": {"ad_id": ad_id}})
    raw = trace_id if isinstance(trace_id, bytes) else trace_id.encode()
    return FakeRecord(
        {"event": name, "payload": {"ad_id": ad_id}}, (("X-Trace-Id", raw),)
    )


def make_consumer(
    records: list[FakeRecord],
    uow: FakeUnitOfWork,
    ad_source: FakeAdSource,
) -> tuple[KafkaAdsConsumer, FakeConsumer]:
    fake = FakeConsumer(records)
    consumer = KafkaAdsConsumer(
        consumer=fake,
        index_ad=IndexAd(uow, ad_source),
        remove_ad=RemoveAd(uow),
    )
    return consumer, fake


def assert_uuid4(value: str | None) -> None:
    assert value is not None
    assert uuid.UUID(value).version == 4


async def test_header_trace_id_reaches_ad_source(
    fake_uow: FakeUnitOfWork, fake_ad_source: FakeAdSource
) -> None:
    fake_ad_source.set(make_snapshot(ad_id=1))
    consumer, fake = make_consumer(
        [event("ad.created", 1, "trace-kafka-1")], fake_uow, fake_ad_source
    )

    await consumer.run()

    assert fake_ad_source.trace_ids == ["trace-kafka-1"]
    assert 1 in fake_uow.search.snapshot()
    assert fake.trace_ids_at_commit == ["trace-kafka-1"]


async def test_missing_header_generates_uuid4(
    fake_uow: FakeUnitOfWork, fake_ad_source: FakeAdSource
) -> None:
    consumer, fake = make_consumer(
        [event("ad.updated", 1), event("ad.updated", 2)], fake_uow, fake_ad_source
    )

    await consumer.run()

    first, second = fake_ad_source.trace_ids
    assert_uuid4(first)
    assert_uuid4(second)
    assert first != second
    assert fake.commits == 2


@pytest.mark.parametrize(
    "headers",
    [
        (("X-Trace-Id", b"bad value"),),
        (("X-Trace-Id", b"line\nbreak"),),
        (("X-Trace-Id", b""),),
        (("X-Trace-Id", b"a" * 129),),
        (("X-Trace-Id", b"\xff\xfe"),),
        (("X-Trace-Id", b"one"), ("X-Trace-Id", b"two")),
    ],
)
async def test_invalid_header_is_replaced(
    fake_uow: FakeUnitOfWork,
    fake_ad_source: FakeAdSource,
    headers: tuple[tuple[str, bytes], ...],
) -> None:
    record = FakeRecord({"event": "ad.updated", "payload": {"ad_id": 1}}, headers)
    consumer, _ = make_consumer([record], fake_uow, fake_ad_source)

    await consumer.run()

    assert_uuid4(fake_ad_source.trace_ids[0])


async def test_header_name_is_case_insensitive(
    fake_uow: FakeUnitOfWork, fake_ad_source: FakeAdSource
) -> None:
    record = FakeRecord(
        {"event": "ad.updated", "payload": {"ad_id": 1}},
        (("x-trace-id", b"lower-case"),),
    )
    consumer, _ = make_consumer([record], fake_uow, fake_ad_source)

    await consumer.run()

    assert fake_ad_source.trace_ids == ["lower-case"]


async def test_each_message_gets_its_own_trace_id(
    fake_uow: FakeUnitOfWork, fake_ad_source: FakeAdSource
) -> None:
    consumer, fake = make_consumer(
        [
            event("ad.created", 1, "trace-a"),
            event("ad.updated", 2, "trace-b"),
            event("ad.deleted", 1, "trace-c"),
        ],
        fake_uow,
        fake_ad_source,
    )

    await consumer.run()

    assert fake_ad_source.trace_ids == ["trace-a", "trace-b"]
    assert fake.trace_ids_at_commit == ["trace-a", "trace-b", "trace-c"]


async def test_context_is_restored_after_run(
    fake_uow: FakeUnitOfWork, fake_ad_source: FakeAdSource
) -> None:
    consumer, _ = make_consumer(
        [event("ad.created", 1, "trace-1")], fake_uow, fake_ad_source
    )

    token = set_trace_id("outer")
    try:
        await consumer.run()
        assert current_trace_id() == "outer"
    finally:
        reset_trace_id(token)

    assert current_trace_id() is None


async def test_failure_skips_commit_and_resets_context(
    fake_uow: FakeUnitOfWork, fake_ad_source: FakeAdSource
) -> None:
    failing = RaisingIndexAd()
    fake = FakeConsumer(
        [event("ad.created", 1, "trace-fail"), event("ad.deleted", 2, "trace-next")]
    )
    consumer = KafkaAdsConsumer(
        consumer=fake, index_ad=failing, remove_ad=RemoveAd(fake_uow)
    )

    await consumer.run()

    assert failing.trace_ids == ["trace-fail"]
    assert fake.trace_ids_at_commit == ["trace-next"]
    assert current_trace_id() is None


async def test_skipped_messages_are_committed(
    fake_uow: FakeUnitOfWork, fake_ad_source: FakeAdSource
) -> None:
    consumer, fake = make_consumer(
        [
            FakeRecord({"event": "ad.created", "payload": {}}),
            event("ad.unknown", 1, "trace-unknown"),
        ],
        fake_uow,
        fake_ad_source,
    )

    await consumer.run()

    assert fake.commits == 2
    assert fake_ad_source.calls == []


async def test_logs_contain_message_trace_id(
    fake_uow: FakeUnitOfWork,
    fake_ad_source: FakeAdSource,
    caplog: pytest.LogCaptureFixture,
) -> None:
    caplog.handler.addFilter(TraceIdFilter())
    caplog.set_level(logging.INFO, logger="src.application.services.kafka_ads_consumer")
    failing = RaisingIndexAd()
    consumer = KafkaAdsConsumer(
        consumer=FakeConsumer([event("ad.created", 7, "trace-log")]),
        index_ad=failing,
        remove_ad=RemoveAd(fake_uow),
    )

    await consumer.run()

    assert [(r.levelname, r.trace_id) for r in caplog.records] == [
        ("INFO", "trace-log"),
        ("ERROR", "trace-log"),
    ]
