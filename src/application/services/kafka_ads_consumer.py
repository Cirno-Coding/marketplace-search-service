import logging
import typing
from collections.abc import Sequence

from aiokafka import AIOKafkaConsumer, ConsumerRecord

from src.application.ports.usecases import IndexAdPort, RemoveAdPort
from src.application.tracing import (
    TRACE_ID_HEADER,
    new_trace_id,
    reset_trace_id,
    resolve_trace_id,
    set_trace_id,
)

logger = logging.getLogger(__name__)

_TRACE_ID_HEADER_LOWER = TRACE_ID_HEADER.lower()


class KafkaAdsConsumer:
    def __init__(
        self,
        consumer: AIOKafkaConsumer,
        index_ad: IndexAdPort,
        remove_ad: RemoveAdPort,
    ) -> None:
        self._consumer = consumer
        self._index_ad = index_ad
        self._remove_ad = remove_ad

    async def run(self) -> None:
        async for msg in self._consumer:
            token = set_trace_id(_trace_id_from_headers(msg.headers))
            try:
                await self._process(msg)
            finally:
                reset_trace_id(token)

    async def _process(self, msg: ConsumerRecord) -> None:
        try:
            await self._handle(msg.value)
        except Exception:
            logger.exception("failed to handle message %s", msg)
            return
        await self._consumer.commit()

    async def _handle(self, value: dict[str, typing.Any]) -> None:
        event = value.get("event")
        payload = value.get("payload") or {}
        ad_id = payload.get("ad_id")
        if not isinstance(ad_id, int):
            logger.warning("skip message without ad_id: %s", value)
            return

        logger.info("handling %s ad_id=%s", event, ad_id)
        if event in ("ad.created", "ad.updated"):
            await self._index_ad.execute(ad_id)
        elif event == "ad.deleted":
            await self._remove_ad.execute(ad_id)
        else:
            logger.warning("unknown event type: %s", event)


def _trace_id_from_headers(headers: Sequence[tuple[str, bytes]] | None) -> str:
    values = [
        value
        for name, value in headers or ()
        if name.lower() == _TRACE_ID_HEADER_LOWER and isinstance(value, bytes)
    ]
    if len(values) != 1:
        return new_trace_id()
    try:
        return resolve_trace_id(values[0].decode("utf-8"))
    except UnicodeDecodeError:
        return new_trace_id()
