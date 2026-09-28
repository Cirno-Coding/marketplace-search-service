from datetime import UTC, datetime
from types import TracebackType
from typing import List

import pytest

from src.application.ports.ad_source import AdSnapshot, AdSource
from src.application.ports.repositories import SearchRepository, SortKey
from src.application.ports.uow import UnitOfWork
from src.application.tracing import current_trace_id
from src.domain.entities import SearchDocument


class FakeSearchRepository(SearchRepository):
    def __init__(self) -> None:
        self._docs: dict[int, SearchDocument] = {}
        self._next_id = 1

    async def upsert(
        self,
        ad_id: int,
        title: str,
        description: str,
        price: int,
        category: str,
        city: str,
    ) -> None:
        existing = self._docs.get(ad_id)
        doc_id = existing.id if existing else self._next_id
        if existing is None:
            self._next_id += 1
        self._docs[ad_id] = SearchDocument(
            id=doc_id,
            ad_id=ad_id,
            title=title,
            description=description,
            price=price,
            category=category,
            city=city,
            indexed_at=datetime.now(UTC),
        )

    async def delete(self, ad_id: int) -> None:
        self._docs.pop(ad_id, None)

    async def search(
        self,
        query: str | None,
        category: str | None,
        city: str | None,
        min_price: int | None,
        max_price: int | None,
        sort: SortKey | None,
        limit: int,
        offset: int,
    ) -> tuple[List[SearchDocument], int]:
        items = list(self._docs.values())
        if query is not None and query.strip():
            needle = query.lower()
            items = [
                d for d in items
                if needle in d.title.lower() or needle in d.description.lower()
            ]
        if category is not None:
            items = [d for d in items if d.category == category]
        if city is not None:
            items = [d for d in items if d.city == city]
        if min_price is not None:
            items = [d for d in items if d.price >= min_price]
        if max_price is not None:
            items = [d for d in items if d.price <= max_price]

        if sort == "price_asc":
            items.sort(key=lambda d: d.price)
        elif sort == "price_desc":
            items.sort(key=lambda d: d.price, reverse=True)
        else:
            items.sort(key=lambda d: d.indexed_at, reverse=True)

        total = len(items)
        return items[offset : offset + limit], total

    async def suggest(self, prefix: str, limit: int) -> list[str]:
        titles = sorted({
            d.title for d in self._docs.values()
            if d.title.lower().startswith(prefix.lower())
        })
        return titles[:limit]

    def snapshot(self) -> dict[int, SearchDocument]:
        return dict(self._docs)


class FakeAdSource(AdSource):
    def __init__(self, snapshots: dict[int, AdSnapshot] | None = None) -> None:
        self._snapshots: dict[int, AdSnapshot] = snapshots or {}
        self.calls: list[int] = []
        self.trace_ids: list[str | None] = []

    async def get(self, ad_id: int) -> AdSnapshot | None:
        self.calls.append(ad_id)
        self.trace_ids.append(current_trace_id())
        return self._snapshots.get(ad_id)

    def set(self, snapshot: AdSnapshot) -> None:
        self._snapshots[snapshot.ad_id] = snapshot

    def remove(self, ad_id: int) -> None:
        self._snapshots.pop(ad_id, None)


class FakeUnitOfWork(UnitOfWork):
    def __init__(self, search_repo: FakeSearchRepository | None = None) -> None:
        self.search = search_repo or FakeSearchRepository()
        self.committed = False
        self.rolled_back = False

    async def commit(self) -> None:
        self.committed = True

    async def rollback(self) -> None:
        self.rolled_back = True

    async def __aenter__(self) -> "FakeUnitOfWork":
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: TracebackType | None,
    ) -> None:
        if exc_type is not None:
            await self.rollback()


@pytest.fixture
def fake_search_repo() -> FakeSearchRepository:
    return FakeSearchRepository()


@pytest.fixture
def fake_uow(fake_search_repo: FakeSearchRepository) -> FakeUnitOfWork:
    return FakeUnitOfWork(fake_search_repo)


@pytest.fixture
def fake_ad_source() -> FakeAdSource:
    return FakeAdSource()


def make_snapshot(
    ad_id: int = 1,
    title: str = "MacBook Pro",
    description: str = "Отличный ноутбук",
    price: int = 180000,
    category: str = "Электроника",
    city: str = "Москва",
    status: str = "active",
) -> AdSnapshot:
    return AdSnapshot(
        ad_id=ad_id,
        title=title,
        description=description,
        price=price,
        category=category,
        city=city,
        status=status,
    )
