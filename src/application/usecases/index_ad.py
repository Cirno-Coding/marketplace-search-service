from src.application.ports.ad_source import AdSource
from src.application.ports.uow import UnitOfWork
from src.application.ports.usecases import IndexAdPort


class IndexAd(IndexAdPort):
    def __init__(self, uow: UnitOfWork, ad_source: AdSource) -> None:
        self._uow = uow
        self._ad_source = ad_source

    async def execute(self, ad_id: int) -> None:
        ad_source = await self._ad_source.get(ad_id)
        async with self._uow as uow:
            if ad_source is None or ad_source.status != "active":
                await uow.search.delete(ad_id)
                await uow.commit()
            else:
                await uow.search.upsert(
                    ad_id=ad_source.ad_id,
                    title=ad_source.title,
                    description=ad_source.description,
                    price=ad_source.price,
                    category=ad_source.category,
                    city=ad_source.city,
                )
                await uow.commit()
