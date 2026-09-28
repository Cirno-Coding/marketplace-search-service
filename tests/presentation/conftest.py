import pytest
from fastapi import FastAPI
from httpx import ASGITransport, AsyncClient

from src.presentation.api.dependencies import get_uow
from src.presentation.api.middleware import TraceIdMiddleware
from src.presentation.api.routes.public import router as public_router
from tests.conftest import FakeUnitOfWork


@pytest.fixture
def app(fake_uow: FakeUnitOfWork) -> FastAPI:
    app = FastAPI()
    app.add_middleware(TraceIdMiddleware)
    app.include_router(public_router)
    app.dependency_overrides[get_uow] = lambda: fake_uow
    return app


@pytest.fixture
async def client(app: FastAPI) -> AsyncClient:
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as ac:
        yield ac
