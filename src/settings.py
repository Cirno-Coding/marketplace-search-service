from pydantic import AliasChoices, Field, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        populate_by_name=True,
    )

    database_url: str = Field(
        default=("postgresql+asyncpg://postgres:postgres@localhost:5435/search_db"),
        validation_alias=AliasChoices(
            "DATABASE_URL",
            "POSTGRES_CONNECTION_STRING",
        ),
    )

    kafka_bootstrap_servers: str = Field(
        default="kafka:9092",
        validation_alias=AliasChoices(
            "KAFKA_BOOTSTRAP_SERVERS",
            "KAFKA_BROKERS",
        ),
    )
    kafka_topic_ads: str = Field(
        default="ads",
        validation_alias=AliasChoices(
            "KAFKA_TOPIC_ADS",
            "KAFKA_TOPIC_MARKETPLACE_ADS",
        ),
    )
    kafka_consumer_group: str = "search-service"
    ad_service_url: str = "http://ad-service:8002"

    @field_validator("database_url", mode="before")
    @classmethod
    def normalize_database_url(cls, value: str) -> str:
        if value.startswith("postgresql+asyncpg://"):
            return value
        if value.startswith("postgresql://"):
            return value.replace("postgresql://", "postgresql+asyncpg://", 1)
        return value.replace("postgres://", "postgresql+asyncpg://", 1)
