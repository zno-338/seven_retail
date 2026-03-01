import os
from typing import Optional


class Settings:
    host: str = os.getenv("APP_HOST", "0.0.0.0")
    port: int = int(os.getenv("APP_PORT", "8000"))
    database_url: str = os.getenv("DATABASE_URL", "sqlite:///./data.db")
    opening_keywords_csv: Optional[str] = os.getenv("OPENING_KEYWORDS_CSV")
    keyword_reload_seconds: int = int(os.getenv("KEYWORD_RELOAD_SECONDS", "300"))
    vendor_api_base: Optional[str] = os.getenv("VENDOR_API_BASE")
    vendor_api_token: Optional[str] = os.getenv("VENDOR_API_TOKEN")
    vendor_poll_seconds: int = int(os.getenv("VENDOR_POLL_SECONDS", "300"))
    webhook_secret: Optional[str] = os.getenv("WEBHOOK_SECRET")
    kafka_bootstrap_servers: Optional[str] = os.getenv("KAFKA_BOOTSTRAP_SERVERS")
    kafka_topic_raw: str = os.getenv("KAFKA_TOPIC_RAW", "messages.raw")
    kafka_topic_dlq: str = os.getenv("KAFKA_TOPIC_DLQ", "messages.dlq")


settings = Settings()
