from pydantic_settings import BaseSettings
from typing import List


class Settings(BaseSettings):
    # Database
    DATABASE_URL: str = "postgresql+asyncpg://postgres:password@localhost:5432/loopsense"

    # Groq — Llama 3.2 3B hazard classification (free tier: console.groq.com)
    GROQ_API_KEY: str = ""

    # Anthropic Claude — LoopNav AI natural language routing
    ANTHROPIC_API_KEY: str = ""

    # CTA — Customer Alerts (public, no key) + Train Tracker (free key required)
    CTA_API_KEY: str = ""
    CTA_TRAIN_TRACKER_KEY: str = ""

    # Weather
    OPENWEATHER_API_KEY: str = ""

    # Chicago Data Portal
    CHICAGO_APP_TOKEN: str = ""

    # Redis (LoopNav caching)
    REDIS_URL: str = "redis://localhost:6379"

    # LoopNav graph — flip to false when Dev1's real graph is ready
    USE_STUB_GRAPH: bool = True

    # App
    DEBUG: bool = True
    CORS_ORIGINS: List[str] = ["http://localhost:3000", "http://localhost:5173"]
    REPORT_EXPIRY_HOURS: int = 6

    class Config:
        env_file = ".env"
        extra = "ignore"


settings = Settings()
