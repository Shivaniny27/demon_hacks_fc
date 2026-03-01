from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine, async_sessionmaker
from sqlalchemy.orm import DeclarativeBase
from app.config import settings

engine = create_async_engine(
    settings.DATABASE_URL,
    echo=settings.DEBUG,
    pool_pre_ping=True,
    pool_size=10,
    max_overflow=20,
)

AsyncSessionLocal = async_sessionmaker(
    engine,
    class_=AsyncSession,
    expire_on_commit=False,
)


class Base(DeclarativeBase):
    pass


async def get_db():
    async with AsyncSessionLocal() as session:
        try:
            yield session
        finally:
            await session.close()


async def init_db():
    """Create all tables. Also enables PostGIS extension if available."""
    import sqlalchemy
    # PostGIS in its own transaction so a failure doesn't abort create_all
    async with engine.begin() as conn:
        try:
            await conn.execute(sqlalchemy.text("CREATE EXTENSION IF NOT EXISTS postgis;"))
        except Exception:
            pass  # PostGIS not installed — fall back to plain lat/lng
    # Schema creation in a fresh transaction
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
