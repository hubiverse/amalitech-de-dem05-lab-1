import pytest_asyncio
import httpx
import asyncio
import pytest
from pathlib import Path

from config import Settings


@pytest.fixture
def mock_settings():
    return Settings(
        tmdb_api_base_url="https://api.themoviedb.org/3",
        tmdb_api_access_token="fake_token",
        tmdb_api_key="fake_key"
    )

@pytest_asyncio.fixture
async def client():
    """Provides an async client for the duration of the test."""
    async with httpx.AsyncClient() as client:
        yield client

@pytest_asyncio.fixture
async def semaphore():
    """Provides a semaphore to control concurrency in tests."""
    return asyncio.Semaphore(1)

@pytest.fixture
def tmp_path():
    dir_path =  Path(__file__).parent / "data"
    Path(dir_path).mkdir(parents=True, exist_ok=True)
    return dir_path

@pytest.fixture(scope="session")
def spark_session():
    from pyspark.sql import SparkSession
    spark = (
        SparkSession.builder
        .appName("tmdb_pipeline_test")
        .master("local[*]")
        .config("spark.sql.shuffle.partitions", "8")
        .config("spark.driver.memory", "4g")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("ERROR")
    yield spark
    spark.stop()
