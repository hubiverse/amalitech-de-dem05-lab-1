"""
Author: Hubert Apana
Date: 2026-03-18
"""

import asyncio
from pyspark.sql import SparkSession, DataFrame
from tqdm.asyncio import tqdm
import httpx
import logging
from typing import List, Tuple, Optional
from tenacity import (
    AsyncRetrying,
    stop_after_attempt,
    wait_exponential,
    retry_if_exception_type,
    before_sleep_log,
)
from pathlib import Path
from typing import Iterable

from config import Settings, get_settings
from utils.types import Movie
from utils.helpers import (
    movie_url,
    auth_headers,
    default_cache_path,
    load_dataframe,
    to_dataframe,
    save_dataframe,
    merge_movies_dataframe,
    filter_movies_by_ids,
    get_missing_ids,
)

# Setup Logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("TMDB_Pipeline")


#  CUSTOM EXCEPTIONS
class TMDBHardError(Exception): """4xx errors except 429 - do not retry."""


class TMDBSoftError(Exception): """5xx or 429 errors - retryable."""


async def fetch_movie_by_id(
        client: httpx.AsyncClient,
        settings: Settings,
        movie_id: int,
        semaphore: asyncio.Semaphore,
        max_retries: int = 3,
        wait_factor: float = 2.0
) -> Optional[Movie]:
    async with semaphore:
        try:
            async for attempt in AsyncRetrying(
                    retry=retry_if_exception_type(TMDBSoftError),
                    stop=stop_after_attempt(max_retries + 1),
                    wait=wait_exponential(multiplier=wait_factor, min=1, max=10),
                    before_sleep=before_sleep_log(logger, logging.WARNING),
                    reraise=True
            ):
                with attempt:
                    url = movie_url(settings.tmdb_api_base_url, movie_id)
                    params = {"append_to_response": "credits"}

                    response = await client.get(
                        url,
                        headers=auth_headers(settings.tmdb_api_access_token),
                        params=params
                    )

                    status = response.status_code
                    if status == 429 or 500 <= status < 600:
                        raise TMDBSoftError(f"Status {status}")
                    if 400 <= status < 500:
                        raise TMDBHardError(f"Status {status}")

                    response.raise_for_status()
                    data = response.json()

                    if "credits" in data:
                        data["cast"] = data["credits"].get("cast", [])
                        data["crew"] = data["credits"].get("crew", [])

                    return Movie.model_validate(data)

        except (TMDBHardError, Exception) as e:
            logger.error(f"Final failure for Movie ID {movie_id}: {str(e)}")
            return None


async def download_movies_by_ids(
        settings: Settings,
        movie_ids: Iterable[int],
        max_retries: int = 3,
        wait_factor: float = 2.0,
        max_workers: int = 10
) -> Tuple[List[Movie], List[int]]:
    valid_movies: List[Movie] = []
    failed_ids: List[int] = []
    semaphore = asyncio.Semaphore(max_workers)

    async with httpx.AsyncClient(timeout=15.0) as client:
        tasks = [fetch_movie_by_id(client, settings, mid, semaphore, max_retries=max_retries, wait_factor=wait_factor)
                 for mid in movie_ids]

        results: List[Optional[Movie]] = await tqdm.gather(*tasks, desc="Fetching Movies")

        for movie_id, movie in zip(movie_ids, results):
            if movie:
                valid_movies.append(movie)
            else:
                failed_ids.append(movie_id)

    return valid_movies, failed_ids


async def get_movies_df_from_ids(
        spark: SparkSession,
        movie_ids: List[int],
        settings: Settings | None = None,
        cache_path: Optional[Path] = None,
        force_redownload: bool = False,
        max_retries: int = 3,
        wait_factor: float = 2.0,
        max_workers: int = 10
) -> Tuple[Optional[DataFrame], List[int]]:
    path = cache_path or default_cache_path()
    config = get_settings() if settings is None else settings

    try:
        cached_df = load_dataframe(spark, path)
    except (FileNotFoundError, Exception):
        cached_df = None

    ids_to_download = movie_ids if force_redownload else get_missing_ids(spark, movie_ids, cached_df)

    all_failed_ids = []

    if ids_to_download:
        downloaded_movies, failed_ids = await download_movies_by_ids(
            config,
            ids_to_download,
            max_retries=max_retries,
            wait_factor=wait_factor,
            max_workers=max_workers
        )

        all_failed_ids = failed_ids

        if downloaded_movies:
            new_df = to_dataframe(spark, downloaded_movies)

            if cached_df is not None:
                cached_df = cached_df.localCheckpoint(eager=True)

            cached_df = merge_movies_dataframe(cached_df, new_df)

            save_dataframe(cached_df, path)

    final_df = filter_movies_by_ids(cached_df, movie_ids)
    return final_df, all_failed_ids
