"""
Unit tests for validating TMDb movie data retrieval and cache logic.

These test cases are designed to verify that the `get_movies_dataframe_from_ids`
method behaves as expected under different conditions involving cached data, API calls,
and forced redownloads. Specifically, it ensures that the function respects cache files,
handles API interactions correctly, and can gracefully handle failed requests.

Fixtures:
    mock_settings: A pytest fixture that provides mock configuration data for the
        application, including TMDb API base URL, access token, and API key.

Tests:
    test_cache_logic_avoids_network_call:
        Verifies that no network call is made when a requested movie ID is already
        present in the cache.

    test_cache_partial_hit:
        Ensures that only uncached movie IDs trigger API calls while cached IDs are skipped.

    test_force_redownload_ignores_cache:
        Validates that setting `force_redownload` to True forces API calls even for movie
        IDs present in the cache.

    test_failed_api_returns_ids_and_doesnt_crash:
        Confirms that 404 errors return failed IDs properly in the `failed` list and the
        function does not crash when such scenarios occur.
"""

import pytest
import respx
from httpx import Response

from utils import get_movies_df_from_ids, movie_url, save_dataframe
from utils.schema import MovieSchema




@pytest.mark.asyncio
@respx.mock
async def test_cache_logic_avoids_network_call(mock_settings, tmp_path, spark_session):
    """
    Scenario: ID 19995 is already in the CSV.
    Expectation: No API call is made when requesting ID 19995.
    """
    movie_id = 19995
    # Setup a fake cache file
    cache_file = tmp_path / "cache1.parquet"
    save_dataframe(
        spark_session.createDataFrame([{"id": movie_id, "title": "Avatar"}], schema=MovieSchema),
        cache_file
    )

    # Mock the API
    api_route = respx.get(movie_url(mock_settings.tmdb_api_base_url, movie_id)).mock(
        return_value=Response(200, json={"id": movie_id, "title": "Avatar"})
    )

    # Run the function
    df, failed = await get_movies_df_from_ids(
        spark=spark_session,
        settings=mock_settings,
        movie_ids=[movie_id],
        cache_path=cache_file,
        force_redownload=False
    )

    assert df is not None
    assert api_route.call_count == 0
    assert not df.rdd.isEmpty()
    row = df.first()
    assert row is not None
    assert row["title"] == "Avatar"


@pytest.mark.asyncio
@respx.mock
async def test_cache_partial_hit(mock_settings, tmp_path, spark_session):
    """
    Scenario: ID 1 is cached, ID 2 is not.
    Expectation: Only 1 API call is made (for ID 2).
    """
    movie_1_id = 1
    cache_file = tmp_path / "cache2.parquet"
    save_dataframe(
        spark_session.createDataFrame([{"id": movie_1_id, "title": "Movie 1"}], schema=MovieSchema),
        cache_file
    )

    # Mock API for movie 2
    movie_2_id = 2
    route_2 = respx.get(movie_url(mock_settings.tmdb_api_base_url, movie_2_id)).mock(
        return_value=Response(200, json={"id": movie_2_id, "title": "Movie 2", "status": "Released"})
    )
    # Mock API for movie 1 (should not be called)
    route_1 = respx.get(movie_url(mock_settings.tmdb_api_base_url, movie_1_id)).mock(Response(200))

    df, failed = await get_movies_df_from_ids(
        spark_session,
        settings=mock_settings,
        movie_ids=[movie_1_id, movie_2_id],
        cache_path=cache_file,
    )



    assert route_2.call_count == 1
    assert route_1.call_count == 0
    assert  df is not None
    assert df.count() == 2


@pytest.mark.asyncio
@respx.mock
async def test_force_redownload_ignores_cache(mock_settings, tmp_path, spark_session):
    """
    Scenario: ID 1 is cached, but force_redownload is True.
    Expectation: API is called despite existing data.
    """
    movie_id = 1
    cache_file = tmp_path / "cache3.parquet"
    save_dataframe(
        spark_session.createDataFrame([{"id": movie_id, "title": "Old Title"}], schema=MovieSchema),
        cache_file
    )

    api_route = respx.get(movie_url(mock_settings.tmdb_api_base_url, movie_id)).mock(
        return_value=Response(200, json={"id": movie_id, "title": "New Title", "status": "Released"})
    )

    df, failed = await get_movies_df_from_ids(
        spark_session,
        settings=mock_settings,
        movie_ids=[movie_id],
        cache_path=cache_file,
        force_redownload=True
    )

    assert api_route.call_count == 1
    assert df is not None

    row = df.first()
    assert row is not None
    assert row["title"] == "New Title"
    assert row["id"] == movie_id
    assert df.count() == 1


@pytest.mark.asyncio
@respx.mock
async def test_failed_api_returns_ids_and_doesnt_crash(mock_settings, tmp_path, spark_session):
    """
    Scenario: Requesting a movie that doesn't exist (404).
    Expectation: The ID is returned in the 'failed' list.
    """
    cache_file = tmp_path / "no_file.parquet"
    movie_id = 404
    respx.get(movie_url(mock_settings.tmdb_api_base_url, movie_id)).mock(return_value=Response(404))

    df, failed = await get_movies_df_from_ids(
        spark_session,
        settings=mock_settings,
        movie_ids=[movie_id],
        cache_path=cache_file
    )

    assert movie_id in failed
    assert df is None
