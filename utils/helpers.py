"""
Author: Hubert Apana
Date: 2026-03-18

Helper functions for the application. Utils functions are used to perform various operations on data.
"""

from pathlib import Path
from pyspark.sql import DataFrame, SparkSession, functions as F
from pyspark.sql.window import Window
from typing import Iterable, List, Optional

from .types import Movie
from .schema import MovieSchema


def movie_url(base_url: str, movie_id: int) -> str:
    return f"{base_url.rstrip('/')}/movie/{movie_id}"


def auth_headers(access_token: str) -> dict[str, str]:
    return {
        "Authorization": f"Bearer {access_token}",
        "Accept": "application/json",
    }


def default_cache_path() -> Path:
    return Path(__file__).parent.parent / "data" / "tmdb_movies.parquet"


def save_dataframe(df: DataFrame, path: Path) -> None:
    """Saves Spark DataFrame to Parquet."""
    try:
        df.write.mode("overwrite").parquet(str(path))
    except Exception as e:
        raise Exception(f"Failed to save Spark DataFrame to Parquet: {path}. Error: {e}")


def load_dataframe(spark: SparkSession, path: Path) -> DataFrame:
    """Loads Spark DataFrame from Parquet."""
    if path.exists():
        try:
            return spark.read.parquet(str(path))
        except Exception as e:
            raise Exception(f"Failed to read Parquet file: {path}. Error: {e}")
    raise FileNotFoundError(f"No cached Parquet found at {path}")


def to_dataframe(spark: SparkSession, movies: Iterable[Movie]) -> DataFrame:
    """
    Converts Pydantic Movie objects to a Spark DataFrame.
    """
    rows = [movie.model_dump() for movie in movies]
    return spark.createDataFrame(rows, schema=MovieSchema)


def get_missing_ids(
        spark: SparkSession,
        movie_ids: list[int],
        cached_df: DataFrame | None
) -> list[int]:
    ids_df = spark.createDataFrame([(i,) for i in movie_ids], ["id"])

    if cached_df is None:
        return movie_ids

    missing_df = (
        ids_df.join(
            cached_df.select("id").distinct(),
            on="id",
            how="left_anti"
        )
    )

    return [r.id for r in missing_df.collect()]


def merge_movies_dataframe(cached_df: DataFrame | None, new_df: DataFrame) -> DataFrame:
    """
    Merges two Spark DataFrames. New records overwrite old ones based on 'id'.
    """
    if cached_df is None:
        return new_df

    cached_df = cached_df.withColumn("_src", F.lit(0))
    new_df = new_df.withColumn("_src", F.lit(1))

    combined = cached_df.unionByName(new_df)

    w = Window.partitionBy("id").orderBy(F.col("_src").desc())

    return (
        combined
        .withColumn("rn", F.row_number().over(w))
        .filter("rn = 1")
        .drop("rn", "_src")
    )

def filter_movies_by_ids(movie_df: DataFrame | None, movie_ids: List[int]) -> Optional[DataFrame]:
    """
    Filters DataFrame and preserves the order of movie_ids using a Spark join.
    """

    if movie_df is None or not movie_ids:
        return movie_df

    spark = movie_df.sparkSession

    spark = movie_df.sparkSession
    ids_df = spark.createDataFrame([(i,) for i in movie_ids], ["id"])

    return movie_df.join(ids_df, on="id", how="inner")


def dedupe_keep_order(cols: list[str]) -> list[str]:
    seen = set()
    out = []
    for c in cols:
        if c not in seen:
            seen.add(c)
            out.append(c)
    return out


def rank_movies(movie_df: DataFrame, column: str, n: int = 10, ascending: bool = False, min_votes: int = 0,
                min_budget: float = 0) -> DataFrame:
    """
    Ranks movies in Spark based on a specific column with quality filters.
    """
    # Determine sort order
    sort_order = F.asc(column) if ascending else F.desc(column)

    cols = dedupe_keep_order([
        "title",
        column,
        "vote_count",
        "budget_musd",
        "revenue_musd"
    ])

    return (
        movie_df
        .filter(F.col(column).isNotNull())
        .filter((F.col("vote_count") >= min_votes) & (F.col("budget_musd") >= min_budget))
        .withColumn("budget_musd", F.round("budget_musd", 3))
        .withColumn("revenue_musd", F.round("revenue_musd", 3))
        .withColumn("profit_musd", F.round("profit_musd", 3))
        .withColumn("roi", F.round("roi", 3))
        .orderBy(sort_order)
        .select(*cols)
        .limit(n)
    )
