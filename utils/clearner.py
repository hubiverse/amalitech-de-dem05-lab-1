"""
Author: Hubert Apana
Date: 2026-03-18

Data cleaning function for movie DataFrame.

This module provides a utility function for cleaning and preprocessing
a movie dataset represented as a pandas DataFrame. The function handles
operations such as dropping irrelevant columns, extracting relevant data
from JSON-like columns, type conversions, zero-value handling, value imputation,
string cleanup, and filtering to ensure the dataset is prepared for subsequent
analysis or modeling.

Functions:
    - clean_movie_df: Cleans and preprocesses the given DataFrame based
      on specified parameters and returns the cleaned DataFrame.
"""
from pyspark.sql import DataFrame, functions as F


def clean_movie_df(df: DataFrame, cols_to_drop=None, final_column_order=None) -> DataFrame:
    """
    Spark implementation of TMDB movie data cleaning.
    """

    if final_column_order is None: final_column_order = []
    if cols_to_drop is None: cols_to_drop = []

    #  Calculate Median Runtime
    median_runtime = df.stat.approxQuantile("runtime", [0.5], 0.01)[0]

    df_cleaned = (
        df
        # Handle Computed Fields first
        .withColumn("cast_size", F.size("cast"))
        .withColumn("crew_size", F.size("crew"))
        .withColumn("director",
                    F.expr("array_join(transform(filter(crew, x -> lower(x.job) == 'director'), x -> x.name), '|')"))

        # Data Cleaning & Standardizing
        .withColumn("id", F.col("id").cast("long"))
        .withColumn("budget", F.when(F.col("budget") > 0, F.col("budget")).otherwise(F.lit(None)))
        .withColumn("revenue", F.when(F.col("revenue") > 0, F.col("revenue")).otherwise(F.lit(None)))
        .withColumn("runtime", F.coalesce(F.col("runtime").cast("double"), F.lit(median_runtime)))

        # Vote Logic
        .withColumn("vote_count", F.coalesce(F.col("vote_count").cast("int"), F.lit(0)))
        .withColumn("vote_average",
                    F.when(F.col("vote_count") == 0, F.lit(None)).otherwise(F.col("vote_average").cast("double")))

        # Nested Structure Extraction
        .withColumn("belongs_to_collection", F.col("belongs_to_collection.name"))
        .withColumn("genres",
                    F.array_join(
                        F.expr("transform(genres, x -> x.name)"),
                        "|"
                    ))
        .withColumn("spoken_languages",
                    F.array_join(
                        F.expr("transform(spoken_languages, x -> x.english_name)"),
                        "|"
                    ))
        .withColumn("production_countries",
                    F.array_join(
                        F.expr("transform(production_countries, x -> x.name)"),
                        "|"
                    ))
        .withColumn("production_companies",
                    F.array_join(
                        F.expr("transform(production_companies, x -> x.name)"),
                        "|"
                    ))
        .withColumn("cast",
                    F.array_join(
                        F.expr("transform(cast, x -> x.name)"),
                        "|"
                    ))

        # Dates & Derived Columns
        .withColumn("release_date",
                    F.to_date(
                        F.when(F.col("release_date") == "", None).otherwise(F.col("release_date"))
                    ))
        .withColumn("budget_musd", F.col("budget") / 1_000_000)
        .withColumn("revenue_musd", F.col("revenue") / 1_000_000)

        # String Cleanup
        .withColumn("overview", F.when(F.col("overview").isin("No Data", ""), F.lit(None)).otherwise(F.col("overview")))
        .withColumn("tagline", F.when(F.col("tagline").isin("No Data", ""), F.lit(None)).otherwise(F.col("tagline")))
    )

    # Filtering & Housekeeping
    df_cleaned = (
        df_cleaned
        # Filter: Only Released movies
        .filter(F.col("status") == "Released")

        # Drop requested columns + standard drops
        .drop(*(cols_to_drop + ["status", "crew"]))

        # Ensure ID and Title are present
        .dropna(subset=["id", "title"])

        # Remove duplicates
        .dropDuplicates(["id"])

        # Threshold: Keep rows with at least 10 non-null values
        .dropna(thresh=10)
    )

    # Final Reordering
    if final_column_order:
        # Check which columns actually exist before selecting
        existing_cols = [c for c in final_column_order if c in df_cleaned.columns]
        df_cleaned = df_cleaned.select(*existing_cols)

    return df_cleaned


def compute_finances(df: DataFrame) -> DataFrame:
    """
    Calculates financial metrics including profit and return on investment (ROI) based on the input DataFrame.
    """
    final_df = (
        df
        .withColumn("profit_musd", F.col("revenue_musd") - F.col("budget_musd"))
        .withColumn("roi", F.col("revenue_musd") / F.col("budget_musd"))
    )

    return final_df
