"""

"""

import pandas as pd
from utils import clean_movie_df
import datetime
from pyspark.sql import DataFrame



def test_cleaning_pipeline_logic(spark_session):
    budget = 356000000
    revenue = 2799439100

    # Create the raw data in Spark
    raw_data = spark_session.createDataFrame([{
        "id": 299534,
        "title": "Avengers: Endgame",
        "budget": budget,
        "revenue": revenue,
        "genres": [{"name": "Action"}, {"name": "Sci-Fi"}],
        "crew": [
            {"name": "Anthony Russo", "job": "Director"},
            {"name": "Joe Russo", "job": "Director"}
        ],
        "cast": [{"name": "Robert Downey Jr."}, {"name": "Chris Evans"}],
        "belongs_to_collection": {"name": "Avengers Collection"},
        "production_countries": [{"iso_3166_1": "US", "name": "United States of America"}],
        "production_companies": [{"name": "Marvel Studios"}, {"name": "DC Comics"}],
        "spoken_languages": [{"english_name": "English", "iso_639_1": "en"}],
        "popularity": 10.0,
        "status": "Released",
        "tagline": "Never again will humanity be the same.",
        "release_date": "2019-04-24",
        "overview": "The Avengers and their allies must be willing to sacrifice all.",
        "vote_count": 10,
        "vote_average": 8.2,
        "runtime": 181
    }])

    # Run your Spark cleaning function
    cleaned_df = clean_movie_df(raw_data)

    # Collect the first row to Python
    results = cleaned_df.collect()
    assert len(results) == 1
    row = results[0]

    budget_musd = budget / 1_000_000
    revenue_musd = revenue / 1_000_000

    # Value Assertions
    assert row["belongs_to_collection"] == "Avengers Collection"
    assert row["production_countries"] == "United States of America"
    assert row["production_companies"] == "Marvel Studios|DC Comics"
    assert row["spoken_languages"] == "English"
    assert row["genres"] == "Action|Sci-Fi"
    assert row["director"] == "Anthony Russo|Joe Russo"
    assert row["budget_musd"] == budget_musd
    assert row["revenue_musd"] == revenue_musd

    # Date Assertion
    assert isinstance(row["release_date"], datetime.date)
    assert row["release_date"].year == 2019

    schema_dict = {f.name: f.dataType.typeName() for f in cleaned_df.schema}

    assert schema_dict["id"] in ["long", "integer"]
    assert schema_dict["budget"] in ["double", "float", "long"]
    assert schema_dict["revenue"] in ["double", "float", "long"]
    assert schema_dict["popularity"] in ["double", "float"]
    assert schema_dict["vote_count"] in ["integer", "long"]
    assert schema_dict["runtime"] in ["double", "float", "integer"]
    assert schema_dict["vote_average"] in ["double", "float"]
