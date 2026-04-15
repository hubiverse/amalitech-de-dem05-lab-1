from pyspark.sql.types import *

MovieGenreSchema = StructType([
    StructField("id", IntegerType(), True),
    StructField("name", StringType(), True),
])

MovieProductionCompanySchema = StructType([
    StructField("id", IntegerType(), True),
    StructField("name", StringType(), True),
    StructField("logo_path", StringType(), True),
    StructField("origin_country", StringType(), True),
])

MovieProductionCountrySchema = StructType([
    StructField("iso_3166_1", StringType(), True),
    StructField("name", StringType(), True),
])

MovieSpokenLanguageSchema = StructType([
    StructField("iso_639_1", StringType(), True),
    StructField("name", StringType(), True),
    StructField("english_name", StringType(), True),
])

MovieCastSchema = StructType([
    StructField("adult", BooleanType(), True),
    StructField("gender", IntegerType(), True),
    StructField("id", IntegerType(), True),
    StructField("known_for_department", StringType(), True),
    StructField("name", StringType(), True),
    StructField("original_name", StringType(), True),
    StructField("popularity", DoubleType(), True),
    StructField("profile_path", StringType(), True),
    StructField("cast_id", IntegerType(), True),
    StructField("character", StringType(), True),
    StructField("credit_id", StringType(), True),
    StructField("order", IntegerType(), True),
])


MovieCrewSchema = StructType([
    StructField("adult", BooleanType(), True),
    StructField("gender", IntegerType(), True),
    StructField("id", IntegerType(), True),
    StructField("known_for_department", StringType(), True),
    StructField("name", StringType(), True),
    StructField("original_name", StringType(), True),
    StructField("popularity", DoubleType(), True),
    StructField("profile_path", StringType(), True),
    StructField("credit_id", StringType(), True),
    StructField("department", StringType(), True),
    StructField("job", StringType(), True),
])

MovieBelongsToCollectionSchema = StructType([
    StructField("id", IntegerType(), True),
    StructField("name", StringType(), True),
    StructField("poster_path", StringType(), True),
    StructField("backdrop_path", StringType(), True),
])

MovieSchema = StructType([
    StructField("adult", BooleanType(), True),
    StructField("backdrop_path", StringType(), True),
    StructField("belongs_to_collection", MovieBelongsToCollectionSchema, True),
    StructField("budget", DoubleType(), True),

    StructField("genres", ArrayType(MovieGenreSchema), True),
    StructField("homepage", StringType(), True),

    StructField("id", IntegerType(), False),
    StructField("imdb_id", StringType(), True),
    StructField("original_language", StringType(), True),
    StructField("original_title", StringType(), True),
    StructField("overview", StringType(), True),

    StructField("popularity", DoubleType(), True),
    StructField("poster_path", StringType(), True),

    StructField("production_companies", ArrayType(MovieProductionCompanySchema), True),
    StructField("production_countries", ArrayType(MovieProductionCountrySchema), True),

    StructField("release_date", StringType(), True),
    StructField("revenue", DoubleType(), True),
    StructField("runtime", DoubleType(), True),

    StructField("spoken_languages", ArrayType(MovieSpokenLanguageSchema), True),

    StructField("status", StringType(), True),
    StructField("tagline", StringType(), True),
    StructField("title", StringType(), True),

    StructField("video", BooleanType(), True),
    StructField("vote_average", DoubleType(), True),
    StructField("vote_count", IntegerType(), True),

    StructField("cast", ArrayType(MovieCastSchema), True),
    StructField("crew", ArrayType(MovieCrewSchema), True),
])