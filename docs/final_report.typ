#set page(paper: "a4", margin: (x: 2cm, y: 2.5cm))
#set text(font: "Libertinus Serif", size: 11pt)
#set heading(numbering: "1.")

// --- Title Section ---
#align(center)[
  #text(size: 24pt, weight: "bold")[TMDB Movie Data Analysis Report] \
  #text(size: 14pt, style: "italic", fill: gray)[High-Performance Engineering with Pydantic & Pandas] \
  #v(1cm)
  #grid(
    columns: (1fr, 1fr),
    align(left)[*Author:* Hubert Apana],
    align(right)[*Date:* #datetime.today().display()]
  )
  #line(length: 100%, stroke: 0.5pt + gray)
]

#v(1cm)

= Introduction
This report describes a movie data analysis pipeline built with Apache Spark. The project objective was to create a scalable system for processing film data. Using the The Movie Database (TMDB) API, the pipeline collects detailed information on finances, cast, and crew. This data helps identify trends in the global movie market.

The project focus was on code efficiency and data reliability. It uses Spark's distributed architecture and strict schema validation to ensure the analysis is fast and accurate.

= Methodology
The technical workflow uses a modern stack optimized for large datasets and parallel processing.

== Data Acquisition and Resilience
We used an asynchronous approach with the `httpx` library to fetch data quickly.
- *Schema Validation:* `Pydantic` models enforced data types during ingestion. This moved the logic into the data layer, where `spark schema` was used to enforce strict typing and validation
- *API Optimization:* We used the `append_to_response` parameter to get movie details and credits in one request.
- *Retry Logic:* We used the `tenacity` library to handle network issues. It uses exponential backoff to retry requests when the API is busy.

== Columnar Caching and Persistence
The project moved from Pickle to *Apache Parquet* for local storage.
- *Nested Data:* Parquet stores complex structures like lists of crew members as native objects. This is much faster than saving them as strings.
- *Efficiency:* Spark reads specific columns from a Parquet file without loading the whole dataset. This reduced memory usage and improved speed during analysis.

== Distributed Cleaning Pipeline
We cleaned the data using the Spark DataFrame API and functional method chaining.
- *Normalization:* We converted zero values in budget and revenue columns to null values. This ensures that calculations like ROI are accurate.
- *Median Imputation:* We filled missing runtime values with the dataset median to keep the statistics balanced.
- *Native Transformations:* We used Spark higher-order functions to flatten nested lists into strings. This replaced slow Python loops with optimized Spark code.

== Quality Assurance
We used `pytest` to verify the pipeline.
- *API Mocking:* The `respx` library simulated different API responses. This allowed us to test how the system handles errors without using real API credits.
- *Cache Testing:* Integration tests confirmed that the system correctly skips the network when a movie is already in the Parquet cache.

= Summary of Key Insights
The TMDB dataset shows several important financial and critical trends. The addition of Bruce Willis and Quentin Tarantino movies has slightly shifted some metrics, but the general patterns remain clear.

== Franchises vs. Standalone Movies
The data reveals a gap in performance between original films and franchises.
- *Efficiency:* Standalone movies often have a higher median ROI (around 1.25x) compared to franchises.
- *Revenue Power:* Franchises generate significantly more revenue on average (588 million USD) than standalone films (97 million USD).
- *Budget Risk:* Franchises have much higher average budgets (97 million USD) compared to standalone projects (36 million USD).

== Genre Investment Strategies
Sci-Fi and Action movies create the most revenue, but they require high initial costs.
- *High ROI Genres:* Lower-budget genres like Comedy and Romance often show a more stable return on investment.
- *The Blockbuster Trend:* Sci-Fi and Adventure genres are highly efficient because they appeal to a global audience.

== Director and Actor Influence
The analysis of directors and specific actors shows where commercial success is concentrated.
- *Tarantino Impact:* Movies directed by Quentin Tarantino show high average ratings and longer runtimes.
- *Commercial Success:* The Russo Brothers and James Cameron lead in total revenue. They have maintained high ratings across massive projects.
- *Bruce Willis:* Adding Bruce Willis movies shows that consistent action stars drive steady popularity scores even when ratings vary.

== Chronological Growth
Box office performance peaked between 2015 and 2019. This period saw the largest gap between total budgets and total revenue. This was driven by the peak of the Marvel Cinematic Universe and the Star Wars sequels.

= Conclusion
The Spark and Parquet pipeline creates a robust and fast environment for business intelligence. By using Pydantic for validation and Spark for distributed processing, the system is prepared for much larger datasets.

From a business perspective, franchises offer a high revenue ceiling but require massive investment. Standalone films and character-driven genres offer higher capital efficiency. For movie studios, the most reliable results appear in the 150 million to 200 million dollar budget range. Projects that exceed this budget face a higher risk of diminishing returns.


#pagebreak()
#include "appendix.typ"
