# Amalitech Data Engineering Lab Submission
This repository contains the code and documentation for my submission to the Amalitech Data Engineering Labs. The project builds a data pipeline to extract, transform, and load movie data from the TMDB API. 

The pipeline uses **Apache Spark** for distributed processing and **Parquet** for efficient columnar storage. It handles large nested datasets like movie cast and crew without the performance bottlenecks found in traditional CSV or Pickle formats.

## System Requirements
The project requires the following environment:
- **Python 3.14**
- **uv 0.10.11**
- **Java 21 (LTS)** (Required for the Apache Spark engine)

## Installation
To install the project:
1. Clone the repository.
2. Install `uv` by following the [official instructions](https://docs.astral.sh/uv/getting-started/installation).
3. Run `uv sync` in the project directory to install all dependencies.
4. Run `uv run jupyter lab` to open the analysis environment.

## Environment Variables
The project requires access to the TMDB API. You can get an API key [here](https://www.themoviedb.org/settings/api).

**Create a `.env` file in the project root directory:**
```env
TMDB_API_KEY=your_tmdb_api_key
TMDB_API_ACCESS_TOKEN=your_tmdb_api_access_token
TMDB_API_BASE_URL=https://api.themoviedb.org/3
```

## Project Structure
- `main.ipynb`: The primary analysis notebook using PySpark.
- `utils/`: Contains the core logic for API calls, Spark cleaning, and schema definitions.
- `data/`: Stores the raw movie data in Parquet format.
- `docs/`: Includes the final report and appendix generated with Typst.
- `tests/`: Async and Spark-native test suites.

## Usage
To run the analysis:
- Start the Jupyter server: `uv run jupyter lab`
- Open `main.ipynb`.
- Execute the cells to fetch data and generate KPIs.

## Tests
The project uses `pytest` with `pytest-asyncio` for non-blocking API tests and Spark testing.
- Run all tests: `uv run pytest tests`

## Reporting
The final report and visual appendix are written in **Typst**.
- The source files are located in the `docs/` directory.
- Plots used in the report are automatically saved to `docs/plots/`.

## Key Features
- **Async Data Fetching:** Uses `httpx` and `tenacity` for fast, resilient API requests.
- **Spark Cleaning Pipeline:** Uses higher-order functions to flatten nested movie data efficiently.
- **Parquet Storage:** Uses columnar storage to preserve complex structures like cast and crew.
- **Automated Reporting:** Integrates Python-generated plots with Typst for professional PDF documentation.