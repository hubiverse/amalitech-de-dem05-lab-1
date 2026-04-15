from __future__ import annotations

from pathlib import Path
import sys

import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
import asyncio
from typing import cast
from pyspark.sql import SparkSession, DataFrame, functions as F

project_root = Path(__file__).parent.parent.parent
sys.path.append(str(project_root))

from utils import (
    get_movies_df_from_ids,
    clean_movie_df,
    compute_finances,
    rank_movies
)


def df_to_typst(df_spark: DataFrame, caption="", columns_spec=None, label=""):
    df = df_spark.toPandas()

    if not columns_spec:
        columns_spec = f"({', '.join(['1fr'] * len(df.columns))})"

    typst_lines = []
    typst_lines.append(f"#figure(")
    typst_lines.append(f"  table(")
    typst_lines.append(f"    columns: {columns_spec},")
    typst_lines.append(f"    inset: 10pt,")
    # Logic: If column (x) is 0, align left. Otherwise, center.
    typst_lines.append(f"    align: (x, y) => if x == 0 {{ left + horizon }} else {{ center + horizon }},")
    typst_lines.append(f"    fill: (x, y) => if y == 0 {{ gray.lighten(60%) }},")

    headers = ", ".join([f"[* {col.replace('_', ' ').title()} *]" for col in df.columns])
    typst_lines.append(f"    {headers},")

    for _, row in df.iterrows():
        row_vals = []
        for val in row:
            if pd.isna(val):
                row_vals.append("[---]")
            elif isinstance(val, (int, float)):
                row_vals.append(f"[{val:,.2f}]")
            else:
                row_vals.append(f"[{str(val)}]")
        typst_lines.append(f"    {', '.join(row_vals)},")

    typst_lines.append("  ),")
    if caption: typst_lines.append(f"  caption: [{caption}],")
    if label:
        typst_lines.append(f") <{label}>")
    else:
        typst_lines.append(")")

    return "\n".join(typst_lines)


async def run_data_analysis():
    # Download movies
    movie_ids = [
        299534, 19995, 140607, 299536, 597, 135397, 420818, 24428, 168259, 99861, 284054, 12445, 181808, 330457,
        351286,
        109445, 321612, 260513, 476161, 5, 1180681, 9741, 7183, 18, 2069, 1035803, 1035806, 1024546, 1571, 1572, 1573,
        761892,
        46122, 562, 5172, 774714, 63, 59967, 278086, 1507910, 2122, 2124, 9292, 1113682, 32855, 39514, 95, 30815, 3172,
        718949,
        241254, 9319, 136296, 3179, 9333, 50298, 77948, 8324, 8838, 531593, 9356, 17043, 4244, 480404, 280217, 9882,
        1414301,
        9374, 2207, 38560, 1637024, 24226, 10403, 163, 686245, 680, 137896, 234158, 486068, 186, 187, 454330, 189,
        23742, 12479,
        85693, 986824, 83666, 118483, 169173, 20694, 395990, 4824, 894169, 14043, 181471, 384737, 285923, 82150, 745,
        8944,
        872177, 504562, 641790, 9471, 18176, 584962, 7944, 921353, 1645833, 921355, 921360, 9494, 326425, 763164,
        1265440,
        7457, 146216, 542508, 883502, 139567, 32047, 651571, 345915, 479040, 11074, 11593, 345934, 84305, 1528146,
        916821,
        47964, 7518, 9567, 10592, 31586, 1460067, 30565, 12647, 381288, 360295, 126314, 829799, 787310, 72559, 843633,
        9586,
        864116, 25975, 693113, 826749, 76163, 135051, 918, 1533851, 145308, 681887, 450465, 552865, 43939, 714666,
        1049516,
        9644, 70586, 27578, 12220, 410554, 132542, 1146302, 766907, 153538, 31683, 742341, 28614, 1992, 307663, 75736,
        253414,
        1127399, 2026, 63472, 359412, 19959, 536056, 724989, 290304, 5, 66566, 22538, 67083, 1588237, 278542, 62488, 24,
        754721,
        1502241, 761892, 99368, 185896, 46122, 374317, 20013, 1173040, 539199, 1261119, 480834, 35907, 124998, 342091,
        79, 1623134,
        1228384, 1341540, 281702, 68718, 114287, 1071215, 58492, 1058940, 962192, 245906, 97430, 455319, 533658, 1690,
        1691, 458399,
        171168, 986277, 680, 92850, 1567925, 184, 187, 20668, 1598142, 1002181, 1110728, 986824, 1457866, 414419, 13025,
        285923, 63206, 594158, 413422, 1418478, 241, 1145586, 755, 443129, 515834, 36606, 1645833, 56591, 199951,
        225554, 8982,
        28447, 166183, 333106, 224562, 61752, 44345, 289083, 319, 12095, 16194, 44535, 144708, 13637, 1445188, 101204,
        466272,
        273248, 540003, 1242980, 1460067, 1088359, 631143, 1005428, 393076, 1629557, 288122, 1145722, 833916, 8068, 393,
        56224,
        19361, 149922, 396194, 1310632, 20910, 82865, 399794, 1065395, 264117, 1146302, 1010623, 1991, 1992, 339403,
        1390028,
        9678, 12241, 353746, 102868, 161239, 19416, 85984, 16869, 10213, 500, 1569780, 13300, 599031, 854521, 464890,
        507
    ]

    spark = (
        SparkSession.builder
        .appName("tmdb_pipeline_appendix")
        .master("local[*]")
        .config("spark.sql.shuffle.partitions", "8")
        .config("spark.driver.memory", "4g")
        .getOrCreate()
    )

    df, failed = await get_movies_df_from_ids(spark, movie_ids=movie_ids)

    if failed:
        print(f"\nThe following IDs could not be fetched: {failed}")

    # Cast to avoid type warning
    df = cast(DataFrame, df)

    # Define the columns to drop
    cols_to_drop = ['adult', 'imdb_id', 'original_title', 'video', 'homepage']

    # Define the final column order as per instructions
    final_column_order = [
        'id', 'title', 'tagline', 'release_date', 'genres', 'belongs_to_collection',
        'original_language', 'budget_musd', 'revenue_musd', 'production_companies',
        'production_countries', 'vote_count', 'vote_average', 'popularity', 'runtime',
        'overview', 'spoken_languages', 'poster_path', 'cast', 'cast_size', 'director', 'crew_size'
    ]

    # Cleaning pipeline using chaining
    df_cleaned = clean_movie_df(df, cols_to_drop=cols_to_drop, final_column_order=final_column_order)

    # Add Profit and ROI columns to our cleaned dataframe
    df_final = compute_finances(df_cleaned)

    # ============= KPIs ===========================
    # 1. Highest Revenue
    top_revenue = rank_movies(df_final, 'revenue_musd', n=5)

    # 2. Highest Budget
    top_budget = rank_movies(df_final, 'budget_musd', n=5)

    # 3. Highest Profit
    top_profit = rank_movies(df_final, 'profit_musd', n=5)

    # 4. Lowest Profit (Worst Financial Performance)
    worst_profit = rank_movies(df_final, 'profit_musd', ascending=True, n=5)

    # 5. Highest ROI (Only movies with Budget >= 10M)
    top_roi = rank_movies(df_final, 'roi', min_budget=10)

    # 6. Lowest ROI (Only movies with Budget >= 10M)
    worst_roi = rank_movies(df_final, 'roi', min_budget=10, ascending=True, n=5)

    # 7. Most Voted
    most_voted = rank_movies(df_final, 'vote_count', n=5)

    # 8. Highest Rated (Only movies with >= 10 votes)
    top_rated = rank_movies(df_final, 'vote_average', min_votes=10, n=5)

    # 9. Lowest Rated (Only movies with >= 10 votes)
    worst_rated = rank_movies(df_final, 'vote_average', min_votes=10, ascending=True, n=5)

    # 10. Most Popular
    most_popular = rank_movies(df_final, 'popularity', n=5)

    # Search 1: Find the best-rated Science Fiction Action movies starring Bruce Willis (sorted by Rating - highest to lowest).
    search_bruce = (
        df_final.filter(F.col("cast").contains("Bruce Willis"))
        .filter(F.col("genres").contains("Science Fiction"))
        .filter(F.col("genres").contains("Action"))
        .orderBy(F.desc("vote_average"))
        .select("title", "vote_average", "genres", "revenue_musd")
    )

    # Search 2: Find movies starring Uma Thurman, directed by Quentin Tarantino (sorted by runtime - shortest to longest).
    search_quentin = (
        df_final.filter(F.col("cast").contains("Uma Thurman"))
        .filter(F.col("director").contains("Quentin Tarantino"))
        .orderBy("runtime")
        .select("title", "runtime", "director")
    )

    dp_num = 3

    # Compare movie franchises (belongs_to_collection) vs. standalone movies
    franchise_comparison = (
        df_final
        .withColumn("status",
                    F.when(F.col("belongs_to_collection").isNull(), "Standalone")
                    .otherwise("Franchise")
                    )
        .groupBy("status")
        .agg(
            F.round(F.mean("revenue_musd"), dp_num).alias("mean_revenue"),
            F.round(F.percentile_approx("roi", 0.5), dp_num).alias("median_roi"),
            F.round(F.mean("budget_musd"), dp_num).alias("mean_budget"),
            F.round(F.mean("popularity"), dp_num).alias("mean_popularity"),
            F.round(F.mean("vote_average"), dp_num).alias("mean_rating")
        )
    )

    #  Find the Most Successful Movie Franchises based on:
    top_franchises = (
        df_final
        .filter(F.col("belongs_to_collection").isNotNull())
        .groupBy("belongs_to_collection")
        .agg(
            F.count("id").alias("movie_count"),
            F.sum("budget_musd").alias("total_budget"),
            F.round(F.mean("budget_musd"), dp_num).alias("mean_budget"),
            F.round(F.sum("revenue_musd"), dp_num).alias("total_revenue"),
            F.round(F.mean("revenue_musd"), dp_num).alias("mean_revenue"),
            F.round(F.mean("vote_average"), dp_num).alias("mean_rating")
        )
        .orderBy(F.desc("movie_count"))
        .limit(10)
    )

    # Find the Most Successful Directors based on:
    top_directors = (
        df_final
        .filter(F.col("director").isNotNull())
        .withColumn("director_name", F.explode(F.split(F.col("director"), "\\|")))
        .groupBy("director_name")
        .agg(
            F.count("id").alias("movie_count"),
            F.round(F.sum("revenue_musd"), dp_num).alias("total_revenue"),
            F.round(F.mean("vote_average"), dp_num).alias("avg_rating")
        )
        .orderBy(F.desc("total_revenue"))
        .limit(10)
    )

    ### ============ Plots ===================
    # Plots directory
    base_dir = Path(__file__).parent
    plots_dir = base_dir / 'plots'
    plots_dir.mkdir(exist_ok=True)
    appendix_file = base_dir / 'appendix.typ'

    # Revenue vs. Budget Trends
    df_rev_vs_budget = (
        df_final
        .select("budget_musd", "revenue_musd", "popularity", "roi")
        .toPandas()
    )

    stats = df_final.select(
        F.max("revenue_musd").alias("max_rev"),
        F.max("budget_musd").alias("max_bud")
    ).collect()[0]

    plt.figure(figsize=(10, 6))
    sns.set_style("whitegrid")

    # Scatter plot
    sns.scatterplot(
        data=df_rev_vs_budget,
        x='budget_musd',
        y='revenue_musd',
        size='popularity',
        hue='roi',
        palette='viridis',
        sizes=(50, 500)
    )

    # Add a diagonal line for breakeven (Revenue = Budget)
    max_val = max(stats["max_rev"] or 0, stats["max_bud"] or 0)
    plt.plot([0, max_val / 5], [0, max_val / 5], color='red', linestyle='--', label='Breakeven Line (1x)')

    plt.title('Movie Success: Revenue vs. Budget (Millions USB)', fontsize=15)
    plt.xlabel('Budget (Million USD)')
    plt.ylabel('Revenue (Million USD)')
    plt.legend(loc="upper left", bbox_to_anchor=(1, 1), fontsize=12)
    plt.tight_layout()
    plt.savefig(plots_dir / "revenue_vs_budget.png", bbox_inches="tight", dpi=300)

    # ROI Distribution by Genre
    df_genre_roi = (
        df_final.select("id", "genres", "roi")
        .filter(F.col("genres").isNotNull())
        .withColumn("genre", F.explode(F.split(F.col("genres"), "\\|")))
        .groupBy("genre")
        .agg(F.percentile_approx("roi", 0.5).alias("median_roi"))
        .orderBy(F.desc("median_roi"))
    )

    df_genre_roi_pd = df_genre_roi.toPandas().set_index("genre")["median_roi"]

    plt.figure(figsize=(12, 6))
    sns.barplot(
        x=df_genre_roi_pd.values,
        y=df_genre_roi_pd.index,
        hue=df_genre_roi_pd.index,
        palette='magma',
        legend=False
    )
    plt.title('Median ROI Multiple by Genre', fontsize=15)
    plt.xlabel('ROI (Revenue / Budget)')
    plt.ylabel('Genre')
    plt.axvline(1, color='red', linestyle='--', label='Breakeven (1x)')
    plt.savefig(plots_dir / 'roi_by_genre.png')

    # Popularity vs. Rating
    df_pop_vs_rating = (
        df_final
        .filter(
            (F.col("vote_count") > 0) &
            (F.col("vote_average").isNotNull()) &
            (F.col("popularity").isNotNull())
        )
        .select("vote_average", "popularity", "vote_count")
        .toPandas()
    )

    plt.figure(figsize=(10, 6))
    sns.regplot(data=df_pop_vs_rating, x='vote_average', y='popularity',
                scatter_kws={'s': df_pop_vs_rating['vote_count'] / 100, 'alpha': 0.5},  # Size based on vote count
                line_kws={'color': 'red'})

    plt.title('Popularity vs. User Rating', fontsize=15)
    plt.xlabel('Average Rating (out of 10)')
    plt.ylabel('Popularity Score')
    plt.savefig(plots_dir / 'popularity_vs_rating.png')

    # Yearly Trends in Box Office Performance
    # Extract Year and group
    df_yearly_trends = (
        df_final
        .withColumn("year", F.year("release_date"))
        .filter(F.col("year").isNotNull())
        .groupBy("year")
        .agg(
            F.sum("revenue_musd").alias("revenue_musd"),
            F.sum("budget_musd").alias("budget_musd")
        )
        .orderBy("year")
    )

    df_yearly_trends_pd = df_yearly_trends.toPandas().set_index("year")

    plt.figure(figsize=(12, 6))
    plt.plot(df_yearly_trends_pd.index, df_yearly_trends_pd['revenue_musd'], marker='o', label='Total Revenue',
             linewidth=2)
    plt.plot(df_yearly_trends_pd.index, df_yearly_trends_pd['budget_musd'], marker='s', label='Total Budget',
             linestyle='--')

    plt.fill_between(
        df_yearly_trends_pd.index,
        df_yearly_trends_pd['budget_musd'],
        df_yearly_trends_pd['revenue_musd'],
        color='green',
        alpha=0.1,
        label='Total Profit Zone'
    )

    plt.title('Yearly Box Office Performance', fontsize=15)
    plt.xlabel('Year')
    plt.ylabel('Millions USD')
    plt.legend()
    plt.grid(True, alpha=0.3)
    plt.savefig(plots_dir / 'yearly_box_office.png')

    # Comparison of Franchise vs. Standalone Success
    df_viz_spark = (
        df_final
        .withColumn("type",
                    F.when(F.col("belongs_to_collection").isNull(), "Standalone")
                    .otherwise("Franchise")
                    )
        .select("type", "revenue_musd", "roi", "budget_musd", "popularity", "vote_average", "vote_count")
    )

    df_viz = df_viz_spark.toPandas()

    fig, ax = plt.subplots(2, 3, figsize=(14, 6))

    comp_plots = [
        {
            "y": "revenue_musd",
            "y_label": "Revenue (Millions USD)",
            "title": "Mean Revenue: Franchise vs Standalone",
            "palette": "Blues",
            "estimator": np.mean,
            "position": (0, 0)
        },
        {
            "y": "roi",
            "y_label": "Median ROI (Revenue / Budget)",
            "title": "Median ROI: Franchise vs Standalone",
            "palette": "Oranges",
            "estimator": np.median,
            "position": (1, 0)
        },
        {
            "y": "budget_musd",
            "y_label": "Mean Budget (Millions USD)",
            "title": "Mean Budget: Franchise vs Standalone",
            "palette": "Greens",
            "estimator": np.mean,
            "position": (0, 1)
        },
        {
            "y": "popularity",
            "y_label": "Mean Popularity",
            "title": "Mean Popularity: Franchise vs Standalone",
            "palette": "Purples",
            "estimator": np.mean,
            "position": (1, 1)
        },
        {
            "y": "vote_average",
            "y_label": "Mean Rating (out of 10)",
            "title": "Mean Rating: Franchise vs Standalone",
            "palette": "Reds",
            "estimator": np.mean,
            "position": (0, 2)
        },
        {
            "y": "vote_count",
            "y_label": "Mean Vote Count",
            "title": "Mean Vote Count: Franchise vs Standalone",
            "palette": "Greys",
            "estimator": np.mean,
            "position": (1, 2)
        }

    ]

    for comp_plot in comp_plots:
        sns.barplot(
            data=df_viz,
            x='type',
            y=comp_plot['y'],
            estimator=comp_plot['estimator'],
            hue='type',
            ax=ax[comp_plot['position'][0], comp_plot['position'][1]],
            palette=comp_plot['palette'],
            legend=False
        )

        ax[comp_plot['position'][0], comp_plot['position'][1]].set_title(comp_plot['title'])
        ax[comp_plot['position'][0], comp_plot['position'][1]].set_ylabel(comp_plot['y_label'])

    plt.tight_layout()
    plt.savefig(plots_dir / 'franchise_vs_standalone.png')

    appendix_content = [
        "= Appendix <appendix>",
        f"== Appendix A: Highest Revenue <appendix-highest-revenue>\n",
        f'{
        df_to_typst(
            top_revenue, "Top 5 Highest Revenue Movies", "(2fr, 1fr, 1fr, 1fr)",
            label="tab:top_revenue"
        )
        }',
        f" == Appendix B: Highest Budget <appendix-highest-budget>\n",
        f'{
        df_to_typst(
            top_budget, "Top 5 Highest Budget Movies", "(2fr, 1fr, 1fr, 1fr)",
            label="tab:top_budget"
        )
        }',
        f"== Appendix C: Highest Profit <appendix-highest-profit>\n"
        f'{
        df_to_typst(
            top_profit, "Top 5 Highest Profit Movies", "(2fr, 1fr, 1fr, 1fr, 1fr)",
            label="tab:top_profit"
        )
        }',
        f"== Appendix D: Lowest Profit <appendix-lowest-profit>\n",
        f'{
        df_to_typst(
            worst_profit, "Top 5 Lowest Profit Movies", "(2fr, 1fr, 1fr, 1fr, 1fr)",
            label="tab:worst_profit"
        )
        }',
        f"== Appendix E: Highest ROI <appendix-highest-roi>\n",
        f'{
        df_to_typst(
            top_roi, "Top 5 Highest ROI Movies", "(2fr, 1fr, 1fr, 1fr, 1fr)",
            label="tab:top_roi"
        )
        }',
        f"== Appendix F: Lowest ROI <appendix-lowest-roi>\n",
        f'{
        df_to_typst(
            worst_roi, "Top 5 Lowest ROI Movies", "(2fr, 1fr, 1fr, 1fr, 1fr)",
            label="tab:worst_roi"
        )
        }',
        f"== Appendix G: Most Voted <appendix-most-voted>\n",
        f'{
        df_to_typst(
            most_voted, "Top 5 Most Voted Movies", "(2fr, 1fr, 1fr, 1fr)",
            label="tab:most_voted"
        )
        }',
        f"== Appendix H: Highest Rated <appendix-highest-rated>\n",
        f'{
        df_to_typst(
            top_rated, "Top 5 Highest Rated Movies", "(2fr, 1fr, 1fr, 1fr, 1fr)",
            label="tab:top_rated"
        )
        }',
        f"== Appendix I: Lowest Rated <appendix-lowest-rated>\n",
        f'{
        df_to_typst(
            worst_rated, "Top 5 Lowest Rated Movies", "(2fr, 1fr, 1fr, 1fr, 1fr)",
            label="tab:worst_rated"
        )
        }',
        f"== Appendix J: Most Popular <appendix-most-popular>\n",
        f'{
        df_to_typst(
            most_popular, "Top 5 Most Popular Movies", "(2fr, 1fr, 1fr, 1fr, 1fr)",
            label="tab:most_popular"
        )
        }',
        f"== Appendix K: Best-rated Science Fiction Action Movies starring Bruce Willis <appendix-best-rated-bruce-willis>\n",
        f'{
        df_to_typst(
            search_bruce,
            "Best-rated Science Fiction Action Movies starring Bruce Willis", "(2fr, 1fr, 2fr, 1fr,)",
            label="tab:search_bruce"
        )
        }',
        f"== Appendix L: Shortest Movies starring Uma Thurman directed by Quentin Tarantino <appendix-shortest-tarantino>\n",
        f'{
        df_to_typst(
           search_quentin,
            "Shortest Movies starring Uma Thurman directed by Quentin Tarantino", "(2fr, 1fr, 1fr)",
            label="tab:search_quentin"
        )
        }',
        f"== Appendix M: Franchise vs. Standalone <appendix-franchise-standalone>\n",
        f'{
        df_to_typst(
            franchise_comparison, "Franchise vs. Standalone Movie Comparison", "(2fr, 1fr, 1fr, 1fr, 1fr, 1fr)",
            label="tab:franchise_comparison"
        )
        }',
        f"== Appendix N: Most Successful Franchises <appendix-most-successful-franchises>\n",
        f'{
        df_to_typst(
            top_franchises, "Top 5 Most Successful Movie Franchises", "(2fr, 1fr, 1fr, 1fr, 1fr, 1fr, 1fr)",
            label="tab:top_franchises"
        )
        }',
        f"== Appendix O: Most Successful Directors <appendix-most-successful-directors>\n",
        f'{
        df_to_typst(
            top_directors, "Top 5 Most Successful Directors", "(2fr, 1fr, 1fr, 1fr)",
            label="tab:top_directors"
        )
        }'
    ]

    visual_analysis_content = f"""
    == Appendix P: Visual Analysis <appendix-visuals>
    
    #figure(
      image("plots/revenue_vs_budget.png", width: 100%),
      caption: [Revenue vs. Budget Trends. The red dashed line represents the 1.0x Multiple (Breakeven). Size indicates movie popularity.],
    ) <fig-rev-bud>
    
    #v(1cm)
    
    #grid(
      columns: (1fr, 1fr),
      gutter: 15pt,
      [
        #figure(
          image("plots/roi_by_genre.png", width: 100%),
          caption: [Median ROI Multiple by Genre. Note the efficiency of Sci-Fi and Adventure genres.],
        ) <fig-genre-roi>
      ],
      [
        #figure(
          image("plots/popularity_vs_rating.png", width: 100%),
          caption: [Popularity Score vs. User Rating. The red regression line indicates the trend of audience reception.],
        ) <fig-pop-rating>
      ]
    )
    
    #v(1cm)
    
    #figure(
      image("plots/yearly_box_office.png", width: 90%),
      caption: [Yearly Box Office Performance. The shaded green area represents the aggregate profit zone across the sample set.],
    ) <fig-yearly-trends>
    
    #v(1cm)
    
    #figure(
      image("plots/franchise_vs_standalone.png", width: 100%),
      caption: [Comprehensive Comparison: Franchise vs. Standalone Performance across six key metrics (Revenue, ROI, Budget, Popularity, Rating, and Vote Count).],
    ) <fig-franchise-comp>
    """

    appendix_content.append(visual_analysis_content)

    # Final save
    with open(appendix_file, "w", encoding="utf-8") as f:
        f.write("\n\n".join(appendix_content))

    print(f"Successfully generated {appendix_file}")


if __name__ == "__main__":
    asyncio.run(run_data_analysis())
