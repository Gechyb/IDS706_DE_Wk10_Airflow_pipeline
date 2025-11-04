"""
AAirflow DAG: MovieLens End-to-End ETL and Analysis Pipeline
Author: Ogechukwu Blessing Ezenwa
Date: 2025-11-03

Description:
This Airflow pipeline automates the ingestion, transformation, merging, loading, and
analysis of the MovieLens dataset. It demonstrates an end-to-end data engineering
workflow with task parallelism, modular design, and database integration.

Datasets:
- movies.csv — contains movie IDs, titles, and genres
- ratings.csv — contains user ratings for movies along with timestamps
"""

from __future__ import annotations
import os, zipfile, shutil
from datetime import datetime
from pathlib import Path

import pandas as pd
from sqlalchemy import create_engine
from airflow import DAG
from airflow.decorators import task
from airflow.utils.task_group import TaskGroup


# Paths & Config
RAW_DIR = Path("/opt/airflow/data/raw")
TMP_DIR = Path("/opt/airflow/data/tmp")
PROC_DIR = Path("/opt/airflow/data/processed")
ART_DIR = Path("/opt/airflow/data/artifacts")
for d in (RAW_DIR, TMP_DIR, PROC_DIR, ART_DIR):
    d.mkdir(parents=True, exist_ok=True)

DB = os.getenv("POSTGRES_DB", "airflow_db")
DB_USER = os.getenv("POSTGRES_USER", "vscode")
DB_PW = os.getenv("POSTGRES_PASSWORD", "vscode")
DB_HOST = os.getenv("POSTGRES_HOST", "db")
DB_PORT = 5432
SQLALCHEMY_URL = f"postgresql+psycopg2://{DB_USER}:{DB_PW}@{DB_HOST}:{DB_PORT}/{DB}"

MOVIELENS_URL = os.getenv(
    "MOVIELENS_ZIP_URL",
    "https://files.grouplens.org/datasets/movielens/ml-latest-small.zip",
)

default_args = {"owner": "airflow", "retries": 1}

with DAG(
    dag_id="movielens_spark_etl_ml",
    description="Pandas+Spark ETL: ingest movies/ratings, transform (parallel), merge (Spark), load Postgres, analyze, cleanup.",
    start_date=datetime(2025, 1, 1),
    schedule="@once",  # "00 22 * * *"
    catchup=False,
    default_args=default_args,
    max_active_runs=1,
    tags=["assignment", "etl", "spark", "parallel"],
) as dag:

    # Ingestion
    @task
    def download_and_extract_zip(execution_date: str) -> dict:
        """
        Download MovieLens 'latest-small' zip and extract to raw/<ds_nodash>/.
        Returns dict with paths to movies.csv, ratings.csv, and extracted base dir.
        """
        import requests

        target_dir = RAW_DIR / execution_date
        target_dir.mkdir(parents=True, exist_ok=True)

        zip_path = target_dir / "ml-latest-small.zip"
        if not zip_path.exists():
            r = requests.get(MOVIELENS_URL, timeout=60)
            r.raise_for_status()
            with open(zip_path, "wb") as f:
                f.write(r.content)

        with zipfile.ZipFile(zip_path) as zf:
            zf.extractall(target_dir)

        base = target_dir / "ml-latest-small"
        movies = base / "movies.csv"
        ratings = base / "ratings.csv"
        return {
            "movies_csv": str(movies),
            "ratings_csv": str(ratings),
            "base_dir": str(base),
        }

    # Transform (Parallel via TaskGroup)
    with TaskGroup(group_id="transform_group") as transform_group:

        @task
        def transform_movies(paths: dict) -> str:
            """
            Pandas: clean movies.csv -> one-hot top genres, extract year.
            Output: tmp/movies_clean.parquet (return file path)
            """
            df = pd.read_csv(paths["movies_csv"])

            # Extract year from title "(YYYY)"
            df["year"] = df["title"].str.extract(r"\((\d{4})\)").astype("Int64")
            df["title_clean"] = df["title"].str.replace(r"\s\(\d{4}\)$", "", regex=True)

            # One-hot the top 8 genres
            genres = df["genres"].str.get_dummies(sep="|")
            top_genres = genres.sum().sort_values(ascending=False).head(8).index
            out_df = pd.concat(
                [df[["movieId", "title_clean", "year"]], genres[top_genres]], axis=1
            )

            out = TMP_DIR / "movies_clean.parquet"
            out_df.to_parquet(out, index=False)
            return str(out)

        @task
        def transform_ratings_spark(paths: dict) -> str:
            """
            PySpark: filter ratings and aggregate to movie-level stats.
            Output: tmp/ratings_clean.parquet (return file path)
            """
            from pyspark.sql import SparkSession
            from pyspark.sql import functions as F

            spark = (
                SparkSession.builder.master("local[*]")
                .appName("ratings-clean")
                .getOrCreate()
            )

            df = (
                spark.read.option("header", True)
                .csv(paths["ratings_csv"])
                .withColumn("rating", F.col("rating").cast("double"))
            )
            df = df.filter((F.col("rating") >= 0.5) & (F.col("rating") <= 5.0)).drop(
                "timestamp"
            )

            movie_stats = df.groupBy("movieId").agg(
                F.count(F.lit(1)).alias("n_ratings"),
                F.avg("rating").alias("rating_mean"),
            )

            out = str(TMP_DIR / "ratings_clean.parquet")
            movie_stats.write.mode("overwrite").parquet(out)

            spark.stop()
            return out  # only path via XCom

    # Merge (Spark) -> Parquet
    @task
    def spark_merge(movies_path: str, ratings_path: str, execution_date: str) -> str:
        """
        PySpark: join cleaned movies & ratings on movieId.
        Output: processed/merged_<ds>.parquet (return file path)
        """
        from pyspark.sql import SparkSession
        from pyspark.sql import functions as F

        spark = (
            SparkSession.builder.master("local[*]")
            .appName("merge-parquet")
            .getOrCreate()
        )

        movies = (
            spark.read.parquet(movies_path)
            .withColumn("movieId", F.col("movieId").cast("int"))
            .withColumn("year", F.coalesce(F.col("year"), F.lit(0)).cast("int"))
        )
        ratings = spark.read.parquet(ratings_path).withColumn(
            "movieId", F.col("movieId").cast("int")
        )

        merged = movies.join(ratings, on="movieId", how="inner")

        out = str(PROC_DIR / f"merged_{execution_date}.parquet")
        merged.write.mode("overwrite").parquet(out)

        spark.stop()
        return out  # only path via XCom

    # Load to Postgres (from Parquet)
    @task
    def load_to_postgres(merged_parquet_path: str) -> str:
        """
        Read merged parquet with pandas and load into Postgres as 'movies_features'.
        Returns table name.
        """
        df = pd.read_parquet(merged_parquet_path)

        engine = create_engine(SQLALCHEMY_URL)
        df.to_sql(
            "movies_features",
            con=engine,
            if_exists="replace",
            index=False,
            method="multi",
            chunksize=5000,
        )
        engine.dispose()
        return "movies_features"

    # Analysis (read from DB -> sklearn -> artifacts)
    @task
    def analyze_and_model() -> str:
        """
        Train a simple LinearRegression to predict rating_mean from year+genres.
        Saves plot + metrics in artifacts/. Returns artifact summary string.
        """
        from sklearn.linear_model import LinearRegression
        from sklearn.model_selection import train_test_split
        import matplotlib.pyplot as plt

        engine = create_engine(SQLALCHEMY_URL)
        df = pd.read_sql("SELECT * FROM movies_features", con=engine)
        engine.dispose()

        # Features: numeric columns except identifiers/targets
        drop_cols = {"movieId", "title_clean", "rating_mean", "n_ratings"}
        feature_cols = [c for c in df.columns if c not in drop_cols]
        X = (
            df[feature_cols]
            .select_dtypes(include=["int64", "float64", "Int64"])
            .fillna(0)
            .astype(float)
        )
        y = df["rating_mean"].astype(float)

        if X.empty or y.empty:
            return "No data available for modeling."

        X_train, X_test, y_train, y_test = train_test_split(
            X, y, test_size=0.25, random_state=42
        )
        model = LinearRegression().fit(X_train, y_train)
        r2 = model.score(X_test, y_test)
        y_pred = model.predict(X_test)

        ART_DIR.mkdir(parents=True, exist_ok=True)
        plot_path = ART_DIR / "predicted_vs_actual.png"
        plt.figure()
        plt.scatter(y_test, y_pred, alpha=0.5)
        plt.xlabel("Actual mean rating")
        plt.ylabel("Predicted mean rating")
        plt.title(f"Linear Regression (R^2={r2:.3f})")
        plt.grid(True)
        plt.tight_layout()
        plt.savefig(plot_path)
        plt.close()

        metrics_path = ART_DIR / "metrics.txt"
        with open(metrics_path, "w") as f:
            f.write(f"Test R2: {r2:.4f}\n")
            f.write(f"n_train: {len(X_train)}, n_test: {len(X_test)}\n")
            f.write(f"features: {feature_cols}\n")

        return f"Artifacts saved: {plot_path} , {metrics_path}"

    # Cleanup intermediates
    @task
    def cleanup(paths: dict, execution_date: str) -> str:
        """
        Remove extracted raw folder and tmp parquet files.
        Keep processed merged parquet and artifacts.
        """
        base = Path(paths["base_dir"])
        if base.exists():
            shutil.rmtree(base, ignore_errors=True)

        for p in TMP_DIR.glob("*.parquet"):
            try:
                p.unlink()
            except Exception:
                pass

        return f"Cleaned {base} and tmp parquet files."

    # Orchestration / Dependencies
    z = download_and_extract_zip(execution_date="{{ ds_nodash }}")

    with transform_group:
        movies_p = transform_movies(z)  # Pandas
        ratings_p = transform_ratings_spark(z)  # PySpark

    merged_parquet = spark_merge(movies_p, ratings_p, execution_date="{{ ds_nodash }}")
    loaded_tbl = load_to_postgres(merged_parquet)
    analysis = analyze_and_model()
    done = cleanup(z, execution_date="{{ ds_nodash }}")

    z >> transform_group >> merged_parquet >> loaded_tbl >> analysis >> done
