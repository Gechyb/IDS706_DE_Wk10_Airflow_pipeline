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
import os, io, zipfile, shutil
from datetime import datetime
from pathlib import Path

import pandas as pd
from sqlalchemy import create_engine
from airflow import DAG
from airflow.decorators import task
from airflow.utils.task_group import TaskGroup

# ==== Config ====
RAW_DIR = Path("/opt/airflow/data/raw")
TMP_DIR = Path("/opt/airflow/data/tmp")
PROC_DIR = Path("/opt/airflow/data/processed")
ART_DIR = Path("/opt/airflow/data/artifacts")

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
    dag_id="movielens_end_to_end_parallel",
    description="Ingest two related datasets (movies, ratings), transform, merge, load to Postgres, analyze, and cleanup.",
    start_date=datetime(2025, 1, 1),
    schedule="0 2 * * *",  # daily 02:00
    catchup=False,
    default_args=default_args,
    max_active_runs=1,
    tags=["assignment", "etl", "parallel", "ml"],
) as dag:

    for d in (RAW_DIR, TMP_DIR, PROC_DIR, ART_DIR):
        d.mkdir(parents=True, exist_ok=True)

    # ---------------------------
    # Ingestion
    # ---------------------------
    @task
    def download_and_extract_zip(execution_date: str) -> dict:
        """
        Download MovieLens 'latest-small' zip and extract to raw/<ds_nodash>/.
        Returns paths to movies.csv and ratings.csv.
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

        # The zip contains folder ml-latest-small with CSVs
        base = target_dir / "ml-latest-small"
        movies = base / "movies.csv"
        ratings = base / "ratings.csv"

        return {
            "movies_csv": str(movies),
            "ratings_csv": str(ratings),
            "base_dir": str(base),
        }

    # ---------------------------
    # Transform (parallel via TaskGroups)
    # ---------------------------
    with TaskGroup(group_id="transform_group") as transform_group:

        @task
        def transform_movies(paths: dict) -> str:
            """
            Clean 'movies.csv': split title/year, one-hot top genres.
            Save to tmp/movies_clean.parquet and return its path.
            """
            df = pd.read_csv(paths["movies_csv"])
            # Extract year from title e.g., "Toy Story (1995)"
            df["year"] = df["title"].str.extract(r"\((\d{4})\)").astype("Int64")
            df["title_clean"] = df["title"].str.replace(r"\s\(\d{4}\)$", "", regex=True)

            # Expand genres into columns (top K to keep it small)
            genres = df["genres"].str.get_dummies(sep="|")
            top_genres = genres.sum().sort_values(ascending=False).head(8).index
            df = pd.concat(
                [df[["movieId", "title_clean", "year"]], genres[top_genres]], axis=1
            )

            out = TMP_DIR / "movies_clean.parquet"
            df.to_parquet(out, index=False)
            return str(out)

        @task
        def transform_ratings(paths: dict) -> str:
            """
            Clean 'ratings.csv': drop timestamp, basic filtering, compute user/movie stats.
            Save to tmp/ratings_clean.parquet and return its path.
            """
            df = pd.read_csv(paths["ratings_csv"])
            # Drop timestamp and any obvious outliers (ratings are 0.5–5.0 in 0.5 steps)
            df = df[(df["rating"] >= 0.5) & (df["rating"] <= 5.0)]
            df = df.drop(columns=["timestamp"])

            # Aggregate movie-level targets to keep final size manageable
            movie_stats = df.groupby("movieId", as_index=False).agg(
                n_ratings=("rating", "size"), rating_mean=("rating", "mean")
            )

            out = TMP_DIR / "ratings_clean.parquet"
            movie_stats.to_parquet(out, index=False)
            return str(out)

    # ---------------------------
    # Merge & Load to Postgres
    # ---------------------------
    @task
    def merge_and_load(movies_path: str, ratings_path: str, execution_date: str) -> str:
        movies = pd.read_parquet(movies_path)
        ratings = pd.read_parquet(ratings_path)

        merged = pd.merge(movies, ratings, on="movieId", how="inner")

        # Basic transformations: fill missing year with 0 for types, keep compact schema
        merged["year"] = merged["year"].fillna(0).astype(int)

        # Save a copy of merged for reproducibility
        out_csv = PROC_DIR / f"merged_{execution_date}.csv"
        merged.to_csv(out_csv, index=False)

        # Load into Postgres
        engine = create_engine(SQLALCHEMY_URL)
        # Small table size: replace each run (idempotent)
        merged.to_sql(
            "movies_features",
            con=engine,
            if_exists="replace",
            index=False,
            method="multi",
            chunksize=5000,
        )
        engine.dispose()
        return str(out_csv)

    # ---------------------------
    # Analysis (read from DB → train simple ML → artifact)
    # ---------------------------
    @task
    def analyze_and_model() -> str:
        """
        Read movies_features from Postgres, train a simple linear model predicting rating_mean
        from year + genre dummies, write metrics and a plot to artifacts/.
        """
        from sklearn.linear_model import LinearRegression
        from sklearn.model_selection import train_test_split
        import matplotlib.pyplot as plt

        engine = create_engine(SQLALCHEMY_URL)
        df = pd.read_sql("SELECT * FROM movies_features", con=engine)
        engine.dispose()

        # Features: year + genre one-hots (all non-numeric are ignored)
        feature_cols = [
            c
            for c in df.columns
            if c not in ["movieId", "title_clean", "rating_mean", "n_ratings"]
        ]
        X = (
            df[feature_cols]
            .select_dtypes(include=["int64", "float64", "Int64"])
            .fillna(0)
            .astype(float)
        )
        y = df["rating_mean"].astype(float)

        if X.empty or y.empty:
            # In case something odd happens
            return "No data for modeling."

        X_train, X_test, y_train, y_test = train_test_split(
            X, y, test_size=0.25, random_state=42
        )
        model = LinearRegression()
        model.fit(X_train, y_train)
        r2 = model.score(X_test, y_test)

        # Save a quick scatter of predicted vs actual
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

        # Write simple metrics txt
        metrics_path = ART_DIR / "metrics.txt"
        with open(metrics_path, "w") as f:
            f.write(f"Test R2: {r2:.4f}\n")
            f.write(f"n_train: {len(X_train)}, n_test: {len(X_test)}\n")
            f.write(f"features: {feature_cols}\n")

        return f"Artifacts: {plot_path} and {metrics_path}"

    # ---------------------------
    # Cleanup
    # ---------------------------
    @task
    def cleanup(paths: dict, execution_date: str) -> str:
        """
        Remove intermediate extracted folders and tmp parquet files.
        Keep processed CSV and artifacts as deliverables.
        """
        # Remove raw extracted folder for the run
        base = Path(paths["base_dir"])
        if base.exists():
            shutil.rmtree(base, ignore_errors=True)

        # Clear tmp parquet
        for p in TMP_DIR.glob("*.parquet"):
            try:
                p.unlink()
            except Exception:
                pass

        return f"Cleaned {base} and tmp parquet files."

    # Orchestration
    z = download_and_extract_zip(execution_date="{{ ds_nodash }}")

    with transform_group:
        movies_p = transform_movies(z)
        ratings_p = transform_ratings(z)

    merged_csv = merge_and_load(movies_p, ratings_p, execution_date="{{ ds_nodash }}")
    analysis = analyze_and_model()
    done = cleanup(z, execution_date="{{ ds_nodash }}")

    # Dependencies
    z >> transform_group >> merged_csv >> analysis >> done
