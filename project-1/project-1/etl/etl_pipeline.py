"""
ETL pipeline for the Netflix revenue dataset.

This script reads the `netflix_revenue_updated.csv` file, performs basic
transformations (parsing dates, renaming columns, computing year/quarter and
unpivoting regional metrics) and loads the results into a SQLite database.

The resulting database contains a simple star schema with three tables:

* **dim_date** – one record per unique quarter. Includes the original date
  (quarter‑end), the calendar year and the quarter number.
* **dim_region** – one record per region (`UCAN`, `EMEA`, `LATM`, `APAC` and
  `Global`).
* **fact_revenue_subscribers** – a fact table with the revenue, subscriber
  counts and ARPU for every region and quarter.

You can run this script from the project root using:

    python etl/etl_pipeline.py

The SQLite database will be created (or replaced) at
`data/netflix.db`. Feel free to modify the code to load into another
database (e.g. PostgreSQL) by replacing the SQLAlchemy engine URI.
"""

import os
from pathlib import Path
import pandas as pd
import sqlite3


def load_raw_data(csv_path: Path) -> pd.DataFrame:
    """Read the raw CSV into a DataFrame and perform basic cleaning."""
    df = pd.read_csv(csv_path)
    # Standardise column names (strip spaces)
    df.columns = [c.strip().replace("  ", " ") for c in df.columns]
    # Parse the Date column into datetime
    df["Date"] = pd.to_datetime(df["Date"], format="%d-%m-%Y")
    return df


def transform_long_format(df: pd.DataFrame) -> pd.DataFrame:
    """Transform the dataset into a long format suitable for a star schema.

    Unpivots the regional metrics so that each row represents one region and
    quarter. Adds calculated fields for year and quarter.
    """
    # Define mapping from wide column names to region names and measure names
    regions = {
        "UCAN": {
            "revenue_col": "UCAN Streaming Revenue",
            "members_col": "UCAN Members",
            "arpu_col": "UCAN ARPU",
        },
        "EMEA": {
            "revenue_col": "EMEA Streaming Revenue",
            # Column names have consecutive spaces stripped in load_raw_data,
            # so 'EMEA  Members' becomes 'EMEA Members'
            "members_col": "EMEA Members",
            "arpu_col": "EMEA ARPU",
        },
        "LATM": {
            "revenue_col": "LATM Streaming Revenue",
            "members_col": "LATM Members",
            # Double spaces in the original column name are removed during cleaning
            "arpu_col": "LATM ARPU",
        },
        "APAC": {
            "revenue_col": "APAC Streaming Revenue",
            "members_col": "APAC Members",
            # Double spaces removed during cleaning
            "arpu_col": "APAC ARPU",
        },
    }

    records = []
    for _, row in df.iterrows():
        for region, cols in regions.items():
            records.append(
                {
                    "date": row["Date"],
                    "year": row["Date"].year,
                    "quarter": row["Date"].quarter,
                    "region": region,
                    "revenue": float(row[cols["revenue_col"]]),
                    "members": int(row[cols["members_col"]]),
                    "arpu": float(row[cols["arpu_col"]]),
                }
            )
    long_df = pd.DataFrame.from_records(records)

    # Also create a 'Global' region by using the aggregated columns
    for _, row in df.iterrows():
        long_df = pd.concat(
            [
                long_df,
                pd.DataFrame(
                    {
                        "date": [row["Date"]],
                        "year": [row["Date"].year],
                        "quarter": [row["Date"].quarter],
                        "region": ["Global"],
                        "revenue": [float(row["Global Revenue"])],
                        # Column name trimmed of extra spaces
                        "members": [int(row["Netflix Streaming Memberships"])],
                        # Compute global ARPU as weighted average revenue per member
                        "arpu": [
                            float(row["Global Revenue"]) / float(row["Netflix Streaming Memberships"])
                            if row["Netflix Streaming Memberships"] != 0
                            else 0.0
                        ],
                    }
                ),
            ],
            ignore_index=True,
        )

    return long_df


def load_to_database(long_df: pd.DataFrame, db_path: Path) -> None:
    """Load the transformed DataFrame into a SQLite database using sqlite3.

    We avoid SQLAlchemy here to reduce external dependencies.  Tables are
    created manually and data inserted using pandas' `to_sql` helper.
    """
    # Ensure the parent directory exists
    os.makedirs(db_path.parent, exist_ok=True)
    # Connect to SQLite
    conn = sqlite3.connect(str(db_path))
    try:
        # Build dim_date
        dim_date = long_df[["date", "year", "quarter"]].drop_duplicates().reset_index(drop=True)
        dim_date["date_id"] = range(1, len(dim_date) + 1)
        dim_date.to_sql("dim_date", conn, index=False, if_exists="replace")

        # Build dim_region
        dim_region = pd.DataFrame({"region": sorted(long_df["region"].unique())})
        dim_region["region_id"] = range(1, len(dim_region) + 1)
        dim_region.to_sql("dim_region", conn, index=False, if_exists="replace")

        # Merge IDs for fact table
        fact = long_df.merge(dim_date, on=["date", "year", "quarter"], how="left")
        fact = fact.merge(dim_region, on="region", how="left")
        fact_table = fact[["date_id", "region_id", "revenue", "members", "arpu"]].copy()
        fact_table["id"] = range(1, len(fact_table) + 1)
        fact_table.to_sql("fact_revenue_subscribers", conn, index=False, if_exists="replace")

        # Create indexes
        cur = conn.cursor()
        cur.execute("CREATE INDEX IF NOT EXISTS idx_fact_date ON fact_revenue_subscribers(date_id)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_fact_region ON fact_revenue_subscribers(region_id)")
        conn.commit()
    finally:
        conn.close()


def main():
    base_dir = Path(__file__).resolve().parent.parent
    csv_path = base_dir / "data" / "netflix_revenue_updated.csv"
    db_path = base_dir / "data" / "netflix.db"
    df = load_raw_data(csv_path)
    long_df = transform_long_format(df)
    load_to_database(long_df, db_path)
    print(f"ETL complete. Database created at {db_path}")


if __name__ == "__main__":
    main()