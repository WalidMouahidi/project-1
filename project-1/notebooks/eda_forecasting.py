"""
Exploratory Data Analysis (EDA) and time‑series forecasting for the Netflix
revenue dataset.

This script loads the cleaned long‑format data produced by the ETL pipeline,
performs basic exploration, plots revenue and membership trends, and trains
a simple ARIMA model to forecast global streaming revenue for the next four
quarters.  The resulting forecast plot is saved to the `project-1/outputs`
directory.

To run this analysis, first execute the ETL pipeline (`python etl/etl_pipeline.py`)
to create the SQLite database.  Then run:

    python notebooks/eda_forecasting.py

The script will connect to the database, load the tables using pandas, and
generate summary statistics and visualisations.
"""
import os
from pathlib import Path
import matplotlib.pyplot as plt
import pandas as pd
import numpy as np
import sqlite3
from statsmodels.tsa.arima.model import ARIMA


def load_tables(db_path: Path) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """Load dimension and fact tables from the SQLite database using sqlite3."""
    conn = sqlite3.connect(str(db_path))
    try:
        dim_date = pd.read_sql_query("SELECT * FROM dim_date", conn)
        dim_region = pd.read_sql_query("SELECT * FROM dim_region", conn)
        fact = pd.read_sql_query("SELECT * FROM fact_revenue_subscribers", conn)
        return dim_date, dim_region, fact
    finally:
        conn.close()


def merge_tables(dim_date: pd.DataFrame, dim_region: pd.DataFrame, fact: pd.DataFrame) -> pd.DataFrame:
    """Join dimension and fact tables into a single DataFrame."""
    df = fact.merge(dim_date, on="date_id", how="left").merge(dim_region, on="region_id", how="left")
    # Reconstruct a datetime column from the year and quarter (using quarter end dates)
    df["period"] = pd.PeriodIndex(year=df["year"], quarter=df["quarter"], freq="Q").to_timestamp(how="end")
    df = df.sort_values(["period", "region"])
    return df


def plot_trends(df: pd.DataFrame, output_dir: Path) -> None:
    """Plot revenue and membership trends by region and save figures."""
    output_dir.mkdir(parents=True, exist_ok=True)
    # Revenue trends
    fig, ax = plt.subplots(figsize=(10, 6))
    for region in df["region"].unique():
        region_df = df[df["region"] == region]
        ax.plot(region_df["period"], region_df["revenue"], marker='o', label=region)
    ax.set_title("Streaming Revenue by Region")
    ax.set_xlabel("Quarter")
    ax.set_ylabel("Revenue (USD)")
    ax.legend()
    fig.autofmt_xdate()
    plt.tight_layout()
    fig.savefig(output_dir / "revenue_trends.png")
    plt.close(fig)

    # Membership trends
    fig, ax = plt.subplots(figsize=(10, 6))
    for region in df["region"].unique():
        region_df = df[df["region"] == region]
        ax.plot(region_df["period"], region_df["members"], marker='o', label=region)
    ax.set_title("Streaming Memberships by Region")
    ax.set_xlabel("Quarter")
    ax.set_ylabel("Members")
    ax.legend()
    fig.autofmt_xdate()
    plt.tight_layout()
    fig.savefig(output_dir / "membership_trends.png")
    plt.close(fig)

    # ARPU trends
    fig, ax = plt.subplots(figsize=(10, 6))
    for region in df["region"].unique():
        region_df = df[df["region"] == region]
        ax.plot(region_df["period"], region_df["arpu"], marker='o', label=region)
    ax.set_title("ARPU by Region")
    ax.set_xlabel("Quarter")
    ax.set_ylabel("ARPU (USD)")
    ax.legend()
    fig.autofmt_xdate()
    plt.tight_layout()
    fig.savefig(output_dir / "arpu_trends.png")
    plt.close(fig)


def forecast_global_revenue(df: pd.DataFrame, output_dir: Path) -> None:
    """Fit an ARIMA model on global revenue and forecast the next four quarters.

    Saves a plot of the historical and forecast values.
    """
    global_df = df[df["region"] == "Global"].copy()
    # Ensure time series is indexed by period
    ts = global_df.set_index("period")["revenue"].asfreq("Q")

    # Fit a simple ARIMA model (p=1,d=1,q=1) – chosen for demonstration
    model = ARIMA(ts, order=(1, 1, 1))
    fitted = model.fit()
    # Forecast next 4 quarters
    forecast = fitted.forecast(4)
    # Generate dates for the next four quarter ends.  We use PeriodIndex to
    # ensure proper quarterly frequency handling.  Start at the quarter
    # immediately following the last observed period.
    last_period = pd.Period(ts.index[-1], freq="Q")
    future_periods = pd.period_range(last_period + 1, periods=4, freq="Q")
    forecast_dates = future_periods.to_timestamp(how="end")
    # Plot
    fig, ax = plt.subplots(figsize=(10, 6))
    ts.plot(ax=ax, label="Historical Revenue", marker='o')
    ax.plot(forecast_dates, forecast, label="Forecast", marker='x', linestyle='--', color='r')
    ax.set_title("Global Streaming Revenue Forecast")
    ax.set_xlabel("Quarter")
    ax.set_ylabel("Revenue (USD)")
    ax.legend()
    fig.autofmt_xdate()
    plt.tight_layout()
    output_dir.mkdir(parents=True, exist_ok=True)
    fig.savefig(output_dir / "global_revenue_forecast.png")
    plt.close(fig)
    # Print forecast values to console for reference
    print("Forecast for next four quarters:")
    for date, value in zip(forecast_dates, forecast):
        print(f"{date.date()}: ${value:,.2f}")


def main():
    base_dir = Path(__file__).resolve().parent.parent
    db_path = base_dir / "data" / "netflix.db"
    outputs_dir = base_dir / "outputs"
    dim_date, dim_region, fact = load_tables(db_path)
    df = merge_tables(dim_date, dim_region, fact)
    # Plot trends
    plot_trends(df, outputs_dir)
    # Forecast global revenue
    forecast_global_revenue(df, outputs_dir)


if __name__ == "__main__":
    main()