"""
Airflow DAG for orchestrating the Netflix ETL pipeline.

This DAG demonstrates how Apache Airflow can coordinate a flow‑based ETL
process in NiFi, forward data via Kafka and load it into a relational
database.  It is a conceptual example and uses placeholder functions
(`trigger_nifi_flow`, `consume_from_kafka`) that should be implemented
according to your NiFi process group and Kafka topic configuration.

Before running this DAG, ensure that the services defined in
`docker-compose.yml` are running (NiFi on port 8080, Kafka on port 9092
and PostgreSQL on port 5432) and that Airflow is properly initialised.
"""
from __future__ import annotations

from datetime import datetime, timedelta
import json
import os
import requests
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.task_group import TaskGroup


def trigger_nifi_flow(process_group_id: str, nifi_url: str = "http://nifi:8080") -> None:
    """Trigger a NiFi process group via the REST API.

    Args:
        process_group_id: The ID of the process group to run.  You can find
            this in the NiFi UI (Settings > Flow Configuration).
        nifi_url: Base URL for the NiFi API (defaults to service name in
            docker‑compose).

    Raises:
        requests.HTTPError if the REST call fails.
    """
    # Start all processors in the process group
    url = f"{nifi_url}/nifi-api/flow/process-groups/{process_group_id}/schedule"
    payload = {"id": process_group_id, "state": "RUNNING"}
    response = requests.put(url, json=payload)
    response.raise_for_status()
    # Optionally, poll the process group status until it finishes


def produce_to_kafka(csv_path: str, topic: str = "netflix") -> None:
    """Produce CSV rows to Kafka.  This is a simplified example.

    In a real deployment NiFi can directly write to Kafka; this function
    illustrates how to send messages using kafka-python.
    """
    from kafka import KafkaProducer

    producer = KafkaProducer(bootstrap_servers=["kafka:9092"], value_serializer=lambda v: json.dumps(v).encode("utf-8"))
    import csv
    with open(csv_path, newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            producer.send(topic, row)
    producer.flush()


def consume_from_kafka_and_load(topic: str = "netflix") -> None:
    """Consume messages from Kafka and insert them into PostgreSQL.

    This task uses the `kafka-python` library to read messages and
    `psycopg2` to insert into the `public.netflix` table.  Ensure the
    database and table exist before running.
    """
    from kafka import KafkaConsumer
    import psycopg2

    consumer = KafkaConsumer(
        topic,
        bootstrap_servers=["kafka:9092"],
        value_deserializer=lambda m: json.loads(m.decode("utf-8")),
        auto_offset_reset="earliest",
        enable_auto_commit=True,
        group_id="netflix_etl",
    )
    conn = psycopg2.connect(dbname="netflixdb", user="netflix", password="netflix", host="postgres")
    cur = conn.cursor()
    # Create table if it does not exist.  Adjust column types as needed.
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS netflix_raw (
            date TEXT,
            global_revenue NUMERIC,
            ucan_revenue NUMERIC,
            emea_revenue NUMERIC,
            latm_revenue NUMERIC,
            apac_revenue NUMERIC,
            ucan_members BIGINT,
            emea_members BIGINT,
            latm_members BIGINT,
            apac_members BIGINT,
            ucan_arpu NUMERIC,
            emea_arpu NUMERIC,
            latm_arpu NUMERIC,
            apac_arpu NUMERIC,
            total_members BIGINT
        )
        """
    )
    conn.commit()
    for message in consumer:
        record = message.value
        # Insert each record into the target table.  Spaces in keys are preserved
        # from the CSV header; use dict key access accordingly.
        cur.execute(
            """
            INSERT INTO netflix_raw (
                date, global_revenue, ucan_revenue, emea_revenue, latm_revenue, apac_revenue,
                ucan_members, emea_members, latm_members, apac_members,
                ucan_arpu, emea_arpu, latm_arpu, apac_arpu, total_members
            ) VALUES (
                %(Date)s, %(Global Revenue)s, %(UCAN Streaming Revenue)s, %(EMEA Streaming Revenue)s,
                %(LATM Streaming Revenue)s, %(APAC Streaming Revenue)s,
                %(UCAN Members)s, %(EMEA  Members)s, %(LATM Members)s, %(APAC Members)s,
                %(UCAN ARPU)s, %(EMEA ARPU)s, %(LATM  ARPU)s, %(APAC  ARPU)s,
                %(Netflix Streaming Memberships )s
            )
            """,
            record,
        )
        conn.commit()
    cur.close()
    conn.close()


default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}


with DAG(
    dag_id="netflix_etl_dag",
    default_args=default_args,
    description="Orchestrate NiFi ETL, publish to Kafka and load into Postgres",
    schedule_interval=None,  # run on demand
    start_date=datetime(2025, 1, 1),
    catchup=False,
) as dag:
    # Task to trigger NiFi flow.  Replace 'your-process-group-id' with the actual ID.
    start_nifi = PythonOperator(
        task_id="trigger_nifi_flow",
        python_callable=trigger_nifi_flow,
        op_kwargs={"process_group_id": "your-process-group-id"},
    )

    # Task to publish the raw CSV to Kafka (if not using NiFi for this step)
    produce_to_kafka_task = PythonOperator(
        task_id="produce_to_kafka",
        python_callable=produce_to_kafka,
        op_kwargs={"csv_path": "/opt/airflow/data/netflix_revenue_updated.csv"},
    )

    # Task to consume from Kafka and load to Postgres
    consume_and_load = PythonOperator(
        task_id="consume_from_kafka_and_load",
        python_callable=consume_from_kafka_and_load,
    )

    # Define dependencies
    start_nifi >> produce_to_kafka_task >> consume_and_load