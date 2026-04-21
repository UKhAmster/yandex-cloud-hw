"""
DAG: hive_to_clickhouse_replication
Задание 4*: Репликация агрегаций из Hive (HDFS/Parquet) в ClickHouse.

Схема работы:
  1. spark_export  — Spark-задача: читает transactions_v2 и logs_v2 из Hive,
                     считает агрегации, сохраняет CSV во временную папку HDFS.
  2. clickhouse_load — PythonOperator: читает CSV с HDFS, загружает в ClickHouse.

Запуск: ежедневно в 03:00 UTC.
"""

from __future__ import annotations

import io
import subprocess
from datetime import datetime, timedelta

import pandas as pd
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

# ── Настройки ──────────────────────────────────────────────────────────────
HDFS_TMP = "hdfs:///tmp/airflow_export/tx_daily_agg"
CH_HOST  = "rc1b-<your-ch-host>.mdb.yandexcloud.net"
CH_PORT  = 8123
CH_DB    = "default"
CH_USER  = "admin"
CH_PASS  = "{{ var.value.ch_password }}"   # Airflow Variable

DEFAULT_ARGS = {
    "owner": "data-team",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "email_on_failure": False,
}

# ── Spark-скрипт (inline через heredoc) ───────────────────────────────────
SPARK_SCRIPT = """
from pyspark.sql import SparkSession, functions as F

spark = SparkSession.builder.appName("HiveToClickHouse_Export").enableHiveSupport().getOrCreate()

tx = spark.table("transactions_v2")

agg = (
    tx
    .groupBy("transaction_date")
    .agg(
        F.count("*").alias("tx_count"),
        F.round(F.sum("amount"), 2).alias("daily_total"),
        F.round(F.avg("amount"), 2).alias("daily_avg"),
        F.sum(F.when(F.col("is_fraud") == 1, 1).otherwise(0)).alias("fraud_count"),
    )
    .orderBy("transaction_date")
)

agg.coalesce(1).write.mode("overwrite").option("header", "true").csv("{hdfs_path}")
spark.stop()
""".format(hdfs_path=HDFS_TMP)


def _write_spark_script(**ctx) -> str:
    """Записывает Spark-скрипт во временный файл на Driver-узле."""
    path = "/tmp/hive_export.py"
    with open(path, "w") as f:
        f.write(SPARK_SCRIPT)
    return path


def _load_to_clickhouse(**ctx):
    """Читает CSV из HDFS и грузит в ClickHouse через clickhouse-driver."""
    import clickhouse_driver  # pip install clickhouse-driver

    # Читаем CSV с HDFS через hdfs CLI
    result = subprocess.run(
        ["hdfs", "dfs", "-cat", f"{HDFS_TMP}/part-*.csv"],
        capture_output=True,
        text=True,
        check=True,
    )
    df = pd.read_csv(io.StringIO(result.stdout))

    # Создаём таблицу-приёмник (если не существует)
    client = clickhouse_driver.Client(
        host=CH_HOST, port=9000,
        database=CH_DB, user=CH_USER, password=CH_PASS,
    )
    client.execute("""
        CREATE TABLE IF NOT EXISTS tx_daily_agg (
            transaction_date Date,
            tx_count         UInt64,
            daily_total      Float64,
            daily_avg        Float64,
            fraud_count      UInt64
        ) ENGINE = ReplacingMergeTree()
        ORDER BY transaction_date
    """)

    # Загружаем данные
    rows = df.to_dict(orient="records")
    client.execute(
        "INSERT INTO tx_daily_agg VALUES",
        [[r["transaction_date"], r["tx_count"],
          r["daily_total"], r["daily_avg"], r["fraud_count"]]
         for r in rows],
    )
    print(f"Loaded {len(rows)} rows into ClickHouse tx_daily_agg")


# ── DAG ───────────────────────────────────────────────────────────────────
with DAG(
    dag_id="hive_to_clickhouse_replication",
    default_args=DEFAULT_ARGS,
    description="Ежедневная репликация агрегаций Hive → ClickHouse",
    schedule_interval="0 3 * * *",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["dataproc", "clickhouse", "replication"],
) as dag:

    write_script = PythonOperator(
        task_id="write_spark_script",
        python_callable=_write_spark_script,
    )

    spark_export = SparkSubmitOperator(
        task_id="spark_export_hive_agg",
        application="/tmp/hive_export.py",
        conn_id="spark_default",
        conf={
            "spark.sql.catalogImplementation": "hive",
            "spark.hadoop.fs.s3a.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem",
        },
        name="hive_daily_agg_export",
    )

    ch_load = PythonOperator(
        task_id="clickhouse_load",
        python_callable=_load_to_clickhouse,
    )

    write_script >> spark_export >> ch_load
