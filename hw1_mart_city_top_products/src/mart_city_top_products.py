"""
Витрина mart_city_top_products: Top-2 товаров по выручке в каждом городе.

Запускается либо в Apache Zeppelin (интерпретатор %spark.pyspark),
либо как самостоятельный PySpark-скрипт через spark-submit.
"""

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window


HDFS_PATH = "/tmp/sandbox_zeppelin/mart_city_top_products/"
S3_PATH = "s3a://hw-hse1/tmp/sandbox_zeppelin/mart_city_top_products/"
TOP_N = 2


def build_source_dataframes(spark):
    users = spark.createDataFrame(
        [
            ("u1", "Berlin"),
            ("u2", "Berlin"),
            ("u3", "Munich"),
            ("u4", "Hamburg"),
        ],
        ["user_id", "city"],
    )

    orders = spark.createDataFrame(
        [
            ("o1", "u1", "p1", 2, 10.0),
            ("o2", "u1", "p2", 1, 30.0),
            ("o3", "u2", "p1", 1, 10.0),
            ("o4", "u2", "p3", 5, 7.0),
            ("o5", "u3", "p2", 3, 30.0),
            ("o6", "u3", "p3", 1, 7.0),
            ("o7", "u4", "p1", 10, 10.0),
        ],
        ["order_id", "user_id", "product_id", "qty", "price"],
    )

    products = spark.createDataFrame(
        [
            ("p1", "Ring VOLA"),
            ("p2", "Ring POROG"),
            ("p3", "Ring TISHINA"),
        ],
        ["product_id", "product_name"],
    )

    return users, orders, products


def build_mart(users, orders, products, top_n=TOP_N):
    orders_with_revenue = orders.withColumn(
        "revenue", F.col("qty") * F.col("price")
    )

    enriched = (
        orders_with_revenue
        .join(users, on="user_id", how="inner")
        .join(products, on="product_id", how="inner")
    )

    aggregated = (
        enriched
        .groupBy("city", "product_id", "product_name")
        .agg(
            F.count("order_id").alias("orders_cnt"),
            F.sum("qty").alias("qty_sum"),
            F.sum("revenue").alias("revenue_sum"),
        )
    )

    # тай-брейкер по product_id, чтобы порядок был детерминированным
    # (в Berlin p1 и p2 имеют одинаковую revenue_sum = 30.0)
    city_revenue_window = (
        Window
        .partitionBy("city")
        .orderBy(F.col("revenue_sum").desc(), F.col("product_id").asc())
    )

    return (
        aggregated
        .withColumn("rn", F.row_number().over(city_revenue_window))
        .filter(F.col("rn") <= top_n)
        .drop("rn")
    )


def main():
    spark = (
        SparkSession.builder
        .appName("mart_city_top_products")
        .getOrCreate()
    )

    users, orders, products = build_source_dataframes(spark)
    mart = build_mart(users, orders, products, top_n=TOP_N)

    mart.write.mode("overwrite").parquet(HDFS_PATH)
    mart.write.mode("overwrite").parquet(S3_PATH)

    spark.read.parquet(HDFS_PATH) \
        .orderBy("city", F.col("revenue_sum").desc()) \
        .show(truncate=False)

    spark.stop()


if __name__ == "__main__":
    main()
