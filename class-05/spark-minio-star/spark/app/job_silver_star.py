import os
from pyspark.sql import SparkSession, functions as F

S3_ENDPOINT = os.getenv("S3_ENDPOINT")
AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")
BUCKET = os.getenv("S3_BUCKET", "lake")
BRONZE = os.getenv("S3_BRONZE_PREFIX", "bronze")
SILVER = os.getenv("S3_SILVER_PREFIX", "silver")


def s3url(path):
    return f"s3a://{BUCKET}/{path}"


def build_spark():
    spark = SparkSession.builder.appName("BronzeToSilver").getOrCreate()

    conf = spark._jsc.hadoopConfiguration()
    conf.set("fs.s3a.endpoint", S3_ENDPOINT)
    conf.set("fs.s3a.access.key", AWS_ACCESS_KEY_ID)
    conf.set("fs.s3a.secret.key", AWS_SECRET_ACCESS_KEY)
    conf.set("fs.s3a.path.style.access", "true")
    conf.set("fs.s3a.connection.ssl.enabled", "false")

    return spark


def main():
    spark = build_spark()

    # --- Odczyt danych bronze ---
    customers = spark.read.option("header", True).csv(s3url(f"{BRONZE}/customers.csv"))
    orders = spark.read.option("header", True).csv(s3url(f"{BRONZE}/orders.csv"))

    # --- Dim Customer ---
    dim_customer = (
        customers.withColumn("customer_id", F.col("customer_id").cast("long"))
        .withColumn("customer_name", F.concat_ws(" ", "first_name", "last_name"))
        .select(
            F.col("customer_id").alias("customer_key"),
            "customer_name",
            F.lower("email").alias("email"),
            F.upper("country").alias("country"),
        )
        .dropDuplicates(["customer_key"])
    )

    # --- Dim Date ---
    orders = orders.withColumn("order_timestamp", F.to_timestamp("order_timestamp"))
    dim_date = (
        orders.select(F.to_date("order_timestamp").alias("order_date"))
        .dropna()
        .dropDuplicates()
        .withColumn("date_key", F.date_format("order_date", "yyyyMMdd").cast("int"))
        .withColumn("year", F.year("order_date"))
        .withColumn("month", F.month("order_date"))
    )

    # --- Fact Orders ---
    fact_orders = (
        orders.join(dim_customer, F.col("customer_id") == F.col("customer_key"), "left")
        .join(dim_date, F.to_date("order_timestamp") == F.col("order_date"), "left")
        .select(
            F.col("order_id").cast("long"),
            F.col("customer_key"),
            F.col("date_key"),
            F.col("order_amount").cast("double").alias("amount"),
            F.col("currency").alias("currency"),
            F.col("order_timestamp"),
        )
    )

    # --- Zapis silver ---
    dim_customer.write.mode("overwrite").parquet(s3url(f"{SILVER}/star/dim_customer"))
    dim_date.write.mode("overwrite").parquet(s3url(f"{SILVER}/star/dim_date"))
    fact_orders.write.mode("overwrite").partitionBy("currency").parquet(
        s3url(f"{SILVER}/star/fact_orders")
    )

    print("✅ Warstwa silver zapisana do MinIO.")
    spark.stop()


if __name__ == "__main__":
    main()
