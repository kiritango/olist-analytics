import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import (
    StructType, StructField,
    StringType, DoubleType, IntegerType, TimestampType
)

KAFKA_BROKER = os.getenv('KAFKA_BROKER', 'olist_kafka:9092')
CLICKHOUSE_HOST = os.getenv('CLICKHOUSE_HOST', 'olist_clickhouse')
CLICKHOUSE_USER = os.getenv('CLICKHOUSE_USER', 'admin')
CLICKHOUSE_PASSWORD = os.getenv('CLICKHOUSE_PASSWORD', '')

# Схемы для каждого топика
order_schema = StructType([
    StructField("order_id",                     StringType()),
    StructField("customer_id",                  StringType()),
    StructField("order_status",                 StringType()),
    StructField("order_purchase_timestamp",     StringType()),
    StructField("order_approved_at",            StringType()),
    StructField("order_delivered_carrier_date", StringType()),
    StructField("order_delivered_customer_date",StringType()),
    StructField("order_estimated_delivery_date",StringType()),
])

item_schema = StructType([
    StructField("order_id",            StringType()),
    StructField("order_item_id",       IntegerType()),
    StructField("product_id",          StringType()),
    StructField("seller_id",           StringType()),
    StructField("shipping_limit_date", StringType()),
    StructField("price",               DoubleType()),
    StructField("freight_value",       DoubleType()),
])

payment_schema = StructType([
    StructField("order_id",       StringType()),
    StructField("payment_type",   StringType()),
    StructField("payment_value",  DoubleType()),
])

def write_to_clickhouse(batch_df, batch_id, table):
    if batch_df.count() == 0:
        return
    url = f"jdbc:clickhouse://{CLICKHOUSE_HOST}:9000/raw"  # native порт 9000
    print(f"Connecting to: {url}", flush=True)

    batch_df.write \
        .format("jdbc") \
        .option("url", url) \
        .option("dbtable", table) \
        .option("user", CLICKHOUSE_USER) \
        .option("password", CLICKHOUSE_PASSWORD) \
        .option("driver", "com.github.housepower.jdbc.ClickHouseDriver") \
        .mode("append") \
        .save()
    print(f"✅ {table}: записано {batch_df.count()} строк (batch {batch_id})")

def create_stream(spark, topic, schema):
    return spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_BROKER) \
        .option("subscribe", topic) \
        .option("startingOffsets", "earliest") \
        .load() \
        .select(from_json(col("value").cast("string"), schema).alias("data")) \
        .select("data.*")

def main():
    spark = SparkSession.builder \
    .appName("olist-streaming") \
    .config("spark.jars.packages",
            "org.apache.spark:spark-sql-kafka-0-10_2.13:4.0.0,"
            "com.clickhouse:clickhouse-jdbc:0.4.6,"
            "org.apache.httpcomponents:httpclient:4.5.14") \
    .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    # Создаём стримы для каждого топика
    orders_stream = create_stream(spark, "olist.orders", order_schema)
    items_stream  = create_stream(spark, "olist.order_items", item_schema)
    payments_stream = create_stream(spark, "olist.payments", payment_schema)

    # Запускаем запись в ClickHouse каждые 30 секунд
    orders_query = orders_stream.writeStream \
        .foreachBatch(lambda df, id: write_to_clickhouse(df, id, "orders_stream")) \
        .trigger(processingTime="30 seconds") \
        .option("checkpointLocation", "/tmp/checkpoints/orders") \
        .start()

    items_query = items_stream.writeStream \
        .foreachBatch(lambda df, id: write_to_clickhouse(df, id, "order_items_stream")) \
        .trigger(processingTime="30 seconds") \
        .option("checkpointLocation", "/tmp/checkpoints/items") \
        .start()

    payments_query = payments_stream.writeStream \
        .foreachBatch(lambda df, id: write_to_clickhouse(df, id, "payments_stream")) \
        .trigger(processingTime="30 seconds") \
        .option("checkpointLocation", "/tmp/checkpoints/payments") \
        .start()

    print("Стриминг запущен. Ожидаем данные из Kafka...")
    spark.streams.awaitAnyTermination()

if __name__ == "__main__":
    main()