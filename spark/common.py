"""Shared Spark setup: session builder and explicit schemas for the raw JSON files."""
import os

from pyspark.sql import SparkSession
from pyspark.sql.types import (ArrayType, BooleanType, DoubleType, IntegerType, LongType, MapType,
                               StringType, StructField, StructType)

DATA_DIR = os.environ.get("DATA_DIR", "data")
CLEAN_DIR = os.path.join(DATA_DIR, "clean")
RESULTS_DIR = "results"


def get_spark(app_name):
    spark = (SparkSession.builder
             .appName(app_name)
             .master("local[*]")                              # use every CPU core of the laptop
             .config("spark.driver.memory", os.environ.get("SPARK_DRIVER_MEMORY", "4g"))
             .config("spark.sql.shuffle.partitions", "16")    # default 200 is too many for one machine
             .config("spark.sql.session.timeZone", "UTC")
             .config("spark.ui.showConsoleProgress", "false")         # keep the terminal output readable
             .getOrCreate())
    spark.sparkContext.setLogLevel("ERROR")
    return spark


# Explicit schemas: faster than letting Spark infer them (no extra pass over 560 MB of JSON),
# and they pin the types (e.g. prices are always double, ids always string).
GEO = StructType([StructField("city", StringType()), StructField("state", StringType()),
                  StructField("country", StringType())])

USERS = StructType([
    StructField("user_id", StringType()), StructField("geo_data", GEO),
    StructField("registration_date", StringType()), StructField("last_active", StringType())])

CATEGORIES = StructType([
    StructField("category_id", StringType()), StructField("name", StringType()),
    StructField("subcategories", ArrayType(StructType([
        StructField("subcategory_id", StringType()), StructField("name", StringType()),
        StructField("profit_margin", DoubleType())])))])

PRODUCTS = StructType([
    StructField("product_id", StringType()), StructField("name", StringType()),
    StructField("category_id", StringType()), StructField("subcategory_id", StringType()),
    StructField("base_price", DoubleType()), StructField("current_stock", IntegerType()),
    StructField("is_active", BooleanType()),
    StructField("price_history", ArrayType(StructType([
        StructField("price", DoubleType()), StructField("date", StringType())]))),
    StructField("creation_date", StringType())])

TRANSACTIONS = StructType([
    StructField("transaction_id", StringType()), StructField("session_id", StringType()),
    StructField("user_id", StringType()), StructField("timestamp", StringType()),
    StructField("items", ArrayType(StructType([
        StructField("product_id", StringType()), StructField("quantity", IntegerType()),
        StructField("unit_price", DoubleType()), StructField("subtotal", DoubleType())]))),
    StructField("subtotal", DoubleType()), StructField("discount", DoubleType()),
    StructField("total", DoubleType()), StructField("payment_method", StringType()),
    StructField("status", StringType())])

SESSIONS = StructType([
    StructField("session_id", StringType()), StructField("user_id", StringType()),
    StructField("start_time", StringType()), StructField("end_time", StringType()),
    StructField("duration_seconds", LongType()),
    StructField("geo_data", StructType(GEO.fields + [StructField("ip_address", StringType())])),
    StructField("device_profile", StructType([
        StructField("type", StringType()), StructField("os", StringType()),
        StructField("browser", StringType())])),
    StructField("viewed_products", ArrayType(StringType())),
    StructField("page_views", ArrayType(StructType([
        StructField("timestamp", StringType()), StructField("page_type", StringType()),
        StructField("product_id", StringType()), StructField("category_id", StringType()),
        StructField("view_duration", LongType())]))),
    # cart_contents is a JSON object whose KEYS are product ids -> a map, not a struct
    StructField("cart_contents", MapType(StringType(), StructType([
        StructField("quantity", IntegerType()), StructField("price", DoubleType())]))),
    StructField("conversion_status", StringType()), StructField("referrer", StringType())])
