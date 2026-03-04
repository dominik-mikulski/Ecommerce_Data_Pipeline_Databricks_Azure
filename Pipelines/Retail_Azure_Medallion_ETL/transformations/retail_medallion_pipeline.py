from pyspark import pipelines as dp
import pyspark.sql.functions as F
from transformations.bronze_to_silver_transformation import (
    unpack_orders,
    unpack_order_items
)
from transformations.dq_rules import (
    fact_order_rules,
    fact_order_items_rules
)

# --------------------------------------------------
# 1. ENVIRONMENT CONFIGURATION (Dynamic Namespacing)
# --------------------------------------------------
# These pull from the "Spark Config" in your DLT Pipeline settings.
# Default values are provided so your existing setup doesn't break.
BRONZE = spark.conf.get("pipeline.names.bronze", "bronze")
SILVER = spark.conf.get("pipeline.names.silver", "silver")
GOLDEN = spark.conf.get("pipeline.names.golden", "golden")

# Use a config variable for the storage path to avoid hardcoding secrets
RAW_PATH = spark.conf.get("pipeline.raw_path")


# --------------------------------------------------
# 1. BRONZE INGESTION (Autoloader)
# --------------------------------------------------
schema=spark.read.option("multiLine", "true").json(RAW_PATH).schema

@dp.table(
    name=f"{BRONZE}.orders_bronze",
    comment="Raw orders from Azure"
)
def orders_bronze():
    return (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "json")
        .option("multiLine","true")
        .schema(schema)
        .load(RAW_PATH)
        .withColumn("file_path",F.col("_metadata.file_path"))
        .withColumn("file_name",F.col("_metadata.file_name"))
        .withColumn("file_size",F.col("_metadata.file_size"))
        .withColumn("modification_time", F.col("_metadata.file_modification_time"))
        .withColumn("ingested_at", F.current_timestamp())
        )

# -----------------------
# BRONZE TO SILVER
# -----------------------

# --------------------------------------------------
# Orders (Type 1)
# --------------------------------------------------
@dp.view
def silver_orders_prepared():
    return unpack_orders(dp.read_stream(f"{BRONZE}.orders_bronze"))

dp.create_streaming_table(name=f"{SILVER}.orders")

dp.apply_changes(
    target=f"{SILVER}.orders",
    source="silver_orders_prepared",
    keys=["order_id"],
    sequence_by=F.col("ingested_at")
)

# --------------------------------------------------
# Immutable Order Items
# --------------------------------------------------
@dp.view
def silver_order_items_prepared():
    return unpack_order_items(
        dp.read_stream(f"{BRONZE}.orders_bronze")
    )

@dp.table(name=f"{SILVER}.order_items")
def order_items():
    return dp.read_stream("silver_order_items_prepared")

# -----------------------
# SILVER TO GOLDEN
# -----------------------

# -----------------------
# DIMENSIONS
# -----------------------

# -----------------------
# customer_dim
# -----------------------

# create target dim_customer table
dp.create_streaming_table(
    name=f"{GOLDEN}.dim_customer_scd",
)

# create view and extract customer attributes
@dp.view
def customer_changes():
    return (
        dp.read_stream(f"{SILVER}.orders")
        .select(
            "customer_id",
            "loyalty_tier",
            "segment",
            "ingested_at"
        )
    )

# merge source into target on customer_id key, using ingested_at to determine the latest
dp.apply_changes(
    target=f"{GOLDEN}.dim_customer_scd",
    source="customer_changes",
    keys=["customer_id"],
    sequence_by=F.col("ingested_at"),
    stored_as_scd_type=2
)

# finally add a key to the table for joins with orders and order_items
@dp.table(name=f"{GOLDEN}.dim_customer")
def dim_customer():
    return (
        dp.read("golden.dim_customer_scd")
        .withColumn(
            "customer_sk",
                F.xxhash64(
                    F.col("customer_id"),
                    F.col("__START_AT")
                )
            )
        )
    
# -----------------------
# product_dim
# -----------------------
# create target dim_product table
dp.create_streaming_table(
    name=f"{GOLDEN}.dim_product_scd"
)

# create view and extract product attributes
@dp.view
def product_changes():
    return (
        dp.read_stream(f"{SILVER}.order_items")
        .select(
            "product_id",
            "category",
            "ingested_at"
        )
    )

# merge source into target on customer_id key, using ingested_at to determine the latest
dp.apply_changes(
    target=f"{GOLDEN}.dim_product_scd",
    source="product_changes",
    keys=["product_id"],
    sequence_by=F.col("ingested_at"),
    stored_as_scd_type=2
)

# finally add a key to the table for joins with orders and order_items
@dp.table(name=f"{GOLDEN}.dim_product")
def dim_product():
    return (
        dp.read(f"{GOLDEN}.dim_product_scd")
        .withColumn(
            "product_sk",
                F.xxhash64(
                    F.col("product_id"),
                    F.col("__START_AT")
                )
            )
        )

# -----------------------
# FACTS
# -----------------------

# -----------------------
# FACT_ORDERS
# -----------------------
dp.create_streaming_table(f"{GOLDEN}.fact_orders")

@dp.view
@dp.expect_all(fact_order_rules)
def fact_orders_prepared():

    orders = dp.read_stream(f"{SILVER}.orders")
    dim = dp.read(f"{GOLDEN}.dim_customer")

    return (
        orders.alias("o")
        .join(
            dim.alias("d"),
            (
                (F.col("o.customer_id") == F.col("d.customer_id")) &
                (
                    F.col("o.ingested_at")
                    .between(
                        F.col("d.__START_AT"),
                        F.coalesce(F.col("d.__END_AT"), F.lit("9999-12-31"))
                    )
                )
            ),
            "left"
        )
        .select(
            "o.order_id",
            "d.customer_sk",
            "o.order_date",
            "o.status",
            "o.amount",
            "o.currency",
            "o.ingested_at"
        )
    )

dp.apply_changes(
    target=f"{GOLDEN}.fact_orders",
    source="fact_orders_prepared",
    keys=["order_id"],
    sequence_by=F.col("ingested_at"),
    stored_as_scd_type=1
)

# -----------------------
# FACT_ORDER_ITEMS
# -----------------------

dp.create_streaming_table(name=f"{GOLDEN}.fact_order_items")

@dp.view
@dp.expect_all(fact_order_items_rules)
def fact_order_items_prepared():

    order_items=dp.read_stream(f"{SILVER}.order_items")
    orders=dp.read_stream(f"{SILVER}.orders")
    dim_c=dp.read(f"{GOLDEN}.dim_customer")
    dim_p = dp.read(f"{GOLDEN}.dim_product")

    return (
    order_items.alias("oi")
    .join(
        orders.alias("o"), 
        (F.col("oi.order_id") == F.col("o.order_id"))) 
    .join(
        dim_c.alias("c"), 
        (F.col("o.customer_id") == F.col("c.customer_id")) & 
            (F.col("oi.ingested_at")
            .between(F.col("c.__START_AT"), F.coalesce(F.col("c.__END_AT"), F.lit("9999-12-31"))))
        )
    .join(
        dim_p.alias("p"), 
        (F.col("oi.product_id") == F.col("p.product_id")) & 
            (F.col("oi.ingested_at")
            .between(F.col("p.__START_AT"), F.coalesce(F.col("p.__END_AT"), F.lit("9999-12-31")))
            )
        )
    .select("oi.*","c.customer_sk","p.product_sk")
)

dp.apply_changes(
    target=f"{GOLDEN}.fact_order_items",
    source="fact_order_items_prepared",
    keys=["order_id", "product_id"],
    sequence_by=F.col("ingested_at"),
    stored_as_scd_type=1
)
