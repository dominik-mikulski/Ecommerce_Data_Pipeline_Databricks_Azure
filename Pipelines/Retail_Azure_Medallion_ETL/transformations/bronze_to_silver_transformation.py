from pyspark.sql import functions as F

def unpack_orders(df):
    return (
        df.select(
            F.col("order_id").cast("long").alias("order_id"),
            F.col("order_date").cast("timestamp").alias("order_date"),
            F.col("customer.customer_id").cast("long").alias("customer_id"),
            F.col("customer.loyalty_tier").cast("string").alias("loyalty_tier"),
            F.col("customer.segment").cast("string").alias("segment"),
            F.col("payment.amount").cast("decimal(12,2)").alias("amount"),
            F.col("payment.currency").cast("string").alias("currency"),
            F.col("payment.method").cast("string").alias("method"),
            F.col("shipping.city").cast("string").alias("city"),
            F.col("shipping.country").cast("string").alias("country"),
            F.col("shipping.delivery_type").cast("string").alias("delivery_type"),
            F.col("status").cast("string").alias("status"),
            F.col("ingested_at")
        )
    )

def unpack_order_items(df):
    return (
        df.select(
            F.col("order_id").cast("long"),
            F.col("order_date").cast("timestamp"),
            F.explode_outer("items").alias("item"),
            "ingested_at"
        )
        .select(
            F.col("order_id"),
            F.col("order_date"),
            F.col("item.product_id").cast("long").alias("product_id"),
            F.col("item.category").cast("string").alias("category"),
            F.col("item.quantity").cast("int").alias("quantity"),
            F.col("item.unit_price").cast("decimal(10,2)").alias("unit_price"),
            F.col("ingested_at")
        )
    )