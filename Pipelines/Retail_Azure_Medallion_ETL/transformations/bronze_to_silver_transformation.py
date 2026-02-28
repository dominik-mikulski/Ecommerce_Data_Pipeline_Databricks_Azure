from pyspark.sql import functions as F

def unpack_orders(df):
    return (
        df.select(
            "order_id",
            "order_date",
            "customer.customer_id",
            "customer.loyalty_tier",
            "customer.segment",
            "payment.amount",
            "payment.currency",
            "payment.method",
            "shipping.city",
            "shipping.country",
            "shipping.delivery_type",
            "status",
            "ingested_at"
        )
    )

def unpack_order_items(df):
    return (
        df.select(
            "order_id",
            "order_date",
            F.explode("items").alias("item"),
            "ingested_at"
        )
        .select(
            "order_id",
            "order_date",
            "item.product_id",
            "item.category",
            "item.quantity",
            "item.unit_price",
            "ingested_at"
        )
    )