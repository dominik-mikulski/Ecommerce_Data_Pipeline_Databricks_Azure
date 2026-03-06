from pyspark.sql.types import *

expected_schema = StructType(
    [StructField('customer', 
                StructType([StructField('customer_id', LongType(), True), 
                StructField('loyalty_tier', StringType(), True),
                StructField('segment', StringType(), True)]), True), 
    StructField('items', 
                ArrayType(
                StructType([StructField('category', StringType(), True), 
                StructField('product_id', LongType(), True), 
                StructField('quantity', IntegerType(), True),
                StructField('unit_price', DecimalType(12,2), True)]), True), True), 
    StructField('order_date', TimestampType(), True), 
    StructField('order_id', LongType(), True), 
    StructField('payment', 
                StructType([StructField('amount', DoubleType(), True), 
                StructField('currency', StringType(), True), 
                StructField('method', StringType(), True)]), True),
    StructField('shipping', 
                StructType([StructField('city', StringType(), True), 
                StructField('country', StringType(), True), 
                StructField('delivery_type', StringType(), True)]), True), 
    StructField('status', StringType(), True)])