# Databricks notebook source
import pyspark.sql.functions as F
from pyspark.sql.types import StringType, IntegerType, DateType, TimestampType, FloatType

# COMMAND ----------

catalog_name = 'ecommerce'

# COMMAND ----------

df_gold_order = spark.table(f'{catalog_name}.silver.slv_order_items')

df_gold_order


# COMMAND ----------

display(df_gold_order)

# COMMAND ----------

df_gold_order = df_gold_order.withColumn(
    'gross_amount', F.col('quantity') * F.col('unit_price')
)
# 2) Add discount_amount (discount_pct is already numeric, e.g., 21 -> 21%)
df_gold_order = df_gold_order.withColumn(
    "discount_amount",
    F.ceil(F.col("gross_amount") * (F.col("discount_pct") / 100.0))
)

# 3) Add sale_amount = gross - discount
df_gold_order= df_gold_order.withColumn(
    "sale_amount",
    F.col("gross_amount") - F.col("discount_amount") + F.col("tax_amount")
)

# add date id
df_gold_order = df_gold_order.withColumn("date_id", F.date_format(F.col("dt"), "yyyyMMdd").cast(IntegerType()))  # Create date_key

# Coupon flag
#  coupon flag = 1 if coupon_code is not null else 0
df_gold_order = df_gold_order.withColumn(
    "coupon_flag",
    F.when(F.col("coupon_code").isNotNull(), F.lit(1))
     .otherwise(F.lit(0))
)

df_gold_order.limit(5).display()    

# COMMAND ----------

# MAGIC %md
# MAGIC

# COMMAND ----------

# currency conversion

fx_rates = {
    "INR": 1.00,
    "AED": 24.18,
    "AUD": 57.55,
    "CAD": 62.93,
    "GBP": 117.98,
    "SGD": 68.18,
    "USD": 88.29,
}

rates = [(k, float(v)) for k, v in fx_rates.items()]
rates_df = spark.createDataFrame(rates, ["currency", "inr_rate"])
rates_df.show()

# COMMAND ----------

df_gold_order = (
    df_gold_order
    .join(
        rates_df,
        rates_df.currency == F.upper(F.trim(F.col("unit_price_currency"))),
        "left"
    )
    .withColumn("sale_amount_inr", F.col("sale_amount") * F.col("inr_rate"))
    .withColumn("sale_amount_inr", F.ceil(F.col("sale_amount_inr")))
)

# COMMAND ----------

display(df_gold_order)

# COMMAND ----------

df_gold_order = df_gold_order.select(
    F.col("date_id"),
    F.col("dt").alias("transaction_date"),
    F.col("order_ts").alias("transaction_ts"),
    F.col("order_id").alias("transaction_id"),
    F.col("customer_id"),
    F.col("item_seq").alias("seq_no"),
    F.col("product_id"),
    F.col("channel"),
    F.col("coupon_code"),
    F.col("coupon_flag"),
    F.col("unit_price_currency"),
    F.col("quantity"),
    F.col("unit_price"),
    F.col("gross_amount"),
    F.col("discount_pct").alias("discount_percent"),
    F.col("discount_amount"),
    F.col("tax_amount"),
    F.col("sale_amount").alias("net_amount"),
    F.col("sale_amount_inr").alias("net_amount_inr")
)

# COMMAND ----------

df_gold_order.display()

# COMMAND ----------

df_gold_order.write.format("delta") \
    .mode("overwrite") \
    .option("mergeSchema", "true") \
    .saveAsTable(f"{catalog_name}.gold.gld_fact_order_items")

# COMMAND ----------

spark.sql(f" select count(*) from {catalog_name}.gold.gld_fact_order_items").show()