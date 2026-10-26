# Databricks notebook source
import pyspark.sql.functions as F
from pyspark.sql.types import StringType, IntegerType, DateType, TimestampType, FloatType

# COMMAND ----------

catalog_name = "ecommerce"

# COMMAND ----------

df_order_silver = spark.table(f"{catalog_name}.bronze.brz_order_items")

display(df_order_silver)

# COMMAND ----------

df_order_silver.printSchema()

# COMMAND ----------

# Transformation: Drop any duplicates
df_order_silver = df_order_silver.dropDuplicates(["order_id", "item_seq"])

# Transformation: Convert 'Two' → 2 and cast to Integer
df_order_silver = df_order_silver.withColumn(
    "quantity",
    F.when(F.col("quantity") == "Two", 2).otherwise(F.col("quantity")).cast("int")
)

# Transformation : Remove any '$' or other symbols from unit_price, keep only numeric
df_order_silver = df_order_silver.withColumn(
    "unit_price",
    F.regexp_replace("unit_price", "[$]", "").cast("double")
)

# Transformation : Remove '%' from discount_pct and cast to double
df_order_silver = df_order_silver.withColumn(
    "discount_pct",
    F.regexp_replace("discount_pct", "%", "").cast("double")
)

# Transformation : coupon code processing (convert to lower)
df_order_silver = df_order_silver.withColumn(
    "coupon_code", F.lower(F.trim(F.col("coupon_code")))
)

# Transformation : channel processing 
df_order_silver = df_order_silver.withColumn(
    "channel",
    F.when(F.col("channel") == "web", "Website")
    .when(F.col("channel") == "app", "Mobile")
    .otherwise(F.col("channel")),
)

# COMMAND ----------


# Transformation: datatype conversions
# 1) Convert dt (string → date)
df_order_silver = df_order_silver.withColumn(
    "dt",
    F.to_date("dt", "yyyy-MM-dd")     
)

# 2) Convert order_ts (string → timestamp)
df_order_silver = df_order_silver.withColumn(
    "order_ts",
    F.coalesce(
        F.to_timestamp("order_ts", "yyyy-MM-dd HH:mm:ss"),  # matches 2025-08-01 22:53:52
        F.to_timestamp("order_ts", "dd-MM-yyyy HH:mm")      # fallback for 01-08-2025 22:53
    )
)


# 3) Convert item_seq (string → integer)
df_order_silver = df_order_silver.withColumn(
    "item_seq",
    F.col("item_seq").cast("int")
)

# 4) Convert tax_amount (string → double, strip non-numeric characters)
df_order_silver = df_order_silver.withColumn(
    "tax_amount",
    F.regexp_replace("tax_amount", r"[^0-9.\-]", "").cast("double")
)


#Transformation : Add processed time 
df_order_silver = df_order_silver.withColumn(
    "processed_time", F.current_timestamp()
)

# COMMAND ----------

display(df_order_silver)


# COMMAND ----------

df_order_silver.printSchema()

# COMMAND ----------

df_order_silver.write.format("delta") \
    .mode("overwrite") \
    .option("mergeSchema", "true") \
    .saveAsTable(f"{catalog_name}.silver.slv_order_items")