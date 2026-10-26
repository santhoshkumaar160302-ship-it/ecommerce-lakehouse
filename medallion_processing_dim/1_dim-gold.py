# Databricks notebook source
import pyspark.sql.functions as F
from pyspark.sql.types import StringType, IntegerType, DateType, TimestampType, FloatType
from pyspark.sql import Row

# COMMAND ----------

catalog_name = "ecommerce"

# COMMAND ----------

df_products = spark.table(f"{catalog_name}.silver.slv_products")
df_brands = spark.table(f"{catalog_name}.silver.slv_brands")
df_category = spark.table(f"{catalog_name}.silver.slv_category")

# COMMAND ----------

df_products.createOrReplaceTempView("v_products")
df_brands.createOrReplaceTempView("v_brands")
df_category.createOrReplaceTempView("v_category")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from v_products limit 4

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select * from v_category limit 4

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select * from v_brands limit 4

# COMMAND ----------

spark.sql(f"USE CATALOG {catalog_name}")


# COMMAND ----------

# MAGIC %md
# MAGIC ## Products

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC create or replace table gold.gld_dim_products as
# MAGIC select
# MAGIC p.product_id,
# MAGIC p.sku,
# MAGIC p.category_code,
# MAGIC coalesce(c.category_name,'Not Available') as category_name,
# MAGIC p.brand_code,
# MAGIC coalesce(b.brand_name,'Not Available') as brand_name,
# MAGIC p.color,
# MAGIC p.size,
# MAGIC p.material,
# MAGIC p.weight_grams,
# MAGIC p.length_cm,
# MAGIC p.width_cm,
# MAGIC p.height_cm,
# MAGIC p.rating_count,
# MAGIC p._source_file,
# MAGIC p._ingest_timestamp
# MAGIC from v_products p
# MAGIC left join v_brands b on p.brand_code = b.brand_code
# MAGIC left join v_category c on p.category_code = c.category_code;

# COMMAND ----------

# MAGIC %sql 
# MAGIC select * from gold.gld_dim_products

# COMMAND ----------

# MAGIC %md
# MAGIC ## Customers

# COMMAND ----------

# India states
india_region = {
    "MH": "West", "GJ": "West", "RJ": "West",
    "KA": "South", "TN": "South", "TS": "South", "AP": "South", "KL": "South",
    "UP": "North", "WB": "North", "DL": "North"
}
# Australia states
australia_region = {
    "VIC": "SouthEast", "WA": "West", "NSW": "East", "QLD": "NorthEast"
}

# United Kingdom states
uk_region = {
    "ENG": "England", "WLS": "Wales", "NIR": "Northern Ireland", "SCT": "Scotland"
}

# United States states
us_region = {
    "MA": "NorthEast", "FL": "South", "NJ": "NorthEast", "CA": "West", 
    "NY": "NorthEast", "TX": "South"
}

# UAE states
uae_region = {
    "AUH": "Abu Dhabi", "DU": "Dubai", "SHJ": "Sharjah"
}

# Singapore states
singapore_region = {
    "SG": "Singapore"
}

# Canada states
canada_region = {
    "BC": "West", "AB": "West", "ON": "East", "QC": "East", "NS": "East", "IL": "Other"
}

# Combine into a master dictionary
country_state_map = {
    "India": india_region,
    "Australia": australia_region,
    "United Kingdom": uk_region,
    "United States": us_region,
    "United Arab Emirates": uae_region,
    "Singapore": singapore_region,
    "Canada": canada_region
}  


# COMMAND ----------

country_state_map

# COMMAND ----------

rows = []
for country, state_map in country_state_map.items():
    for state, region in state_map.items():
        rows.append(Row(country=country, state=state, region=region))
rows[:10]

# COMMAND ----------

df_region_mapping = spark.createDataFrame(rows)

df_region_mapping.show(truncate=False)

# COMMAND ----------

df_silver_customer = spark.table(f"{catalog_name}.silver.slv_customers")

display(df_silver_customer.limit(5))

# COMMAND ----------

df_gold_customer = df_silver_customer.join(df_region_mapping, on=["country",'state'], how="left")
df_gold_customer = df_gold_customer.fillna({'region': 'Other'})

display(df_gold_customer.limit(5))

# COMMAND ----------

df_gold_customer.write.format("delta") \
    .mode("overwrite") \
    .option("mergeSchema", "true") \
    .saveAsTable(f"{catalog_name}.gold.gld_dim_customers")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Date/Calendar

# COMMAND ----------

df_silver_dates = spark.table(f"{catalog_name}.silver.slv_dates")

display(df_silver_dates.limit(5))

# COMMAND ----------


df_gold_dates = df_silver_dates.withColumn('date_id', F.date_format(F.col('date'), 'yyyyMMdd').cast('int'))

df_gold_dates = df_gold_dates.withColumn('month_name', F.date_format(F.col('date'), 'MMMM'))

df_gold_dates = df_gold_dates.withColumn('is_weekend', F.when(F.col('day_name').isin(['Saturday', 'Sunday']), 1).otherwise(0))



display(df_gold_dates.limit(5))


# COMMAND ----------

desired_columns_order = ['date_id', 'date', 'year', 'month_name','day_name', 'is_weekend', 'quarter', 'week', '_ingest_timestamp', '_source_file']
df_gold_dates = df_gold_dates.select(desired_columns_order)

display(df_gold_dates.limit(5))


# COMMAND ----------

df_gold_dates.write.format("delta") \
    .mode("overwrite") \
    .option("mergeSchema", "true") \
    .saveAsTable(f"{catalog_name}.gold.gld_dim_date")