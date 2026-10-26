# Databricks notebook source
import pyspark.sql.functions as F
from pyspark.sql.types import *

catalog_name = 'ecommerce'

# COMMAND ----------

# spark.sql(f"DROP TABLE {catalog_name}.silver.slv_products")


# COMMAND ----------

# spark.sql(f"DROP TABLE {catalog_name}.silver.slv_brands")


# COMMAND ----------

# spark.sql(f"DROP TABLE {catalog_name}.silver.slv_category")


# COMMAND ----------

df_bronze = spark.table(f'{catalog_name}.bronze.brz_brands')
display(df_bronze)

# COMMAND ----------

# removed the leading and trailing space from brand name
df_silver_brand = df_bronze.withColumn('brand_name', F.trim('brand_name'))
display(df_silver_brand)

# COMMAND ----------


# removed the special character from brand_code
df_silver_brand = df_silver_brand.withColumn('brand_code', F.regexp_replace(df_silver_brand.brand_code, '[^A-Za-z0-9]+', ''))

display(df_silver_brand)


# COMMAND ----------

df_silver_brand.select('catalog_code').distinct().show()


# COMMAND ----------

anamolies = {
    "GROCERY" : "GRCY",
    "BOOKS" : "BKS",
    "TOYS" : "TOY"
}
df_silver_brand = df_silver_brand.replace(anamolies, subset='catalog_code')

df_silver_brand.select('catalog_code').distinct().show()

# COMMAND ----------

display(df_silver_brand)

# COMMAND ----------


df_silver_brand.write.mode("overwrite").option('mergeSchema', 'true').saveAsTable(f"{catalog_name}.silver.slv_brands")

# COMMAND ----------

display(df_silver_brand)

# COMMAND ----------

# MAGIC %md
# MAGIC ## category

# COMMAND ----------

df_bronze = spark.table(f"{catalog_name}.bronze.brz_category")

df_bronze.show(10)


# COMMAND ----------

df_duplicates_category = df_bronze.groupBy('category_code').count().filter(F.col('count') > 1)
display(df_duplicates_category)


# COMMAND ----------

df_silver_category = df_bronze.dropDuplicates(['category_code'])

display(df_silver_category)

# COMMAND ----------

df_silver=df_silver.drop('_ingest_at')


# COMMAND ----------

df_silver_category = df_silver_category.withColumn("category_code", F.upper(df_silver_category.category_code))
display(df_silver_category)

# COMMAND ----------

df_silver_category.write.format('delta').mode("overwrite").option('mergeSchema', 'true').saveAsTable(f"{catalog_name}.silver.slv_category")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Products
# MAGIC

# COMMAND ----------

df_bronze = spark.read.table(f"{catalog_name}.bronze.brz_products")

row_count , column_count = df_bronze.count(), len(df_bronze.columns)
print(f"row_count = {row_count}, column_count = {column_count}")


# COMMAND ----------

display(df_bronze.limit(5))

# COMMAND ----------

df_bronze.select('weight_grams').show(5, truncate=False)

# COMMAND ----------

df_silver_product = df_bronze.withColumn(
    "weight_grams",
    F.regexp_replace(F.col("weight_grams"), "g", "").cast(IntegerType())
)
df_silver_product.select("weight_grams").show(5, truncate=False)


# COMMAND ----------

df_silver_product.select('length_cm').show()

# COMMAND ----------

df_silver_product = df_silver_product.withColumn(
    "length_cm",
    F.regexp_replace(F.col("length_cm"), ",", ".").cast(FloatType())
)
df_silver_product.select("length_cm").show(3, truncate=False)




# COMMAND ----------

display(df_silver_product.select('length_cm'))

# COMMAND ----------

df_silver_product.select('category_code', "brand_code").show(2)

# COMMAND ----------

df_silver_product= df_silver_product.withColumn('category_code', F.upper('category_code')).withColumn('brand_code', F.upper('brand_code'))

df_silver_product.select('category_code', "brand_code").show(2)

# COMMAND ----------

df_silver_product.select('material').distinct().show()

# COMMAND ----------

df_silver_product = df_silver_product.withColumn(
    "material",
    F.when(F.col("material") == "Coton", "Cotton")
     .when(F.col("material") == "Alumium", "Aluminum")
     .when(F.col("material") == "Ruber", "Rubber")
     .otherwise(F.col("material"))
)
df_silver_product.select("material").distinct().show()    

# COMMAND ----------

df_silver_product.filter(F.col("rating_count") < 0).select('rating_count').show(3)

# COMMAND ----------

df_silver_product = df_silver_product.withColumn('rating_count', F.when(F.col('rating_count').isNotNull(), F.abs(F.col('rating_count'))).otherwise(F.lit(0)))



# COMMAND ----------

df_silver_product.select('weight_grams', 'length_cm', 'category_code', 'brand_code', 'material', 'rating_count').show(10, truncate = False)

# COMMAND ----------

df_silver_product.show(10, truncate= False)

# COMMAND ----------

# DBTITLE 1,Write products silver table (float length_cm)
df_silver_product.write.format("delta") \
    .mode("overwrite") \
    .option("mergeSchema", "true") \
    .saveAsTable(f"{catalog_name}.silver.slv_products")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Customers

# COMMAND ----------

# Read the raw data from the bronze table (ecommerce.bronze.brz_calendar)
df_bronze_customer = spark.read.table(f"{catalog_name}.bronze.brz_customers")

# Get row and column count
row_count, column_count = df_bronze_customer.count(), len(df_bronze_customer.columns)

# Print the results
print(f"Row count: {row_count}")
print(f"Column count: {column_count}")

df_bronze.show(10)

# COMMAND ----------

null_count_customer = df_bronze_customer.filter(F.col('customer_id').isNull()).count()

null_count_customer

# COMMAND ----------

df_bronze_customer.filter(F.col('customer_id').isNull()).show(5)

# COMMAND ----------

df_silver_customer =  df_bronze_customer.dropna(subset=['customer_id'])

row_count = df_silver_customer.count()

row_count

# COMMAND ----------

null_count_phone = df_silver_customer.filter(F.col("phone").isNull()).count()
null_count_phone

# COMMAND ----------

df_silver_customer.filter(F.col("phone").isNull()).display()

# COMMAND ----------

df_silver_customer= df_silver_customer.fillna("Not Available", subset=["phone"])

df_silver_customer.filter(F.col('phone').isNull()).display()

# COMMAND ----------

display(df_silver_customer)

# COMMAND ----------

# Write raw data to the silver layer (catalog: ecommerce, schema: silver, table: slv_customers)
df_silver_customer.write.format("delta") \
    .mode("overwrite") \
    .option("mergeSchema", "true") \
    .saveAsTable(f"{catalog_name}.silver.slv_customers")

# COMMAND ----------

# MAGIC %md
# MAGIC ## calender

# COMMAND ----------

df_bronze_calendar = spark.read.table(f"{catalog_name}.bronze.brz_dates")

# Get row and column count
row_count, column_count = df_bronze_calendar.count(), len(df_bronze_calendar.columns)

# Print the results
print(f"Row count: {row_count}")
print(f"Column count: {column_count}")

df_bronze_calendar.show(3)

# COMMAND ----------

df_bronze_calendar.printSchema()

# COMMAND ----------

from pyspark.sql.functions import to_date

df_silver_calendar = df_bronze_calendar.withColumn("date", to_date(df_bronze_calendar["date"], "dd-MM-yyyy"))

df_silver_calendar.show(3)
df_silver_calendar.printSchema()


# COMMAND ----------

# Find duplicate rows in the DataFrame
duplicates = df_silver_calendar.groupBy('date').count().filter("count > 1")

# Show the duplicate rows
print("Total duplicated Rows: ", duplicates.count())
display(duplicates)

# COMMAND ----------

df_silver_calendar_duplicates = df_silver_calendar.dropDuplicates(['date'])

row_count = df_silver_calendar_duplicates.count()

row_count


# COMMAND ----------

df_silver_calendar = df_silver_calendar_duplicates.withColumn('week_of_year', F.abs(F.col('week_of_year')))
df_silver_calendar.show(3)

# COMMAND ----------

df_silver_calendar = df_silver_calendar.withColumn('day_name', F.initcap(F.col('day_name')))

df_silver_calendar.select('day_name').show()

# COMMAND ----------

display(df_silver_calendar)

# COMMAND ----------

df_silver_calendar = df_silver_calendar.withColumn("quarter", F.concat_ws("", F.concat(F.lit("Q"), F.col("quarter"), F.lit("-"), F.col("year"))))

df_silver_calendar = df_silver_calendar.withColumn("week_of_year", F.concat_ws("-", F.concat(F.lit("Week"), F.col("week_of_year"), F.lit("-"), F.col("year"))))

df_silver_calendar.show(3)

# COMMAND ----------

df_silver_calendar = df_silver_calendar.withColumnRenamed("week_of_year", "week")

# COMMAND ----------

display(df_silver_calendar)

# COMMAND ----------

df_silver_calendar.write.format("delta") \
    .mode("overwrite") \
    .option("mergeSchema", "true") \
    .saveAsTable(f"{catalog_name}.silver.slv_dates")