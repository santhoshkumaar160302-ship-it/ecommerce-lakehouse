# Databricks notebook source
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType, TimestampType
import pyspark.sql.functions as F

# COMMAND ----------

# spark.sql(f"DROP TABLE {catalog_name}.bronze.brz_brands")

# COMMAND ----------

catalog_name = 'ecommerce'

# define schema for the data file
schema = StructType([
  StructField('brand_code', StringType(), False),
  StructField('brand_name', StringType(), True),
  StructField('catalog_code', StringType(), True)
  ])


# COMMAND ----------

# MAGIC %md
# MAGIC ## Brand

# COMMAND ----------

raw_data_path = '/Volumes/ecommerce/source_data/raw/brands/*.csv'

df  = spark.read.option("header", "true").option('delimiter', ',').schema(schema).csv(raw_data_path)

df = df.withColumn('_source_file', F.col('_metadata.file_path')) \
        .withColumn('_ingest_timestamp', F.current_timestamp())

display(df.limit(5))

# COMMAND ----------

df = df.drop('_ingest_at')
df.show(truncate=False)

# COMMAND ----------

# delta table 

df.write.format('delta').mode('overwrite') \
        .option('mergeSchema', 'true') \
        .saveAsTable(f'{catalog_name}.bronze.brz_brands')

display(spark.table(f'{catalog_name}.bronze.brz_brands').limit(5))

# COMMAND ----------

display(spark.table(f'{catalog_name}.bronze.brz_brands').limit(5))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Category

# COMMAND ----------

# spark.sql(f" drop table {catalog_name}.bronze.brz_category")

# COMMAND ----------

category_schema = StructType([
  StructField('category_code', StringType(), False),
  StructField('category_name', StringType(), True)
])

raw_data_path = '/Volumes/ecommerce/source_data/raw/category/*.csv'

df = spark.read.option("header", "true").option('delimiter', ',').schema(category_schema).csv(raw_data_path)

df = df.withColumn('_source_file', F.col('_metadata.file_path')) \
        .withColumn('_ingest_timestamp', F.current_timestamp())

display(df.limit(5))
df.write.format('delta').mode('overwrite') \
        .option('mergeSchema', 'true') \
        .saveAsTable(f'{catalog_name}.bronze.brz_category')

# COMMAND ----------

# MAGIC %md
# MAGIC ## Products

# COMMAND ----------

# spark.sql(f" drop table {catalog_name}.bronze.brz_products")

# COMMAND ----------

product_schema = StructType([
  StructField('product_id', StringType(), False),
  StructField('sku', StringType(), True),
  StructField('category_code', StringType(), True),
  StructField('brand_code', StringType(), True),
  StructField('color', StringType(), True),
  StructField('size', StringType(), True),
  StructField('material', StringType(), True),
  StructField('weight_grams', StringType(), True),
  StructField('length_cm', StringType(), True),
  StructField("width_cm", FloatType(), True),
  StructField("height_cm", FloatType(), True),
  StructField("rating_count", IntegerType(), True),
  StructField("_source_file", StringType(), False),
  StructField("_ingest_timestamp", TimestampType(), False)
])

raw_data_path = '/Volumes/ecommerce/source_data/raw/products/*.csv'

df = spark.read.option("header", "true").option('delimiter', ',').schema(product_schema).csv(raw_data_path) \
  .withColumn('_source_file', F.col('_metadata.file_path')) \
  .withColumn('_ingest_timestamp', F.current_timestamp())


df.write.format('delta')\
  .mode('overwrite') \
        .option('overwriteSchema', 'true') \
        .saveAsTable(f'{catalog_name}.bronze.brz_products')

display(df.limit(5))


# COMMAND ----------

# MAGIC %md
# MAGIC ## Customer

# COMMAND ----------

# spark.sql(f" drop table {catalog_name}.bronze.brz_customers")

# COMMAND ----------

customer_schema = StructType([
  StructField('customer_id', StringType(), False),
  StructField('phone', StringType(), True),
  StructField('country_code', StringType(), True),
  StructField('country', StringType(), True),
  StructField('state', StringType(), True),
])

raw_data_path = '/Volumes/ecommerce/source_data/raw/customers/*.csv'

df = spark.read.option("header", "true").option('delimiter', ',').schema(customer_schema).csv(raw_data_path)

df = df.withColumn('_source_file', F.col('_metadata.file_path')) \
        .withColumn('_ingest_timestamp', F.current_timestamp())

display(df.limit(5))
df.write.format('delta').mode('overwrite') \
        .option('mergeSchema', 'true') \
        .saveAsTable(f'{catalog_name}.bronze.brz_customers')

# COMMAND ----------

# MAGIC %md
# MAGIC ## Date

# COMMAND ----------

date_schema = StructType([
    StructField('date', StringType(), True),
    StructField('year', IntegerType(), True),
    StructField('day_name', StringType(), False),
    StructField('quarter', IntegerType(), True),
    StructField('week_of_year', IntegerType(), True)
])


raw_data_path = '/Volumes/ecommerce/source_data/raw/date/*.csv'

df = spark.read.option("header", "true").option('delimiter', ',').schema(date_schema).csv(raw_data_path)

df= df.withColumn('_source_file', F.col('_metadata.file_path')) \
        .withColumn('_ingest_timestamp', F.current_timestamp())


df.write.format('delta').mode('overwrite') \
        .option('mergeSchema', 'true') \
        .saveAsTable(f'{catalog_name}.bronze.brz_dates')
