# Databricks notebook source
# MAGIC %md
# MAGIC # Tableau export
# MAGIC Joins the gold fact table to its dimensions and writes one CSV to a Unity Catalog volume
# MAGIC (Tableau Public can't connect to Databricks directly).
# MAGIC Download it from Catalog → ecommerce → source_data → exports.

# COMMAND ----------

import pyspark.sql.functions as F

catalog_name = "ecommerce"
spark.sql(f"USE CATALOG {catalog_name}")
spark.sql("CREATE VOLUME IF NOT EXISTS source_data.exports")
export_path = f"/Volumes/{catalog_name}/source_data/exports"

fact = spark.table("gold.gld_fact_order_items")
prod = spark.table("gold.gld_dim_products").select("product_id", "category_name", "brand_name")
cust = spark.table("gold.gld_dim_customers").select("customer_id", "country", "state", "region")
dates = spark.table("gold.gld_dim_date").select("date_id", "year", "quarter", "month_name", "day_name", "is_weekend")

sales = (
    fact.join(F.broadcast(prod), "product_id", "left")
        .join(F.broadcast(cust), "customer_id", "left")
        .join(F.broadcast(dates), "date_id", "left")
        .select(
            "transaction_id", "seq_no", "transaction_date", "year", "quarter", "month_name",
            "day_name", "is_weekend", "customer_id", "country", "state", "region",
            "product_id", "category_name", "brand_name", "channel", "coupon_flag",
            "quantity", "gross_amount", "discount_amount", "net_amount", "net_amount_inr",
        )
)

# Write one CSV file (Spark normally writes a folder of part files)
tmp = f"{export_path}/_tmp_sales"
sales.coalesce(1).write.mode("overwrite").option("header", "true").csv(tmp)
part = [f.path for f in dbutils.fs.ls(tmp) if f.name.endswith(".csv")][0]
dbutils.fs.cp(part, f"{export_path}/sales_report.csv")
dbutils.fs.rm(tmp, recurse=True)

print(f"Exported {sales.count():,} rows to {export_path}/sales_report.csv")
