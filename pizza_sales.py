# Databricks notebook source
dbutils.fs.mount(
    source = "wasbs://<container-name>@<storage-account-name>.blob.core.windows.net",
    mount_point = "/mnt/iotdata", #mount location
    extra_configs = {"fs.azure.account.key.<storage-account-name>.blob.core.windows.net":dbutils.secrets.get(scope = "<scope-name>",key = "<key-name>")}
)

# COMMAND ----------

dbutils.fs.ls("/mnt/iotdata/")

# COMMAND ----------

df = spark.read.format("csv").options(header = 'True', inferSchema = 'True').load('dbfs:/mnt/iotdata/dbo.pizza_sales.txt')

# COMMAND ----------

display(df)

# COMMAND ----------

df.createOrReplaceTempView("pizza_sales_analysis")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from pizza_sales_analysis

# COMMAND ----------

# MAGIC %sql
# MAGIC select
# MAGIC order_id,
# MAGIC quantity,
# MAGIC date_format(order_date,'MMM') as month_name,
# MAGIC date_format(order_date,'EEEE') as day_name,
# MAGIC hour(order_time) as order_time,
# MAGIC unit_price,
# MAGIC total_price,
# MAGIC pizza_size,
# MAGIC pizza_category,
# MAGIC pizza_name
# MAGIC from pizza_sales_analysis

# COMMAND ----------

# MAGIC %sql
# MAGIC select
# MAGIC count(distinct order_id) order_id,
# MAGIC sum(quantity) quantity,
# MAGIC date_format(order_date,'MMM') as month_name,
# MAGIC date_format(order_date,'EEEE') as day_name,
# MAGIC hour(order_time) as order_time,
# MAGIC sum(unit_price) unit_price,
# MAGIC sum(total_price) total_sales,
# MAGIC pizza_size,
# MAGIC pizza_category,
# MAGIC pizza_name
# MAGIC from pizza_sales_analysis
# MAGIC group by 3,4,5,8,9,10
