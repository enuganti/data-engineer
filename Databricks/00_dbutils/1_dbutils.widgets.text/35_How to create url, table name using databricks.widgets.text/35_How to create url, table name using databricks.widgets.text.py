# Databricks notebook source
# MAGIC %md
# MAGIC #### How to generate below parameters using databricks.widgets.text & databricks.widgets.get?
# MAGIC - **restapi url**
# MAGIC - **Full table name** (catalog name.schema.table name)
# MAGIC - Extract **catalog name** from full table name
# MAGIC - Extract **schema** from full table name

# COMMAND ----------

# MAGIC %md
# MAGIC #### Syntax
# MAGIC
# MAGIC      dbutils.widgets.text(name='your_name_text', defaultValue='Enter your name', label='Your name')

# COMMAND ----------

# MAGIC %md
# MAGIC      # Remove a specific widget
# MAGIC      dbutils.widgets.remove("API_URL")
# MAGIC
# MAGIC      # Remove all widgets
# MAGIC      dbutils.widgets.removeAll()

# COMMAND ----------

# Remove a specific widget
dbutils.widgets.remove("ENV_ID")

# COMMAND ----------

# Remove all widgets
dbutils.widgets.removeAll()

# COMMAND ----------

dbutils.widgets.text("SCHEDULE_DATE", "", "SCHEDULE DATE")

# COMMAND ----------

dbutils.widgets.get("SCHEDULE_DATE")

# COMMAND ----------

PARAM_SCHEDULE_DATE = dbutils.widgets.get("SCHEDULE_DATE")
print(PARAM_SCHEDULE_DATE)

# COMMAND ----------

dbutils.widgets.text("DATABASE_NAME", "sap_dev.bronze", "DATABASE_NAME")

# COMMAND ----------

PARAM_DATABASE_NAME = dbutils.widgets.get("DATABASE_NAME")
print(PARAM_DATABASE_NAME)

# COMMAND ----------

dbutils.widgets.removeAll()

dbutils.widgets.text("SCHEDULE_DATE", "", "SCHEDULE_DATE")
dbutils.widgets.text("ENV_ID", "", "ENV_ID")
dbutils.widgets.text("DATABASE_NAME", "sap_dev.bronze", "DATABASE_NAME")
dbutils.widgets.text("SOURCE_TABLE_NAME", "", "SOURCE_TABLE_NAME")
dbutils.widgets.text("TARGET_TABLE_NAME", "", "TARGET_TABLE_NAME")
dbutils.widgets.text("API_URL", "", "API_URL")
dbutils.widgets.text("INGESTION_TYPE", "", "INGESTION_TYPE")

PARAM_SCHEDULE_DATE = dbutils.widgets.get("SCHEDULE_DATE")
PARAM_ENV_ID = dbutils.widgets.get("ENV_ID")
PARAM_DATABASE_NAME = dbutils.widgets.get("DATABASE_NAME")
PARAM_SOURCE_TABLE_NAME = dbutils.widgets.get("SOURCE_TABLE_NAME")
PARAM_TARGET_TABLE_NAME = dbutils.widgets.get("TARGET_TABLE_NAME")
PARAM_API_URL = dbutils.widgets.get("API_URL")
PARAM_TYPE_OF_INGESTION = dbutils.widgets.get("INGESTION_TYPE")

print("PARAM_SCHEDULE_DATE:", PARAM_SCHEDULE_DATE)
print("PARAM_ENV_ID:", PARAM_ENV_ID)
print("PARAM_DATABASE_NAME:", PARAM_DATABASE_NAME)
print("PARAM_SOURCE_TABLE_NAME:", PARAM_SOURCE_TABLE_NAME)
print("PARAM_TARGET_TABLE_NAME:", PARAM_TARGET_TABLE_NAME)
print("PARAM_API_URL:", PARAM_API_URL)
print("PARAM_TYPE_OF_INGESTION:", PARAM_TYPE_OF_INGESTION)

# COMMAND ----------

PARAM_UNITY_CATELOG_NAME = PARAM_DATABASE_NAME.split(".")[0]
PARAM_TABLE_NAME = PARAM_DATABASE_NAME.split(".")[1]

full_tableName = PARAM_DATABASE_NAME + "." + PARAM_SOURCE_TABLE_NAME

URL_PREFIX = ("https://my" + PARAM_ENV_ID + PARAM_API_URL + "&$format=json" + "&$inlinecount=allpages")

print("PARAM_UNITY_CATELOG_NAME:", PARAM_UNITY_CATELOG_NAME)
print("PARAM_TABLE_NAME:", PARAM_TABLE_NAME)
print("full_tableName:", full_tableName)
print("URL_PREFIX:", URL_PREFIX)