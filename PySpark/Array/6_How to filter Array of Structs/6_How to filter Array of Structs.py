# Databricks notebook source
# MAGIC %md
# MAGIC #### **How to filter Array of Structs?**
# MAGIC
# MAGIC Filtering an **array of structs** in PySpark can be achieved using the **filter** function available in PySpark SQL expressions.

# COMMAND ----------

# MAGIC %md
# MAGIC #### **Syntax**
# MAGIC
# MAGIC      filter(expr, func)
# MAGIC
# MAGIC **expr:** An ARRAY expression.
# MAGIC
# MAGIC **func:** A lambda function.
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT filter(array(0, 1, null, 2, 3, 4, 5, 6, null), x -> x IS NOT NULL) AS LAMDA;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT filter(array(1, 2, 3, 5, 7, 8, 9), x -> x % 2 == 1) AS LAMDA;

# COMMAND ----------

# MAGIC %md
# MAGIC - **array(1, 2, 3):** This creates an **array** with elements **1, 2, and 3**.
# MAGIC - **filter(array, x -> x % 2 == 1):** This applies the **filter** function to the **array**.
# MAGIC   - **x:** Represents **each element** of the **array**.
# MAGIC   - **x % 2 == 1:** Checks if the element x is **odd**. % is the **modulo operator**, which gives the **remainder** of the division of **x by 2**.
# MAGIC   - If x % 2 equals 1, x is odd.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT filter(array(0, 2, 3), (x, i) -> x > i) AS LAMDA;

# COMMAND ----------

# MAGIC %md
# MAGIC - **x:** Represents the **array element**.
# MAGIC - **i:** Represents the **index** of the **element in the array**.
# MAGIC - **x > i:** The condition that keeps the elements where the element value **x is greater than its index i**.
# MAGIC         
# MAGIC         0 (index 0): 0 > 0 (False)
# MAGIC         2 (index 1): 2 > 1 (True)
# MAGIC         3 (index 2): 3 > 2 (True)

# COMMAND ----------

# MAGIC %md
# MAGIC **EX 01**

# COMMAND ----------

# DBTITLE 1,Load required libraries
import pyspark.sql.functions as f
from pyspark.sql.functions import col, expr, current_timestamp
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, ArrayType, BooleanType

# COMMAND ----------

# DBTITLE 1,create dataframe for array of struct
# Sample schema for demonstration
schema1 = ArrayType(StructType([
    StructField("name", StringType(), True),
    StructField("age", IntegerType(), True),
    StructField("Exp", IntegerType(), True)
]))

data1 = [([{"name": "John", "age": 30, "Exp":5},
           {"name": "Paul", "age": 25, "Exp":8},
           {"name": "Watson", "age": 35, "Exp":3},
           {"name": "Sophia", "age": 32, "Exp":7},
           {"name": "Behra", "age": 45, "Exp":9},
           {"name": "Stalin", "age": 55, "Exp":12}],)]

df_flt_01 = spark.createDataFrame(data1, schema=["people"])

# Display the original DataFrame
display(df_flt_01)

# COMMAND ----------

# MAGIC %md
# MAGIC - To **filter** this array to **include only the structs** where age is **greater than 30**, you can use the expr function with a SQL expression.
# MAGIC
# MAGIC - **expr("filter(people, person -> person.age > 30)")** applies the **filter** function on the people array column. It retains only those structs (person) where the age field is **greater than 30**.
# MAGIC

# COMMAND ----------

# DBTITLE 1,filter records greater than 30
filtered_df_01 = df_flt_01.withColumn("filtered_people", expr("filter(people, person -> person.age > 30)"))

# Display the filtered DataFrame
display(filtered_df_01)

# COMMAND ----------

# MAGIC %md
# MAGIC - **df.filter** for **filtering rows** in a **DataFrame** based on a condition applied to column values.
# MAGIC
# MAGIC - However, the **expr("filter(people, person -> person.age > 30)")** expression is specifically used for **filtering elements within an array column of a DataFrame**, not for filtering rows of the DataFrame itself.

# COMMAND ----------

# MAGIC %md
# MAGIC **EX 02**

# COMMAND ----------

# MAGIC %md
# MAGIC - The code snippet you're asking about uses the expr function from PySpark to apply a SQL expression within a DataFrame transformation. Specifically, it **filters** a column named **person**, which is expected to be an **array of structs** (or similar complex type), to retain only those elements where the key field matches one of the specified values: **'SALESID', 'MAKEID', or 'VEHICLEID'**.
# MAGIC
# MAGIC - Here's a breakdown of the expression:
# MAGIC
# MAGIC   - **filter(person, x -> ...):** This applies a filter function to each element in the person array. The x represents each element in the array as the filter function iterates through it.
# MAGIC   - **array_contains(array('SALESID', 'MAKEID', 'VEHICLEID'), x.key):** This checks if the key field of the current element x is one of the specified values ('SALESID', 'MAKEID', or 'VEHICLEID'). The array_contains function returns true if the key is found in the provided array, and false otherwise.
# MAGIC   - **.alias('person'):** This renames the result of the expression to person, effectively creating a new column in the DataFrame with this name.
# MAGIC - The purpose of this code is to **filter and retain only specific person information** from a potentially larger set of person based on the key names. This can be useful for extracting and working with only relevant metadata from a message or data record, such as tracing or identification information indicated by **'SALESID', 'MAKEID', and 'VEHICLEID'**.

# COMMAND ----------

# Define the schema for the array of structs
schema2 = ArrayType(StructType([
    StructField("key", StringType(), True),
    StructField("value", BooleanType(), True)
]))

# Sample data
data2 = [([{"key": "SERIALID", "value": "batch_20230312"}, {"key": "SALESID", "value": "abghd2sjkhfl56"}, {"key": "MAKEID", "value": "acfgd2srtufl98"}, {"key": "VEHICLEID", "value": "abgqa5sjtopfl62"}, {"key": "SERIALID", "value": "batch_202431026"}, {"key": "RAWID", "value": "abghd2sjkhfl56"}, {"key": "ADMINID", "value": "acfgd2srtufl98"}, {"key": "GMID", "value": "abgqa5sjtopfl62"}, {"key": "SERIALID", "value": "batch_202431026"}, {"key": "BOXID", "value": "abghd2sjkhfl56"}, {"key": "TAXID", "value": "acfgd2srtufl98"}, {"key": "SALESID", "value": "abgqa5sjtopfl62"}],)]

# Create DataFrame using the defined schema
df_flt_02 = spark.createDataFrame(data=data2, schema=["person"])

# Display the original DataFrame
display(df_flt_02)

# COMMAND ----------

# MAGIC %md
# MAGIC      # 12.2 LTS (includes Apache Spark 3.3.2, Scala 2.12)
# MAGIC      from pyspark.sql.functions import expr
# MAGIC      df.select(expr("current_user()").alias("meta_created_by"))
# MAGIC
# MAGIC      # 15.4 LTS (includes Apache Spark 3.5.0, Scala 2.12)
# MAGIC      from pyspark.sql.functions import current_user
# MAGIC      df.select(current_user().alias("meta_created_by"))

# COMMAND ----------

df_per = df_flt_02.select(
    current_timestamp().alias("meta_created_timestamp"),
    expr("current_user()").alias("meta_created_by"),
    expr("filter(person, x -> x.key == 'SERIALID')[0].value").cast(StringType()).alias('serial_id'),
    expr("filter(person, x -> array_contains(array('SALESID', 'MAKEID', 'VEHICLEID'), x.key))").alias('person'),
    # JSON string into a VariantType
    # f.parse_json(f.to_json((f.struct(f.col('person')))).cast(StringType())).alias('batch_person')
    )

# Display the filtered DataFrame
display(df_per)
