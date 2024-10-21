# Databricks notebook source
# MAGIC %md
# MAGIC #### **lit()**
# MAGIC
# MAGIC - PySpark 𝐥𝐢𝐭() function is used to **add constant or literal value** as a **new column** to the DataFrame.
# MAGIC
# MAGIC - We can also use this function to derive the **new column** based on **some conditions**.

# COMMAND ----------

from pyspark.sql.functions import lit, col, when, current_timestamp, to_timestamp
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, LongType
from datetime import datetime

# COMMAND ----------

# MAGIC %md
# MAGIC #### **EX 01**

# COMMAND ----------

data =[{'rollno':'01', 'name':'sravan', 'age':23, 'height':5.79, 'weight':67, 'address':'Guntur'},
       {'rollno':'02', 'name':'ojaswi', 'age':26, 'height':3.79, 'weight':34, 'address':'Hyderabad'},
       {'rollno':'03', 'name':'gnanesh', 'age':37, 'height':2.79, 'weight':37, 'address':'Chennai'},
       {'rollno':'04', 'name':'rohith', 'age':29, 'height':3.69, 'weight':28, 'address':'Bangalore'},
       {'rollno':'05', 'name':'sridevi', 'age':45, 'height':5.59, 'weight':54, 'address':'Hyderabad'},
       {'rollno':'01', 'name':'Amit Mishra', 'age':26, 'height':5.79, 'weight':67, 'address':'Delhi'},
       {'rollno':'02', 'name':'Niraj Guptha', 'age':56, 'height':4.99, 'weight':34, 'address':'Mumbai'},
       {'rollno':'03', 'name':'Sridharan', 'age':57, 'height':3.79, 'weight':47, 'address':'Delhi'},
       {'rollno':'04', 'name':'Kiran', 'age':49, 'height':4.69, 'weight':38, 'address':'Bangalore'},
       {'rollno':'05', 'name':'Dhiraj', 'age':42, 'height':6.00, 'weight':34, 'address':'Nasik'},
       {'rollno':'05', 'name':'Dhiraj', 'age':42, 'height':6.00, 'weight':34, 'address':'Kolkata'},
       {'rollno':'05', 'name':'Dhiraj', 'age':42, 'height':6.00, 'weight':34, 'address':'Gurgaon'}]

# create the dataframe
df = spark.createDataFrame(data)
df.show(truncate=False)
df.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ##### a) Adding Constant Column
# MAGIC - PySpark lit() function is used to **add constant or literal value** as a **new column** to the DataFrame.

# COMMAND ----------

# add a new column: "source_id"
df = df.select("*", lit(2).alias("source_id"))

# display the final dataframe
df.show(truncate=False)

# COMMAND ----------

# add a new column: "source_id"
df = df.select("*", lit(datetime.now()).alias("Today's Date"),
                    lit('').alias('vehicle_description'),
                    to_timestamp(lit("1900-01-01 00:00:00"),'yyyy-MM-dd HH:mm:ss').alias('valid_from_datetime'),
                    to_timestamp(lit("9999-12-31 23:59:59"),'yyyy-MM-dd HH:mm:ss').alias('valid_to_datetime'))

# display the final dataframe
display(df)

# COMMAND ----------

# add a new column: "PinCode City from address column
df = df.select("*", lit(df.address).alias("PinCode_City"))

# display the final dataframe
display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### b) withColumn

# COMMAND ----------

df = df.withColumn("PinCode", when((col("address") == "Guntur"), lit("522002")). \
                              when((col("address") == "Hyderabad"), lit("500001")). \
                              when((col("address") == "Chennai"), lit("600011")). \
                              when((col("address") == "Bangalore"), lit("560001")). \
                              when((col("address") == "Delhi"), lit("110006")). \
                              when((col("address") == "Mumbai"), lit("400001")). \
                              otherwise(lit("402343")))
display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### **EX 02**

# COMMAND ----------

df1 = spark.read.csv("/FileStore/tables/StructType-5.csv", header=True, inferSchema=True)
display(df1)
df1.printSchema()
print("Total Number of Rows: ", df1.count())
print("List of Column Names: ", df1.columns)
print("No of Columns in dataset: ", len(df1.columns))

# COMMAND ----------

df1 = df1.select("*", lit(2).cast(LongType()).alias('source_id'),
                      lit('').alias('vehicle_buy_or_sell'),
                      lit('').alias('vehicle_description'),
                      lit('').alias('delivery_status'),
                      lit('').alias('vehicle_classification_id'),
                      lit('').alias('vehicle_product_type'),
                      lit('').alias('pricing_model_id'),
                      lit('').alias('vehicle_agreement_id'),
                      lit('').alias('commercial_vehicle_venue_id'),
                      to_timestamp(lit("1999-01-01 00:00:00"),'yyyy-MM-dd HH:mm:ss').alias('valid_from_datetime'),
                      to_timestamp(lit("2023-12-31 23:59:59"),'yyyy-MM-dd HH:mm:ss').alias('valid_to_datetime'),
                      lit(datetime.now()).alias("Today's Date"),
                      current_timestamp().alias("created_datetime"),
                      current_timestamp().alias("updated_datetime")
                )

display(df1)
