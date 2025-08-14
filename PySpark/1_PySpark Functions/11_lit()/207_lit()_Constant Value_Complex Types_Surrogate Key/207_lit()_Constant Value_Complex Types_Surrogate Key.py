# Databricks notebook source
# MAGIC %md
# MAGIC #### **PySpark: lit()**
# MAGIC
# MAGIC - The name **lit** stands for **literal**.
# MAGIC - **lit()** can be used with various **constant values, including strings, integers, floats, and booleans**.
# MAGIC - It enables you to **create a column** with a **constant value** that can be used for **various purposes**:
# MAGIC   - adding metadata
# MAGIC   - flagging specific rows
# MAGIC   - performing calculations based on a fixed value.

# COMMAND ----------

# MAGIC %md
# MAGIC **Different usage of lit() function**
# MAGIC
# MAGIC      1) Creating a column by constant value
# MAGIC      2) Concatenating Strings (two columns)
# MAGIC      3) Assigning default values

# COMMAND ----------

# Sample data
data = [(101, "Girish", "Kumar", "Bangalore", 26),
        (102, "Ramesh", "Rathod", "Chennai", 29),
        (103, "Somesh", "Sekar", "Hyderabad", 32),
        (104, "Pradeep", "Rao", "Pune", 25),
        (105, "Kamal", "Prathap", "Mumbai", 27),
        (106, "Kishore", "Kumar", "Nasik", 34),
        (107, "Hari", "Shetty", None, 35),
        (108, "Basha", "Azmath", None, 24)
       ]

columns = ["EmpId", "first_name", "last_name", "City", "Age"]

# Create DataFrame
df_emp = spark.createDataFrame(data, columns)
display(df_emp)

# COMMAND ----------

from pyspark.sql.functions import lit, col, coalesce, concat

# COMMAND ----------

# MAGIC %md
# MAGIC ##### 1) Creating a column by constant value
# MAGIC - **lit()** can be used with various **constant values**, including:
# MAGIC   - strings
# MAGIC   - integers
# MAGIC   - floats
# MAGIC   - booleans

# COMMAND ----------

# Add constant & calculated columns using lit()
df_with_constant = (
    df_emp
    .withColumn("Country", lit("USA"))              # String constant
    .withColumn("DiscountRate", lit(5))             # Integer constant
    .withColumn("TaxRate", lit(18.5))               # Float constant
    .withColumn("InStock", lit(True))               # Boolean constant
    .withColumn(
        "TotalPriceWithTax",
        (col("Age") * lit(1000)) * (lit(1) + lit(0.18))  # Example calc using lit
    )
    .withColumn("DiscountStr", lit(5).cast("string"))   # Casted constant
)

display(df_with_constant)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### 2) Concatenating Strings (two columns)

# COMMAND ----------

# DBTITLE 1,a) Concatenate first name and last name
# Concatenate first name and last name

display(df_emp.withColumn("FullName", concat(col("first_name"), lit(" "), col("last_name"))))

# COMMAND ----------

# DBTITLE 1,b) generate surrogate_key
# Sample data
data = [
    ("guid-001", 101, 1, 0, 10, 1001, "2025-01-01 10:00:00"),
    ("guid-002", 102, 2, 1, 11, 1002, "2025-02-15 15:30:00"),
    ("guid-003", 103, 1, 2, 12, 1003, "2025-03-20 09:45:00"),
    ("guid-004", 104, 3, 0, 13, 1004, "2025-04-11 10:00:00"),
    ("guid-005", 105, 4, 1, 14, 1005, "2021-09-25 18:39:00"),
    ("guid-006", 106, 6, 2, 15, 1006, "2024-06-29 02:45:00"),
    ("guid-007", 107, 9, 0, 16, 1007, "2022-08-30 19:45:00"),
    ("guid-008", 108, 2, 1, 17, 1008, "2023-11-17 25:30:00"),
    ("guid-009", 109, 7, 2, 18, 1009, "2024-12-19 29:45:00")
]

columns = [
    "sales_event_guid",
    "make_id",
    "sub_version_id",
    "item_subversion_id",
    "sales_priority_id",
    "vehicle_engine_profile_id",
    "period_start_timestamp"
]

# Create DataFrame
df_concat = spark.createDataFrame(data, columns)
display(df_concat)

# COMMAND ----------

# Create surrogate_key column using concat and lit
df_concat_lit = df_concat.select("*",
    concat(
        col("sales_event_guid"), lit("-"),
        col("make_id"), lit("-"),
        col("sub_version_id"), lit("-"),
        col("item_subversion_id"), lit("-"),
        col("sales_priority_id"), lit("-"),
        col("vehicle_engine_profile_id"), lit("-"),
        col("period_start_timestamp")
    ).alias("surrogate_key")
)

# Show result
display(df_concat_lit)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### 3) Limitations with Complex Types
# MAGIC - creating columns with complex types:
# MAGIC   - array
# MAGIC   - struct
# MAGIC   - create_map

# COMMAND ----------

from pyspark.sql.functions import col, lit, array, struct, create_map

# Sample employee data
data = [
    (101, "Girish", 26),
    (102, "Ramesh", 29),
    (103, "Somesh", 32),
    (104, "Sirisha", 39),
    (105, "Kishore", 23),
    (106, "Akash", 35)
]
columns = ["EmpId", "Name", "Age"]

df_emp = spark.createDataFrame(data, columns)
display(df_emp)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### a) Creating an array column with lit()

# COMMAND ----------

df_with_array = df_emp.withColumn(
    "Skills",
    array(lit("Python"), lit("SQL"), lit("Spark"))
)
display(df_with_array)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### b) Creating a struct column with lit()

# COMMAND ----------

df_with_struct = df_emp.withColumn(
    "Address",
    struct(
        lit("Bangalore").alias("City"),
        lit("Karnataka").alias("State"),
        lit(560001).alias("Pincode")
    )
)
display(df_with_struct)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### c) Creating a map column with lit()

# COMMAND ----------

from pyspark.sql.functions import create_map, lit

df_with_map = df_emp.withColumn(
    "Properties",
    create_map(
        lit("Department"), lit("IT").cast("string"),
        lit("Level"), lit("Senior").cast("string"),
        lit("Experience"), lit(5).cast("string")
    )
)
display(df_with_map)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### d) All in one DataFrame

# COMMAND ----------

from pyspark.sql.functions import array, lit, struct, create_map

df_all_complex = (
    df_emp
    .withColumn("Skills", array(lit("Python"), lit("SQL"), lit("Spark")))
    .withColumn("Address", struct(lit("Bangalore").alias("City"), lit("Karnataka").alias("State"), lit(560001).alias("Pincode")))
    .withColumn("Properties", create_map(lit("Department"), lit("IT"), lit("Level"), lit("Senior"), lit("Experience"), lit("5")))
)

display(df_all_complex)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### 3) Assigning default values

# COMMAND ----------

from pyspark.sql.functions import col, lit, coalesce, array, struct, create_map
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, ArrayType, MapType

# Define schema explicitly so Struct column is actually StructType
schema = StructType([
    StructField("EmpId", IntegerType()),
    StructField("Name", StringType()),
    StructField("City", StringType()),
    StructField("Age", IntegerType()),
    StructField("Array", ArrayType(StringType())),
    StructField("Struct", StructType([
        StructField("City", StringType()),
        StructField("State", StringType()),
        StructField("Pincode", StringType())
    ])),
    StructField("map", MapType(StringType(), StringType()))
])

# Data with Struct as tuple (to match StructType) instead of dict
data = [
    (101, "Girish", "Bangalore", 26, ["Python","SQL","Spark"], ("Bangalore","Karnataka","560001"), None),
    (102, "Ramesh", None, 29, None, ("Bangalore","Karnataka","560001"), None),
    (103, "Somesh", "Hyderabad", None, ["Python","SQL","PySpark"], None, {"Department":"IT","Level":"Senior","Experience":"5"}),
    (104, "Pradeep", None, None, ["Git","SQL","Spark"], None, {"Department":"IT","Level":"Senior","Experience":"5"}),
    (105, "Kamal", "Mumbai", 27, None, ("Bangalore","Karnataka","560001"), None),
    (106, "Umesh", "Bangalore", 28, ["Python","Devops","PySpark"], None, {"Department":"IT","Level":"Senior","Experience":"5"}),
    (107, "Bobby", None, 32, None, ("Bangalore","Karnataka","560001"), None),
    (108, "Sandhya", "Chennai", None, ["Python","VS Code","SparkSQL"], None, {"Department":"IT","Level":"Senior","Experience":"5"}),
    (109, "Piyush", None, None, None, ("Bangalore","Karnataka","560001"), None),
    (110, "Goel", "Mumbai", 31, ["AWS","Azure","databricks"], None, {"Department":"IT","Level":"Senior","Experience":"5"})
]

df_emp = spark.createDataFrame(data, schema=schema)
display(df_emp)

# COMMAND ----------

# Applying coalesce with struct default
df_with_defaults = (
    df_emp
    .withColumn("City", coalesce(col("City"), lit("Unknown")))
    .withColumn("Age", coalesce(col("Age"), lit(30)))
    .withColumn(
        "Array",
        coalesce(col("Array"), array(lit("databricks"), lit("scala"), lit("pycharm")))
    )
    .withColumn(
        "Struct",
        coalesce(
            col("Struct"),
            struct(
                lit("Indore").alias("City"),
                lit("Delhi").alias("State"),
                lit("569581").alias("Pincode")
            )
        )
    )
    .withColumn(
        "map",
        coalesce(
            col("map"),
            create_map(
                lit("Department"), lit("IT"),
                lit("Level"), lit("Senior"),
                lit("Experience"), lit("5")
            )
        )
    )
)

display(df_with_defaults)

# COMMAND ----------

# MAGIC %md
# MAGIC - **coalesce(col("City"), lit("Unknown"))**
# MAGIC   - If City is **null**, replace it with **"Unknown"**.
# MAGIC   
# MAGIC - **coalesce(col("Age"), lit(30))**
# MAGIC   - If **Age** is **null**, replace it with **30**.