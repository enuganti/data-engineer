# Databricks notebook source
# MAGIC %md
# MAGIC ### **How to join multiple datasets?**
# MAGIC
# MAGIC - used to combine fields from **two or multiple** DataFrames by **chaining join()**.
# MAGIC - how to **eliminate the duplicate columns** on the result DataFrame
# MAGIC - Join is a **wider transformation** that does a lot of **shuffling**.
# MAGIC - This notebook covers **inner join** and **left join**.

# COMMAND ----------

# MAGIC %md
# MAGIC **Syntax**
# MAGIC
# MAGIC      df1.join(df2, df1.emp_id == df2.emp_id, 'left')
# MAGIC
# MAGIC      df1.join(df2, on=[f.col('emp_id') == f.col('emp_id')], how='left')
# MAGIC
# MAGIC      df1.join(df2, on=[f.col('emp_id') == f.col('emp_id')], how='left') \
# MAGIC         .join(df3, on=["emp_dept_id"], 'left')
# MAGIC
# MAGIC      df1.join(df2, ["emp_id"], 'left') \
# MAGIC         .join(df3, df1["emp_dept_id"] == df3["dept_id"], 'left')

# COMMAND ----------

# MAGIC %fs ls /FileStore/tables/

# COMMAND ----------

import pyspark.sql.functions  as f
from pyspark.sql.functions import col
from pyspark.sql.types import StringType, DoubleType, LongType

# COMMAND ----------

# MAGIC %md
# MAGIC #### **Ex 01**
# MAGIC
# MAGIC - How to join three datasets **Emp, Address and Dept** datasets.
# MAGIC - Inner join, this is the **default join** and it’s mostly used
# MAGIC - **Inner Join** joins two DataFrames on **key** columns, and where keys **don’t match** the rows get **dropped from both datasets**.

# COMMAND ----------

# DBTITLE 1,Dataset 1: Employee
# Emp Table

emp_data = [(1,"Smith",10), (2,"Rose",20), (3,"Williams",10), (4,"Jones",30)]
emp_Columns = ["emp_id","name","emp_dept_id"]

df_emp = spark.createDataFrame(emp_data, emp_Columns)
display(df_emp)

# COMMAND ----------

# DBTITLE 1,Dataset 2: Address
# Address Table

add_data=[(1,"1523 Main St","SFO","CA"),
          (2,"3453 Orange St","SFO","NY"),
          (3,"34 Warner St","Jersey","NJ"),
          (4,"221 Cavalier St","Newark","DE"),
          (5,"789 Walnut St","Sandiago","CA")
         ]
add_Columns = ["emp_id","addline1","city","state"]

df_add = spark.createDataFrame(add_data, add_Columns)
display(df_add)

# COMMAND ----------

# DBTITLE 1,Dataset 3: Department
# Dept Table

dept_data = [("Finance",10), ("Marketing",20), ("Sales",30),("IT",40)]
dept_Columns = ["dept_name","dept_id"]

df_dept = spark.createDataFrame(dept_data, dept_Columns)  
display(df_dept)

# COMMAND ----------

# DBTITLE 1,join Dataset 1 (Employee) & Dataset 2 (Address)
# join Employee and Address datasets
df_emp.join(df_add, df_emp.emp_id == df_add.emp_id, 'left').display()

# COMMAND ----------

# MAGIC %md
# MAGIC #### **Drop Duplicate Columns After Join**
# MAGIC - If you notice above Join DataFrame **emp_id** is **duplicated** on the result, In order to remove this duplicate column, specify the join column as an **array type or string**.
# MAGIC
# MAGIC - In order to use join columns as an **array**, you need to have the **same join columns** on **both DataFrames**.

# COMMAND ----------

# DBTITLE 1,Drop Duplicate Columns (emp_id) After Join Employee & Address
# Removes duplicate columns emp_id
df_emp.join(df_add, ["emp_id"], 'left').display()

# COMMAND ----------

# DBTITLE 1,join Dataset 1 (Employee), Dataset 2 (Address) & Dataset 3 (Department)
# Join Multiple DataFrames (Employee, Address and Department) by chaining
df_emp.join(df_add, ["emp_id"], 'left') \
      .join(df_dept, df_emp["emp_dept_id"] == df_dept["dept_id"], 'left') \
      .display()

# COMMAND ----------

# MAGIC %md
# MAGIC #### **Ex 02**
# MAGIC
# MAGIC - Read all input .csv files
# MAGIC   - Sales_Collect_Rev02.csv
# MAGIC   - SalesData_Rev02.csv
# MAGIC   - RunningData_Rev02.csv
# MAGIC
# MAGIC - join two datasets:
# MAGIC   - **Sales_Collect_Rev02** and **SalesData_Rev02**
# MAGIC - join three datasets:
# MAGIC   - **Sales_Collect_Rev02**, **SalesData_Rev02** and **RunningData_Rev02**

# COMMAND ----------

# DBTITLE 1,Read: Sales_Collect_Rev02.csv
# Read "Sales_Collect_Rev02.csv"
Sales_Collect_Rev02 = spark.read.option('header',True).option("quote", "\"").option('InferSchema',True).csv("/FileStore/tables/Sales_Collect_Rev02.csv")

display(Sales_Collect_Rev02.limit(10))
Sales_Collect_Rev02.printSchema()
print("Number of Rows:", Sales_Collect_Rev02.count())

# COMMAND ----------

# DBTITLE 1,Read: SalesData_Rev02.csv
# Read "SalesData_Rev02.csv"
SalesData_Rev02 = spark.read.option('header',True).option("quote", "\"").option('InferSchema',True).csv("/FileStore/tables/SalesData_Rev02.csv")

display(SalesData_Rev02.limit(10))
SalesData_Rev02.printSchema()
print("Number of Rows:", SalesData_Rev02.count())

# COMMAND ----------

# DBTITLE 1,Read: RunningData_Rev02.csv
# Read "RunningData_Rev02.csv"
RunningData_Rev02 = spark.read.option('header',True).option("quote", "\"").option('InferSchema',True).csv("/FileStore/tables/RunningData_Rev02.csv")

display(RunningData_Rev02.limit(10))
RunningData_Rev02.printSchema()
print("Number of Rows:", RunningData_Rev02.count())

# COMMAND ----------

# DBTITLE 1,join: Sales_Collect_Rev02 & SalesData_Rev02
# left join "Sales_Collect_Rev02" & "SalesData_Rev02"
Sales_Collect_df_Rev02_01 = Sales_Collect_Rev02.\
                             join(SalesData_Rev02, how='left',
                                  on=[f.col('Target_Simulation_Id') == f.col('Target_Simulation_Identity')])
                        
display(Sales_Collect_df_Rev02_01.limit(100))

# COMMAND ----------

# DBTITLE 1,join: Sales_Collect_Rev02, SalesData_Rev02 & RunningData_Rev02
# left join "Sales_Collect_Rev02", "SalesData_Rev02" and "RunningData_Rev02"
Sales_Collect_df_Rev02_02 = Sales_Collect_Rev02.\
                             join(SalesData_Rev02, how='left',
                                  on=[f.col('Target_Simulation_Id') == f.col('Target_Simulation_Identity')]).\
                             join(RunningData_Rev02, how='left', on=['Target_Event_Identity'])
                        
display(Sales_Collect_df_Rev02_02.limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC #### **Ex 03**
# MAGIC
# MAGIC - Read all input .csv files
# MAGIC   - Sales_Collect_Rev03.csv
# MAGIC   - SalesData_Rev03.csv
# MAGIC   - RunningData_Rev03.csv
# MAGIC
# MAGIC - join two datasets:
# MAGIC   - **Sales_Collect_Rev03** and **SalesData_Rev03**
# MAGIC - join three datasets:
# MAGIC   - **Sales_Collect_Rev03**, **SalesData_Rev03** and **RunningData_Rev03**

# COMMAND ----------

# DBTITLE 1,Read: Sales_Collect_Rev03.csv
# Read "Sales_Collect_Rev03.csv"
Sales_Collect_Rev03 = spark.read.option('header',True).option("quote", "\"").option('InferSchema',True).csv("/FileStore/tables/Sales_Collect_Rev03.csv")

display(Sales_Collect_Rev03.limit(10))
Sales_Collect_Rev03.printSchema()
print("Number of Rows:", Sales_Collect_Rev03.count())

# COMMAND ----------

# DBTITLE 1,Read: SalesData_Rev03.csv
# Read "SalesData_Rev03.csv"
SalesData_Rev03 = spark.read.option('header',True).option("quote", "\"").option('InferSchema',True).csv("/FileStore/tables/SalesData_Rev03.csv")

display(SalesData_Rev03.limit(10))
SalesData_Rev03.printSchema()
print("Number of Rows:", SalesData_Rev03.count())

# COMMAND ----------

# DBTITLE 1,Read: RunningData_Rev03.csv
# Read "RunningData_Rev03.csv"
RunningData_Rev03 = spark.read.option('header',True).option("quote", "\"").option('InferSchema',True).csv("/FileStore/tables/RunningData_Rev03.csv")

display(RunningData_Rev03.limit(10))
RunningData_Rev03.printSchema()
print("Number of Rows:", RunningData_Rev03.count())

# COMMAND ----------

# DBTITLE 1,join: Sales_Collect_Rev03 & SalesData_Rev03
# left join "Sales_Collect_Rev03" & "SalesData_Rev03"
Sales_Collect_df_Rev03_01 = Sales_Collect_Rev03.\
                             join(SalesData_Rev03, how='left',
                                  on=['Target_Simulation_Id'])
                        
display(Sales_Collect_df_Rev03_01.limit(10))

# COMMAND ----------

# DBTITLE 1,join: Sales_Collect_Rev03, SalesData_Rev03 & RunningData_Rev03
# left join "Sales_Collect_Rev03", "SalesData_Rev03" and "RunningData_Rev03"
Sales_Collect_df_Rev03_02 = Sales_Collect_Rev03.\
                             join(SalesData_Rev03, how='left',
                                  on=['Target_Simulation_Id']).\
                             join(RunningData_Rev03, how='left', on=['Target_Event_Id'])
                        
display(Sales_Collect_df_Rev03_02.limit(10))