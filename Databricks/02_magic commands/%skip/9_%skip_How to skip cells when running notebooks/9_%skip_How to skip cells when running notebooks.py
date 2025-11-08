# Databricks notebook source
# MAGIC %md
# MAGIC ##### %skip (Magic Command)
# MAGIC
# MAGIC - if you wanted to run your **entire notebook** but **skip** a few **heavy or optional or experimental** cells eliminating the need to **manually comment out or delete or modify** code.
# MAGIC
# MAGIC   - Makes **debugging faster**
# MAGIC   - Keeps experiments in place without running them
# MAGIC   - Prevents accidental re-runs of costly cells
# MAGIC
# MAGIC - Just add it at the **top** of any cell, and it’ll be **ignored** when you run **multiple cells** together.
# MAGIC
# MAGIC   - You can now **skip individual cells** when running **multiple cells** in a notebook using the `%skip` magic command.
# MAGIC
# MAGIC   - To **skip** a cell during a **Run All** execution, use the **%skip** magic command at the **beginning** of that cell.
# MAGIC
# MAGIC   - The **code** within that **cell** will **not be executed**.
# MAGIC
# MAGIC - you can now skip **specific cells** during multi-cell execution using the new **%skip** magic command, making your workflow **cleaner and more efficient**.
# MAGIC
# MAGIC **databricks Link:** https://docs.databricks.com/aws/en/release-notes/product/2025/october#skip-cells-when-running-notebooks

# COMMAND ----------

# DBTITLE 1,import required functions
from pyspark.sql.functions import element_at, col, when

# COMMAND ----------

# DBTITLE 1,create dataframe
data = [('Jay', 25, 5, ['Java','Scala','Python','PySpark',None], {'product':'bmw', 'color':'brown', 'type':'sedan'}),
        ('Midun', 27, 6, ['Spark','Java','Spark SQL','SQL',None], {'product':'audi', 'color':None, 'type':'truck'}),
        ('Robert', 28, 7, ['CSharp','',None,'VS Code'], {'product':'volvo', 'color':'', 'type':'sedan'}),
        ('Paul', 23, 2, None, None),
        ('Basha', 22, 1, ['1','2','3','4','5'], {}),
        ('Gopesh', 29, 8, ['Python','R','SQL','Tableau'], {'product':'tesla', 'color':'red', 'type':'electric'}),
        ('Bobby', 30, 9, ['Go','Rust','C++','Docker'], {'product':'ford', 'color':'blue', 'type':'suv'}),
        ('Chetan', 31, 10, ['JavaScript','React','NodeJS','MongoDB'], {'product':'toyota', 'color':'white', 'type':'hatchback'}),
        ('Dravid', 32, 11, ['Python','Java','AWS','Spark'], {'product':'honda', 'color':'black', 'type':'sedan'}),
        ('Eshwar', 33, 12, ['C','C++','Embedded','Matlab'], {'product':'nissan', 'color':'grey', 'type':'truck'}),
        ('Firoz', 34, 13, ['Scala','Spark','Kafka'], {'product':'mercedes', 'color':'silver', 'type':'suv'}),
        ('Gayathri', 35, 14, ['Java','Spring Boot','Hibernate'], {'product':'bmw', 'color':'blue', 'type':'coupe'}),
        ('Hemanth', 36, 15, ['Python','Pandas','Numpy','ML'], {'product':'audi', 'color':'green', 'type':'sedan'}),
        ('Ishan', 37, 16, ['Ruby','Rails','Postgres'], {'product':'volkswagen', 'color':'yellow', 'type':'hatchback'}),
        ('Jack', 38, 17, ['Kotlin','Android','Firebase'], {'product':'hyundai', 'color':'white', 'type':'suv'})
       ]

columns = ['name', 'age', 'exp', 'knownLanguages', 'properties']

df_array = spark.createDataFrame(data, columns)
display(df_array)

# COMMAND ----------

# DBTITLE 1,transformation
# Implement both array and map columns uisng element_at()
df_trans = df_array.select("name", "age", "exp",
                           element_at("knownLanguages", 1).alias("first_element"),
                           element_at("knownLanguages", -1).alias("last_element"),
                           element_at("properties", "product").alias("Product"),
                           element_at("properties", "color").alias("Color"))
                           
display(df_trans)

# COMMAND ----------

# DBTITLE 1,aggregation
# MAGIC %skip
# MAGIC
# MAGIC df_desg = df_trans.withColumn("designation", when((col("exp") >= 1) & (col("exp") <= 3), "Intern")
# MAGIC                               .when((col("exp") >= 4) & (col("exp") <= 6), "Junior data engineer")
# MAGIC                               .when((col("exp") >= 7) & (col("exp") <= 10), "Senior data engineer")
# MAGIC                               .when((col("exp") >= 11) & (col("exp") <= 15), "Lead data engineer")
# MAGIC                               .when((col("exp") >= 16) & (col("exp") <= 20), "Manager")
# MAGIC                               .otherwise("invalid")).orderBy("exp")
# MAGIC display(df_desg)

# COMMAND ----------

# DBTITLE 1,filter
df_fltr = df_desg.filter((col("exp") >= 7) & (col("exp") <= 12))
display(df_fltr)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### To make Cell `filter` run correctly:
# MAGIC
# MAGIC **Method 01:**
# MAGIC
# MAGIC - **Remove** the **%skip** command from Cell: `aggregation`.
# MAGIC
# MAGIC **Method 02:**
# MAGIC
# MAGIC - `Run All` before apply `%skip`.
# MAGIC
# MAGIC    - click **Run All** before applying the `%skip`.
# MAGIC    - After it `completes` successfully, then apply `%skip`.
# MAGIC    - **Run All** again, it runs without error (Databricks notebook maintains its execution state and variable definitions within the active cluster session).
# MAGIC
# MAGIC **Method 03:**
# MAGIC
# MAGIC - `Run` -> `Clear` -> `Clear state and run all`
# MAGIC - This action will **reset all variables and function** definitions in the **Python environment**.