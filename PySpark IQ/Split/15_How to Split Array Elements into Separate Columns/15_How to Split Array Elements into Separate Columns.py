# Databricks notebook source
# MAGIC %md
# MAGIC **PROBLEM STATEMENT**
# MAGIC - Split Array Elements into Separate Columns

# COMMAND ----------

schema = ["Name", "SkillSet"]
    
data = (["ABC", ['.Net', 'Git', 'C#']],
        ["XYZ", ['Wordpress', 'PHP']],
        ["IJK", ['Python', 'MongoDB', 'Git']],
        ["DEF", ['SSIS', 'SSAS', 'Power BI', 'SQL Server', 'Data Warehouse']],
        ["PQR", ['Azure']])

df = spark.createDataFrame(data, schema)
df.show(truncate=False)
df.printSchema()

# COMMAND ----------

from pyspark.sql.functions import split, col, size

# COMMAND ----------

# MAGIC %md
# MAGIC **Method 01**

# COMMAND ----------

d21 = df.withColumn("Skill1", df['SkillSet'][0]) \
        .withColumn("Skill2", df['SkillSet'][1]) \
        .withColumn("Skill3", df['SkillSet'][2]) \
        .withColumn("Skill4", df['SkillSet'][3]) \
        .withColumn("Skill5", df['SkillSet'][4])
d21.show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC **Method 02**

# COMMAND ----------

df.select('Name', 'SkillSet', df.SkillSet[0].alias('Skill1'),\
                              df.SkillSet[1].alias('Skill2'),\
                              df.SkillSet[2].alias('Skill3'),\
                              df.SkillSet[3].alias('Skill4'),\
                              df.SkillSet[4].alias('Skill5')).show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC **Method 03**

# COMMAND ----------

for i in range(5):
  df = df.withColumn(f"skill_{i}", df.SkillSet[i])
df.show()

# COMMAND ----------

# MAGIC %md
# MAGIC **Method 04**

# COMMAND ----------

# MAGIC %md
# MAGIC **Determine the size of each array**

# COMMAND ----------

dfsize = df.select("Name", "SkillSet", size("SkillSet").alias("NoOfArrayElements"))
dfsize.show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC **Get the Maximum Size of All Arrays**

# COMMAND ----------

max_value = dfsize.agg({"NoOfArrayElements": "max"}).collect()[0][0]
print(max_value)

# COMMAND ----------

# MAGIC %md
# MAGIC **UDF to Convert Array Elements into Columns**

# COMMAND ----------

def arraySplitIntoCols(df, maxElements):
  for i in range(maxElements):
    df = df.withColumn(f"new_col_{i}", df.SkillSet[i])
  return df

# COMMAND ----------

# MAGIC %md
# MAGIC **Call UDF**

# COMMAND ----------

dfout = arraySplitIntoCols(df, max_value)
display(df)
