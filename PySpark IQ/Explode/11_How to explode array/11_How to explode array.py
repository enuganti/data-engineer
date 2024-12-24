# Databricks notebook source
# MAGIC %md
# MAGIC **PROBLEM STATEMENT**
# MAGIC
# MAGIC - How to write code to get as output?
# MAGIC
# MAGIC

# COMMAND ----------

Input:

name        languages
James       ['Java','Python','Python','Scala']
Anna        ['PHP','Javascript','PHP','HTML']
Maria       ['Java','C++']

Output:

[('James','Java'),
 ('James','Python'),
 ('James','Python'),
 ('Anna','PHP'),
 ('Anna','Javascript'),
 ('Maria','Java'),
 ('Maria','C++'),
 ('James','Scala'),
 ('Anna','PHP'),
 ('Anna','HTML')]

# COMMAND ----------

 # Assuming you have a DataFrame named 'input_df' with the provided structure
 data = [("James", ['Java', 'Python', 'Python', 'Scala']),
         ("Anna", ['PHP', 'Javascript', 'PHP', 'HTML']),
         ("Maria", ['Java', 'C++'])]

 columns = ["name", "languages"]

 input_df = spark.createDataFrame(data, columns)
 display(input_df)

# COMMAND ----------

 from pyspark.sql.functions import explode
 # Explode the 'languages' array column and select 'name' along with the exploded 'languages'
 output_df = input_df.select("name", explode("languages").alias("language"))
 display(output_df)

# COMMAND ----------

 # Show the result as a list of tuples
 result = output_df.rdd.map(lambda row: (row.name, row.language)).collect()

 # Display the result
 display(result)
