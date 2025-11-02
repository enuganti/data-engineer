# Databricks notebook source
# MAGIC %md
# MAGIC **collect_set():**
# MAGIC - Helps you **collect unique values** from a **group** into an **array** by **removing duplicates** automatically.
# MAGIC - returns an **array of unique elements (removes duplicates)**.
# MAGIC - If you use **collect_list()**, it would **keep duplicates** instead.

# COMMAND ----------

# MAGIC %md
# MAGIC - is an **aggregation function** in Apache Spark that is used to **aggregate data within groups** based on a specified grouping condition.
# MAGIC - **collect_set** is typically used after a **groupBy** operation to **aggregate values within each group**.
# MAGIC - Returns an **array of unique values** from a column within **each group**.
# MAGIC - **Duplicates** are automatically **removed**.
# MAGIC - **Order of elements** is **not guaranteed**, since it’s treated as a **set**.

# COMMAND ----------

# MAGIC %md
# MAGIC **Syntax**
# MAGIC 	
# MAGIC      collect_set(column)
# MAGIC
# MAGIC | Parameter Name | Required	| Description |
# MAGIC |----------------|----------|-------------|
# MAGIC | column (str, Column)	| Yes	| It represents the column value to be collected together |

# COMMAND ----------

# MAGIC %md
# MAGIC **Difference Between collect_set() and collect_list()**
# MAGIC
# MAGIC | Function         | Keeps Duplicates | Returns Type | Common Use Case                                       |
# MAGIC | ---------------- | ---------------- | ------------ | ----------------------------------------------------- |
# MAGIC | `collect_set()`  | ❌ No           | Array        | When you only need **unique values**                      |
# MAGIC | `collect_list()` | ✅ Yes          | Array        | When you want to preserve all values, **even duplicates** |

# COMMAND ----------

# MAGIC %md
# MAGIC **1) collect_set() function returns all values from an input column with duplicate values eliminated**
# MAGIC - To **collect** the “salary” column values **without duplication**.
# MAGIC - we have all the values, **excluding the duplicates**, and there are **no null values**.
# MAGIC - The collect_set() function omits the **null values**.

# COMMAND ----------

from pyspark.sql.functions import lit, col, collect_set, collect_list, col
import pyspark.sql.functions as f

# COMMAND ----------

simpleData = [("James", "Sales", 3000),
    ("Michael", "Sales", 4600),
    ("Robert", "Sales", 4100),
    ("Maria", "Finance", 3000),
    ("James", "Sales", 3000),
    ("Scott", "Finance", 3300),
    ("Jen", "Finance", 3900),
    ("Jeff", "Marketing", 3000),
    ("Kumar", "Marketing", 2000),
    ("Saif", "Sales", 4100)
  ]
schema = ["employee_name", "department", "salary"]

df = spark.createDataFrame(data=simpleData, schema = schema)
display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Single Column

# COMMAND ----------

df_list_set = df.select(
    collect_list("salary").alias("Salary_List"),
    collect_set("salary").alias("Salary_Set")
)
display(df_list_set)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Multiple Columns
# MAGIC
# MAGIC - it can only take **one column** at a time.
# MAGIC - If you want to apply it on multiple columns together, you need to combine them first (for example, using **concat_ws()**), or apply **collect_set()** separately on **each column**.

# COMMAND ----------

# MAGIC %md
# MAGIC **a) collect_set() for each column separately**

# COMMAND ----------

from pyspark.sql import functions as F

df_list_set = df.select(
    F.collect_set("employee_name").alias("Employee_Set"),
    F.collect_set("department").alias("Department_Set"),
    F.collect_set("salary").alias("Salary_Set")
)
display(df_list_set)


# COMMAND ----------

# MAGIC %md
# MAGIC **b) Collect set of combinations of multiple columns**

# COMMAND ----------

from pyspark.sql import functions as F

df_list_set = df.select(F.collect_set(
                        F.concat_ws(" : ", F.col("department"), F.col("salary").cast("string"))
                        ).alias("Dept_Salary_Set"))
display(df_list_set)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### 2) groupBy: Single Column

# COMMAND ----------

df_list_set_grp = df.groupBy("department").agg(collect_list("salary").alias("Salaries_List"),
                                               collect_set("salary").alias("Salaries_Set"))
df_list_set_grp.display()

# COMMAND ----------

data = [
    ("S1", "Engineering", 2024, "Math", 85),
    ("S1", "Engineering", 2024, "Physics", 78),
    ("S1", "Engineering", 2024, "Math", 85),     # duplicate subject
    ("S2", "Engineering", 2024, "Chemistry", 72),
    ("S2", "Engineering", 2024, "Math", 80),
    ("S3", "Arts", 2024, "History", 90),
    ("S3", "Arts", 2024, "History", 90),          # duplicate
    ("S3", "Arts", 2024, "Political Science", 88),
    ("S4", "Science", 2023, "Biology", 76),
    ("S4", "Science", 2023, "Physics", 69),
    ("S4", "Science", 2023, "Biology", 76),       # duplicate
    ("S5", "Science", 2023, "Chemistry", 91),
    ("S5", "Science", 2023, "Math", 82),
    ("S5", "Science", 2024, "Math", 83)
]

columns = ["student_id", "department", "year", "subject", "marks"]

df_single = spark.createDataFrame(data, columns)
display(df_single)

# COMMAND ----------

# DBTITLE 1,Example 1: collect_set() per student
# Example 1: collect_set() per student
print("Unique and total subjects taken by each student:")

df_single_col = df_single.groupBy("student_id").agg(
    collect_list("subject").alias("total_subjects_list"),
    collect_set("subject").alias("unique_subjects_set")
)

display(df_single_col)

# COMMAND ----------

# DBTITLE 1,Example 2: collect_set() per department and year
# Example 2: collect_set() per department and year
print("Unique subjects offered in each department per year:")

df_mulple_col = df_single.groupBy("department", "year").agg(
    collect_list("subject").alias("total_subjects_list"),
    collect_set("subject").alias("unique_subjects_set"))
display(df_mulple_col)

# COMMAND ----------

# DBTITLE 1,Example 3: collect_set() inside SQL query
# Example 3: collect_set() inside SQL query
df_mulple_col.createOrReplaceTempView("student_table")

# Check the columns in the DataFrame
print(df_mulple_col.columns)

sql_query = """
SELECT department,
       year,
       collect_set(total_subjects_list) AS unique_subjects,
       collect_set(unique_subjects_set) AS subjects_set
FROM student_table
GROUP BY department, year
ORDER BY department, year
"""

display(spark.sql(sql_query))

# COMMAND ----------

# MAGIC %md
# MAGIC ##### 3) groupBy: Multiple Columns

# COMMAND ----------

data = [
    ("A", "Math", "Science", 2023, 85, "Physics"),
    ("A", "Math", "Science", 2023, 92, "Chemistry"),
    ("A", "Math", "Science", 2023, 85, "Physics"),     # duplicate record
    ("B", "Arts", "Humanities", 2024, 76, "History"),
    ("B", "Arts", "Humanities", 2024, 80, "Civics"),
    ("B", "Arts", "Humanities", 2024, 76, "History"),  # duplicate record
    ("C", "Commerce", "Business", 2023, 91, "Economics"),
    ("C", "Commerce", "Business", 2023, 95, "Accounts"),
    ("C", "Commerce", "Business", 2023, 91, "Economics"),
    ("D", "Science", "Biology", 2024, 88, "Botany"),
    ("D", "Science", "Biology", 2024, 90, "Zoology"),
    ("E", "Science", "Physics", 2023, 94, "Quantum Mechanics"),
]

columns = ["student_id", "department", "stream", "year", "marks", "subject"]

df_mulple_set = spark.createDataFrame(data, columns)
display(df_mulple_set)

# COMMAND ----------

result_df = df_mulple_set.groupBy("department", "year") \
              .agg(
                  collect_set("student_id").alias("unique_students"),
                  collect_set("subject").alias("unique_subjects"),
                  collect_set("marks").alias("unique_marks")
              )

print("Aggregated Results using collect_set():")
result_df.display()

# COMMAND ----------

result_df.createOrReplaceTempView("student_table_multi")

# Check the columns in the DataFrame
print(result_df.columns)

sql_result_multi = """
SELECT department,
       year,
       unique_students,
       unique_subjects,
       unique_marks
FROM student_table_multi
ORDER BY department
"""

display(spark.sql(sql_result_multi))

# COMMAND ----------

nested_group = df_mulple_set.groupBy("department", "stream") \
                 .agg(
                     collect_set("student_id").alias("students"),
                     collect_set("subject").alias("subjects"),
                     collect_set("year").alias("years")
                 )

nested_group.display()