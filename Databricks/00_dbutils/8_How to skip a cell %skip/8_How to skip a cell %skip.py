# Databricks notebook source
# MAGIC %md
# MAGIC ##### %skip
# MAGIC - In Databricks notebooks, lines starting with **%** are called **magic commands**.
# MAGIC
# MAGIC - To **skip a cell** when running **multiple cells** (e.g., **Run All, Run All Above, or Run All Below**) in a Databricks notebook, you can use the **%skip** magic command.
# MAGIC
# MAGIC - When you execute a **run all** type command, any cell starting with **%skip** will be **bypassed**, and its code **will not** be **executed**.

# COMMAND ----------

data = [('2025-10-07', "google", "Medium", 100, 50, 1.41, 22, 0, 0, 500),
        ('2025-10-07', "google", "Medium", 120, 60, 1.61, 25, 0, 0, 700),
        ('2025-10-07', "metadata", "Average", 200, 30, 2.41, 33, 0, 0, 600),
        ('2025-10-07', "adls", "High", 300, 40, 5.41, 25, 0, 0, 700),
        ('2025-10-07', "storage", "Low", 250, 60, 7.41, 19, 0, 0, 800)
        ]

columns = ["Report_Date", "Name", "Category", "Sales", "Profit", "Avg_Sales_Duration", "Sales_Views", "Discount", "Purchase_Revenue", "Total_Revenue"]

df = spark.createDataFrame(data, columns)
display(df)

# COMMAND ----------

from pyspark.sql.functions import sum

# COMMAND ----------

# MAGIC %skip
# MAGIC print("This cell will be skipped during Execution")

# COMMAND ----------

# MAGIC %skip
# MAGIC print("This cell will be skipped during Execution")
# MAGIC
# MAGIC nums_list = [1, 2, 3, 4, 5]
# MAGIC
# MAGIC print(nums_list)
# MAGIC print(*nums_list)

# COMMAND ----------

# List of columns to aggregate (except the metrics columns)
lst_agg_clms = [
    "Sales",
    "Profit",
    "Avg_Sales_Duration",
    "Sales_Views",
    "Discount",
    "Purchase_Revenue",
    "Total_Revenue"
]

# Determine grouping columns by excluding aggregation columns
group_by_cols = [col for col in df.columns if col not in lst_agg_clms]
group_by_cols

# COMMAND ----------

# MAGIC %skip
# MAGIC # This cell's code will be skipped during execution
# MAGIC print("This will not be printed if the cell is skipped.")
# MAGIC
# MAGIC def print_dict(d):
# MAGIC     for key, value in d.items():
# MAGIC         if isinstance(value, dict):
# MAGIC             print_dict(value)
# MAGIC         else:
# MAGIC             print(key, ":", value)
# MAGIC
# MAGIC print_dict(student)

# COMMAND ----------

# Create dictionary from a list of keys
keys = ['id', 'name', 'age', 'city']
default_value = None

result = dict.fromkeys(keys, default_value)
print(result)

# COMMAND ----------

subjects = ['Math', 'Science', 'English']

marks = dict.fromkeys(subjects, {'score': 0, 'grade': 'N/A'})
print(marks)

# COMMAND ----------

# subjects = ['Math', 'Science', 'English']

# marks = dict.fromkeys(subjects, {'score': 0, 'grade': 'N/A'})
# print(marks)

# COMMAND ----------

data = {
    "student1": {"name": "Rahul", "age": 20},
    "student2": {"name": "Priya", "age": 22}
}

print(data["student1"]["name"])
print(data["student2"]["age"])

# COMMAND ----------

scores = {"Math": 95, "Science": 82, "English": 76, "Social": 70, "Physics":65}

for subject, score in scores.items():
    if score >= 90:
        print(f"{subject}: Excellent")
    elif score >= 75:
        print(f"{subject}: Good")
    else:
        print(f"{subject}: Needs Improvement")

# COMMAND ----------

col_type_map = {
    "age": "Int",
    "salary": "DOUBLE",
    "is_active": "Boolean",
    "join_date": "date",
    "last_login": "Timestamp",
    "Exp": "int",
    "Count": "INT"
}

for col_name, target_type in col_type_map.items():
    target_type = target_type.lower()
    print(f"{col_name}: {target_type}")

# COMMAND ----------

# MAGIC %skip
# MAGIC dbutils.notebook.exit("Don't Run below cells")

# COMMAND ----------

student = {
    "name": "Ravi",
    "marks": {
        "math": 90,
        "science": 85,
        "languages": {
            "english": 88,
            "hindi": 92
        }
    }
}

# Accessing directly
print(student["marks"]["math"])
print(student["marks"]["languages"]["hindi"])

# COMMAND ----------

dbutils.notebook.exit("Notebook Successfully Completed")