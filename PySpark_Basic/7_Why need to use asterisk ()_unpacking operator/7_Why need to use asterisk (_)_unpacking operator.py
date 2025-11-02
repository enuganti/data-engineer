# Databricks notebook source
# MAGIC %md
# MAGIC ##### What is the * (unpacking) operator?
# MAGIC
# MAGIC - `*` operator is used to **unpack elements** from a **list or tuple.**
# MAGIC - It takes **each element** in the **list / tuple** and passes them as **separate arguments**.

# COMMAND ----------

# MAGIC %md
# MAGIC ##### 1) Basic Python Example

# COMMAND ----------

# DBTITLE 1,List
nums_list = [1, 2, 3, 4, 5]

print(nums_list)
print(*nums_list)

# COMMAND ----------

# DBTITLE 1,tuple
nums_tup = (1, 2, 3, 4, 5)

print(nums_tup)
print(*nums_tup)

# COMMAND ----------

# MAGIC %md
# MAGIC - So instead of printing **[1, 2, 3, 4, 5]**, it prints **1 2 3 4 5**. Because the **list / tuple** was **unpacked** into **separate arguments**.

# COMMAND ----------

List1 = [1, 2, 3, 4, 5]
List2 = [6, 7, 8, 9, 10, 11]

print(List1 + List2)
print(*List1, *List2)

# COMMAND ----------

# MAGIC %md
# MAGIC In PySpark, this is particularly useful when you’re passing **multiple column** expressions to methods like:
# MAGIC - **.select()**
# MAGIC - **.agg()**
# MAGIC - **.groupBy().agg()**
# MAGIC - **.orderBy()**
# MAGIC - **.drop()**

# COMMAND ----------

# MAGIC %md
# MAGIC ##### 1) Using * with .select()

# COMMAND ----------

data = [("Joseph", 25, "New York", 200, 30, "East"),
        ("Janani", 30, "Pune", 250, 40, "East"),
        ("Mukesh", 22, "Noida", 300, 50, "North"),
        ("Naresh", 26, "Chennai", 220, 35, "North"),
        ("Priya", 28, "Mumbai", 400, 60, "West"),
        ("Ravi", 27, "Delhi", 500, 70, "West"),
        ("Rahul", 32, "Bangalore", 150, 10, "West"),
        ("Roshan", 19, "Cochin", 100, 15, "South")]

columns = ["name", "age", "city", "sales", "profit", "region"]

df_select = spark.createDataFrame(data, columns)
display(df_select)

# COMMAND ----------

# DBTITLE 1,Without unpacking:
df_select.select(columns).display()
# df_select.select(["name", "age", "city", "sales", "profit", "region"]).display()

# COMMAND ----------

# DBTITLE 1,With unpacking
df_select.select(*columns).display()

# df_select.select(*["name", "age", "city", "sales", "profit", "region"]).display()
# df_select.select(col("name"), col("age"), col("city"), col("sales"), col("profit"), col("region")).display()

# COMMAND ----------

# MAGIC %md
# MAGIC      columns = ["name", "age", "city", "sales", "profit", "region"]
# MAGIC      *columns → "name", "age", "city", "sales", "profit", "region"
# MAGIC      .select(*columns) = .select("name", "age", "city", "sales", "profit", "region")

# COMMAND ----------

# MAGIC %md
# MAGIC | Expression                    | Works             | Behavior                       | Recommended           |
# MAGIC | ----------------------------- | ----------------- | ------------------------------ | --------------------- |
# MAGIC | `df.select(cols)`             | ✅ (PySpark ≥ 3.0) | Implicitly expands string list | ⚠️ Sometimes works    |
# MAGIC | `df.select(*cols)`            | ✅ Always          | Explicit unpacking             | ✅ Yes (Best practice) |
# MAGIC | When using column expressions | ❌ May fail        | Needs unpacking                | ✅ Required            |

# COMMAND ----------

# MAGIC %md
# MAGIC ##### 2) Using * with .agg() and dynamic columns

# COMMAND ----------

from pyspark.sql.functions import sum, count
lst_agg_cols = ["sales", "profit"]

# COMMAND ----------

# MAGIC %md
# MAGIC **a) without asterisk `*`**
# MAGIC - PySpark will complain, because it’s receiving **one list argument** instead of **multiple columns**.
# MAGIC
# MAGIC       df.groupBy("region").agg(agg_exprs)  # ❌ ERROR
# MAGIC       [Column<'sum(sales) AS sales'>, Column<'sum(profit) AS profit'>]

# COMMAND ----------

agg_exprs_wastr = [sum(col_name).alias(col_name) for col_name in lst_agg_cols]

df_select.groupBy("region").agg(agg_exprs_wastr).display()

# COMMAND ----------

# MAGIC %md
# MAGIC **b) with asterisk `*`**
# MAGIC
# MAGIC - `*agg_exprs` unpacks the **list** into:
# MAGIC
# MAGIC       df.groupBy("region").agg(*agg_exprs).display()
# MAGIC       df.groupBy("region").agg(sum("sales").alias("sales"), sum("profit").alias("profit")).display()

# COMMAND ----------

df_select.groupBy("region").agg(*agg_exprs_wastr).display()

# COMMAND ----------

# MAGIC %md
# MAGIC `*` **(unpacking operator)**
# MAGIC
# MAGIC - The **asterisk `*`** unpacks the list elements so they can be passed as separate arguments.
# MAGIC
# MAGIC       # Without *
# MAGIC       df.groupBy("region").agg([sum("sales").alias("sales"), sum("profit").alias("profit")])  # ❌ Error
# MAGIC
# MAGIC       # With *
# MAGIC       df.groupBy("region").agg(*[sum("sales").alias("sales"), sum("profit").alias("profit")])  # ✅ Works
# MAGIC       df.groupBy("region").agg(sum("sales").alias("sales"), sum("profit").alias("profit")).display()

# COMMAND ----------

# MAGIC %md
# MAGIC       df.groupBy("region").agg(*agg_exprs).display()
# MAGIC
# MAGIC       # *agg_exprs unpacks the list into
# MAGIC       df.groupBy("region").agg(sum("sales").alias("sales"), sum("profit").alias("profit")).display()

# COMMAND ----------

# MAGIC %md
# MAGIC **c) without alias**

# COMMAND ----------

agg_exprs_wo = [sum(col_name) for col_name in lst_agg_cols]
agg_exprs_wo

# COMMAND ----------

df_select.groupBy("region").agg(*agg_exprs_wo).display()

# COMMAND ----------

# MAGIC %md
# MAGIC **d) with alias**

# COMMAND ----------

agg_exprs = [sum(col_name).alias(col_name) for col_name in lst_agg_cols]
agg_exprs

# COMMAND ----------

df_select.groupBy("region").agg(agg_exprs).display()

# COMMAND ----------

df_select.groupBy("region").agg(*agg_exprs).display()

# COMMAND ----------

# DBTITLE 1,All UPPERCASE Aliases
agg_exprs = [sum(col_name).alias(col_name.upper()) for col_name in lst_agg_cols]

df_select.groupBy("region").agg(*agg_exprs).display()

# COMMAND ----------

# DBTITLE 1,Capitalize First Letter
agg_exprs = [sum(col_name).alias(col_name.capitalize()) for col_name in lst_agg_cols]

df_select.groupBy("region").agg(*agg_exprs).display()

# COMMAND ----------

# MAGIC %md
# MAGIC **sum("sales")**   # => Column<'sum(sales)'>

# COMMAND ----------

# MAGIC %md
# MAGIC **.alias(col_name)**
# MAGIC - .alias() **renames** the resulting **aggregated column**.
# MAGIC - **Without alias**, PySpark would name it **sum(sales)**.
# MAGIC - **with alias**, it’s **renamed** to just **sales**.     
# MAGIC           sum("sales").alias("sales")

# COMMAND ----------

# MAGIC %md
# MAGIC **[ ... for col_name in lst_agg_clms_event ]**
# MAGIC  
# MAGIC       [sum("sales").alias("sales"), sum("profit").alias("profit")]

# COMMAND ----------

# MAGIC %md
# MAGIC | Concept              | Description                                 |
# MAGIC | -------------------- | ------------------------------------------- |
# MAGIC | `lst_agg_clms_event` | List of numeric columns to aggregate        |
# MAGIC | `sum(col_name)`      | **Aggregation** function applied to **each column** |
# MAGIC | `.alias(col_name)`   | **Renames the resulting aggregated column** |
# MAGIC | Used in              | `df.groupBy().agg(*agg_exprs)`              |

# COMMAND ----------

# MAGIC %md
# MAGIC      # with list comprehension
# MAGIC      agg_exprs_wastr = [sum(col_name).alias(col_name) for col_name in lst_agg_cols]
# MAGIC      df_select.groupBy("region").agg(*agg_exprs_wastr).display()
# MAGIC
# MAGIC      # Alternate 1: Using a simple for loop
# MAGIC      agg_exprs_wastr = []
# MAGIC      for col_name in lst_agg_cols:
# MAGIC          agg_exprs_wastr.append(sum(col_name).alias(col_name))
# MAGIC
# MAGIC      df_select.groupBy("region").agg(*agg_exprs_wastr).display()
# MAGIC
# MAGIC      # Alternate 2: If you want Uppercase or Capitalized column aliases
# MAGIC      agg_exprs_wastr = []
# MAGIC      for col_name in lst_agg_cols:
# MAGIC          alias_name = col_name.upper()  # or col_name.capitalize()
# MAGIC          agg_exprs_wastr.append(sum(col_name).alias(alias_name))
# MAGIC
# MAGIC      df_select.groupBy("region").agg(*agg_exprs_wastr).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ##### 3) Using * with .orderBy()

# COMMAND ----------

order_cols = ["region", "sales"]
df_select.orderBy(*order_cols).display()

# COMMAND ----------

df_select.orderBy(order_cols).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ##### 4) Combine static and dynamic arguments

# COMMAND ----------

metrics = [sum("sales").alias("total_sales"), sum("profit").alias("total_profit")]
df_select.groupBy("region").agg(*metrics, count("*").alias("count")).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ##### 5) Using * with .drop()

# COMMAND ----------

cols_to_drop = ["sales", "profit"]
drp = df_select.drop(*cols_to_drop)
drp.display()

# COMMAND ----------

# MAGIC %md
# MAGIC | Use Case                   | Without `*`             | With `*`                 | Works? |
# MAGIC | -------------------------- | ----------------------- | ------------------------ | ------ |
# MAGIC | `df.select(selected_cols)` | Passes list as one arg  | Unpacks list             | ✅      |
# MAGIC | `df.groupBy().agg([...])`  | Passes list as one arg  | Unpacks list             | ✅      |
# MAGIC | `df.orderBy([...])`        | Passes list as one arg  | Unpacks list             | ✅      |

# COMMAND ----------

# MAGIC %md
# MAGIC | Expression                    | Works             | Behavior                       | Recommended           |
# MAGIC | ----------------------------- | ----------------- | ------------------------------ | --------------------- |
# MAGIC | `df.select(cols)`             | ✅ (PySpark ≥ 3.0) | Implicitly expands string list | ⚠️ Sometimes works    |
# MAGIC | `df.select(*cols)`            | ✅ Always          | Explicit unpacking             | ✅ Yes (Best practice) |
# MAGIC | When using column expressions | ❌ May fail        | Needs unpacking                | ✅ Required            |