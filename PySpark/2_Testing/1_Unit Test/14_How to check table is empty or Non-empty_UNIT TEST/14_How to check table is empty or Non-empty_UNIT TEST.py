# Databricks notebook source
# MAGIC %md
# MAGIC #### How to check table is empty / Non-Empty?

# COMMAND ----------

# MAGIC %md
# MAGIC #### 1) Count the rows
# MAGIC
# MAGIC - The simplest method is to **count the rows** in the table.
# MAGIC
# MAGIC   - If **row_count = 0** → table is **empty**
# MAGIC   - If **row_count > 0** → table is **not empty**
# MAGIC
# MAGIC **Pros:** Easy to understand.
# MAGIC
# MAGIC **Cons:** On very large tables, COUNT(*) may be **slower**.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(*) AS row_count FROM asn_acc.bronze.sales_collect_bronze;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT CASE 
# MAGIC            WHEN COUNT(*) = 0 THEN 'Table is empty'
# MAGIC            ELSE 'Table is not empty'
# MAGIC        END AS TableStatus
# MAGIC FROM asn_acc.bronze.sales_collect_bronze;

# COMMAND ----------

# MAGIC %md
# MAGIC #### 2) Use EXISTS (faster for large tables)
# MAGIC - EXISTS **stops searching** after finding the **first row**, so it can be **more efficient than COUNT(*)** when you just want to know if the **table has data**.
# MAGIC - **Stops scanning** after finding the **first row** → better performance.
# MAGIC - This method is **fast** because it **doesn't scan** the **entire table**.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT CASE 
# MAGIC          WHEN EXISTS (SELECT 1 FROM asn_acc.bronze.sales_collect_bronze) 
# MAGIC          THEN 'Non-Empty' 
# MAGIC          ELSE 'Empty' 
# MAGIC        END AS table_status;

# COMMAND ----------

# MAGIC %md
# MAGIC #### 3) Limit the check
# MAGIC
# MAGIC - If it **returns a row** → table is **non-empty**
# MAGIC - If it returns **nothing** → table is **empty**

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * 
# MAGIC FROM asn_acc.bronze.sales_collect_bronze
# MAGIC LIMIT 1;

# COMMAND ----------

# MAGIC %md
# MAGIC **When to Use Which Method?**
# MAGIC
# MAGIC - **Small tables:**
# MAGIC   - **COUNT(*)** is fine.
# MAGIC
# MAGIC - **Large tables (performance critical):**
# MAGIC   - Use **EXISTS or LIMIT 1**.
# MAGIC
# MAGIC - If you just want to know if it’s **empty**, use **EXISTS or LIMIT 1** -> much **faster than counting all rows**.