# Databricks notebook source
# MAGIC %md
# MAGIC **Why is .replace() needed?**
# MAGIC - When you add **timedelta(days=1)**, it preserves the original time.
# MAGIC - If current has a **non-midnight time** (e.g., 2024-02-14 15:45:30), then adding a day would result in:

# COMMAND ----------

# DBTITLE 1,without .replace()
import datetime

current = datetime.datetime(2024, 2, 14, 15, 45, 30)
next_day = current + datetime.timedelta(days=1)

print("Current Date:", current)
print("Next Day:", next_day)

# COMMAND ----------

# MAGIC %md
# MAGIC - If you want the **next day's** timestamp at **midnight (00:00:00)**, you must explicitly reset the time portion using **.replace(hour=0, minute=0, second=0)**.

# COMMAND ----------

# DBTITLE 1,with .replace()
# Without min()
from datetime import datetime, timedelta

current = datetime(2024, 2, 14, 22, 30, 0)   # Feb 14, 2024, 10:30 PM
end_date = datetime(2024, 2, 15, 10, 0, 0)   # Feb 15, 2024, 10:00 AM

next_day = (current + timedelta(days=1)).replace(hour=0, minute=0, second=0)

print("Current Date:", current)
print("End Date:", end_date)
print("Next Day:", next_day)

# Problem: What if end_date is 2024-02-15 10:00 AM?
# We want next_interval to be the EARLIEST of next_day or end_date.

# COMMAND ----------

# MAGIC %md
# MAGIC %md
# MAGIC      next_day = (current + timedelta(days=1)).replace(hour=0, minute=0, second=0)
# MAGIC
# MAGIC - The **.replace(hour=0, minute=0, second=0)** in your code is used to ensure that the **time** portion of the **datetime** object is set to **midnight (00:00:00)** while keeping the **date unchanged**.
# MAGIC
# MAGIC - **Adding one day and resetting time**
# MAGIC   - **timedelta(days=1)** adds a **day**.
# MAGIC   - **.replace(hour=0, minute=0, second=0)** resets the time to **midnight**.

# COMMAND ----------

# DBTITLE 1,With min()
# Explicitly referring to built-in functions
import builtins

next_interval = builtins.min(next_day, end_date)

print("Next Interval:", next_interval)
# Output: 2024-02-15 00:00:00 (if next_day < end_date_01)
# Output: 2024-02-15 10:00:00 (if next_day > end_date)
