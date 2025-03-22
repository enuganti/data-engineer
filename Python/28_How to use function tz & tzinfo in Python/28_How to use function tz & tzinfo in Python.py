# Databricks notebook source
# MAGIC %md
# MAGIC %md
# MAGIC #### **Key Differences b/n datetime.now() & datetime.today()**
# MAGIC
# MAGIC **Time Zone Support**
# MAGIC
# MAGIC      datetime.now(tz) can return a timezone-aware datetime.
# MAGIC      datetime.today() does not support time zones.
# MAGIC
# MAGIC **Best Practice**
# MAGIC
# MAGIC - Use **datetime.now(tz=timezone.utc)** if you need **timezone-aware** datetime.
# MAGIC - Use **datetime.today()** only if you **don't** need **timezone** support.

# COMMAND ----------

# MAGIC %md
# MAGIC #### **1) tz (timezone)**
# MAGIC
# MAGIC **tz (Parameter in datetime.astimezone())**
# MAGIC - Used to **convert** a **timezone-aware datetime** to a **different timezone**.
# MAGIC
# MAGIC - **Adjusts** the **time** accordingly.
# MAGIC
# MAGIC - **Timezone-aware objects** are Python **DateTime or time** objects that include **timezone** information.

# COMMAND ----------

# MAGIC %md
# MAGIC **When to Use:**
# MAGIC
# MAGIC - **Converting Time Zones**
# MAGIC
# MAGIC   - When you have an **aware datetime object (one that already has a tzinfo attribute)** and you need to **convert it to another time zone**, pass the **desired time zone** as the **tz** argument.

# COMMAND ----------

import datetime

# COMMAND ----------

help(datetime)

# COMMAND ----------

# MAGIC %md
# MAGIC       datetime.now(timezone.utc)
# MAGIC                 (or)
# MAGIC       datetime.now(tz=timezone.utc)

# COMMAND ----------

# Load datetime from datetime module
from datetime import datetime, timezone

# Get the current date and time
current_datetime = datetime.now()                      # Applying datetime.now
print("Current Date and Time:", current_datetime)      # show current date and time

# Get the current date and time with UTC timezone
dt = datetime.now(tz=timezone.utc)      # Applying datetime.now
print("Current Date and Time:", dt)     # show current date and time

# COMMAND ----------

# MAGIC %md
# MAGIC - **tz** is a **keyword argument** in **datetime.now()**, used to specify the **timezone**.
# MAGIC
# MAGIC - This ensures that the returned datetime object is **timezone-aware**.
# MAGIC
# MAGIC - If you **don't** need **timezone** information, **datetime.now() (without tz)** is fine for simple **local timestamps**.
# MAGIC
# MAGIC - If you **only need UTC**, prefer **datetime.now(timezone.utc)** instead of setting a **custom tz**.

# COMMAND ----------

# MAGIC %md
# MAGIC **datetime.now(tz=None):**
# MAGIC - Returns the **current local date and time**.

# COMMAND ----------

from datetime import datetime, timezone

dt = datetime.now(tz=None)
print("Current Date and Time:", dt)

# COMMAND ----------

# DBTITLE 1,convert timezones
from datetime import datetime, timezone, timedelta

# Create a timezone-aware datetime in UTC
dt_utc = datetime(2025, 3, 22, 14, 30, 45, tzinfo=timezone.utc)
print(dt_utc)

# Define a custom time zone (e.g., UTC+5)
tz_utc_plus_5 = timezone(timedelta(hours=5))

# Convert datetime to UTC+5 using the tz parameter
dt_new_tz = dt_utc.astimezone(tz=tz_utc_plus_5)

print(dt_new_tz)

# COMMAND ----------

# MAGIC %md
# MAGIC - Here, **.astimezone()** adjusts the time from **UTC+00:00 to UTC+05:00**.

# COMMAND ----------

# MAGIC %md
# MAGIC #### **2) class datetime.tzinfo**
# MAGIC
# MAGIC **tzinfo (Attribute in datetime.replace())**
# MAGIC - Used with the **.replace()** method to **set or assign** a **timezone without conversion**.
# MAGIC
# MAGIC - Only **attaches timezone** info to a **naive datetime**, but does **NOT adjust the time**.
# MAGIC
# MAGIC - These are used by the **datetime and time classes** to provide a customizable notion of time adjustment (for example, to **account for time zone and/or daylight saving time**).

# COMMAND ----------

# MAGIC %md
# MAGIC #### **When to Use**
# MAGIC **a) Constructing a Datetime**

# COMMAND ----------

from datetime import datetime
event_time = datetime(2024, 11, 4, 16, 9)
print(event_time.tzinfo)

# COMMAND ----------

from datetime import datetime
from zoneinfo import ZoneInfo

later_utc = datetime(2024, 11, 4, 23, 9, tzinfo=ZoneInfo('UTC'))
print(later_utc, later_utc.tzinfo)

# COMMAND ----------

# DBTITLE 1,Assigning UTC to a naive datetime
from datetime import datetime, timezone

# naive datetime
unaware = datetime(2021, 8, 15, 8, 15, 12, 0)
print(unaware)

# aware datetime
aware = datetime(2021, 8, 15, 8, 15, 12, 0, tzinfo=timezone.utc)
print(aware)

# COMMAND ----------

# MAGIC %md
# MAGIC - This **does NOT** convert the **time**; it just **labels** it.
# MAGIC
# MAGIC - **tzinfo** is an argument for the **datetime constructor**, not for **datetime.now()**.
# MAGIC
# MAGIC - It defines the **timezone** for a **manually created** datetime object.

# COMMAND ----------

from datetime import datetime

print("Current DateTime: ", datetime.now())
print("tzinfo: ", datetime.now().tzinfo)

# COMMAND ----------

# MAGIC %md
# MAGIC **b) Assigning a Time Zone**
# MAGIC
# MAGIC - When you want to mark a **naive datetime object** (one without any time zone) as belonging to a particular time zone, you set its **tzinfo** attribute.

# COMMAND ----------

from datetime import datetime, timezone

# Naive datetime
dt = datetime(2025, 3, 22, 14, 30, 45)
print(dt)

# Assign UTC timezone using replace
dt_utc = dt.replace(tzinfo=timezone.utc)
print(dt_utc)

# COMMAND ----------

# MAGIC %md
# MAGIC #### **Incorrect Usage**
# MAGIC
# MAGIC - **datetime.now(tzinfo=timezone.utc):**  
# MAGIC - ❌ Error: **tzinfo** is **not a valid keyword** for **now()**.
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC #### **Summary**
# MAGIC
# MAGIC |   Context	   |  Correct Argument     | Example |
# MAGIC |--------------|-----------------------|---------|
# MAGIC | datetime.now()	| tz	| datetime.now(tz=timezone.utc) |
# MAGIC | datetime() constructor |	tzinfo	| datetime(2024, 3, 18, tzinfo=timezone.utc) |
# MAGIC
# MAGIC
# MAGIC **Use:**
# MAGIC
# MAGIC - **tz** with **datetime.now()**
# MAGIC - **tzinfo** with **datetime() constructor**
