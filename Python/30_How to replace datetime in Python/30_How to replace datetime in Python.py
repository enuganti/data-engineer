# Databricks notebook source
# MAGIC %md
# MAGIC #### **Replace**
# MAGIC
# MAGIC ✅ The **replace()** method in Python’s **datetime module** allows you to create a **new datetime object** with **specific attributes modified** while keeping the **rest unchanged**.
# MAGIC
# MAGIC ✅ This is useful for **adjusting parts of a datetime**, such as **changing the time, date, or timezone** without **affecting other components**.
# MAGIC
# MAGIC ✅ replace() is **immutable** (it returns a **new object, does not modify the original**).
# MAGIC
# MAGIC ✅ Useful for **partial updates of a datetime**.
# MAGIC
# MAGIC ✅ Does not **automatically convert timezones**, only sets **tzinfo**.

# COMMAND ----------

# MAGIC %md
# MAGIC **Key Purposes of replace() in datetime Objects**
# MAGIC
# MAGIC 1) Modify **Specific Parts** of a **datetime**
# MAGIC
# MAGIC    - Change the **year, month, or time** while keeping other values **intact**.
# MAGIC
# MAGIC 2) Set or Change Timezone **(tzinfo)**
# MAGIC
# MAGIC    - Assign a timezone to a **naive datetime object**.
# MAGIC
# MAGIC 3) Normalize or Reset Time Components
# MAGIC
# MAGIC    - Set the time to **midnight for date-only** operations.
# MAGIC
# MAGIC 4) Standardizing Datetime Formatting
# MAGIC
# MAGIC    - Force a **datetime** to always have certain **fixed values** for consistency.

# COMMAND ----------

# MAGIC %md
# MAGIC - **replace(tzinfo=ZoneInfo(...))** → Assigns a timezone **without changing the time**.
# MAGIC
# MAGIC - **astimezone(ZoneInfo(...))** → Converts **time to another timezone** correctly.
# MAGIC
# MAGIC - **strftime()** → Formats timestamps in **ISO 8601 format (yyyy-MM-ddTHH:mm:ss.SSSZ)**.

# COMMAND ----------

# MAGIC %md
# MAGIC #### **1) Changing Date (Year, Month, or Day)**

# COMMAND ----------

# DBTITLE 1,Changing Specific date Components
from datetime import date
d = date(2002, 12, 31)
print(d)
d.replace(day=26) # changing day from 31 to 26

# COMMAND ----------

# DBTITLE 1,Changing Specific date Components
from datetime import datetime

dt = datetime(2024, 3, 22, 12, 30, 45)
print(dt)

# Replace the year
new_dt = dt.replace(year=2025)
print(new_dt)

# Replace the month and day
new_dt = dt.replace(month=6, day=15)
print(new_dt)

# COMMAND ----------

# DBTITLE 1,Changing Specific date Components
from datetime import datetime

# Create a datetime object
dt = datetime(2022, 5, 15, 12, 30, 0)
print("Original:", dt)

# Replace the year, month, and day
new_dt = dt.replace(year=2023, month=8, day=20)
print("Modified:", new_dt)

# COMMAND ----------

# MAGIC %md
# MAGIC #### **2) Changing the Time (Hour, Minute, Second & Microsecond)**
# MAGIC
# MAGIC **a) Changing Only the Time**

# COMMAND ----------

dt = datetime(2024, 3, 22, 12, 30, 45)
print(dt)

# Change only the hour and minute
new_dt = dt.replace(hour=8, minute=0)
print(new_dt)

# COMMAND ----------

from datetime import datetime

# Create a datetime object
dt = datetime(2022, 5, 15, 12, 30, 0)
print("Original:", dt)

# Replace the hour, minute, and second
new_dt = dt.replace(hour=18, minute=45, second=30)
print("Modified:", new_dt)

# COMMAND ----------

# MAGIC %md
# MAGIC **b) Adding Microseconds**

# COMMAND ----------

dt = datetime(2024, 3, 22, 12, 30, 45)
print(dt)

# Add microseconds
new_dt = dt.replace(microsecond=500000)
print(new_dt)

# COMMAND ----------

# DBTITLE 1,Normalizing Datetime for Consistency
dt = datetime(2025, 3, 22, 10, 45, 30, 123456)
print(dt)

normalized_dt = dt.replace(microsecond=0)
print(normalized_dt)

# COMMAND ----------

# MAGIC %md
# MAGIC **c) Changing the Time (Hour, Minute, Second, Microsecond)**

# COMMAND ----------

dt = datetime(2024, 3, 22, 14, 30, 45, 500000)
print(dt)

dt_new = dt.replace(hour=9, minute=15, second=0, microsecond=0)
print(dt_new)

# COMMAND ----------

# MAGIC %md
# MAGIC **d) Resetting Time to Midnight**

# COMMAND ----------

dt = datetime(2025, 3, 22, 14, 30, 45)
print(dt)

midnight_dt = dt.replace(hour=0, minute=0, second=0, microsecond=0)
print(midnight_dt)

# COMMAND ----------

# MAGIC %md
# MAGIC #### **4) Combining Date and Time Replacements**

# COMMAND ----------

from datetime import datetime, timezone

# Create a datetime object
dt = datetime(2022, 5, 15, 12, 30, 0)
print("Original:", dt)

# Replace date, time, and add timezone information
new_dt = dt.replace(year=2024, month=1, day=1, hour=0, minute=0, second=0, tzinfo=timezone.utc)
print("Modified:", new_dt)

# COMMAND ----------

# MAGIC %md
# MAGIC **Note:** 
# MAGIC - The **replace** method creates a **new datetime object** with the **specified fields replaced**.
# MAGIC
# MAGIC - It **does not modify** the **original object**.

# COMMAND ----------

# MAGIC %md
# MAGIC #### **5) Changing Timezone**

# COMMAND ----------

# MAGIC %md
# MAGIC **a) Removing Timezone Information**

# COMMAND ----------

from datetime import datetime, timezone

dt = datetime(2024, 3, 22, 14, 30, tzinfo=timezone.utc)
print(dt)

# Removing timezone info
dt_naive = dt.replace(tzinfo=None)
print(dt_naive)

# COMMAND ----------

# MAGIC %md
# MAGIC **b) Setting a Different Timezone (Without Converting Time)**

# COMMAND ----------

# DBTITLE 1,Setting a Different Timezone
from datetime import datetime, timezone, timedelta

dt = datetime(2024, 3, 22, 12, 30, 45)
print(dt)

tz_offset = timezone(timedelta(hours=-5))  # Assigning EST (UTC-5)

dt_tz = dt.replace(tzinfo=tz_offset)
print(dt_tz, dt_tz.tzname())

# COMMAND ----------

# MAGIC %md
# MAGIC **c) Setting a Timezone**
# MAGIC
# MAGIC - You can add **timezone information** to a **naive datetime object** (one without timezone info).

# COMMAND ----------

from datetime import datetime, timezone

# Create a naive datetime object (without tzinfo)
dt = datetime(2022, 5, 15, 12, 30, 0)
print("Before:", dt, dt.tzinfo)

# Replace the tzinfo with UTC timezone
dt_utc = dt.replace(tzinfo=timezone.utc)
print("After:", dt_utc, dt_utc.tzinfo)

# COMMAND ----------

# DBTITLE 1,Replace today with ZoneInfo
from datetime import datetime
from zoneinfo import ZoneInfo

# Get current local datetime (without timezone)
d = datetime.today()
print("today(), Without timezone: ", d, d.tzname(), '\n')

# Get current UTC time
dt = datetime.now()
d_utc = datetime.utcnow()
print("now(), Without timezone: ", dt, dt.tzname(), '\n')
print("utcnow(), Without timezone: ", d_utc, d_utc.tzname(), '\n')

d1 = dt.replace(tzinfo=timezone.utc)                     # Assign a UTC timezone
d2 = d.replace(tzinfo=ZoneInfo('Asia/Tokyo'))            # Assign a timezone (Asia/Tokyo)
d3 = d.replace(tzinfo=ZoneInfo(key='US/Central'))        # Assign a timezone (US/Central)
d4 = d.replace(tzinfo=ZoneInfo('America/Chicago'))       # Assign a timezone (America/Chicago)

print("now(), With UTC timezone: ", d1, d1.tzname(), '\n')
print("today(), With Tokyo timezone: ", d2, d2.tzname(), '\n')
print("today(), With Central timezone: ", d3, d3.tzname(), '\n')
print("today(), With Chicago timezone: ", d4, d4.tzname())

# COMMAND ----------

# MAGIC %md
# MAGIC **Note:**
# MAGIC - **replace()** only **attaches a timezone**, it **doesn’t convert the time**.

# COMMAND ----------

# DBTITLE 1,Replace current datetime
import time
from datetime import datetime, timezone

dt = datetime.now()
print(dt, dt.tzname())

utc_time = dt.replace(tzinfo=timezone.utc) # This does not convert time; it only sets the timezone.
print(utc_time, utc_time.tzname())

strUTCdt = utc_time.strftime("%Y-%m-%d %H:%M:%S")
print(strUTCdt)

# COMMAND ----------

# DBTITLE 1,Replace custom datetime
# Naive datetime (no timezone)
dt = datetime(2024, 3, 22, 12, 30, 45)
print(dt, dt.tzname())

dt_utc = dt.replace(tzinfo=timezone.utc)  # This does not convert time; it only sets the timezone.
print(dt_utc, dt_utc.tzname())

dt_Chic = dt.replace(tzinfo=ZoneInfo('America/Chicago'))  # This does not convert time;it only sets the timezone.
print(dt_Chic,  dt_Chic.tzname())

# COMMAND ----------

# DBTITLE 1,Handling Timezones with User Input
from datetime import datetime
from zoneinfo import ZoneInfo

# Simulate user input as a string
user_time = "2025-03-20 12:45:00"

# Parse string and set timezone
dt = datetime.strptime(user_time, "%Y-%m-%d %H:%M:%S")
print("User Time (India):", dt)

dt_kolk = dt.replace(tzinfo=ZoneInfo("Asia/Kolkata"))
print("User Time Asia/Kolkata:", dt_kolk, dt_kolk.tzname())

# Convert to UTC
dt_utc = dt.astimezone(ZoneInfo("UTC"))
print("Converted to UTC:", dt_utc, dt_utc.tzname())

# COMMAND ----------

from datetime import datetime, timezone, timedelta

# create a naive datetime object
naive_datetime = datetime(2023, 10, 20, 15, 30)
print("Customize datetime: ", naive_datetime, naive_datetime.tzname())

# define the timezone using timedelta
eastern = timezone(timedelta(hours=-5))
print("timedelta: ", eastern)

# Remove the timezone information from the datetime object
dt_obj_wo_tz = naive_datetime.replace(tzinfo=None)
print("tzinfo_None: ", dt_obj_wo_tz, dt_obj_wo_tz.tzname())

# Make the datetime object timezone aware
aware_datetime = naive_datetime.replace(tzinfo=eastern)
print("tzinfo_eastern: ", aware_datetime, aware_datetime.tzname())

# Add timezone information to the datetime object
dt_obj_w_tz = naive_datetime.replace(tzinfo=timezone.utc)
print("tzinfo_UTC: ", dt_obj_w_tz, dt_obj_w_tz.tzname())

strUTCdt = dt_obj_w_tz.strftime("%Y-%m-%d %H:%M:%S")
print("formatted: ", strUTCdt)
