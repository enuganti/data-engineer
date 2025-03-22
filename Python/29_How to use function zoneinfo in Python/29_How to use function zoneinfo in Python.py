# Databricks notebook source
# MAGIC %md
# MAGIC ### **zoneinfo**
# MAGIC
# MAGIC - In Python, the **ZoneInfo** module is a **built-in library** that provides a way to work with **time zones**.

# COMMAND ----------

# MAGIC %md
# MAGIC **What is ZoneInfo?**
# MAGIC
# MAGIC - The **ZoneInfo** module is a part of the standard library introduced in **Python 3.9**.
# MAGIC - It provides access to the **IANA Time Zone database**, which is a comprehensive database of **all time zones worldwide**.
# MAGIC - The IANA Time Zone database is the most widely used time zone database in the world and is maintained by the **Internet Assigned Numbers Authority (IANA)**.
# MAGIC - Using the ZoneInfo module, we can retrieve information about a specific time zone, including the 
# MAGIC  
# MAGIC   - **time zone name**
# MAGIC   - **offset from Coordinated Universal Time (UTC)**
# MAGIC   - **Handling Daylight Saving Time (DST) Properly**
# MAGIC   
# MAGIC - It also provides us with the ability to **convert** a given **datetime object** to a **different time zone**, allowing us to work with times in **different regions of the world** easily.

# COMMAND ----------

# Load datetime from datetime module
from datetime import datetime, timezone, timedelta
from zoneinfo import ZoneInfo, available_timezones

# COMMAND ----------

available_timezones()

# COMMAND ----------

# DBTITLE 1,Print all available timezones
from zoneinfo import ZoneInfo, available_timezones

for z in available_timezones():
    print(z)

# COMMAND ----------

# DBTITLE 1,List all timezones
zone_list = []
for z in available_timezones():
    zone_list.append(z)
    print(z)

# COMMAND ----------

# DBTITLE 1,convert timezones
# create datetime object for May 5, 2023 12:00 PM
dt = datetime(2023, 5, 5, 12, 0)
print(dt, dt.tzname())

# associate the datetime object with the New York timezone
my_tz = ZoneInfo('America/New_York')
dt_ny = dt.astimezone(my_tz)

# print the datetime object in the New York timezone
print(dt_ny, dt_ny.tzname())

# COMMAND ----------

now = datetime.now()
print("Timezone:", now, now.tzname())

# retrieve information about the New York timezone
my_tz = ZoneInfo('America/New_York')

# Get the current time in New York (timezone-aware)
now_ny = datetime.now(my_tz)
print("Timezone for America/New_York: ", now_ny, now_ny.tzname())

# Print the timezone name, offset from UTC, and daylight saving time rules
print("UTC Offset:", now_ny.utcoffset())   # Offset from UTC
print("DST Offset:", now_ny.dst())         # Daylight Saving Time (DST) offset

# COMMAND ----------

# DBTITLE 1,tzinfo=None
dt_local = datetime(year=2020, month=11, day=1, hour=5, minute=20, second=40, microsecond=2344)
dt_naive = datetime(year=2020, month=11, day=1, hour=5, minute=20, second=40, microsecond=2344, tzinfo=None)

print(dt_local, dt_local.tzname())
print(dt_naive, dt_naive.tzname())
dt_naive

# COMMAND ----------

# DBTITLE 1,tzinfo=timezone.utc
dt_local = datetime(year=2020, month=11, day=1, hour=5, minute=20, second=40, microsecond=2344)
dt_na = datetime(year=2020, month=11, day=1, hour=5, minute=20, second=40, microsecond=2344, tzinfo=timezone.utc)

print("Local_datetime: ", dt_local, dt_local.tzname())
print("Naive_datetime: ", dt_na, dt_na.tzname())

# COMMAND ----------

# DBTITLE 1,Customize timezone of New_York using timedelta
my_timezone = timezone(timedelta(hours=-4), name="New_York")
dt_object_aware = datetime(year=2020, month=11, day=1, hour=5, minute=20, second=40, microsecond=2344, tzinfo=my_timezone)

print(dt_object_aware)
print(dt_object_aware.tzname())

# COMMAND ----------

# DBTITLE 1,tzinfo=ZoneInfo('America/New_York')
dt_object_aware = datetime(year=2025, month=11, day=1, hour=5, minute=20, second=40, microsecond=2344, tzinfo=ZoneInfo('America/New_York'))

print(dt_object_aware)
print(dt_object_aware.tzname())

# COMMAND ----------

# DBTITLE 1,day light
dt_object_aware = datetime(year=2025, month=3, day=1, hour=5, minute=20, second=40, microsecond=2344, tzinfo=ZoneInfo('America/New_York'))

print(dt_object_aware)
print(dt_object_aware.tzname())

# COMMAND ----------

# MAGIC %md
# MAGIC - we should never use **customized method** and always use zoneinfo library.

# COMMAND ----------

# MAGIC %md
# MAGIC      datetime.now(ZoneInfo("Europe/Paris"))
# MAGIC                       (or)
# MAGIC      datetime.now(tz=ZoneInfo("Europe/Paris"))

# COMMAND ----------

# DBTITLE 1,Print "Paris" timezone
paris = datetime.now(tz=ZoneInfo("Europe/Paris"))

print(paris)
print(paris.tzname())

# COMMAND ----------

# DBTITLE 1,Print "Berlin" timezone
berlin = datetime.now(ZoneInfo("Europe/Berlin"))

print(berlin)
print(berlin.tzname())

# COMMAND ----------

# DBTITLE 1,Print "New_York" timezone
New_York = datetime.now(tz=ZoneInfo("America/New_York"))

print(New_York)
print(New_York.tzname())

# COMMAND ----------

# DBTITLE 1,Print "Singapore" timezone
sinp = datetime.now(tz=ZoneInfo("Singapore"))

print(sinp)
print(sinp.tzname())

# COMMAND ----------

# DBTITLE 1,Print "Indian/Mahe" timezone
ind = datetime.now(tz=ZoneInfo("Indian/Mahe"))

print(ind)
print(ind.tzname())
