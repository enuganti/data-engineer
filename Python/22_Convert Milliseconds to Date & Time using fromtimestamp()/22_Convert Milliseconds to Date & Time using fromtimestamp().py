# Databricks notebook source
# MAGIC %md
# MAGIC - The **datetime.fromtimestamp()** method in Python is used to **convert a timestamp** (a number representing the **seconds since the Unix epoch, January 1, 1970**) into a **datetime object**.

# COMMAND ----------

# MAGIC %md
# MAGIC **Syntax**
# MAGIC
# MAGIC      datetime.fromtimestamp(timestamp, tz=None)
# MAGIC
# MAGIC **timestamp:**
# MAGIC - A **float or int**, representing **seconds** since the **epoch (January 1, 1970, 00:00:00 UTC)**.
# MAGIC - The **timestamp** should be in **seconds, NOT in milliseconds**.
# MAGIC
# MAGIC **tz (optional):**
# MAGIC - A **timezone info** object (like **from zoneinfo.ZoneInfo**).
# MAGIC - If **not provided**, it uses your **local time zone**.

# COMMAND ----------

# DBTITLE 1,Converting a Timestamp to a Readable Date
from datetime import datetime

timestamp = 1609459200  # seconds
dt = datetime.fromtimestamp(timestamp)
print(dt)

# COMMAND ----------

# MAGIC %md
# MAGIC - If your **timestamp** were in **milliseconds (like 1609459200000)**, you would need to **divide it by 1000**.

# COMMAND ----------

# DBTITLE 1,milliseconds
timestamp_ms = 1609459200000  # milliseconds
dt = datetime.fromtimestamp(timestamp_ms / 1000)
print(dt)

# COMMAND ----------

# MAGIC %md
# MAGIC **Summary**:
# MAGIC
# MAGIC - **timestamp = seconds** ➔ use directly
# MAGIC - **timestamp = milliseconds** ➔ **divide by 1000** before using

# COMMAND ----------

# DBTITLE 1,Handling Time Zones
from datetime import datetime, timezone

timestamp = 1609459200
dt = datetime.fromtimestamp(timestamp, tz=timezone.utc)
print(dt, dt.tzname(), dt.tzinfo)

# COMMAND ----------

# DBTITLE 1,Converting Future Timestamps
from datetime import datetime

future_timestamp = 1798787865
dt = datetime.fromtimestamp(future_timestamp)
print(dt)

# COMMAND ----------

# DBTITLE 1,Handling Timestamps Before Epoch (Negative Timestamps)
negative_timestamp = -123456789
dt = datetime.fromtimestamp(negative_timestamp)
print(dt)

# COMMAND ----------

# MAGIC %md
# MAGIC **datetime.date**

# COMMAND ----------

# DBTITLE 1,Getting a date corresponding to an Epoch & Unix Timestamp
# Importing datetime and time module 
import datetime 
import time 

# Calling the time() function to return current time 
Todays_time = time.time() 

# Printing today's time 
print(Todays_time) 


# Calling the fromtimestamp() function to get date from the current time 
date_From_CurrentTime = datetime.date.fromtimestamp(Todays_time); 

# Printing the current date 
print("Date for the Timestamp is: %s"%date_From_CurrentTime);

# COMMAND ----------

# DBTITLE 1,Getting a date corresponding to a specified timestamp
# Importing datetime and time module 
import datetime 
import time 

# Initializing a timestamp value 
Timestamp = 1323456464; 

# Calling the fromtimestamp() function over the above specified Timestamp 
date_From_Timestamp = datetime.date.fromtimestamp(Timestamp); 

# Printing the date 
print("Date for the Timestamp is: %s"%date_From_Timestamp);

# COMMAND ----------

from datetime import date

timestamp = date.fromtimestamp(1326244364)
print("Date =", timestamp)

# COMMAND ----------

# MAGIC %md
# MAGIC **formatting**

# COMMAND ----------

# DBTITLE 1,formatting the datetime
from datetime import datetime

timestamp = 1651015200  # This represents a Unix timestamp (time in seconds)

# Convert the timestamp to a datetime object
dt = datetime.fromtimestamp(timestamp)

# Print the resulting datetime object
print("Datetime:", dt)

# Print the date in a custom format (e.g., YYYY-MM-DD HH:MM:SS)
print("Formatted date:", dt.strftime("%Y-%m-%d %H:%M:%S"))

# Getting only specific parts of the datetime
print("Year:", dt.year)
print("Month:", dt.month)
print("Day:", dt.day)

# COMMAND ----------

# DBTITLE 1,Handling fractional seconds
from datetime import datetime
timestamp = 1622505600.123456
dt_frac = datetime.fromtimestamp(timestamp)
print(dt_frac)

# COMMAND ----------

import time
from datetime import datetime

timestamp = time.time()  # Current time in seconds
print(timestamp)

dt = datetime.fromtimestamp(timestamp)
print("Current local datetime:", dt)

# COMMAND ----------

from datetime import datetime, timezone

timestamp = 1651015200

local_dt = datetime.fromtimestamp(timestamp)
utc_dt = datetime.utcfromtimestamp(timestamp)

print("Local time:", local_dt)
print("UTC time:", utc_dt)

# COMMAND ----------

# MAGIC %md
# MAGIC **Handling Timezones (using pytz)**
# MAGIC - By default, **fromtimestamp()** uses the **system's local time zone**.
# MAGIC - If you want to use a **different timezone**, you can use the **timezone module**.

# COMMAND ----------

from datetime import datetime, timezone, timedelta

# Define the timestamp
timestamp = 1609459200

# Convert the timestamp to a datetime object in UTC
dt_utc = datetime.fromtimestamp(timestamp, tz=timezone.utc)

# Print the UTC time
print(dt_utc)

# Convert the timestamp to a datetime object with a custom timezone offset (e.g., UTC +5:30)
custom_tz = timezone(timedelta(hours=5, minutes=30))
dt_custom = datetime.fromtimestamp(timestamp, tz=custom_tz)

# Print the time in the custom timezone
print(dt_custom)

# COMMAND ----------

# MAGIC %md
# MAGIC **Comparing Dates**

# COMMAND ----------

from datetime import datetime

timestamp1 = 1670000000
timestamp2 = 1680000000

dt1 = datetime.fromtimestamp(timestamp1)
dt2 = datetime.fromtimestamp(timestamp2)

if dt2 > dt1:
    print(f"{dt2} is after {dt1}")

# COMMAND ----------

# MAGIC %md
# MAGIC **Converting a list of timestamps**

# COMMAND ----------

from datetime import datetime
timestamps = [1609459200, 1612137600, 1614556800]
datetimes = [datetime.fromtimestamp(ts) for ts in timestamps]

for dt in datetimes:
    print(dt.strftime("%b %d, %Y %H:%M"))

# COMMAND ----------

# MAGIC %md
# MAGIC **Handle invalid timestamps gracefully**
# MAGIC - If the **timestamp** is **too large or too small**, it might not represent a **valid datetime**. You can catch **exceptions** and handle them.

# COMMAND ----------

from datetime import datetime

# Example invalid timestamp (too large)
timestamp = 9999999999999999999999999999

try:
    dt = datetime.fromtimestamp(timestamp)
    print(dt)
except (OverflowError, OSError) as e:
    print("Error:", e)