# Databricks notebook source
# MAGIC %md
# MAGIC #### **utcoffset()**

# COMMAND ----------

# MAGIC %md
# MAGIC - Works only with **timezone-aware (i.e., they have a tzinfo attribute)** datetime objects.
# MAGIC
# MAGIC - Returns **None** if the **datetime object** is **naive (has no timezone info)**.
# MAGIC
# MAGIC - The **utcoffset()** method in Python is used with **datetime objects** to return the **offset** of the **local time** from **UTC** (Coordinated Universal Time).
# MAGIC
# MAGIC - This is useful when working with time zones.

# COMMAND ----------

# MAGIC %md
# MAGIC - The **utcoffset()** function is used to return a **timedelta object** that represents the **difference between the local time and UTC time**.
# MAGIC
# MAGIC   - Here **range** of the **utcoffset** is **“-timedelta(hours=24) <= offset <= timedelta(hours=24)”**.
# MAGIC
# MAGIC   - If the **offset** is **east of UTC**, then it is considered **positive** and if the **offset** is **west of UTC**, then it is considered **negative**.
# MAGIC   
# MAGIC   - Since there are **24 hours in a day**, **-timedelta(24) and timedelta(24)** are the **largest** values possible.

# COMMAND ----------

# MAGIC %md
# MAGIC **Syntax:**
# MAGIC
# MAGIC      utcoffset()
# MAGIC
# MAGIC **Parameters:** 
# MAGIC - This function **does not** accept any **parameter**.
# MAGIC
# MAGIC **Return values:** 
# MAGIC - This function returns a **timedelta object** representing the **difference between the local time and UTC time**.

# COMMAND ----------

# DBTITLE 1,Handling None with Local datettime
# Importing datetime and pytz module
from datetime import datetime
import pytz

# Calling the now() function to get current date and time
dt = datetime.now()
print("current date & time: ", dt, dt.tzname(), dt.tzinfo)

# Calling the utcoffset() function over the above initialized datetime
print("utcoffset(): ", dt.utcoffset())

# COMMAND ----------

# MAGIC %md
# MAGIC - Here output is **None** because **now()** function returns **date and time in UTC format**, hence the **difference** between the **local time** i.e, returned by now() function and **UTC** time is **none**.

# COMMAND ----------

# DBTITLE 1,Handling None (Naive datetime)
from datetime import datetime

# Naive datetime (no tzinfo)
naive_dt = datetime(2025, 3, 22, 12, 0, 0)
print(naive_dt, naive_dt.tzname(), naive_dt.tzinfo)

# Since this datetime has no timezone information, utcoffset() returns None
print(naive_dt.utcoffset())

# COMMAND ----------

# DBTITLE 1,utcoffset() with UTC timezone
from datetime import datetime, timezone

# Aware datetime in UTC
utc_dt = datetime(2025, 3, 22, 12, 0, 0, tzinfo=timezone.utc)
print(utc_dt, utc_dt.tzname())

# Since this datetime is in UTC, the offset from UTC is zero
print(utc_dt.utcoffset())

# COMMAND ----------

# MAGIC %md
# MAGIC - A datetime with **tzinfo=timezone.utc** has an **offset of 0 hours** relative to **UTC**, so **utcoffset()** returns **timedelta(0)**.

# COMMAND ----------

# DBTITLE 1,Getting the UTC Offset of a Timezone
from datetime import datetime
import pytz

dt = datetime.now()
print(dt)

# Get current time in that timezone
now = datetime.now(pytz.timezone('Asia/Tokyo'))
print(now)

# Get UTC offset
utc_offset = now.utcoffset()
print("UTC Offset for Tokyo:", utc_offset)

# COMMAND ----------

from datetime import datetime, timedelta

# Create a timezone with a 5-hour offset (e.g., EST without daylight saving)
est = timezone(timedelta(hours=-5))
dt_est = datetime(2024, 3, 22, 12, 0, tzinfo=est)

print(dt_est, dt_est.tzname(), dt_est.tzinfo)
print("EST Offset:", dt_est.utcoffset())

# COMMAND ----------

# DBTITLE 1,utcoffset() with a timezone-aware datetime
from datetime import datetime, timezone, timedelta

# Define a timezone with UTC+5:30 offset
tz_530 = timezone(timedelta(hours=5, minutes=30))

# Create a timezone-aware datetime object
dt = datetime(2024, 3, 22, 12, 0, tzinfo=tz_530)
print(dt, dt.tzname(), dt.tzinfo)

# Get the UTC offset
print(dt.utcoffset())

# COMMAND ----------

# DBTITLE 1,Using astimezone() and then Calling utcoffset()
from datetime import datetime, timezone, timedelta

# Start with a UTC datetime
utc_dt = datetime(2025, 3, 22, 12, 0, 0, tzinfo=timezone.utc)
print(utc_dt, utc_dt.tzname(), utc_dt.tzinfo)

# Convert it to another time zone (e.g., UTC-5)
utc_minus_5 = timezone(timedelta(hours=-5))
dt_converted = utc_dt.astimezone(utc_minus_5)
print(dt_converted, dt_converted.tzname(), dt_converted.tzinfo)

# Now check the offset in the new time zone
print(dt_converted.utcoffset())

# COMMAND ----------

# MAGIC %md
# MAGIC - We start with a UTC datetime.
# MAGIC
# MAGIC - We use **.astimezone()** to convert it to **UTC-5**.
# MAGIC
# MAGIC - The **utcoffset()** now shows **-5 hours from UTC** (in **Python**, sometimes it shows as **-1 day, 19:00:00** which is mathematically the same as **-5:00:00**).

# COMMAND ----------

# DBTITLE 1,utcoffset() with pytz (better for handling DST)
from datetime import datetime
import pytz

# Get a timezone (New York)
ny_tz = pytz.timezone("America/New_York")

# Create a timezone-aware datetime
dt_ny = ny_tz.localize(datetime(2024, 3, 22, 12, 0))
print(dt_ny)

# Get the UTC offset
print(dt_ny.utcoffset())

# COMMAND ----------

# DBTITLE 1,Finding the offset of time
# difference between the local time and UTC time

# Importing datetime and pytz module
from datetime import datetime
import pytz

# Calling the now() function to get current date and time
naive = datetime.now()
print("current date & time: ", naive, naive.tzname(), naive.tzinfo)

# adding a timezone
timezone = pytz.timezone("Asia/Kolkata")
aware1 = timezone.localize(naive)
print(aware1)

# Calling the utcoffset() function over the above localized time
print("Time ahead of UTC by:", aware1.utcoffset())

# COMMAND ----------

# DBTITLE 1,Finding the offset of time
# Importing datetime and pytz module
from datetime import datetime
import pytz

# Calling the now() function to get current date and time
naive = datetime.now()
print("current date & time: ", naive, naive.tzname(), naive.tzinfo)

# adding a timezone
timezone = pytz.timezone("Asia/Tokyo")
aware1 = timezone.localize(naive)
print(aware1)

# Calling the utcoffset() function over the above localized time
print("Time ahead of UTC by:", aware1.utcoffset())

# COMMAND ----------

# DBTITLE 1,Finding Negative Time Offset
# Importing datetime and pytz module
from datetime import datetime
import pytz

# Calling the now() function to get current date and time
naive = datetime.now()
print("current date & time: ", naive, naive.tzname(), naive.tzinfo)

# adding a timezone
timezone = pytz.timezone("America/Los_Angeles")
aware1 = timezone.localize(naive)
print(aware1)

# Calling the utcoffset() function over the above localized time
print("Time behind UTC by:", -aware1.utcoffset())

# COMMAND ----------

# MAGIC %md
# MAGIC #### **FixedOffset**

# COMMAND ----------

# DBTITLE 1,replace
from datetime import datetime, timedelta, timezone

naive_dt = datetime(2025, 3, 23, 12, 0)
print(naive_dt, naive_dt.tzname())

# Convert to fixed offset timezone (UTC+2:00)
fixed_offset = timezone(timedelta(hours=2))
dt_with_offset = naive_dt.replace(tzinfo=fixed_offset)

print("Converted Datetime:", dt_with_offset)

# COMMAND ----------

# DBTITLE 1,utcoffset() with a Fixed UTC Offset (e.g., UTC+2)
from datetime import datetime, timezone, timedelta

# Create a fixed offset for UTC+2
utc_plus_2 = timezone(timedelta(hours=2))

# Aware datetime in UTC+2
dt_utc_plus_2 = datetime(2025, 3, 22, 12, 0, 0, tzinfo=utc_plus_2)
print(dt_utc_plus_2, dt_utc_plus_2.tzname())

# utcoffset() will show the offset of +2 hours
print(dt_utc_plus_2.utcoffset())

# COMMAND ----------

# MAGIC %md
# MAGIC - Here we created a timezone object with a **timedelta(hours=2)**.
# MAGIC
# MAGIC - The **utcoffset()** for this datetime is **+2:00:00 hours from UTC**.

# COMMAND ----------

from datetime import datetime, timedelta, timezone

# Create a fixed offset timezone (UTC+5:30)
fixed_offset = timezone(timedelta(hours=5, minutes=30))

# Create a datetime object with this fixed offset
dt = datetime(2025, 3, 23, 15, 30, tzinfo=fixed_offset)

print("Datetime with Fixed Offset:", dt)

# COMMAND ----------

# DBTITLE 1,pytz: FixedOffset
from datetime import datetime
import pytz

# Define Europe timezone with a 4.5-hour offset (Asia/Kabul is UTC+4:30, no direct Europe zone)
custom_tz = pytz.FixedOffset(270)  # 4 hours 30 minutes = 270 minutes

# Get current time in that timezone
europe_time = datetime.now(custom_tz)

print(europe_time)
