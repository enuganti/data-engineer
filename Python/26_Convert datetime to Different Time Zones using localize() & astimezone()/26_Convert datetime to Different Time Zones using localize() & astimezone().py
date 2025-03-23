# Databricks notebook source
# MAGIC %md
# MAGIC #### **Convert timezones**
# MAGIC
# MAGIC - **replace(tzinfo=ZoneInfo(...))** → Assigns a timezone **without changing the time**.
# MAGIC
# MAGIC - **astimezone(ZoneInfo(...))** → Converts **time to another timezone** correctly.
# MAGIC
# MAGIC - **strftime()** → Formats timestamps in **ISO 8601 format (yyyy-MM-ddTHH:mm:ss.SSSZ)**.

# COMMAND ----------

# MAGIC %md
# MAGIC #### **a) localize()**
# MAGIC
# MAGIC - The **localize()** method is provided by the **pytz library** in Python to **attach a timezone** to a **naive datetime object**.
# MAGIC
# MAGIC - localize() **does not alter** the **actual time** like **astimezone()**. Instead, it **attaches a timezone** to a **naive datetime object without changing the clock time**.
# MAGIC
# MAGIC - localize() **only assigns a timezone** to a **naive datetime without changing the time value**.

# COMMAND ----------

from datetime import datetime
from pytz import timezone

# Set the time to noon on 2019-08-19
naive = datetime(2019, 8, 19, 12, 0)
print(naive)

# Let's treat this time as being in the UTC timezone
aware = timezone('UTC').localize(naive)
print(aware)

# COMMAND ----------

# DBTITLE 1,Localizing a naive datetime to a specific timezone
from datetime import datetime
import pytz

# Naive datetime (no timezone information)
naive_dt = datetime(2025, 3, 23, 12, 0)
print("Naive datetime:", naive_dt, naive_dt.tzname(), naive_dt.tzinfo)

# Assigning timezone (US/Eastern)
eastern_tz = pytz.timezone("US/Eastern")

# Localize the naive datetime
localized_dt = eastern_tz.localize(naive_dt)

print("Localized datetime:", localized_dt, localized_dt.tzname(), localized_dt.tzinfo)

# COMMAND ----------

# MAGIC %md
# MAGIC **Notice:**
# MAGIC - The **time remains 12:00:00**, but the **-04:00 timezone offset is added**.
# MAGIC
# MAGIC - The **-04:00** offset indicates **Eastern Daylight Time (EDT)** during **daylight saving time**.

# COMMAND ----------

from datetime import datetime

# using localize() function, my system is on IST timezone
ist = pytz.timezone('Asia/Kolkata')
utc = pytz.utc
local_datetime = ist.localize(datetime.now())
print(local_datetime)

print('IST Current Time =', local_datetime.strftime('%Y-%m-%d %H:%M:%S %Z%z'))

print('IST Current Time =', utc.localize(datetime.now()))
print('Wrong UTC Current Time =', utc.localize(
	datetime.now()).strftime('%Y-%m-%d %H:%M:%S %Z%z'))

# COMMAND ----------

# DBTITLE 1,Handling Daylight Saving Time (DST)
from datetime import datetime
import pytz

# Define timezone
paris_tz = pytz.timezone('Europe/Paris')

# Define a date before and after DST change
dt_before_dst = paris_tz.localize(datetime(2025, 3, 29, 12, 0))  # Before DST
dt_after_dst = paris_tz.localize(datetime(2025, 3, 31, 12, 0))   # After DST

print("Before DST:", dt_before_dst)
print("After DST:", dt_after_dst)

# COMMAND ----------

# DBTITLE 1,Creating a Timezone-Aware Datetime
from datetime import datetime
import pytz

# Define timezone
ny_tz = pytz.timezone('America/New_York')

# Create a timezone-aware datetime
ny_time_01 = ny_tz.localize(datetime(2025, 3, 14, 10, 30))
print("New York Time:", ny_time_01)

dt_object_local = datetime(year=2020, month=3, day=1, hour=5, minute=20, second=40, microsecond=2344)
print(dt_object_local)

# Create a timezone-aware datetime
ny_time_02 = ny_tz.localize(dt_object_local)
print("New York Time:", ny_time_02)

# COMMAND ----------

# MAGIC %md
# MAGIC **Navigating Daylight Saving Time Transitions**

# COMMAND ----------

tz = pytz.timezone('US/Eastern')

# Create a datetime during DST
dst_dt = tz.localize(datetime(2024, 7, 1, 12, 0))
print(f"Summer (DST): {dst_dt}")

# Create a datetime not during DST
non_dst_dt = tz.localize(datetime(2024, 1, 1, 12, 0))
print(f"Winter (non-DST): {non_dst_dt}")

# COMMAND ----------

# MAGIC %md
# MAGIC - Notice the difference in UTC offsets: **-04:00** for **summer (EDT)** and **-05:00** for **winter (EST)**, reflecting the **DST change**.

# COMMAND ----------

from datetime import datetime
import pytz

# Define a naive datetime during a fall-back transition
naive_dt = datetime(2024, 11, 3, 1, 30)  # When DST ends in US/Eastern
print(naive_dt)

eastern_tz = pytz.timezone("US/Eastern")

# Localize with handling ambiguous time
localized_dt = eastern_tz.localize(naive_dt, is_dst=None)  # Raises an error if ambiguous
print(localized_dt)

# COMMAND ----------

# MAGIC %md
# MAGIC - If **is_dst=None** is used, it **raises an error for ambiguous times**.
# MAGIC - Use **is_dst=True or is_dst=False** to specify which **offset** to apply.

# COMMAND ----------

from datetime import datetime
import pytz

# Define a naive datetime during a fall-back transition
naive_dt = datetime(2024, 11, 3, 1, 30)  # When DST ends in US/Eastern
print(naive_dt)

eastern_tz = pytz.timezone("US/Eastern")

# Localize with handling ambiguous time
localized_dt = eastern_tz.localize(naive_dt, is_dst=True)
print(localized_dt)

# COMMAND ----------

# MAGIC %md
# MAGIC #### **b) astimezone()**
# MAGIC
# MAGIC - Converting Between Timezones
# MAGIC - astimezone() converts a **datetime** from **one timezone to another, altering the time** accordingly.

# COMMAND ----------

# DBTITLE 1,convert from "naive" to "America/New_York"
from datetime import datetime
from zoneinfo import ZoneInfo

# create datetime object for May 5, 2023 12:00 PM
dt = datetime(2023, 5, 5, 12, 0)
print(dt, dt.tzname(), dt.tzinfo)

# associate the datetime object with the New York timezone
dt_ny = dt.astimezone(ZoneInfo('America/New_York'))

# print the datetime object in the New York timezone
print(dt_ny, dt_ny.tzname(), dt_ny.tzinfo)

# COMMAND ----------

# DBTITLE 1,convert from "naive" to "UTC"
from datetime import datetime
from zoneinfo import ZoneInfo

# create datetime object
event_time = datetime(2024, 11, 4, 16, 9)
print(event_time, event_time.tzname(), event_time.tzinfo)

# associate the datetime object with the UTC timezone
event_time_utc = event_time.astimezone(ZoneInfo('UTC'))

# print the datetime object in the UTC timezone
print(event_time_utc, event_time_utc.tzname(), event_time_utc.tzinfo)

# COMMAND ----------

# DBTITLE 1,convert from "UTC" to "America/New_York" & "Asia/Tokyo"
from datetime import datetime
from zoneinfo import ZoneInfo

# Get current UTC time
d_utc = datetime.utcnow()
print("UTC Time:", d_utc, d_utc.tzname(), d_utc.tzinfo)

d_utc_rep = d_utc.replace(tzinfo=ZoneInfo("UTC"))
print("UTC Time:", d_utc_rep, d_utc_rep.tzname(), d_utc_rep.tzinfo)

# Convert UTC to New York time
d_ny = d_utc.astimezone(ZoneInfo("America/New_York"))
print("New York Time:", d_ny, d_ny.tzname(), d_ny.tzinfo)

# Convert UTC to Tokyo time
d_tokyo = d_utc.astimezone(ZoneInfo("Asia/Tokyo"))
print("Tokyo Time:", d_tokyo, d_tokyo.tzname(), d_tokyo.tzinfo)

# COMMAND ----------

# MAGIC %md
# MAGIC - Here, **.astimezone()** converts the **time** to the **specified timezone** correctly.

# COMMAND ----------

# DBTITLE 1,convert from "Europe/Paris" to "America/New_York"
from datetime import datetime
from zoneinfo import ZoneInfo

now = datetime.now()
print(now)

paris = datetime.now(tz=ZoneInfo("Europe/Paris"))
print(paris, paris.tzname(), paris.tzinfo)

New_York = paris.astimezone(ZoneInfo("America/New_York"))
print(New_York, New_York.tzname(), New_York.tzinfo)

# COMMAND ----------

# DBTITLE 1,convert from "UTC" to "Asia/Kolkata"
import pytz

now = datetime.now()
print(now)

# Get current time in UTC
utc_now = datetime.now(pytz.utc)
print("UTC Time:", utc_now)

# Convert to IST
ist_now = utc_now.astimezone(pytz.timezone('Asia/Kolkata'))
print("IST Time:", ist_now)

# COMMAND ----------

# MAGIC %md
# MAGIC **When to Use tzinfo vs tz**
# MAGIC
# MAGIC | Scenario	| Use tzinfo	| Use tz (with astimezone) |
# MAGIC |-----------|-------------|---------------|
# MAGIC | Attach timezone to naive datetime	| ✅ replace(tzinfo=...)	| ❌ |
# MAGIC | Convert between timezones	| ❌ |	✅ astimezone(tz=...) |
# MAGIC | Get current UTC time	| ✅ datetime.now(timezone.utc)	| ❌ |
# MAGIC | Normalize all datetimes to UTC	| ✅ dt.replace(tzinfo=timezone.utc)	| ✅ dt.astimezone(timezone.utc) |

# COMMAND ----------

# MAGIC %md
# MAGIC **Conclusion:**
# MAGIC
# MAGIC ✅ **localize():**
# MAGIC - Just **adds a timezone without modifying the time**.
# MAGIC
# MAGIC ✅ **astimezone():**
# MAGIC - **Changes the time** to reflect the **new timezone**.
