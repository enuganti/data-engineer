# Databricks notebook source
# MAGIC %md
# MAGIC - The **datetime.datetime.now()** function in Python is used to get the **current local date and time**.
# MAGIC
# MAGIC       # module, class, method
# MAGIC       datetime.datetime.now()

# COMMAND ----------

# MAGIC %md
# MAGIC **Syntax**
# MAGIC  
# MAGIC      import datetime
# MAGIC      datetime.datetime.now()
# MAGIC                  (or)
# MAGIC      from datetime import datetime
# MAGIC      datetime.now()

# COMMAND ----------

# MAGIC %md
# MAGIC ##### 1) Get the Current Date and Time

# COMMAND ----------

import datetime

# Get the current date and time
current_datetime = datetime.datetime.now()             # Applying datetime.now
print("Current Date and Time:", current_datetime)      # show current date and time

# COMMAND ----------

# Load datetime from datetime module
from datetime import datetime

# Get the current date and time
my_datetime = datetime.now()
print("Current Date and Time:", my_datetime)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### 2) Format the Current Date and Time
# MAGIC - You can format the current date and time using the **strftime** method.

# COMMAND ----------

# Get the current date and time
import datetime
current_datetime = datetime.datetime.now()

# Short Version of Year
SVY = current_datetime.strftime("%y")
print("Short Version of Year:", SVY)

# Full Version of Year
FVY = current_datetime.strftime("%Y")
print("Full Version of Year:", FVY)

# Short Version of Month
SVM = current_datetime.strftime("%b")
print("\nShort Version of Month:", SVM)

# Full Version of Month
FVM = current_datetime.strftime("%B")
print("Full Version of Month:", FVM)

# Short Version of Day
SVD = current_datetime.strftime("%a")
print("\nShort Version of Day:", SVD)

# Full Version of Day
FVD = current_datetime.strftime("%A")
print("Full Version of Day:", FVD)

# COMMAND ----------

# Number of Weekday
WD = current_datetime.strftime("%w")
print("Number of Weekday:", WD)

# 24 Hour Format
HFormat24 = current_datetime.strftime("%H")
print("\n24 Hour Format:", HFormat24)

# 12 Hour Format
HFormat12 = current_datetime.strftime("%I")
print("12 Hour Format:", HFormat12)

# AM / PM
AM_PM = current_datetime.strftime("%p")
print("\nAM / PM:", AM_PM)

# COMMAND ----------

# Minutes
Minutes = current_datetime.strftime("%M")
print("Minutes:", Minutes)

# Seconds
Seconds = current_datetime.strftime("%S")
print("Seconds:", Seconds)

# MicroSeconds
MicroSec = current_datetime.strftime("%f")
print("MicroSeconds:", MicroSec)

# COMMAND ----------

# Day Number of Year
DNOY = current_datetime.strftime("%j")
print("Day Number of Year:", DNOY)

# date
date = current_datetime.strftime("%x")
print("date:", date)

# time
time = current_datetime.strftime("%X")
print("time:", time)

# COMMAND ----------

# Format date
format_date = current_datetime.strftime("%Y-%m-%d")
print("Formatted Date:", format_date)

# Format time
format_time = current_datetime.strftime("%H:%M:%S")
print("Formatted Time:", format_time)

# Format the date and time
formatted_datetime = current_datetime.strftime("%Y-%m-%d %H:%M:%S")
print("Formatted Date and Time:", formatted_datetime)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### 3) Get the Current Date

# COMMAND ----------

import datetime
date = datetime.date(2025,2,2)
print(date)

# COMMAND ----------

# Get the current date and time
now_date = datetime.datetime.now()
print("Now Date:", now_date)

# Get the current date
current_date = datetime.datetime.now().date()
print("Current Date:", current_date)

# COMMAND ----------

today = datetime.date.today()
print(today)

# COMMAND ----------

import datetime

# datetime(YEAR MONTH DAY HOURS MINUTES SECONDS MICROSECONDS)
target_datetime = datetime.datetime(2028, 7, 15, 15, 30, 5, 513)
current_datetime = datetime.datetime.now()

if target_datetime > current_datetime:
    print("target date has passed")
else:
    print("target date has not passed")

# COMMAND ----------

# Load datetime from datetime module
from datetime import datetime

start_date = datetime(2025, 2, 12)
print(start_date)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### 4) Get the Current Time

# COMMAND ----------

time = datetime.time(12, 30, 0)
print(time)

# COMMAND ----------

# Get the current time
current_time = datetime.datetime.now().time()
print("Current Time:", current_time)

# COMMAND ----------

# DBTITLE 1,Creating Date & time
# datetime(YEAR MONTH DAY HOURS MINUTES SECONDS MICROSECONDS)
create_date = datetime.datetime(2028, 7, 15, 15, 30, 5, 513)
print("Created Date:", create_date)

# COMMAND ----------

# DBTITLE 1,Creating Date
import datetime
# datetime(YEAR MONTH DAY HOURS MINUTES SECONDS MICROSECONDS)
create_date1 = datetime.datetime(2028, 7, 15)
print("Created Date:", create_date1)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### 5) Use datetime.datetime.now() in a Function

# COMMAND ----------

def log_current_time():
    current_time = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print("Log Time:", current_time)

# Call the function
log_current_time()

# COMMAND ----------

# MAGIC %md
# MAGIC ##### 6) Calculate Time Difference

# COMMAND ----------

# Get the current time
start_time = datetime.datetime.now()

# Simulate a delay
time.sleep(2)

# Get the time after delay
end_time = datetime.datetime.now()

# Calculate the difference
time_difference = end_time - start_time
print("Time Difference:", time_difference)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Key Differences b/n datetime.now() & datetime.today()
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

from datetime import datetime, timezone

now_with_tz = datetime.now(timezone.utc)
print("Now with UTC:", now_with_tz)

# COMMAND ----------

from datetime import datetime
import pytz

# Define Europe timezone with a 4.5-hour offset (Asia/Kabul is UTC+4:30, no direct Europe zone)
custom_tz = pytz.FixedOffset(270)  # 4 hours 30 minutes = 270 minutes

# Get current time in that timezone
europe_time = datetime.now(custom_tz)

print(europe_time)