# Databricks notebook source
# MAGIC %md
# MAGIC #### **total_seconds()**
# MAGIC
# MAGIC - The **total_seconds()** function is used to return the total number of **seconds** covered for the specified **duration of time instance**.

# COMMAND ----------

# MAGIC %md
# MAGIC **Syntax:**
# MAGIC      
# MAGIC       total_seconds()

# COMMAND ----------

# Importing time and timedelta module
from datetime import time, timedelta, datetime

# COMMAND ----------

# For getting total seconds for the given duration of time
 
# Specifying a time duration
duration = timedelta(minutes = 55)
print("duration", duration)
 
totalsecond = duration.total_seconds()
 
# Getting the Total seconds for the duration of 55 minutes
print("Total seconds in 55 minutes:", totalsecond)

# COMMAND ----------

# For getting total seconds for the given duration of time
 
# Specifying a time duration
duration = timedelta(minutes = -3*13)
print("duration", duration)
 
totalsecond = duration.total_seconds()
 
# Getting the Total seconds for the duration of 55 minutes
print("Total seconds in -3*13 minutes:", totalsecond)

# COMMAND ----------

from datetime import timedelta

# Create a timedelta of 2 days, 5 hours, 30 minutes, and 15 seconds
time_difference = timedelta(days=2, hours=5, minutes=30, seconds=15)
print("timediff", time_difference)

# Get total seconds
total_secs = time_difference.total_seconds()

print("Total seconds:", total_secs)

# COMMAND ----------

# MAGIC %md
# MAGIC      2 days      =  172800 seconds
# MAGIC      5 hours     =  18000  seconds
# MAGIC      30 minutes  =  1800   seconds
# MAGIC      15 seconds  =  15     seconds
# MAGIC      -----------------------------
# MAGIC      Total       =  189015 seconds
# MAGIC      -----------------------------

# COMMAND ----------

# Specifying a time duration
date1 = timedelta(days = 2,
                  hours = 3,
                  minutes = 43,
                  seconds = 35)
 
totalsecond = date1.total_seconds()
totalHours = date1.total_seconds() // 3600
 
# Getting the Total seconds
print("datetime:", date1)
print("Total seconds are:", totalsecond)
print("Total Hours are:", totalHours)

# COMMAND ----------

# Specifying a time duration
date2 = timedelta(minutes = 220)

totalsecond1 = date2.total_seconds()
totalHours1 = round(date2.total_seconds() / 3600, 1)
totalHours2 = date2.total_seconds() // 3600
 
# Getting the Total seconds
print("datetime:", date2)
print("Total seconds are:", totalsecond1)
print("Total Hours are:", totalHours1)
print("Total Hours are:", totalHours2)

# COMMAND ----------

# MAGIC %md
# MAGIC **1) Calculate Difference Between Two Datetimes**

# COMMAND ----------

from datetime import datetime

# Define two datetime objects
start_time = datetime(2024, 2, 10, 8, 30, 0)  # Feb 10, 2024, 08:30:00
end_time = datetime(2024, 2, 12, 14, 45, 30)  # Feb 12, 2024, 14:45:30

# Calculate time difference
time_diff = end_time - start_time

# Convert to total seconds
print("Time difference", time_diff)
print("Time difference in seconds:", time_diff.total_seconds())

# COMMAND ----------

# MAGIC %md
# MAGIC - This accounts for **2 days, 6 hours, 15 minutes, and 30 seconds**.

# COMMAND ----------

# MAGIC %md
# MAGIC **2) Use Case: Check if a Time Difference Exceeds a Limit**

# COMMAND ----------

from datetime import datetime, timedelta

# Define start time and limit (e.g., 48 hours)
start_time = datetime(2024, 2, 14, 12, 0, 0)
time_limit = timedelta(hours=48)

# Get current time
current_time = datetime(2024, 2, 16, 14, 30, 0)

# Check if exceeded
time_elapsed = current_time - start_time

print("Starting time:", start_time)
print("time limit:", time_limit)
print("Current time:", current_time)
print("elapsed time:", time_elapsed)

if time_elapsed.total_seconds() > time_limit.total_seconds():
    print("\nTime limit exceeded!")
else:
    print("\nStill within the time limit.")

# COMMAND ----------

# MAGIC %md
# MAGIC **3) Convert Seconds Back to Days, Hours, and Minutes**

# COMMAND ----------

from datetime import timedelta

# Example total seconds
total_seconds = 250000  

# Convert back to timedelta
time_diff = timedelta(seconds=total_seconds)
print("timediff", time_diff)

# Extract days, hours, minutes, seconds
days = time_diff.days
hours, remainder = divmod(time_diff.seconds, 3600)
minutes, seconds = divmod(remainder, 60)

print(f"Equivalent Time: {days} days, {hours} hours, {minutes} minutes, {seconds} seconds")
