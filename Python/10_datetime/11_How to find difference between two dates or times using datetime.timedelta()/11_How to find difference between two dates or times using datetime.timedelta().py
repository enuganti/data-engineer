# Databricks notebook source
# MAGIC %md
# MAGIC ##### datetime.timedelta / timedelta
# MAGIC
# MAGIC - The datetime.timedelta class in Python is used to represent a **duration**, i.e., the **difference between two dates or times**.
# MAGIC - used to **manipulate the date**.
# MAGIC - **timedelta()** function is present under **datetime** library which is generally used for calculating **differences in dates.**

# COMMAND ----------

# MAGIC %md
# MAGIC **Syntax**
# MAGIC
# MAGIC      import datetime
# MAGIC      datetime.timedelta(
# MAGIC           weeks = value,
# MAGIC           days = value,
# MAGIC           hours = value,
# MAGIC           minutes = value,
# MAGIC           seconds = value,
# MAGIC           microseconds = value,
# MAGIC           milliseconds = value
# MAGIC           )
# MAGIC
# MAGIC      from datetime import datetime, timedelta
# MAGIC      timedelta(
# MAGIC           weeks = value,
# MAGIC           days = value,
# MAGIC           hours = value,
# MAGIC           minutes = value,
# MAGIC           seconds = value,
# MAGIC           microseconds = value,
# MAGIC           milliseconds = value
# MAGIC           )

# COMMAND ----------

# MAGIC %md
# MAGIC |   Parameter	   |    Description	                                                                                |       Example                |
# MAGIC |----------------|------------------------------------------------------------------------------------------------|------------------------------|
# MAGIC |days	        | value in days to be passed to our timedelta object. It is an optional parameter.	          | timedelta(days=1)            |
# MAGIC |hours	        | time value in hours to be passed to our timedelta object. It is an optional parameter.	     | timedelta(hours=1)           |
# MAGIC |weeks	        | value in weeks to be passed to our timedelta object. It is an optional parameter.	          | timedelta(weeks=1)           |
# MAGIC |`minutes`       | time value in `minutes` to be passed to our `timedelta object`. It is an optional parameter.	     | `timedelta(minutes=1)`      |
# MAGIC |seconds	        | time value in seconds to be passed to our timedelta object. It is an optional parameter.	     | timedelta(seconds=1)         |
# MAGIC |microseconds	   | time value in microseconds to be passed to our timedelta object. It is an optional parameter.  | timedelta(microseconds=1)    |
# MAGIC |milliseconds	   | time value in milliseconds to be passed to our timedelta object. It is an optional parameter.  | timedelta(milliseconds=1)    |

# COMMAND ----------

# MAGIC %md
# MAGIC **parameters**
# MAGIC
# MAGIC      # Days
# MAGIC      - The timedelta object's number of days is represented by this argument.
# MAGIC      - It can be an integer that is `positive or negative`.
# MAGIC
# MAGIC      # Seconds
# MAGIC      - The timedelta object's total number of seconds is represented by this argument.
# MAGIC      - It can be an integer that is positive or negative.
# MAGIC
# MAGIC      # Microseconds
# MAGIC      - The value of this property indicates how many microseconds are contained in the timedelta object. 
# MAGIC      - It can be an integer that is positive or negative.
# MAGIC
# MAGIC      # Minutes
# MAGIC      - The timedelta object's minute count is represented by this argument.
# MAGIC      - It can be an integer that is positive or negative.
# MAGIC
# MAGIC      # Hours
# MAGIC      - The timedelta object's number of hours is represented by this argument.
# MAGIC      - It can be an integer that is positive or negative.
# MAGIC
# MAGIC      # Weeks
# MAGIC      - The timedelta object's number of weeks is represented by this attribute.
# MAGIC      - It can be an integer that is positive or negative.

# COMMAND ----------

# MAGIC %md
# MAGIC ##### 1) Create a timedelta Object

# COMMAND ----------

# MAGIC %md
# MAGIC      import datetime
# MAGIC      datetime.timedelta(days=10)
# MAGIC                   (or)
# MAGIC      from datetime import datetime, timedelta
# MAGIC      timedelta(days=5)

# COMMAND ----------

import datetime
interval = datetime.timedelta(days=10, seconds=3600)
print("Timedelta in days & time:", interval)

# COMMAND ----------

from datetime import datetime, timedelta
# Create a timedelta object representing 5 days
delta = timedelta(days=5)
print("Timedelta in days:", delta)

# COMMAND ----------

from datetime import datetime, timedelta
# Create a timedelta object representing 5 days
delta = timedelta(minutes=5)
print("Timedelta in minutes:", delta)

# COMMAND ----------

import datetime
interval = datetime.timedelta(weeks=1, days=3, hours=1)
print(interval)

# COMMAND ----------

# MAGIC %md
# MAGIC **Key Differences between `datetime.date.today() and datetime.datetime.today()`:**
# MAGIC
# MAGIC **datetime.date.today()**
# MAGIC
# MAGIC - Returns only the **date (Year, Month, Day)**.
# MAGIC - Use **datetime.date.today()** if you only need the **date** (e.g., filtering by date).
# MAGIC
# MAGIC **datetime.datetime.today()**
# MAGIC
# MAGIC - Returns both **date and time** (Year, Month, Day, Hour, Minute, Second, Microsecond).
# MAGIC - Use **datetime.datetime.today()** if you need **both date and time** (e.g., timestamp logging).

# COMMAND ----------

# MAGIC %md
# MAGIC       | Feature        | datetime.date.today()   |    datetime.datetime.today()       |
# MAGIC       |----------------|-------------------------|------------------------------------|
# MAGIC       | Returns	   | Only Date (YYYY-MM-DD)  | Date & Time (YYYY-MM-DD HH:MM:SS)  |
# MAGIC       | Data Type	   | datetime.date	     | datetime.datetime                  |
# MAGIC       | Includes Time? |        ❌ No	     |        ✅ Yes                      |
# MAGIC       | Example Output |	2025-02-04	     | 2025-02-04 14:30:15.123456         |
# MAGIC

# COMMAND ----------

import datetime
today = datetime.date.today()
three_days_ago = today - datetime.timedelta(days=3)

print("Today:", today)
print("Three days ago:", three_days_ago)

# COMMAND ----------

import datetime
today = datetime.datetime.today()
tomorrow = today + datetime.timedelta(days=1)
yesterday = today - datetime.timedelta(days=1)
day = today + datetime.timedelta(days=5)

print("Today:", today)
print("Tomorrow:", tomorrow)
print("Yesterday:", yesterday)
print("After 5 days from today:", day)

# COMMAND ----------

interval = datetime.timedelta(
    weeks=1,
    hours=10,
    minutes=22,
    milliseconds=1042,
)

print(f"{interval = }")
print(f"{interval.days = }")
print(f"{interval.seconds = }")
print(f"{interval.microseconds = }")

# COMMAND ----------

# MAGIC %md
# MAGIC ##### 2) Add timedelta to the Current Date

# COMMAND ----------

from datetime import datetime, timedelta
# Get the current date and time
current_datetime = datetime.now()

# Create a timedelta object representing 10 days
delta = timedelta(days=10)

# Add the timedelta to the current date
future_date = current_datetime + delta
print("Future Date:", future_date)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### 3) Subtract timedelta from the Current Date

# COMMAND ----------

# Get the current date and time
current_datetime = datetime.now()

# Create a timedelta object representing 7 days
delta = timedelta(days=7)

# Subtract the timedelta from the current date
past_date = current_datetime - delta
print("Past Date:", past_date)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### 4) Calculate the Difference Between Two Dates

# COMMAND ----------

# Define two dates
date1 = datetime(2024, 1, 1)
date2 = datetime(2024, 1, 10)

# Calculate the difference between the two dates
difference = date2 - date1
print("Difference:", difference)
print("Days:", difference.days)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### 5) Use timedelta with Weeks, Hours, Minutes, Seconds & Milliseconds

# COMMAND ----------

# DBTITLE 1,current time
import datetime
# Get the current date and time
now = datetime.datetime.now()
now

# COMMAND ----------

print(now)

# COMMAND ----------

# Create a timedelta object representing 2 hours, 30 minutes, and 45 seconds
delta = timedelta(hours=2, minutes=30, seconds=45)
print('delta:', delta)

# Add the timedelta to the current date
new_datetime = now + delta
print("New Date and Time:", new_datetime)

# COMMAND ----------

# Creating a timedelta object
td = timedelta(days=10, hours=3, minutes=20, seconds=45)

# Printing the timedelta object
print(td)

# COMMAND ----------

# DBTITLE 1,weeks
add_weeks = now + timedelta(weeks=1)
sub_weeks = now - timedelta(weeks=1)

print("Weeks Addition:", add_weeks)
print("Weeks Subtractiob:", sub_weeks)

# COMMAND ----------

# DBTITLE 1,days
# Add the timedelta to the current date
newdate_3days_add = now + timedelta(days=3)
newdate_3days_sub = now - timedelta(days=3)

newdate_365days_add = now + timedelta(days=365)
newdate_365days_sub = now - timedelta(days=366)

print("Add 3 days:", newdate_3days_add)
print("Sub 3 days:", newdate_3days_sub)

print("Add 365 days:", newdate_365days_add)
print("Sub 365 days:", newdate_365days_sub)

# COMMAND ----------

# DBTITLE 1,hours
add_hours = today + timedelta(hours=3)
sub_hours = today - timedelta(hours=3)

print("Add Hours:", add_hours)
print("Sub Hours:", sub_hours)

# COMMAND ----------

# DBTITLE 1,minutes
# Add the timedelta to the current date
newdate_3min_add = now + timedelta(minutes=10)
newdate_3days_sub = now - timedelta(minutes=10)

print("Add 3 minutes:", newdate_3min_add)
print("Sub 3 minutes:", newdate_3days_sub)

# COMMAND ----------

# DBTITLE 1,microseconds
add_microseconds = today + datetime.timedelta(microseconds=10)
sub_microseconds = today - datetime.timedelta(microseconds=10)

print("Add microseconds:", add_microseconds)
print("Sub microseconds:", sub_microseconds)

# COMMAND ----------

# DBTITLE 1,milliseconds
add_milliseconds = now + timedelta(milliseconds=3)
sub_milliseconds = now + timedelta(milliseconds=3)

print("Add milliseconds:", add_milliseconds)
print("Sub milliseconds:", sub_milliseconds)

# COMMAND ----------

# MAGIC %md
# MAGIC       from datetime import datetime, timedelta
# MAGIC       import random
# MAGIC
# MAGIC       # Generate a random date within the past year
# MAGIC       random_date = datetime.now() - timedelta(days=random.randint(0, 365))
# MAGIC       print(random_date)
# MAGIC
# MAGIC - The **datetime** module itself **doesn't** have a **now()** method. Need to call **now()** from **datetime.datetime**.
# MAGIC   - **datetime.now()** correctly retrieves the **current datetime**.
# MAGIC   - **timedelta(days=random.randint(0, 365))** generates a **random number** of days to subtract.

# COMMAND ----------

from datetime import datetime, timedelta
(datetime.now() - timedelta(days=random.randint(0, 365)))

# COMMAND ----------

((datetime.now() - timedelta(days=random.randint(0, 365))).timestamp())

# COMMAND ----------

int((datetime.now() - timedelta(days=random.randint(0, 365))).timestamp())

# COMMAND ----------

# MAGIC %md
# MAGIC ##### 6) Use timedelta in a Function

# COMMAND ----------

def add_days_to_current_date(days):
    current_datetime = datetime.now()
    delta = timedelta(days=days)
    return current_datetime + delta

# Call the function
print("Date after 15 days:", add_days_to_current_date(15))