# Databricks notebook source
# MAGIC %md
# MAGIC #### **Difference Between strptime() and strftime() in Python's datetime Module**
# MAGIC
# MAGIC - Both **strptime() and strftime()** are used to work with **date-time formatting**, but they serve different purposes:
# MAGIC
# MAGIC | Method      |	Purpose   |
# MAGIC |-------------|-----------|
# MAGIC | strptime()	| Parses a **string** into a **datetime** object |
# MAGIC | strftime()	| Formats a **datetime** object into a **string** |

# COMMAND ----------

# MAGIC %md
# MAGIC %md
# MAGIC #### **Common Format Codes**
# MAGIC
# MAGIC | Format Code |	Description	| Example Output |
# MAGIC |-------------|-------------|----------------|
# MAGIC | %Y	| Year (4 digits)	| 2025 |
# MAGIC | %y	| Year (2 digits)	| 25   |
# MAGIC | %d	| Day of month (01-31)	 | 22   |
# MAGIC | %H  | Hour (24-hour)	| 14   |
# MAGIC | %I  | Hour (12-hour)	| 02   |
# MAGIC | %M	| Minute (00-59)	| 30   |
# MAGIC | %S	| Second (00-59)	| 00   |
# MAGIC | %p	| AM/PM	            | PM   |
# MAGIC | %w  | Weekday as a number 0-6, 0 is Sunday | 3 |
# MAGIC | %m	| Month as a number 01-12   | 12       |	
# MAGIC | %f	| Microsecond | 000000-999999	548513 |
# MAGIC | %z |	UTC offset	| +0100 |	
# MAGIC | %Z	| Timezone	| CST |	
# MAGIC | %j	| Day number of year 001-366 |	365 |	
# MAGIC | %U	|Week number of year, Sunday as the first day of week, 00-53 |	52 |	
# MAGIC | %W	| Week number of year, Monday as the first day of week, 00-53	| 52 |	
# MAGIC | %c	| Local version of date and time	Mon Dec 31 17:41:00 | 2018 |	
# MAGIC | %C	| Century |	20 |	
# MAGIC | %x	| Local version of date |	12/31/18 |	
# MAGIC | %X |	Local version of time |	17:41:00 |	
# MAGIC | %%	| A % character |	% |	
# MAGIC | %G	| ISO 8601 year |	2018 |	
# MAGIC | %u	| ISO 8601 weekday (1-7) | 	1 |	
# MAGIC | %V	| ISO 8601 weeknumber (01-53) |	01 |
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC | Format Code |	Description	| Example Output |
# MAGIC |-------------|-------------|----------------|
# MAGIC | %a    | Weekday, short version | Wed       |
# MAGIC | %A    | Weekday, full version  | Wednesday |
# MAGIC | %b	  | Month name, short version | Mar    |
# MAGIC | %B	  | Month name, full version  | March  |

# COMMAND ----------

# MAGIC %md
# MAGIC #### **1) strptime() (Convert String to Datetime)**
# MAGIC
# MAGIC - The strptime() function is used to **convert** a **string representing a date and time**, converting it into a **datetime object**.
# MAGIC
# MAGIC - Requires a format string to interpret the input.

# COMMAND ----------

# MAGIC %md
# MAGIC **Syntax:**
# MAGIC
# MAGIC      datetime.strptime(time_data, format_data)
# MAGIC
# MAGIC **Parameter:**
# MAGIC
# MAGIC - **time_data** is the **time** present in **string** format.
# MAGIC - **format_data** is the data present in **datetime format** which is converted from time_data using this function.

# COMMAND ----------

# DBTITLE 1,strptime()
from datetime import datetime

date_string = "21 June, 2018"
print(date_string, type(date_string))

date_object = datetime.strptime(date_string, "%d %B, %Y")
print(date_object, type(date_object))

# COMMAND ----------

# MAGIC %md
# MAGIC Based on the **string and format code** used, the method returns its **equivalent datetime** object.
# MAGIC
# MAGIC Here,
# MAGIC
# MAGIC - **%d** - Represents the day of the month. **Example: 01, 02, ..., 31**
# MAGIC - **%B** - Month's name in full. **Example: January, February etc.**
# MAGIC - **%Y** - Year in four digits. **Example: 2018, 2019 etc.**

# COMMAND ----------

# MAGIC %md
# MAGIC **ValueError in strptime()**
# MAGIC
# MAGIC - If the **string (first argument) and the format code (second argument)** passed to the **strptime() doesn't match**, you will get **ValueError**.

# COMMAND ----------

from datetime import datetime

date_string = "12/11/2018"
date_object = datetime.strptime(date_string, "%d %m %Y")

print("date_object =", date_object)

# COMMAND ----------

# MAGIC %md
# MAGIC - If you run this program, you will get an error.
# MAGIC
# MAGIC - **ValueError:** time data '12/11/2018' **does not match** format '%d %m %Y'

# COMMAND ----------

# MAGIC %md
# MAGIC **a) Converting a string in a specific format to a datetime object**

# COMMAND ----------

from datetime import datetime

date_string = "21 June, 2018"
print("date_string =", date_string)
print("type of date_string =", type(date_string))

date_object = datetime.strptime(date_string, "%d %B, %Y")
print("date_object =", date_object)
print("type of date_object =", type(date_object))

# COMMAND ----------

from datetime import datetime

dt_string = "12/11/2018 09:15:32"

# Considering date is in dd/mm/yyyy format
dt_object1 = datetime.strptime(dt_string, "%d/%m/%Y %H:%M:%S")
print("dt_object1 =", dt_object1)

# Considering date is in mm/dd/yyyy format
dt_object2 = datetime.strptime(dt_string, "%m/%d/%Y %H:%M:%S")
print("dt_object2 =", dt_object2)

# COMMAND ----------

from datetime import datetime

# standard date and time format
date_str = '2023-02-28 14:30:00'
date_format = '%Y-%m-%d %H:%M:%S'

date_obj = datetime.strptime(date_str, date_format)
print(date_obj)

# COMMAND ----------

date_str = '02/28/2023 02:30 PM'
date_format = '%m/%d/%Y %I:%M %p'

date_obj = datetime.strptime(date_str, date_format)
print(date_obj)

# COMMAND ----------

# MAGIC %md
# MAGIC **b) Converting a string with timezone information to a datetime object**

# COMMAND ----------

from datetime import datetime

date_str = '2023-02-28 14:30:00+05:30'
date_format = '%Y-%m-%d %H:%M:%S%z'

date_obj = datetime.strptime(date_str, date_format)
print(date_obj)

# COMMAND ----------

# MAGIC %md
# MAGIC #### **2) strftime() (Convert Datetime to String)**
# MAGIC
# MAGIC - Used to **convert** a **datetime object** into a **formatted string**.
# MAGIC
# MAGIC - Useful for displaying dates in a readable format.

# COMMAND ----------

# MAGIC %md
# MAGIC #### **a) datetime to string using strftime()**

# COMMAND ----------

from datetime import datetime

# current dateTime
now = datetime.now()
print(now)

# year
year = now.strftime("%Y")
print("year:", year)

# Month
month = now.strftime("%m")
print("month:", month)

# Day
day = now.strftime("%d")
print("day:", day)

# convert to date String
date = now.strftime("%d/%m/%Y")
print('Date String:', date)

# convert to time String
time = now.strftime("%H:%M:%S")
print("time:", time)

date_time = now.strftime("%m/%d/%Y, %H:%M:%S")
print("date and time:", date_time)

# COMMAND ----------

from datetime import datetime

now = datetime.now()
print("Current Datetime: ", now)

now_format = datetime.now().strftime("%Y-%m-%d")
print("formated datetime: ", now_format)

load_date = datetime.now().strftime("%Y-%m-%d") + 'T00:00:00'
print("UTC format datetime: ", load_date)

# COMMAND ----------

from datetime import datetime

# Datetime object
dt = datetime(2025, 3, 22, 14, 30, 0)
print(dt, type(dt), dt.tzname())

# Convert datetime to string
formatted_date = dt.strftime("%d-%b-%Y %I:%M %p")
print(formatted_date, type(formatted_date))

# COMMAND ----------

# MAGIC %md
# MAGIC - The format **"%d-%b-%Y %I:%M %p"** ensures the output is more readable.

# COMMAND ----------

# MAGIC %md
# MAGIC #### **b) Represent Dates in Numerical Format**
# MAGIC - The **numerical format** means to display the **day, month, year, hours, minutes, seconds** in **numbers**.

# COMMAND ----------

from datetime import datetime

# Get current Date
x_date = datetime.now()
print('Current Date:', x_date)

# Represent Dates in numerical format
print("dd-mm-yyyy HH:MM:SS:", x_date.strftime("%d-%m-%y %H:%M:%S"))
print("dd-mm-yyyy:", x_date.strftime("%d-%m-%Y"))
print("dd-mm-yy Format:", x_date.strftime("%d-%m-%y"))

# COMMAND ----------

# MAGIC %md
# MAGIC #### **c) Represent Dates in Textual Format**
# MAGIC
# MAGIC - The textual format means to display the **month name and day name**. like, **Wednesday,07 July, 2021**. You can print the full name and short name of a day and month.
# MAGIC
# MAGIC   - **%A:** Full name of the day. Like, **Monday**
# MAGIC   - **%a:** Short name of the day. Like, **Mon, Tue**
# MAGIC   - **%B:** Full name of the month. Like, **December**
# MAGIC   - **%b:** Short name of the month. Like, **Mar**

# COMMAND ----------

# MAGIC %md
# MAGIC | Format Code |	Description	| Example Output |
# MAGIC |-------------|-------------|----------------|
# MAGIC | %a    | Weekday, short version | Wed       |
# MAGIC | %A    | Weekday, full version  | Wednesday |
# MAGIC | %B	| Month name, full version  | March    |
# MAGIC | %b	| Month name, short version | Mar      |

# COMMAND ----------

from datetime import datetime

# Get current Date
x_date = datetime.now()
print('Current Date:', x_date)

# Represent Dates in full textual format
print("dd-MonthName-yyyy:", x_date.strftime("%d-%B-%Y"))
print("DayName-dd-MonthName-yyyy:", x_date.strftime("%A,%d %B, %Y"))

# Represent dates in short textual format
print("dd-MonthName-yyyy:", x_date.strftime("%d-%b-%Y"))
print("DDD-dd-MMM-yyyy:", x_date.strftime("%a,%d %b, %Y"))

# COMMAND ----------

# MAGIC %md
# MAGIC #### **d) Convert Date to String**

# COMMAND ----------

from datetime import date

# current date
today = date.today()
print("Today's date:", today)

# format date
print('Date String', today.strftime("%d-%m-%y"))

# COMMAND ----------

from datetime import datetime

# extract date object
today = datetime.now().date()
# format date
print('Date String', today.strftime("%d-%m-%y"))

# COMMAND ----------

# MAGIC %md
# MAGIC #### **e) Convert Time Object to String Format**
# MAGIC **i) Represent time in 24-hours and 12-hours Format**
# MAGIC
# MAGIC  - Use the **%H-%M-%S** format code to display time in **24-hours format**.
# MAGIC  - Use the **%I-%M-%S** format code to display time in **12-hours format**.

# COMMAND ----------

from datetime import datetime

# Get current time
x_time = datetime.now().time()
print('Current Time:', x_time)

print("Time in 24 hours format:", x_time.strftime("%H-%M-%S"))
print("Time in 12 hours format:", x_time.strftime("%I-%M-%S"))

# COMMAND ----------

# MAGIC %md
# MAGIC **ii) Represent Time in Microseconds Format**
# MAGIC   - Use the **%f** format code to represent time in **microsecond**.
# MAGIC   - Use the **%p** format code to represent time in **AM/PM** format.

# COMMAND ----------

from datetime import datetime

# Get current time
x_time = datetime.now().time()
print('Current Time:', x_time)

# Represent time in Microseconds (HH:MM:SS.Microsecond)
print("Time is:", x_time.strftime("%H:%M:%S.%f"))

# COMMAND ----------

# MAGIC %md
# MAGIC **iii) Represent DateTime in Milliseconds**

# COMMAND ----------

from datetime import datetime

# Current Date and time with milliseconds
print(datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')[:-3])

# COMMAND ----------

# MAGIC %md
# MAGIC **iv) Represent Time in AM/PM Format**
# MAGIC - Use the **%p** format code to represent time in **AM/PM** format.

# COMMAND ----------

from datetime import datetime

# Get current Datetime
dt = datetime.now()
print('Current Time:', dt)

# %p to represent datetime in AM/PM
print("Time is:", dt.strftime("%d-%b-%Y %I.%M %p"))

# Represent only time in AM/PM
print("Time is:", dt.time().strftime("%H.%M %p"))

# COMMAND ----------

# MAGIC %md
# MAGIC **v) Creating string from a timestamp**

# COMMAND ----------

from datetime import datetime

timestamp = 1528797322
date_time = datetime.fromtimestamp(timestamp)
print("Date time object:", date_time)

d = date_time.strftime("%m/%d/%Y, %H:%M:%S")
print("Output 2:", d)	

d = date_time.strftime("%d %b, %Y")
print("Output 3:", d)

d = date_time.strftime("%d %B, %Y")
print("Output 4:", d)

d = date_time.strftime("%I%p")
print("Output 5:", d)

d = date_time.strftime("%c")
print("Output 1:", d)	

d = date_time.strftime("%x")
print("Output 2:", d)

d = date_time.strftime("%X")
print("Output 3:", d)

# COMMAND ----------

# MAGIC %md
# MAGIC #### **Key Differences**
# MAGIC
# MAGIC | Feature |	strptime() |	strftime() |
# MAGIC |---------|------------|-------------|
# MAGIC | Purpose	| Converts a **string to datetime** |	Converts **datetime to string** |
# MAGIC | Input	  | A **date string** |	A **datetime object** |
# MAGIC | Output	| A **datetime object**	| A formatted **date string** |
# MAGIC | Example	| **"2025-03-22" → datetime(2025, 3, 22)** | **datetime(2025, 3, 22) → "22-Mar-2025"** |