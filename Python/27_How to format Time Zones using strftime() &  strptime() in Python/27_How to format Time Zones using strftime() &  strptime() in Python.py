# Databricks notebook source
# MAGIC %md
# MAGIC #### **1) strftime() Method**
# MAGIC
# MAGIC - The **datetime object** has a method for **formatting** date objects into **readable strings**.
# MAGIC
# MAGIC - The method is called **strftime()**, and takes **one parameter, format**, to specify the format of the returned string.
# MAGIC
# MAGIC - **strftime():** Formats timestamps in **ISO 8601 format (yyyy-MM-ddTHH:mm:ss.SSSZ)**.
# MAGIC
# MAGIC #### **2) strptime() Method**
# MAGIC
# MAGIC - The **strptime()** method creates a **datetime** object from the given **string**.
# MAGIC
# MAGIC **How strptime() works?**
# MAGIC
# MAGIC The strptime() class method takes two arguments:
# MAGIC
# MAGIC - string (that be converted to datetime)
# MAGIC - format code
# MAGIC
# MAGIC Based on the **string and format code** used, the method returns its **equivalent datetime** object.
# MAGIC
# MAGIC       date_string = "21 June, 2018"
# MAGIC       date_object = datetime.strptime(date_string, "%d %B %Y")
# MAGIC
# MAGIC Here,
# MAGIC
# MAGIC - **%d** - Represents the day of the month. **Example: 01, 02, ..., 31**
# MAGIC - **%B** - Month's name in full. **Example: January, February etc.**
# MAGIC - **%Y** - Year in four digits. **Example: 2018, 2019 etc.**
# MAGIC
# MAGIC **ValueError in strptime()**
# MAGIC
# MAGIC - If the **string (first argument) and the format code (second argument)** passed to the **strptime() doesn't match**, you will get **ValueError**.
# MAGIC
# MAGIC       from datetime import datetime
# MAGIC
# MAGIC       date_string = "12/11/2018"
# MAGIC       date_object = datetime.strptime(date_string, "%d %m %Y")
# MAGIC
# MAGIC       print("date_object =", date_object)
# MAGIC
# MAGIC - If you run this program, you will get an error.
# MAGIC
# MAGIC - **ValueError:** time data '12/11/2018' **does not match** format '%d %m %Y'

# COMMAND ----------

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
# MAGIC | %p  |	AM/PM |	PM |	
# MAGIC | %M	| Minute 00-59	| 41 |	
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

# DBTITLE 1,Return the year and name of weekday
import datetime

x = datetime.datetime.now()

print(x)
print(x.year)
print(x.strftime("%A")) #  %A =>  (Weekday, full version)  =>  Tuesday

# COMMAND ----------

# DBTITLE 1,Display the name of the month
import datetime

x = datetime.datetime(2018, 6, 1)

print(x)
print(x.strftime("%B"))  # %B	=>  (Month name, full version)  => 	June

# COMMAND ----------

# MAGIC %md
# MAGIC **1) Formatting a Datetime with UTC Timezone**

# COMMAND ----------

# DBTITLE 1,UTC timezone
import pytz
from datetime import datetime

naive = datetime(2023, 2, 13, 17, 10, 27, tzinfo = pytz.utc)
print(naive, naive.tzname(), naive.tzinfo)

my_datetime_utc = naive.strftime('%Y-%m-%d %H:%M:%S %Z%z')
print(my_datetime_utc)

# COMMAND ----------

# MAGIC %md
# MAGIC | Format Code |	Description	| Example Output |
# MAGIC |-------------|-------------|----------------|
# MAGIC | %z          |	UTC offset	| +0000          |	
# MAGIC | %Z	      | Timezone	| UTC            |

# COMMAND ----------

# DBTITLE 1,Formatting a Datetime with UTC Timezone
from datetime import datetime, timezone

dt = datetime.now(timezone.utc)
print(dt, dt.tzname(), dt.tzinfo)

formatted_dt = dt.strftime("%Y-%m-%d %H:%M:%S %Z%z")
print(formatted_dt)

# COMMAND ----------

# MAGIC %md
# MAGIC **2) Convert and Format UTC to a Local Timezone**

# COMMAND ----------

# DBTITLE 1,strptime
from datetime import datetime
from zoneinfo import ZoneInfo

# Simulate user input as a string
user_time = "2025-03-20 12:45:00"

# Parse string and set timezone
dt = datetime.strptime(user_time, "%Y-%m-%d %H:%M:%S").replace(tzinfo=ZoneInfo("Asia/Kolkata"))
print("User Time (India):", dt)

# Convert to UTC
dt_utc = dt.astimezone(ZoneInfo("UTC"))
print("Converted to UTC:", dt_utc)

# COMMAND ----------

# DBTITLE 1,strftime
from datetime import datetime
from pytz import timezone

format = "%Y-%m-%d %H:%M:%S %Z%z"

# Current time in UTC
now_utc = datetime.now(timezone('UTC'))
print(now_utc.strftime(format))

# Convert to Asia/Kolkata time zone
now_asia = now_utc.astimezone(timezone('Asia/Kolkata'))
print(now_asia.strftime(format))

# COMMAND ----------

# DBTITLE 1,strftime
import time
from datetime import timezone
import datetime

dt = datetime.datetime.now()
print(dt, dt.tzname(), dt.tzinfo)

utc_time = dt.replace(tzinfo=timezone.utc)
print(utc_time, utc_time.tzname(), utc_time.tzinfo)

formt = utc_time.strftime("%Y-%m-%d %H:%M:%S")
print(formt)

# COMMAND ----------

# MAGIC %md
# MAGIC **3) Display ISO 8601 Format with Timezone**

# COMMAND ----------

# MAGIC %md
# MAGIC | Format Code |	Description	| Example Output |
# MAGIC |-------------|-------------|----------------|
# MAGIC | %B	  | Month name, full version  | March  |
# MAGIC | %d    | Day of month (01-31)      | 22     |
# MAGIC | %Y    | Year (4 digits)           | 2025   |

# COMMAND ----------

# DBTITLE 1,strftime
# import the modules
import datetime

d = datetime.datetime(1984, 1, 10, 23, 30)
print("datetime: ", d)

# strftime method allows you to print a string formatted using a series of formatting directives
d1 = d.strftime("%B %d, %Y")

# isoformat method used for quickly generating an ISO 8601 formatted date/time
d2 = d.isoformat()
print("strftime format: ", d1)
print("ISO 8601 formatted date/time: ", d2)

# COMMAND ----------

from datetime import datetime
import pytz

dt = datetime.now(pytz.timezone("Australia/Sydney"))
print(dt, dt.tzname(), dt.tzinfo)

formatted_dt = dt.isoformat()
print(formatted_dt)

# COMMAND ----------

# MAGIC %md
# MAGIC **4) Custom Format with AM/PM, Timezone Name, and Offset**

# COMMAND ----------

# DBTITLE 1,strftime
from datetime import datetime
import pytz

dt = datetime.now(pytz.timezone("America/Los_Angeles"))
print(dt, dt.tzname(), dt.tzinfo)

formatted_dt = dt.strftime("%Y-%m-%d %I:%M:%S %p (%Z, UTC%z)")
print(formatted_dt)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC | Format Code |	Description	| Example Output |
# MAGIC |-------------|-------------|----------------|
# MAGIC | %Y	| Year (4 digits)	| 2025 |
# MAGIC | %d	| Day of month (01-31)	 | 22   |
# MAGIC | %I  | Hour (12-hour)	| 02   |
# MAGIC | %M	| Minute (00-59)	| 30   |
# MAGIC | %S	| Second (00-59)	| 00   |
# MAGIC | %p	| AM/PM	            | PM   |
# MAGIC | %m	| Month as a number 01-12   | 12       |	
# MAGIC | %z |	UTC offset	| +0100 |	
# MAGIC | %Z	| Timezone	| CST |	

# COMMAND ----------

# MAGIC %md
# MAGIC **5) Extracting and Formatting Just the Timezone Offset**

# COMMAND ----------

# DBTITLE 1,strftime
from datetime import datetime
import pytz

dt = datetime.now(pytz.timezone("Asia/Kolkata"))
print(dt, dt.tzname(), dt.tzinfo)

formatted_dt = dt.strftime("UTC Offset: %z")
print(formatted_dt)

# COMMAND ----------

# MAGIC %md
# MAGIC **6) Formatting Datetime with a Specific Timezone (pytz)**

# COMMAND ----------

# MAGIC %md
# MAGIC | Format Code |	Description	| Example Output |
# MAGIC |-------------|-------------|----------------|
# MAGIC | %z |	UTC offset	| +0100 |	
# MAGIC | %Z	| Timezone	| CST |

# COMMAND ----------

# DBTITLE 1,Europe/Berlin
eur_berl = dt.astimezone(pytz.timezone('Europe/Berlin'))
print(eur_berl)

eur_berl_form = eur_berl.strftime('%Y-%m-%d %H:%M:%S %Z%z')
print(eur_berl_form)

# COMMAND ----------

# DBTITLE 0,US/Central
US_centrl = dt.astimezone(pytz.timezone('US/Central'))
print(US_centrl)

US_centrl_formt = US_centrl.strftime('%Y-%m-%d %H:%M:%S %Z%z')
print(US_centrl_formt)

# COMMAND ----------

# DBTITLE 1,US/Eastern
US_Eastern = dt.astimezone(pytz.timezone('US/Eastern'))
print(US_Eastern)

US_Eastern_formt = US_Eastern.strftime('%Y-%m-%d %H:%M:%S %Z%z')
print(US_Eastern_formt)

# COMMAND ----------

# DBTITLE 1,Convert and Format UTC to a Local Timezone
from datetime import datetime
import pytz

utc_dt = datetime.utcnow().replace(tzinfo=pytz.utc)
print(utc_dt, utc_dt.tzname(), utc_dt.tzinfo)

local_dt = utc_dt.astimezone(pytz.timezone("Europe/London"))
print(local_dt, local_dt.tzname(), local_dt.tzinfo)

formatted_dt = local_dt.strftime("%Y-%m-%d %H:%M:%S %Z%z")

print(formatted_dt)

# COMMAND ----------

# MAGIC %md
# MAGIC **7) Formatting Datetime Using "zoneinfo" (Python 3.9+)**

# COMMAND ----------

# DBTITLE 1,Formatting Datetime Using zoneinfo (Python 3.9+)
from datetime import datetime
from zoneinfo import ZoneInfo

dt = datetime.now(ZoneInfo("Asia/Tokyo"))
print(dt, dt.tzname(), dt.tzinfo)

formatted_dt = dt.strftime("%A, %B %d, %Y %I:%M %p %Z%z")
print(formatted_dt)

# COMMAND ----------

from datetime import datetime
from zoneinfo import ZoneInfo

d = datetime.utcnow().replace(tzinfo=ZoneInfo("UTC"))
print(d, d.tzname(), d.tzinfo)

# Convert to ISO 8601 format
iso = d.astimezone(ZoneInfo("Europe/London"))
print("ISO 8601 Timestamp (London):", iso)

iso_timestamp = d.astimezone(ZoneInfo("Europe/London")).strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"
print("ISO 8601 Timestamp (London):", iso_timestamp)

# COMMAND ----------

# MAGIC %md
# MAGIC - Formats **timestamp** in **yyyy-MM-ddTHH:mm:ss.SSSZ** format.

# COMMAND ----------

# MAGIC %md
# MAGIC | Format Code |	Description	| Example Output |
# MAGIC |-------------|-------------|----------------|
# MAGIC | %f          |	Microsecond	| 000000-999999 548513          |