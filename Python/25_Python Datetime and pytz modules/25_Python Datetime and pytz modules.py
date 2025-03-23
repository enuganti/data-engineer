# Databricks notebook source
# MAGIC %md
# MAGIC #### **pytz**
# MAGIC
# MAGIC - **pytz** is a Python library used to work with **time zones**.
# MAGIC - It helps in handling and **converting dates and times** across **different time zones**.

# COMMAND ----------

import datetime
import pytz
from datetime import datetime

# COMMAND ----------

# MAGIC %md
# MAGIC #### **a) all_timezones**
# MAGIC
# MAGIC - It returns the **list** all the available **timezones** with **pytz.all_timezones**.

# COMMAND ----------

# DBTITLE 1,Listing All Time Zones
# Get a list of all time zones
time_zones = pytz.all_timezones
print('the supported timezones by the pytz module:', time_zones)
print(time_zones[:10])  # Print first 10 time zones

# COMMAND ----------

for tz in pytz.all_timezones:
    if(tz.__contains__("America/")):
        print(tz)

# COMMAND ----------

for tz in pytz.all_timezones:
    if(tz.__contains__("US/")):
        print(tz)

# COMMAND ----------

for tz in pytz.all_timezones:
    if(tz.__contains__("Asia/")):
        print(tz)

# COMMAND ----------

# MAGIC %md
# MAGIC #### **b) all_timezones_set**
# MAGIC
# MAGIC - It returns the **set** of all the available **timezones** with **pytz.all_timezones_set**.

# COMMAND ----------

print('all the supported timezones set: ', pytz.all_timezones_set, '\n')

# COMMAND ----------

# MAGIC %md
# MAGIC #### **c) Common_timezones, Common_timezones_set**
# MAGIC
# MAGIC - It returns the **list and set** of commonly used timezones with **pytz.common_timezones, pytz.common_timezones_set**.

# COMMAND ----------

print('Commonly used time-zones:', pytz.common_timezones, '\n')

print('Commonly used time-zones-set:', pytz.common_timezones_set, '\n')

# COMMAND ----------

# MAGIC %md
# MAGIC #### **d) country_names**
# MAGIC
# MAGIC - It returns a **dict** of **country ISO Alpha-2** Code and country name as a **key-value** pair.

# COMMAND ----------

print('country_names =')

for key, val in pytz.country_names.items():
	print(key, '=', val, end=',')
	
print('\n')
print('equivalent country name to the input code: =', pytz.country_names['IN'])

# COMMAND ----------

# MAGIC %md
# MAGIC #### **e) country_timezones**
# MAGIC
# MAGIC - It returns a **dictionary** of country **ISO Alpha-2** Code as **key and list** of supported time-zones for a particular input key (country code) as value.

# COMMAND ----------

print('country_timezones =')

for key, val in pytz.country_timezones.items():
	print(key, '=', val, end=',')
	
print('\n')
print('Time-zones supported by Antarctica =', pytz.country_timezones['AQ'])

# COMMAND ----------

print("\n\nCountry timezones: \n\n", pytz.country_timezones["US"])

# COMMAND ----------

for i in pytz.country_names:
    print(pytz.country_names[i], pytz.country_timezones.get(i))

# COMMAND ----------

# DBTITLE 1,custom datetime
from datetime import datetime, timezone

# naive datetime
unaware = datetime(2021, 8, 15, 8, 15, 12, 0)
print("naive datetime: ", unaware, unaware.tzname(), unaware.tzinfo)

# aware datetime
aware = datetime(2021, 8, 15, 8, 15, 12, 0, pytz.UTC)
print("UTC / aware timezone: ", aware, aware.tzname(), aware.tzinfo)

print(aware == unaware)

# COMMAND ----------

from datetime import datetime, timezone

dt1 = datetime.now()
print("Local Time:  ", dt1, dt1.tzname(), dt1.tzinfo)

# Get current time in UTC
dt2 = datetime.now(pytz.utc)    # datetime.now(tz=pytz.utc)
print("UTC Time:    ", dt2, dt2.tzname(), dt2.tzinfo)

dt3 = datetime.now(pytz.timezone("Europe/Vienna"))  # datetime.now(tz=pytz.timezone('Europe/Vienna'))
print("IST Time =", dt3, dt3.tzname(), dt3.tzinfo)

dt4 = datetime.now(pytz.timezone("Europe/Kiev"))  # datetime.now(tz=pytz.timezone('Europe/Kiev'))
print("IST Time =", dt4, dt4.tzname(), dt4.tzinfo)

# COMMAND ----------

# convert  local to UTC timezone
utc_timezone = pytz.timezone('UTC')
dt_utc = datetime.now(utc_timezone)
print("UTC Timezone: ", dt_utc, type(utc_timezone))

# convert  local to "US/Eastern" timezone
us_timezone = pytz.timezone('US/Eastern')
dt_us = datetime.now(us_timezone)
print("US/Eastern Timezone: ", dt_us, type(us_timezone))

# convert  local to "US/Pacific" timezone
us_pac_timezone = pytz.timezone('US/Pacific')
dt_us_pac = datetime.now(us_pac_timezone)
print("US/Pacific Timezone: ", dt_us_pac, type(us_pac_timezone))

# convert  local to "America/Santiago" timezone
amr_sant_timezone = pytz.timezone('America/Santiago')
dt_amr_sant = datetime.now(amr_sant_timezone)
print("America/Santiago: \n", dt_amr_sant, type(amr_sant_timezone))

# convert  local to "America/New_York" timezone
amr_NY_timezone = pytz.timezone('America/New_York')
dt_amr_NY = datetime.now(amr_NY_timezone)
print("America/New_York: ", dt_amr_NY, type(amr_NY_timezone))
print("strftime_America/New_York: \n", dt_amr_NY.strftime("%m/%d/%Y %H:%M:%S"))

# convert  local to "Europe/Paris" timezone
europe_timezone = pytz.timezone('Europe/Paris')
dt_eur = datetime.now(europe_timezone)
print("Europe/Paris Timezone: ", dt_eur, type(europe_timezone))

# convert  local to "Asia/Tokyo" timezone
asia_timezone = pytz.timezone('Asia/Tokyo')
dt_asia = datetime.now(asia_timezone)
print("Asia/Tokyo Timezone: ", dt_asia, type(asia_timezone))

# COMMAND ----------

import datetime

dt = datetime.datetime(2025, 3, 10, 18, 30, 0, tzinfo=pytz.UTC)
print(dt, dt.tzname())

dt = datetime.datetime.now(pytz.timezone('UCT'))
print(dt, dt.tzname())

dt = datetime.datetime(2025, 3, 10, 18, 30, 0, tzinfo=pytz.timezone('EST'))
print(dt, dt.tzname())
