# Databricks notebook source
# MAGIC %md
# MAGIC - Python time **sleep()** function **suspends execution** for the **given number of seconds**.
# MAGIC
# MAGIC - The Python time sleep function is used to **slow down the execution of a program**.
# MAGIC
# MAGIC - We can use the Python sleep function to **pause the execution of the program** for a specific amount of time in **seconds**.
# MAGIC
# MAGIC - It is important to note that the Python sleep function only **stops the execution of the current thread and not the entire program**.

# COMMAND ----------

# MAGIC %md
# MAGIC **Syntax**
# MAGIC
# MAGIC      sleep(sec)
# MAGIC
# MAGIC **Parameters:** 
# MAGIC - **sec:** Number of seconds for which the code is required to be stopped.
# MAGIC - **Returns:** VOID.

# COMMAND ----------

# importing time package
import time
import random

# COMMAND ----------

# MAGIC %md
# MAGIC **1) Creating a Time Delay in seconds**
# MAGIC - The **start time and end time** will be printed with **15 seconds delay**.

# COMMAND ----------

print("Starting task...")
time.sleep(15)
print("Task completed!")

# COMMAND ----------

# DBTITLE 1,Randomly selects sleep time
secValue = random.choice([5, 8, 10, 13, 16, 19, 22, 25])
print("Random Sleep time", secValue)

print("Starting task...")
time.sleep(secValue)
print("Task completed!")

# COMMAND ----------

# DBTITLE 1,created function
def simulate_task():
    print("Starting task...")
    time.sleep(10)  # Simulate a delay of 10 seconds
    print("Task completed!")

simulate_task()

# COMMAND ----------

# DBTITLE 1,iterate and delay for 5 seconds
def update_log_table():
    for i in range(5):
        print("logs:", i)
        time.sleep(5)  # Delay for 5 seconds

update_log_table()

# COMMAND ----------

# DBTITLE 1,while loop
def simulate_traffic_signal():
    while True:
        print("Red signal")
        time.sleep(5)  # Delay for 5 seconds

        print("Green signal")
        time.sleep(10)  # Delay for 10 seconds

simulate_traffic_signal()

# COMMAND ----------

# MAGIC %md
# MAGIC #### **time.ctime()**
# MAGIC
# MAGIC - converts Python **time** to a **string representing local time**.
# MAGIC
# MAGIC - This **timestamp** will have the **structure** as follows:
# MAGIC
# MAGIC       day, month, date, 24-hour format current local time (in HH-MM-SS order), and year.
# MAGIC       
# MAGIC - Since it is **local time**, the time returned by this method will depend on your **geographical location**.

# COMMAND ----------

# printing the start time
print("The time of code execution begin is : ", time.ctime())

# using sleep() to hault the code execution
time.sleep(6)

# printing the end time
print("The time of code execution end is : ", time.ctime())

# COMMAND ----------

# MAGIC %md
# MAGIC #### **time.localtime()**
# MAGIC
# MAGIC - print the **current local time** inside an infinite while loop.
# MAGIC
# MAGIC - Then, the program **waits for 8 second** before repeating the same process.
# MAGIC
# MAGIC       %I => 12 Hour Format
# MAGIC       %M => Minutes
# MAGIC       %S => Seconds
# MAGIC       %p => AM / PM

# COMMAND ----------

while True:
    
    # get current local time as structured data
    current_time = time.localtime()
    print("Local time: ", current_time)

    # format the time in 12-hour clock with AM/PM
    formatted_time = time.strftime("%I:%M:%S %p", current_time)
    
    print(formatted_time)

    time.sleep(8)

# COMMAND ----------

import time

while True:
    # Get current local time as structured data
    current_time = time.localtime()

    # Format the time in 12-hour format with AM/PM
    formatted_time = time.strftime("%I:%M:%S %p", current_time)

    # Print formatted time with carriage return to overwrite the previous output
    print(f"\r{formatted_time}", end="", flush=True)

    # Sleep for 8 seconds
    time.sleep(8)

# COMMAND ----------

# MAGIC %md
# MAGIC **2) Creating a Time Delay in minutes**
# MAGIC - The **list** will be displayed after the **delay of 3 minutes**.

# COMMAND ----------

# creating and Initializing a list
Languages = ['Java', 'C++', 'Python', 'Javascript', 'C#', 'C', 'Kotlin']

# creating a time delay of 3 minutes
time.sleep(3 * 60)

print(Languages)

# COMMAND ----------

# MAGIC %md
# MAGIC **3) Creating Time Delay in the Python loop**

# COMMAND ----------

# DBTITLE 1,iterate string with delay
# initializing string
string = "azuredataengineer"

# printing azuredataengineer after delay of each character
for i in range(0, len(string)):
	print(string[i], end="")
	time.sleep(5)

# COMMAND ----------

# MAGIC %md
# MAGIC **4) Creating Time Delay in Python List**

# COMMAND ----------

# creating a time delay of 5 seconds
time.sleep(5)

# creating and Initializing a list
myList = ['Jayesh', 'Amruth', 'Ramana', 'Kamalesh', 'Syam', 25, 'March', 2025]

# the list will be displayed after the delay of 5 seconds
print(myList)

# COMMAND ----------

# MAGIC %md
# MAGIC **5) Creating Time Delay in Python Tuple**

# COMMAND ----------

# importing time package
import time

# creating a time delay of 4 seconds
time.sleep(4)

# creating and Initializing a tuple
mytuple = ('Kumar Vijay', 'Sara Guptha', 'Ganesh', 'Rahul', 'Mahendra', 'Danial', 'Murali', 'Sonu')

# the tuple will be displayed after the delay of 4 seconds
print(mytuple)

# COMMAND ----------

# MAGIC %md
# MAGIC **6) Time Delay in a List Comprehension**

# COMMAND ----------

# importing time package
import time

# creating and Initializing a list
List = ['Kumar Vijay', 'Sara Guptha', 'Ganesh', 'Rahul', 'Mahendra', 'Danial', 'Murali', 'Sonu']

# time delay of 7 seconds is created after every 7 seconds item of list gets displayed
cricketers = [(time.sleep(7), print(names)) for names in List]

# COMMAND ----------

# MAGIC %md
# MAGIC **7) Creating Multiple Time Delays**

# COMMAND ----------

# importing time package
import time

# creating and Initializing a list
Languages = ['PySpark', 'SQL', 'Python', 'Javascript', 'R', 'ADF', 'Scala']

# creating a time delay of 5 seconds
time.sleep(5)

# the list will be displayed after the delay of 5 seconds
print(Languages)

for i in Languages: 
	# creating a time delay of 8 seconds 
	time.sleep(8) 
	# After every 8 seconds an item of list will be displayed
	print(i)
