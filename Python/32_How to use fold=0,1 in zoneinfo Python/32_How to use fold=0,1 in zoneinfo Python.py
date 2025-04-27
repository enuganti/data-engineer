# Databricks notebook source
# MAGIC %md
# MAGIC **What is fold in datetime?**
# MAGIC
# MAGIC - **fold=0 and fold=1** in Python's **datetime module** are used when dealing with **ambiguous times** especially during the **end of daylight saving time (DST)**.
# MAGIC
# MAGIC - This feature is available in datetime starting from **Python 3.6**.
# MAGIC
# MAGIC   - **fold=0** refers to the **first occurrence** of an **ambiguous time** (**before** the clock was set back).
# MAGIC
# MAGIC   - **fold=1** refers to the **second occurrence** (after the clock has been set back).
# MAGIC
# MAGIC   - If you **don't specify fold**, Python assumes **fold=0** by default.
# MAGIC
# MAGIC   - The **fold=1** parameter in **dt.replace(fold=1)** is used to **handle ambiguous times** during the **end of daylight saving time (DST)** when **clocks are set back**.
# MAGIC
# MAGIC |  Fold |             Meaning            | Example  |
# MAGIC |-------|--------------------------------|---------| 
# MAGIC |   0   | **First occurrence** of **ambiguous time** (DST active)       |  **1:30 AM during DST**  |
# MAGIC |   1   | **Second occurrence** (after DST ends) (Standard time) | **1:30 AM after DST ends** |

# COMMAND ----------

# MAGIC %md
# MAGIC **What is an ambiguous time?**
# MAGIC - Suppose **DST ends at 2:00 AM**, and the **clock goes back one hour** to **1:00 AM**.
# MAGIC - So, **1:30 AM** happens **twice**
# MAGIC   - once **before fallback** and **once after**.

# COMMAND ----------

# MAGIC %md
# MAGIC **Example: Handling DST Transition**
# MAGIC
# MAGIC - On **November 1, 2020, at 2:00 AM**, Chicago (Central Time) switched from **CDT (UTC-5) → CST (UTC-6)** because of the **end of daylight saving time**.
# MAGIC
# MAGIC - This means the **local time 1:00 AM – 1:59 AM** happens **twice**:
# MAGIC
# MAGIC   - **First occurrence:** Before the clock is **set back (CDT, UTC-5)**.
# MAGIC
# MAGIC   - **Second occurrence:** After the clock is **set back (CST, UTC-6)**.
# MAGIC
# MAGIC - **Day light saving time** starts in **March** and **ends** in **Nov**
# MAGIC
# MAGIC - In **March** clocked is moved **1hr forward** & in **Nov** clock is moved **1 hr back**.
# MAGIC
# MAGIC | Actual Event	|    Clock Time    |	DST Status    |
# MAGIC |---------------|------------------|----------------|
# MAGIC | 1:00 AM (before rollback)	| 1:00 AM	 | DST (Daylight Saving Time) |
# MAGIC | 1:30 AM (before rollback)	| 1:30 AM	 | DST                        |
# MAGIC | 2:00 AM (DST ends)	| Clock moves back to 1:00 AM |	Now Standard Time (not DST) |
# MAGIC | 1:00 AM (after rollback)	| 1:00 AM	 |  Standard Time |
# MAGIC | 1:30 AM (after rollback)	| 1:30 AM	 |  Standard Time |
# MAGIC | 2:00 AM (after rollback)	| 2:00 AM	 |  Standard Time |

# COMMAND ----------

# MAGIC %md
# MAGIC |  Name  |            Description              |
# MAGIC |--------|-------------------------------------|
# MAGIC |  DST   |      Daylight Saving Time           |
# MAGIC |  CDT   |      Central Daylight Time          |
# MAGIC |  CST   |      Central Standard Time          |
# MAGIC |  UTC   |      Coordinated Universal Time     |
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC **1) Understanding fold=0 vs fold=1 During DST Transitions**
# MAGIC - When **daylight saving time (DST) ends**, the clock is **set back one hour**, which means **one hour is repeated**.
# MAGIC
# MAGIC - When the **clocks** go **backward (e.g., from 2:00 AM to 1:00 AM in DST)**, the hour between **1:00 AM and 2:00 AM** occurs **twice**.
# MAGIC
# MAGIC - The **fold** attribute helps Python distinguish between the **first occurrence (fold=0) and the second occurrence (fold=1)**.

# COMMAND ----------

# DBTITLE 1,Using fold=0 and fold=1 without timezones
from datetime import datetime

# Create two datetime objects with the same clock time
dt1 = datetime(2023, 11, 5, 1, 30, fold=0)  # First 1:30 (during DST)
dt2 = datetime(2023, 11, 5, 1, 30, fold=1)  # Second 1:30 (after DST ends)

print("First occurrence:", dt1)
print("Second occurrence:", dt2)

# COMMAND ----------

# DBTITLE 1,Example with timezone
from datetime import datetime
from zoneinfo import ZoneInfo

# We can use the zoneinfo module (Python 3.9+) to attach a timezone.
# New York goes off DST on Nov 6, 2022
zone = ZoneInfo("America/New_York")

# First 1:30 AM (DST is still active)
dt1 = datetime(2022, 11, 6, 1, 30, tzinfo=zone, fold=0)

# Second 1:30 AM (after DST ends)
dt2 = datetime(2022, 11, 6, 1, 30, tzinfo=zone, fold=1)

print("First 1:30 AM (DST active):", dt1)
print("Second 1:30 AM (DST ended):", dt2)

print("UTC Offset first:", dt1.utcoffset())
print("UTC Offset second:", dt2.utcoffset())

# COMMAND ----------

# MAGIC %md
# MAGIC **Notice:**
# MAGIC - The **first 1:30 AM** is in **DST (UTC-4)**.
# MAGIC - The **second 1:30 AM** is in **standard time (UTC-5)**.

# COMMAND ----------

# MAGIC %md
# MAGIC **a) Comparing fold values**
# MAGIC - **Fold doesn't affect** simple datetime equality unless **timezone rules** are involved.

# COMMAND ----------

from datetime import datetime

dt1 = datetime(2022, 11, 6, 1, 30, fold=0)
dt2 = datetime(2022, 11, 6, 1, 30, fold=1)

print(dt1 == dt2)  # True, since datetime ignores fold for comparison unless tz-aware
print(dt1.fold, dt2.fold)  # Different fold values

# COMMAND ----------

from datetime import datetime

dt1 = datetime(2022, 11, 6, 1, 30, tzinfo=ZoneInfo("America/New_York"), fold=0)
dt2 = datetime(2022, 11, 6, 1, 30, tzinfo=ZoneInfo("America/New_York"), fold=1)

print(dt1 == dt2)  # True, since datetime ignores fold for comparison unless tz-aware
print(dt1.fold, dt2.fold)  # Different fold values

# COMMAND ----------

# MAGIC %md
# MAGIC - On **November 6, 2022, at 2:00 AM**, New York switched from **daylight saving time to standard time** — clocks go back **one hour**.
# MAGIC - So **1:30 AM actually happens twice**:
# MAGIC   - First **(fold=0)**: during Daylight Saving Time (UTC-4 hours).
# MAGIC   - Second **(fold=1)**: during Standard Time (UTC-5 hours).
# MAGIC
# MAGIC - fold tells Python which of the two repeated times you mean:
# MAGIC
# MAGIC   - **fold=0:** before the clock is turned back.
# MAGIC   - **fold=1:** after the clock is turned back.
# MAGIC
# MAGIC - Even though **dt1 and dt2** represent different moments in real time, when comparing two datetime objects, Python **ignores** the **fold** unless explicitly doing a **timezone-aware comparison**.
# MAGIC
# MAGIC - Python considers them **equal** unless you **manually check the timezone offsets**.
# MAGIC
# MAGIC

# COMMAND ----------

from datetime import datetime
import pytz

def is_dst ():
    """Determine whether or not Daylight Savings Time (DST)
    is currently in effect"""

    x = datetime(datetime.now().year, 1, 1, 0, 0, 0, tzinfo=pytz.timezone('US/Eastern')) # Jan 1 of this year
    y = datetime.now(pytz.timezone('US/Eastern'))

    # if DST is in effect, their offsets will be different
    return not (y.utcoffset() == x.utcoffset())

print(is_dst())

# COMMAND ----------

# MAGIC %md
# MAGIC **b) Handling an ambiguous time during fall back**
# MAGIC - Let's check what happens when a **time** is **ambiguous** because it **occurs twice** during the **DST transition**.
# MAGIC
# MAGIC **c) Handling the same time twice during a DST transition**
# MAGIC - When **DST ends** and clocks **"fall back"** a **particular time** may **occur twice**.
# MAGIC - The **fold** parameter helps to **distinguish** between the **first and second occurrence** of that **time**.

# COMMAND ----------

# DBTITLE 1,pytz with localize
from datetime import datetime
import pytz

# Define the timezone
eastern = pytz.timezone('US/Eastern')

# Define a datetime string during the DST transition
dt_str = '2025-11-02 01:30:00'   # Correct date for DST end is Nov 2, 2025 (not Nov 1)
dt = datetime.strptime(dt_str, '%Y-%m-%d %H:%M:%S')

# First occurrence (during DST)
dt_first = eastern.localize(dt, is_dst=True)

# Second occurrence (after fall back, Standard Time)
dt_second = eastern.localize(dt, is_dst=False)

print("First 1:30 AM (DST):", dt_first, "| DST:", dt_first.dst())
print("Second 1:30 AM (Standard Time):", dt_second, "| DST:", dt_second.dst())

# COMMAND ----------

# MAGIC %md
# MAGIC - **is_dst=True** (means first occurrence, still DST)
# MAGIC - **is_dst=False** (means second occurrence, standard time)
# MAGIC - **is_dst:** Optional (True/False). Helps resolve **ambiguous times** (like **2:00 AM happening twice**) during DST transitions.

# COMMAND ----------

# MAGIC %md
# MAGIC - When you create a **datetime object** in Python, it’s **naive by default** — meaning it has **no timezone** information attached.
# MAGIC - **.localize()** is a method provided by **pytz** to **attach a timezone** properly to a **naive datetime object**.

# COMMAND ----------

# MAGIC %md
# MAGIC **fold=0** represents:
# MAGIC - **first occurrence (before clocks are set back)**
# MAGIC - The **time** during **daylight saving time (1:30 AM before the clock is turned back)**.
# MAGIC - The time **before the clocks are set back** (i.e., during **daylight saving time**).
# MAGIC
# MAGIC **fold=1** represents:
# MAGIC - **second occurrence (after the clocks fall back)**.
# MAGIC - The **time** after the clock has been **turned back (1:30 AM again, but now it is standard time)**.
# MAGIC - The time **after the clocks are set back** (i.e., **standard time or "fall back" time**).

# COMMAND ----------

# DBTITLE 1,zoneinfo (Python 3.9+)
from datetime import datetime
from zoneinfo import ZoneInfo

# Correct date during fall back
dt1 = datetime(2025, 11, 2, 1, 30, tzinfo=ZoneInfo("America/New_York"))
print("First 1:30:", dt1, "| Fold:", dt1.fold)

# To create second occurrence (after fallback), set fold=1 manually
dt2 = dt1.replace(fold=1)
print("Second 1:30:", dt2, "| Fold:", dt2.fold)

# COMMAND ----------

# MAGIC %md
# MAGIC **d) Handle DST transitions by checking fold value**
# MAGIC - When working with **ambiguous times** during a **DST transition**, you can **check the fold value** and perform specific actions based on whether it is **0 or 1**.

# COMMAND ----------

from datetime import datetime
from zoneinfo import ZoneInfo

# Example: DST end in the US Eastern Time Zone
dt = datetime(2025, 11, 2, 1, 30, tzinfo=ZoneInfo("America/New_York"))

print("Datetime:", dt)
print("Fold:", dt.fold)

if dt.fold == 0:
    print("This is the first occurrence (before DST ends).")
else:
    print("This is the second occurrence (after DST ends).")

# COMMAND ----------

from datetime import datetime
from zoneinfo import ZoneInfo  # available in Python 3.9+

# America/New_York had DST end on Nov 7, 2021
dt1 = datetime(2021, 11, 7, 1, 30, tzinfo=ZoneInfo("America/New_York"))

# By default fold=0 (first 1:30 AM, DST still active)
print("First 1:30 AM:", dt1, "UTC Offset:", dt1.utcoffset())

# Now set fold=1 to represent second 1:30 AM (after DST ended)
dt2 = dt1.replace(fold=1)

print("Second 1:30 AM:", dt2, "UTC Offset:", dt2.utcoffset())

# COMMAND ----------

# MAGIC %md
# MAGIC - The **utcoffset()** now shows **-5 hours from UTC** (in **Python**, sometimes it shows as **-1 day, 19:00:00** which is mathematically the same as **-5:00:00**).
# MAGIC
# MAGIC **See how the UTC offset changes?**
# MAGIC
# MAGIC - **First 1:30 AM** → **UTC-4 hours (during DST)**
# MAGIC - **Second 1:30 AM** → **UTC-5 hours (after DST ends)**

# COMMAND ----------

# MAGIC %md
# MAGIC **2) Checking the Difference Between fold=0 and fold=1**

# COMMAND ----------

from datetime import datetime
from zoneinfo import ZoneInfo

dt1 = datetime(2021, 11, 7, 1, 30, tzinfo=ZoneInfo("America/New_York"), fold=0)  # first 1:30 AM
dt2 = datetime(2021, 11, 7, 1, 30, tzinfo=ZoneInfo("America/New_York"), fold=1)  # second 1:30 AM

# We can calculate the difference between the two identical-looking timestamps.
diff = dt2 - dt1
print("Difference:", diff)

# COMMAND ----------

# MAGIC %md
# MAGIC - There is a **1-hour** difference between **fold=0 (before rollback) and fold=1 (after rollback)**.
# MAGIC
# MAGIC - So although the **"wall clock time" looks identical**, there's actually **1 hour** difference between **fold=0 and fold=1**.

# COMMAND ----------

# MAGIC %md
# MAGIC **3) Handling Forward Transitions (When DST Starts)**
# MAGIC
# MAGIC - When DST starts, clocks are **moved forward by one hour**, so **certain times do not exist** on that day.
# MAGIC
# MAGIC **Example: New York (Eastern Time) DST Start (March 8, 2020)**
# MAGIC - At **2:00 AM**, clocks move **forward to 3:00 AM**, meaning **2:00–2:59 AM** never happened.

# COMMAND ----------

dt_missing = datetime(2020, 3, 8, 2, tzinfo=ZoneInfo("America/New_York"))
print(dt_missing)

# COMMAND ----------

# MAGIC %md
# MAGIC - **ValueError:** 2020-03-08 02:00:00 is **missing** in the **America/New_York** timezone.
# MAGIC
# MAGIC **Explanation:**
# MAGIC
# MAGIC - The time **2:00 AM–2:59 AM never existed** in **America/New_York** on that day due to the **DST jump**.

# COMMAND ----------

# MAGIC %md
# MAGIC **4) Simulating an Event Occurring Before and After DST Ends**
# MAGIC
# MAGIC - Imagine a log entry or a scheduled event happening on **November 1, 2020, at 1:30 AM** CST (Chicago time).
# MAGIC
# MAGIC - Since this time happens **twice**, we can store it properly using **fold**.
# MAGIC
# MAGIC **Example:** Storing Log Entries with fold

# COMMAND ----------

log_entry1 = datetime(2020, 11, 1, 1, 30, tzinfo=ZoneInfo("America/Chicago")).replace(fold=0)
log_entry2 = datetime(2020, 11, 1, 1, 30, tzinfo=ZoneInfo("America/Chicago")).replace(fold=1)

print("Log entry 1:", log_entry1, log_entry1.utcoffset())  # UTC-5
print("Log entry 2:", log_entry2, log_entry2.utcoffset())  # UTC-6

# COMMAND ----------

# MAGIC %md
# MAGIC - The same time appears twice in logs.
# MAGIC - Using fold, we can distinguish between them.

# COMMAND ----------

from datetime import datetime
from zoneinfo import ZoneInfo  # Python 3.9+

# Transition from DST to Standard Time in New York (Nov 3, 2024, 1:30 AM)
tz = ZoneInfo("America/New_York")

# First occurrence (before the rollback, still in DST)
dt1 = datetime(2024, 11, 3, 1, 30, tzinfo=tz)
print(dt1, "Fold:", dt1.fold)

# Second occurrence (after the rollback, now in standard time)
dt2 = datetime(2024, 11, 3, 1, 30, tzinfo=tz).replace(fold=1)
print(dt2, "Fold:", dt2.fold)

#2024-11-03 01:30:00-04:00 Fold: 0  # Still in DST (UTC-4)
#2024-11-03 01:30:00-05:00 Fold: 1  # Standard Time (UTC-5)

# COMMAND ----------

# MAGIC %md
# MAGIC **5) Avoiding Errors in Time Calculations**
# MAGIC
# MAGIC - If you're calculating the time difference across DST transitions, you should always check the fold attribute to ensure correctness.
# MAGIC
# MAGIC **Example:** Time Difference in DST Change

# COMMAND ----------

# Before DST ends
dt_before = datetime(2020, 11, 1, 0, 30, tzinfo=ZoneInfo("America/Chicago"))
# Second occurrence (after the rollbac
dt_after_0 = datetime(2020, 11, 1, 1, 30, tzinfo=ZoneInfo("America/Chicago")).replace(fold=1)
# First occurrence (before the rollback)
dt_after_1 = datetime(2020, 11, 1, 1, 30, tzinfo=ZoneInfo("America/Chicago")).replace(fold=0)

time_diff_0 = dt_after_0 - dt_before
time_diff_1 = dt_after_1 - dt_before

print("Time difference for fold=0:", time_diff_0)
print("Time difference for fold=1:", time_diff_1)

# COMMAND ----------

# MAGIC %md
# MAGIC **Time difference: 2:00:00**
# MAGIC - (12:30 to 2) => 1:30 hrs
# MAGIC
# MAGIC   (1 to 1:30)  => 30 min
# MAGIC
# MAGIC **Explanation:**
# MAGIC - If fold wasn't used properly, the difference might be calculated incorrectly.

# COMMAND ----------

from datetime import datetime, timezone

dt = datetime(2020, 11, 1, 1, tzinfo=ZoneInfo("America/Chicago"))

print("America/Chicago: ", dt, dt.tzname(), dt.tzinfo)

dst = dt.replace(fold=1)

print("After set back: ", dst, dst.tzname(), dst.tzinfo)
print("convert DST to UTC: ", dst.astimezone(timezone.utc))