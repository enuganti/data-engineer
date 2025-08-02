# Databricks notebook source
# MAGIC %md
# MAGIC **translate()**
# MAGIC
# MAGIC - is helpful for **replacing or removing characters** in a **string** column, making it easy to **clean up or modify** text data.
# MAGIC
# MAGIC - You can perform **simple replacements, remove unwanted characters**, or handle more complex string **transformations**.

# COMMAND ----------

# MAGIC %md
# MAGIC **Syntax**
# MAGIC
# MAGIC      from pyspark.sql.functions import translate
# MAGIC      translate(column, matching_chars, replacement_chars)
# MAGIC
# MAGIC - **column:** The column name (or expression) where you want to perform the translation.
# MAGIC - **matching_chars:** A **string of characters** to be **replaced**.
# MAGIC - **replacement_chars:** The **string of characters** that will **replace** the **matching characters**.
# MAGIC
# MAGIC   - If **replacement_chars** is shorter than **matching_chars**, extra characters in **matching_chars** are **removed**.
# MAGIC   - If the **matching_chars and replacement_chars** strings are of **unequal length**, PySpark will consider the **shorter one**, and the characters in the **longer string** without a counterpart will be **ignored**.
# MAGIC
# MAGIC - Characters in **matching_chars** are **mapped one-to-one** to characters in **replacement_chars**.
# MAGIC

# COMMAND ----------

import pyspark.sql.functions as F
from pyspark.sql.functions import translate, col

# COMMAND ----------

# MAGIC %md
# MAGIC **1) Replace specific characters**
# MAGIC - Replace characters **a, b, c** with **x, y, z**.

# COMMAND ----------

data = [("abc@123",), ("spark@456",), ("xyz@789",), ("Chethan@456",), ("Balu@123",)]
df = spark.createDataFrame(data, ["Description"])

# Use translate to replace 'a' with 'x', 'b' with 'y' and 'c' with 'z'
df_abc = df.withColumn("translated_text", translate("Description", "abc", "xyz"))
display(df_abc)

# COMMAND ----------

# MAGIC %md
# MAGIC - Let's **replace all occurrences** of **'a' with '#'** and **'e' with '$'** in a **text** column.

# COMMAND ----------

# Sample DataFrame
data = [("Harish",), ("Rakesh",), ("Swetha",), ("Radha",), ("Sekhar",)]
columns = ["Names"]

df1 = spark.createDataFrame(data, columns)

# Use translate to replace 'a' with '#' and 'e' with '$'
df_ae = df1.withColumn("translated_text", translate("Names", "ae", "#$"))
display(df_ae)

# COMMAND ----------

# MAGIC %md
# MAGIC - For instance, replace **a with 1**, **e with 2**, and **p with 3**.

# COMMAND ----------

df_aep = df.withColumn("translated_text", translate("Description", "aep", "123"))
display(df_aep)

# COMMAND ----------

df_str_int = df.withColumn("trans_string", translate("Description", "abc", "xyz")) \
               .withColumn("trans_int", translate("Description", "123456789", "ABCDEFGHI"))
display(df_str_int)

# COMMAND ----------

# MAGIC %md
# MAGIC **2) Remove unwanted characters**
# MAGIC - Remove **digits** by translating them to **empty** string.

# COMMAND ----------

df.withColumn("no_digits", translate("Description", "0123456789", "")).display()

# COMMAND ----------

# MAGIC %md
# MAGIC **3) Removing Specific Characters**
# MAGIC - If you want to **remove** specific characters from the string, you can use **translate()** by **replacing** those characters with an **empty string**.
# MAGIC

# COMMAND ----------

# DBTITLE 1,Sample Dataframe
# Sample DataFrame
data = [(101, "Desk01", "$100,00/-"),
        (102, "Desk02", "#FootBall!"),
        (103, "Desk03", "!Cricket, @Tennis, #Football and $20,000!"),
        (104, "Desk04", "#2025-07-24$T22:55:58Z%"),
        (105, "Desk05", ":Medium;, Small<> & Large!")]
                              
columns = ["S.No", "Location", "Description"]

df_rm = spark.createDataFrame(data, columns)
display(df_rm)

# COMMAND ----------

characters_to_remove = "$#!%;:<>" # Characters you want to remove
df_cleaned = df_rm.withColumn("clean_Description", translate("Description", characters_to_remove, ""))
display(df_cleaned)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### 4) Digit Masking

# COMMAND ----------

# Sample DataFrame with more rows
data = [
    ("My number is 9876543210",),
    ("Card number: 1234-5678-9012",),
    ("Call me at 987-654-3210",),
    ("ID: AB123CD",),
    ("Account: 1234567890",),
    ("Phone: 8008008008",),
    ("Serial No: SN09876AB",),
    ("Emergency contact: 1122334455",),
    ("Alternate number: 9009090909",),
    ("Code: 000111",),
    ("Backup ID: ZX98765YU",),
    ("Booking ref: 321654987",),
    ("Contact number is 700-111-2222",),
    ("OTP sent is 456789",),
    ("My PAN is A1234BCD",),
    ("Mobile: 98765 43210",),
    ("Employee ID: EMP123456",)
]

columns = ["text"]

df_mask = spark.createDataFrame(data, columns)

display(df_mask)


# COMMAND ----------

# Step: Mask digits using translate (0–9 → X)
digits = '0123456789'
mask_char = '*' * len(digits)  # '**********'

df_masked = df_mask.withColumn("masked_text", translate("text", digits, mask_char))

# Display the result
display(df_masked)

# COMMAND ----------

# MAGIC %md
# MAGIC **5) Stripping punctuation**
# MAGIC - You can easily drop punctuation by translating it to the empty string

# COMMAND ----------

# Extended Data with various punctuation
data2 = [
    ("Hello, world!",),
    ("Spark: fast.",),
    ("Good morning!",),
    ("What's your name?",),
    ("Clean-text, now.",),
    ("Remove (brackets) and [squares].",),
    ("Multiple!!! Exclamations!!!",),
    ("Colon: Semicolon; Period.",),
    ("--Dashes-- and under_scores__",),
    ("Punctuation: gone!",),
    ("End with a period.",),
    ("Numbers 1,234.56 should stay.",),
    ("Quotes 'single' and \"double\"",),
]

# Create DataFrame
dfp = spark.createDataFrame(data2, ["Punctuation"])
display(dfp)

# COMMAND ----------

# Define punctuation characters to remove
punctuation = ",:!?.;()-[]'\"_"

empty_map = ''  # we want to remove these chars

# Use translate to remove punctuation
df_punct = dfp.withColumn("clean", translate(col("Punctuation"), punctuation, empty_map))

# Show results
display(df_punct)

# COMMAND ----------

# MAGIC %md
# MAGIC **6) Case‑folding (manual)**
# MAGIC - map **uppercase → lowercase (or vice versa)** by providing the full alphabet strings (though it’s verbose)

# COMMAND ----------

# Sample data for manual case-folding (mixed casing, acronyms, etc.)
data = [
    ("HELLO WORLD",),
    ("This Is a Title Case Sentence",),
    ("PySpark Is COOL",),
    ("email: USER@DOMAIN.COM",),
    ("123ABCxyz",),
    ("UPPER and lower MIXED",),
    ("NASA and ISRO",),
    ("Sentence with Numbers 12345",),
    ("CamelCaseVariableName",),
    ("ALL CAPS TEXT",),
    ("MiXeD CaSe StrInG",),
]

columns = ["Comments"]
df_fold = spark.createDataFrame(data, columns)
display(df_fold)

# COMMAND ----------

import string

# Manual case-folding: UPPER → lower
upper = string.ascii_uppercase   # "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
lower = string.ascii_lowercase   # "abcdefghijklmnopqrstuvwxyz"

df_fold_upr = df_fold.withColumn("lowered_manual", translate(col("Comments"), upper, lower)) \
                     .withColumn("upper_manual", translate(col("Comments"), lower, upper))

# Show result
display(df_fold_upr)

# COMMAND ----------

# MAGIC %md
# MAGIC **7) Swap numbers:**
# MAGIC
# MAGIC      1 → 9, 2 → 8, 3 → 7
# MAGIC      4 → 6, 5 → 5, 6 → 4
# MAGIC      7 → 3, 8 → 2, 9 → 1

# COMMAND ----------

df.withColumn("swapped_digits", translate("Description", "123456789", "987654321")).display()

# COMMAND ----------

# MAGIC %md
# MAGIC **8) Convert lowercase to uppercase manually**
# MAGIC
# MAGIC      a → A, b → B, c → C
# MAGIC      d → D, e → E, f → F
# MAGIC      g → G, h → H, i → I

# COMMAND ----------

df.withColumn("upper_case", translate("Description", "abcdefghi", "ABCDEFGHI")).display()

# COMMAND ----------

# MAGIC %md
# MAGIC **9) Handling Characters of Different Lengths**
# MAGIC - If **matching_chars and replace_chars** have **different lengths**, PySpark will **match characters one by one**, ignoring **extra characters** in the longer string.
# MAGIC - Notice that here, the **last character (i)** in **"aei"** has no corresponding replacement, so it is ignored.

# COMMAND ----------

# MAGIC %md
# MAGIC      translate(col("text"), "aei", "xy")
# MAGIC
# MAGIC      'a' → 'x'
# MAGIC      'e' → 'y'
# MAGIC      'i' → ❌ No mapping → gets removed
# MAGIC

# COMMAND ----------

from pyspark.sql.functions import translate, col

# Sample data containing 'a', 'e', and 'i'
data = [
    ("apple pie",),
    ("eagle",),
    ("ice cream",),
    ("aim",),
    ("fine wine",),
    ("banana",),
    ("juice",),
    ("nice",),
    ("elite",),
]

columns = ["text"]
df_diff = spark.createDataFrame(data, columns)

# Translate with unequal lengths: 'aei' → 'xy'
# 'a' → 'x', 'e' → 'y', 'i' → removed
df_diff_len = df_diff.withColumn("translated", translate(col("text"), "aei", "xy"))

# Show the result
display(df_diff_len)

# COMMAND ----------

# MAGIC %md
# MAGIC **10) Using translate() with when() to Condition Translation**
# MAGIC
# MAGIC      # If the "Description" contains "a", then apply character replacements:
# MAGIC           'a' → 'x'
# MAGIC           'e' → 'y'
# MAGIC           'i' → 'z'
# MAGIC      # Otherwise, keep the original "Description".

# COMMAND ----------

from pyspark.sql.functions import when, translate, col

# Sample data
data = [
    ("apple pie",),
    ("orange juice",),
    ("Ice cream",),
    ("milk",),
    ("banana",),
    ("tea",),
    ("coffee",),
    ("sugar",),
    ("energy drink",),
    ("juice",)
]

columns = ["Description"]
df_trans = spark.createDataFrame(data, columns)

# Apply condition: if 'a' is in Description, then translate 'aei' → 'xyz'
df_trns_aei = df_trans.withColumn(
    "conditioned_text",
    when(col("Description").contains("a"), translate("Description", "aei", "xyz")).otherwise(col("Description"))
)

# Display result
display(df_trns_aei)

# COMMAND ----------

# MAGIC %md
# MAGIC **11) Replacing characters in PySpark Column**
# MAGIC - Suppose we wanted to make the following character replacements:
# MAGIC
# MAGIC       A -> #
# MAGIC       e -> @
# MAGIC       o -> %

# COMMAND ----------

# Sample data with names (containing A, e, o)
data = [
    ("Alice",),
    ("George",),
    ("Amol",),
    ("Eon",),
    ("Rakesh",),
    ("Leona",),
    ("Sonia",),
    ("Arjun",),
    ("Deepak",),
    ("Monica",),
]

columns = ["name"]
df_repl = spark.createDataFrame(data, columns)

# Apply custom character translation: A → #, e → @, o → %
df_new_repl = df_repl.withColumn("name_masked", translate(col("name"), "Aeo", "#@%"))

# Show results
display(df_new_repl)

# COMMAND ----------

# MAGIC %md
# MAGIC **Notes:**
# MAGIC - Only **single-character** mappings are supported.
# MAGIC - It **does not support substring** replacement (use **regexp_replace** for that).