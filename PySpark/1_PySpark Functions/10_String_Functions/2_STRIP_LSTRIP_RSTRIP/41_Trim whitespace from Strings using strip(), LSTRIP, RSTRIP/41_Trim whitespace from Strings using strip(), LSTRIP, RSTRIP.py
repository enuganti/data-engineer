# Databricks notebook source
# MAGIC %md
# MAGIC #### **strip()**
# MAGIC
# MAGIC - The **strip()** method in Python is used to **remove leading (starting)** and **trailing (ending) whitespaces** characters from a given **string**.
# MAGIC
# MAGIC - By **default**, it **removes whitespace** characters (**spaces, tabs, newlines**).
# MAGIC
# MAGIC - **Leading** means at the **beginning** of the string, **trailing** means at the **end**.
# MAGIC
# MAGIC - You can specify which **character(s) to remove**, if not, any **whitespaces will be removed**.

# COMMAND ----------

# MAGIC %md
# MAGIC #### **Syntax**
# MAGIC      string.strip([chars])
# MAGIC
# MAGIC **characters**:
# MAGIC
# MAGIC - Optional.
# MAGIC - A **set of characters** to be **removed** from both the **leading (left) and trailing (right)** parts of a string.
# MAGIC - If the **chars** argument is **not provided**, all **leading and trailing whitespaces** are **removed** from the **string**.
# MAGIC
# MAGIC **Return Value**
# MAGIC
# MAGIC - The method returns a **string** after **removing** both **leading and trailing** spaces/characters.

# COMMAND ----------

dbutils.widgets.removeAll()
dbutils.widgets.text("PRIMAY_KEYS", "", "PRIMAY_KEYS")
param_PRIMAY_KEYS = dbutils.widgets.get("PRIMAY_KEYS")

# COMMAND ----------

print(param_PRIMAY_KEYS)

lstPKeys = param_PRIMAY_KEYS.strip()
print(lstPKeys)

lstPKeys_spit = param_PRIMAY_KEYS.strip().split(",")
print(lstPKeys_spit)

# COMMAND ----------

# MAGIC %md
# MAGIC **1) Remove leading and trailing whitespace from String**
# MAGIC - **.strip()** with **no arguments** removes all **whitespace characters (spaces, tabs, newlines)** from **both ends** of the string.
# MAGIC
# MAGIC

# COMMAND ----------

# Remove spaces at the beginning and at the end of the string
s = "     Azure Data Factory     "
print(f"Before: '{s}'")
print(f"After: '{s.strip()}'")

# COMMAND ----------

text = """\n\t  This is a messy multi-line string.

       \t    """
text.strip()

# COMMAND ----------

# MAGIC %md
# MAGIC **2) Remove "only leading" or "only trailing" whitespace**
# MAGIC - **.lstrip()** removes whitespace from the **left (start)** of the string.
# MAGIC
# MAGIC - **.rstrip()** removes whitespace from the **right (end)** of the string.

# COMMAND ----------

# DBTITLE 1,removing whitespace
s = "   Hello, Data Engineers!   "
print(f"Leading removed: '{s.lstrip()}'")   # Removes leading (left) spaces
print(f"Trailing removed: '{s.rstrip()}'")  # Removes trailing (right) spaces

# COMMAND ----------

# DBTITLE 1,Removing specific characters
filename = "---report---.pdf---"

print(filename.lstrip('-'))  # Removes dots on the left

print(filename.rstrip('-'))  # Removes dots on the right

# COMMAND ----------

# DBTITLE 1,Multiple characters
text = "xyzPythonxy"

print(text.lstrip('xyz')) # (removes x, y, z from the left)

print(text.rstrip('xyz')) # (removes x, y from the right — not the middle!)

# COMMAND ----------

# MAGIC %md
# MAGIC - Just like **strip()**, both **lstrip() and rstrip()** will **remove** any of the specified characters **until** they **hit a character** that **isn't** in the **list**.

# COMMAND ----------

# MAGIC %md
# MAGIC **3) Remove Custom / specific Characters from String**
# MAGIC - you can pass a **string of characters** to **strip()**, and it will **remove** any of those characters from the **ends**.

# COMMAND ----------

s1 = "!!This product is incredible!!!"
print(s1.strip("!"))

s2 = "¡¡¡This product is incredible!!!"
print(s2.strip("¡!"))

# COMMAND ----------

filename = "<<<report.csv>>>"
print(filename.strip("<>"))  

# COMMAND ----------

# MAGIC %md
# MAGIC - Here **.strip("<>")** removes all **< and >** characters from **both ends**, but stops when no more of those characters appear.

# COMMAND ----------

s = '  ##*#VisualStudioCICD#**##  '

# removes all occurrences of '#', '*', and ' ' from start and end of the string
res = s.strip('#* ')
print(res)

# COMMAND ----------

# MAGIC %md
# MAGIC **Explanation:**
# MAGIC
# MAGIC - strip('#* ') removes any #, *, and **spaces** from both **beginning and end** of the string.
# MAGIC
# MAGIC - It **stops** stripping characters from both end once it encounters a **character** that are **not in the specified set of characters**.

# COMMAND ----------

# DBTITLE 1,Remove the leading and trailing characters
# Remove the leading and trailing characters
txt = ",,,,,rrttgg.....databricks....rrr"
x = txt.strip(",.grt")
print(x)

# COMMAND ----------

date_str = "/Date(1493596800000)/"
millis = int(date_str.strip("/Date()/"))
display(millis)

# COMMAND ----------


# strip() with character parameter
string = '   ....aaaToppr learningaa....   '
print(string)

# Leading and trailing whitespaces are not removed
print(string.strip('.a'))

# All , ., a characters in the left and right of string are removed
print(string.strip(' .a'))

# COMMAND ----------

# MAGIC %md
# MAGIC **print(string.strip('.a'))**
# MAGIC - You're passing **'.a'** to strip(), which means **remove any of these characters**: **'.' and 'a'** from **both the beginning and end of the string**.
# MAGIC
# MAGIC - BUT the problem is that the string **starts with whitespace**, and since **didn't include** the **space character** in the **.strip('.a')**.
# MAGIC
# MAGIC - So the strip() method **stops** at the **whitespace**, because it **only strips** from the **ends** and **stops** when it **hits a character not in the given set**.
# MAGIC
# MAGIC - **Doesn't remove anything**, because the **leading spaces** are not in **'.a'**, so Python **stops** right there.

# COMMAND ----------

# MAGIC %md
# MAGIC **Correct usage**
# MAGIC
# MAGIC      print(string.strip(' .a'))
# MAGIC
# MAGIC - Here, you **included the space** in the **strip** set: **' .a'**.
# MAGIC
# MAGIC - Now Python will strip **spaces, dots, and 'a's** from **both** the **start and end** of the string — but **not from the middle**.
# MAGIC
# MAGIC - So it removes:
# MAGIC
# MAGIC   - Leading spaces
# MAGIC   - Leading dots
# MAGIC   - Leading 'a's
# MAGIC   - Trailing 'a's
# MAGIC   - Trailing dots
# MAGIC   - Trailing spaces

# COMMAND ----------

# MAGIC %md
# MAGIC **Summary**
# MAGIC
# MAGIC - **.strip(chars)** removes only from the **start and end of the string**.
# MAGIC
# MAGIC - Stripping **stops** when it **hits a character not in the set**.

# COMMAND ----------

# MAGIC %md
# MAGIC **4) Remove newline and tab characters**
# MAGIC - We can also **remove** the **leading and trailing newline characters (\n)** from a **string**.

# COMMAND ----------

s = '\nLearn Python\n'

# Removing newline characters from both ends
new_string = s.strip()
print('Updated String:', new_string)

# COMMAND ----------

text = "\n\n\tHello\n\t"
stripped_text = text.strip()
print(stripped_text)

# COMMAND ----------

# MAGIC %md
# MAGIC - This removes **newlines (\n) and tabs (\t)** from **both ends**.

# COMMAND ----------

# MAGIC %md
# MAGIC **5) Use with input()**

# COMMAND ----------

user_input = input("Enter your name: ").strip()
print(user_input)

#    secret   
#   This is a messy multi-line string  

# COMMAND ----------

# MAGIC %md
# MAGIC **6) Strip before comparison**

# COMMAND ----------

password = "secret"
entered_password = " \n\tsecret\t\n ".strip()

if entered_password == password:
    print("Access granted.")
else:
    print("Access denied.")

# COMMAND ----------

password = "secret"
entered_password = " \n\t.,,!!secret..,,,!!!!\t\n ".strip()

if entered_password == password:
    print("Access granted.")
else:
    print("Access denied.")

# COMMAND ----------

# MAGIC %md
# MAGIC **7) Cleaning CSV fields or log entries**
# MAGIC - Using a **list comprehension** to **.strip()** each element.

# COMMAND ----------

raw_fields = [" name ", " age", "email ", "\n\tdescription\t\n"]
clean_fields = [field.strip() for field in raw_fields]
print(clean_fields)

# COMMAND ----------

# MAGIC %md
# MAGIC **8) Chaining strip with other methods**
# MAGIC - You can **combine** strip() with e.g. **.lower()** to standardize user input.
# MAGIC
# MAGIC - First remove **whitespace/newlines**, then **convert to lowercase**.

# COMMAND ----------

user_input = "  YES\n"
normalized = user_input.strip().lower()
if normalized == "yes":
    print("User agreed!")

# COMMAND ----------

# MAGIC %md
# MAGIC **9) Removing delimiters after splitting**

# COMMAND ----------

line = "apple| banana | cherry |"
parts = line.split("|")
print(parts)

ss = [p.strip() for p in parts if p.strip()]
print(ss)  

# COMMAND ----------

# MAGIC %md
# MAGIC **10) Practical: File paths**
# MAGIC - Ensures **no stray newline** at **end** when constructing **file paths**.

# COMMAND ----------

line = "\n\n/home/user/docs/report.pdf\n"

# COMMAND ----------

print(line.lstrip())  # Removes newlines from the beginning

# COMMAND ----------

# MAGIC %md
# MAGIC **print(line.lstrip())**
# MAGIC - This removes all **leading whitespace** characters **(including \n)** from the **start** of the string.
# MAGIC
# MAGIC - So **\n\n** is **removed**, and you're **left with**
# MAGIC   
# MAGIC       /home/user/docs/report.pdf\n
# MAGIC
# MAGIC - Which, when printed, gives: because **\n** at the **end** causes a **newline**, but it’s **not visible**.

# COMMAND ----------

print(line.rstrip())  # Removes newlines from the end

# COMMAND ----------

# MAGIC %md
# MAGIC **print(line.rstrip())**
# MAGIC - This removes all **trailing whitespace** characters **(including \n)** from the **end** of the string.
# MAGIC
# MAGIC - So the **final \n** is **removed**, but the leading **\n\n** remains!
# MAGIC
# MAGIC - When you print that, it outputs you see a **blank line first**. Those **two \n** characters are printed as **two newlines** before the actual path shows up.

# COMMAND ----------

# DBTITLE 1,To visualize the difference
line = "\n\n/home/user/docs/report.pdf\n"

print("LSTRIP:")
print(repr(line.lstrip()))

print("RSTRIP:")
print(repr(line.rstrip()))