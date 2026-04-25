# Databricks notebook source
# MAGIC %md
# MAGIC **widgets**
# MAGIC
# MAGIC - Databricks widgets are interactive input components that can be added to notebooks to **parameterize** them and make them more **reusable**. Widgets enable the passing of **arguments and variables** into notebooks in a simple way, **eliminating** the need to **hardcode values**.
# MAGIC
# MAGIC - Databricks widgets make notebooks **highly reusable** by letting users **change inputs dynamically** at runtime instead of having **hardcoded values**, which allows the same notebook to be used repeatedly with different configurations.

# COMMAND ----------

# MAGIC %md
# MAGIC **There are 4 types of widgets:**
# MAGIC - text
# MAGIC - dropdown
# MAGIC - combobox
# MAGIC - multiselect

# COMMAND ----------

# MAGIC %md
# MAGIC #### Syntax
# MAGIC
# MAGIC      dbutils.widgets.text(name='your_name_text', defaultValue='Enter your name', label='Your name')

# COMMAND ----------

dbutils.help()

# COMMAND ----------

# Get widgets documentation
dbutils.widgets.help()

# COMMAND ----------

dbutils.widgets.help("text")

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Method 1:
# MAGIC - **Create widgets using the UI**
# MAGIC   - **Edit => Add Parameter**

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Method 2:
# MAGIC - With **widget name + default value ("")**

# COMMAND ----------

# MAGIC %md
# MAGIC       dbutils.widgets.text(name="input", defaultValue="")
# MAGIC                        (or)
# MAGIC       dbutils.widgets.text("input", "")

# COMMAND ----------

dbutils.widgets.text("bronze_empty", "")

# COMMAND ----------

dbutils.widgets.text("bronze_empty", "")
print(dbutils.widgets.get("bronze_empty"))

# COMMAND ----------

# MAGIC %md
# MAGIC       dbutils.widgets.text(name="input", defaultValue="internal")
# MAGIC                        (or)
# MAGIC       dbutils.widgets.text("input", "internal")

# COMMAND ----------

dbutils.widgets.text("bronze_default", "internal")
print(dbutils.widgets.get("bronze_default"))

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Method 3:
# MAGIC - With **widget name + default value + custom label**

# COMMAND ----------

# MAGIC %md
# MAGIC       dbutils.widgets.text("bronze_custom", "", "Input Label")
# MAGIC       print(dbutils.widgets.get("bronze_custom"))
# MAGIC                        (or)
# MAGIC       dbutils.widgets.text(name="bronze_custom", defaultValue="", label="Input Label")
# MAGIC       print(dbutils.widgets.get("bronze_custom"))                 

# COMMAND ----------

# Create a text widget with name "input" and default value ""
dbutils.widgets.text("bronze_custom", "", "Input Label")

# Get the value from the widget
value = dbutils.widgets.get("bronze_custom")
print(f"The widget value is: {value}")

# COMMAND ----------

# Create a text widget with name "input" and default value "classified"
dbutils.widgets.text("bronze_custom_default", "classified", "bronze Label")

# Get the value from the widget
value = dbutils.widgets.get("bronze_custom_default")
print(f"The widget value is: {value}")

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Method 4:
# MAGIC - **Overwrite an existing widget**
# MAGIC - If the widget **already exists**, calling the same method again will **overwrite** it.

# COMMAND ----------

# DBTITLE 1,overwrite: label
# Replace with new label

# Create a text widget with name "input" and default value "classified"
dbutils.widgets.text("bronze_custom_default", "classified", "silver Label")

# Get the value from the widget
value = dbutils.widgets.get("bronze_custom_default")
print(f"The widget value is: {value}")

# COMMAND ----------

# First definition
dbutils.widgets.text("run_date", "2025-01-01", "Run Date")

# Redefine with a new default value
dbutils.widgets.text("run_date", "2025-08-22", "Run Date (Updated)")

# Get the latest value
run_date = dbutils.widgets.get("run_date")
print(f"Run date is: {run_date}")

# COMMAND ----------

# MAGIC %md
# MAGIC ##### 1) Displaying All Widget Values

# COMMAND ----------

# DBTITLE 1,Method 01
# Define widgets with default values
dbutils.widgets.text("username", "")
dbutils.widgets.text("city", "")
dbutils.widgets.text("run_date", "")
dbutils.widgets.text("input_path", "")
dbutils.widgets.text("country", "")

# Print out all current widget values
for widget_name in [
    "username",
    "city",
    "run_date",
    "input_path",
    "country"
]:
    print(
        widget_name,
        ":",
        dbutils.widgets.get(widget_name)
    )

# COMMAND ----------

# DBTITLE 1,Method 02
dbutils.widgets.getAll()

# COMMAND ----------

# MAGIC %md
# MAGIC ##### 2) Remove widgets

# COMMAND ----------

# MAGIC %md
# MAGIC **Method 01:**
# MAGIC - Remove widget by manual way (**Remove parameter**)

# COMMAND ----------

# Remove a specific widget
dbutils.widgets.remove("city")

# COMMAND ----------

# DBTITLE 1,Method 02
# Remove a specific widget
# dbutils.widgets.remove("city")
dbutils.widgets.remove("country")

# COMMAND ----------

dbutils.widgets.remove("bronze_custom")

# COMMAND ----------

# DBTITLE 1,Method 03
# Remove all widgets
dbutils.widgets.removeAll()