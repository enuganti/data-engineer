# Databricks notebook source
# MAGIC %md
# MAGIC #### 1) Serverless

# COMMAND ----------

# MAGIC %md
# MAGIC **Python**

# COMMAND ----------



# COMMAND ----------

print("data engineers")

# COMMAND ----------

# Create DataFrame
data = [('James','','Smith','1991-04-01','M',3000),
        ('Michael','Rose','','2000-05-19','M',4000),
        ('Robert','','Williams','1978-09-05','M',4000),
        ('Maria','Anne','Jones','1967-12-01','F',4000),
        ('Jen','Mary','Brown','1980-02-17','F',-1)
       ]

columns = ["firstname","middlename","lastname","dob","gender","salary"]
df = spark.createDataFrame(data=data, schema = columns)
display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC **SQL**

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT "data engineers"

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT UNIX_TIMESTAMP() As TimeStamp;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT UNIX_TIMESTAMP('2020-10-17 02:35:43') As TimeStamp;

# COMMAND ----------

# MAGIC %md
# MAGIC **R**

# COMMAND ----------

# MAGIC %r
# MAGIC # R code to print "Hello, World!"
# MAGIC myString <- "Hello, World!"
# MAGIC
# MAGIC print ( myString)

# COMMAND ----------

# MAGIC %md
# MAGIC **SCALA**

# COMMAND ----------

# MAGIC %scala
# MAGIC // Simple Scala program
# MAGIC object HelloWorld {
# MAGIC   def main(args: Array[String]): Unit = {
# MAGIC     println("Hello, World!")   // prints Hello, World!
# MAGIC
# MAGIC     // Example: simple function
# MAGIC     def add(x: Int, y: Int): Int = {
# MAGIC       x + y
# MAGIC     }
# MAGIC
# MAGIC     val result = add(5, 7)
# MAGIC     println("Sum of 5 and 7 is: " + result)
# MAGIC   }
# MAGIC }

# COMMAND ----------

# MAGIC %md
# MAGIC #### 2) Serverless Starter Warehouse

# COMMAND ----------

# MAGIC %md
# MAGIC **Python**

# COMMAND ----------

print("data engineers")

# COMMAND ----------

# MAGIC %md
# MAGIC **SQL**

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT "data engineers"

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT UNIX_TIMESTAMP('2020-10-17 02:35:43') As TimeStamp;

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS tblEmployeesFreeAccount;
# MAGIC CREATE TABLE tblEmployeesFreeAccount (
# MAGIC     EmployeeID INT PRIMARY KEY,
# MAGIC     FirstName VARCHAR(50),
# MAGIC     LastName VARCHAR(50),
# MAGIC     Department VARCHAR(50),
# MAGIC     Salary DECIMAL(10,2),
# MAGIC     HireDate DATE
# MAGIC );
# MAGIC
# MAGIC INSERT INTO tblEmployeesFreeAccount (EmployeeID, FirstName, LastName, Department, Salary, HireDate)
# MAGIC VALUES
# MAGIC (1, 'David', 'Xav', 'IT', 55000.00, '2020-06-15'),
# MAGIC (2, 'Swaroop', 'Kumar', 'ADMIN', 65000.00, '2022-09-15'),
# MAGIC (3, 'Swetha', 'Kiran', 'Sales', 75000.00, '2024-08-15'),
# MAGIC (4, 'Arun', 'Das', 'Finance', 85000.00, '2025-02-25'),
# MAGIC (5, 'Selvam', 'Kumar', 'Accounts', 99000.00, '2021-10-05');
# MAGIC
# MAGIC SELECT * FROM tblEmployeesFreeAccount;

# COMMAND ----------

# MAGIC %md
# MAGIC **R**

# COMMAND ----------

# MAGIC %r
# MAGIC # R code to print "Hello, World!"
# MAGIC myString <- "Hello, World!"
# MAGIC
# MAGIC print ( myString)

# COMMAND ----------

# MAGIC %md
# MAGIC **Scala**

# COMMAND ----------

# MAGIC %scala
# MAGIC // Simple Scala program
# MAGIC object HelloWorld {
# MAGIC   def main(args: Array[String]): Unit = {
# MAGIC     println("Hello, World!")   // prints Hello, World!
# MAGIC
# MAGIC     // Example: simple function
# MAGIC     def add(x: Int, y: Int): Int = {
# MAGIC       x + y
# MAGIC     }
# MAGIC
# MAGIC     val result = add(5, 7)
# MAGIC     println("Sum of 5 and 7 is: " + result)
# MAGIC   }
# MAGIC }