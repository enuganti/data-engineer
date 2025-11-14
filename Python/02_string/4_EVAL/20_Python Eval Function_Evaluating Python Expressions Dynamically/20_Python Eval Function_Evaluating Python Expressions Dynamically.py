# Databricks notebook source
# MAGIC %md
# MAGIC #### **eval()**
# MAGIC
# MAGIC - The **eval()** function **evaluates the specified expression, if the expression is a legal Python statement**, it will be **executed**.
# MAGIC
# MAGIC - The Python eval() is a **built-in function** that allows us to **evaluate** the Python expression as a **‘string’** and return the value as an **integer**.

# COMMAND ----------

# MAGIC %md
# MAGIC **Syntax:**
# MAGIC
# MAGIC      eval(expression, globals=None, locals=None)
# MAGIC
# MAGIC **Parameters:**
# MAGIC
# MAGIC - **expression:**
# MAGIC   - String is parsed and evaluated as a Python expression
# MAGIC - **globals [optional]:**
# MAGIC   - Dictionary to specify the available global methods and variables.
# MAGIC - **locals [optional]:**
# MAGIC   - Another dictionary to specify the available local methods and variables.
# MAGIC - **Return:**
# MAGIC   - Returns output of the expression.

# COMMAND ----------

eval()

# COMMAND ----------

eval(1)

# COMMAND ----------

a = eval("1")
print(a, type(a))

# COMMAND ----------

b = eval("-1")
print(b, type(b))

# COMMAND ----------

# DBTITLE 1,integer 5 without eval
x = input('Enter Something:')
print(x, type(x))

# COMMAND ----------

# DBTITLE 1,integer 10
x = eval(input('Enter Something:'))
print(x, type(x))

# COMMAND ----------

# DBTITLE 1,float 10.5
x = eval(input('Enter Something:'))
print(x, type(x))

# COMMAND ----------

# DBTITLE 1,Boolean value: True
x = eval(input('Enter Something:'))
print(x, type(x))

# COMMAND ----------

# DBTITLE 1,enter string: 'suresh'
x = eval(input('Enter Something:'))
print(x, type(x))

# COMMAND ----------

# DBTITLE 1,Enter List: [10, 20, 30]
x = eval(input('Enter Something:'))
print(x, type(x))

# COMMAND ----------

# DBTITLE 1,Enter Tuple: (10, 20, 30)
x = eval(input('Enter Something: '))
print(x, type(x))

# COMMAND ----------

# DBTITLE 1,Enter tuple: (10)
x = eval(input('Enter Something: '))
print(x, type(x))

# COMMAND ----------

# DBTITLE 1,Enter tuple: (10,)
x = eval(input('Enter Something: '))
print(x, type(x))

# COMMAND ----------

# DBTITLE 1,data type: sum of integers
x = eval("10+20+30")
print(x, type(x))

y = eval("10+20/3**4//5*40")
print(y, type(y))

# COMMAND ----------

# DBTITLE 1,data types: sum of integers, float
a = eval("2 ** 8")
print(a, type(a))

b = eval("1024.5 + 1024.3")
print(b, type(b))

c = eval("sum([8, 16, 32])")
print(c, type(c))

# Arithmetic operations
arth1 = eval("5 + 7")
print("Arithmetic 01: ", arth1, type(arth1))

arth2 = eval("5 * 7")
print("Arithmetic 02: ", arth2, type(arth2))

arth3 = eval("5 ** 7")
print("Arithmetic 03: ", arth3, type(arth3))

arth4 = eval("(5 + 7) / 2")
print("Arithmetic 04: ", arth4, type(arth4))

# COMMAND ----------

# DBTITLE 1,data type: square root
x = 100
sqr = eval("x * 2")
print(sqr, type(sqr))

# COMMAND ----------

# DBTITLE 1,data type: boolean
x = 100
y = 100
cond = eval("x != y")
print(cond, type(cond))

cond1 = eval("x is y")
print(cond1, type(cond1))

boole = eval("x < 200 and y > 100")
print(boole, type(boole))

# COMMAND ----------

# DBTITLE 1,data type: boolean
x = 100
in_ope = eval("x in {50, 100, 150, 200}")
print(in_ope, type(in_ope))

# COMMAND ----------

# DBTITLE 1,len([3,5,7,9,12,15])
lengt = eval(input("Enter the python expression: "))
print(lengt, type(lengt))

# COMMAND ----------

# DBTITLE 1,Find sum of List: sum([1,2,3,4,5])
summ = eval(input("Enter the python expression: "))
print(summ, type(summ))

# COMMAND ----------

# DBTITLE 1,Enter 50, 50
# input function returns always string value
# we are getting "50" + "50" = 5050
x = input("Enter No 1: ")
y = input("Enter No 2: ")
z = x+y
print("Sum:", z, type(z))

# COMMAND ----------

# DBTITLE 1,Enter 50, 50
# int converts string to integer means "50" to 50 So 50+50=100
x = int(input("Enter No 1: "))
y = int(input("Enter No 2: "))
z = x+y
print("Sum:", z, type(z))

# COMMAND ----------

# DBTITLE 1,Enter 50, 50.5
# by using int only integers not float or any other data type
x = int(input("Enter No 1:"))
y = int(input("Enter No 2:"))
z = x+y
print("Sum:", z)

# COMMAND ----------

# DBTITLE 1,Enter 50, 50
# by using int only integers not float or any other data type
x = eval(input("Enter No 1: "))
y = eval(input("Enter No 2: "))
z = x+y
print("Sum:", z)
print(type(z))

# COMMAND ----------

# DBTITLE 1,Enter 50, 50.5
# by using int only integers not float or any other data type
x = eval(input("Enter No 1: "))
y = eval(input("Enter No 2: "))
z = x+y
print("Sum:", z)
print(type(z))

# COMMAND ----------

# DBTITLE 1,Enter strings: "Naresh", "Sundar"
# by using int only integers not float or any other data type
x = eval(input("Enter No 1: "))
y = eval(input("Enter No 2: "))
z = x+y
print("Sum:", z)
print(type(z))

# COMMAND ----------

# DBTITLE 1,[10, 5], [12, 15, 25]
# by using int only integers not float or any other data type
x = eval(input("Enter No 1: "))
y = eval(input("Enter No 2: "))
z = x+y
print("Sum:", z, type(z))

# COMMAND ----------

# DBTITLE 1,(4, 5, 6), (7, 3, 9)
# by using int only integers not float or any other data type
x = eval(input("Enter No 1: "))
y = eval(input("Enter No 2: "))
z = x+y
print("Sum:", z)
print(type(z))

# COMMAND ----------

# DBTITLE 1,Global: dictionary
# Globals
glb = eval("x+50+x**2", {'x':10})
print(glb, type(glb))

# COMMAND ----------

# Globals
x=10
glb1 = eval("x+50+x**2", {'x':x})
print(glb1, type(glb1))

# COMMAND ----------

x=100
y=100
z=1000
sep = eval("x+y+z", {'x':x, 'y':y, 'z':z})
print(sep, type(sep))

sep1 = eval("x+y+z")
print(sep1, type(sep1))

# COMMAND ----------

import math

# Area of a circle
crl = eval("math.pi * pow(25, 2)")
print(crl, type(crl))

# Volume of a sphere
vol_sph = eval("4 / 3 * math.pi * math.pow(25, 3)")
print(vol_sph, type(vol_sph))

# Hypotenuse of a right triangle
trgl = eval("math.sqrt(math.pow(10, 2) + math.pow(15, 2))")
print(trgl, type(trgl))