# Databricks notebook source
json_sample = """
[
    {
      "id": "0001",
      "FirstName": "Sonalika",
      "LastName": "kishan",
      "Qualification": "Masters",
      "Technology": "ADF",
      "Age": 34,
      "Address": [
                    {
                        "city": "Bangalore",
                        "HouseAndPIN": [
                            [
                                "#234",
                                "Agara",
                                "Bangalore",
                                "Karnataka"
                            ],
                            [
                                "560105",
                                "560107"
                            ]
                        ]
                    },
                    {
                        "city": "Chennai",
                        "HouseAndPIN": [
                            [
                                "#569",
                                "Velachery",
                                "Chennai",
                                "Tamilnadu"
                            ],
                            [
                                "405603",
                                "408613"
                            ]
                        ]
                    }
                ]
    },
    {
      "id": "0002",
      "FirstName": "Santhosh",
      "LastName": "kumar",
      "Qualification": "Masters",
      "Technology": "Azure",
      "Age": 44,
      "Address": [
                    {
                        "city": "Hyderabad",
                        "HouseAndPIN": [
                            [
                                "667",
                                "Jublihills",
                                "Hyderabad"
                            ],
                            [
                              "711178",
                              "711354"
                            ]
                        ]
                    },
                    {
                        "city": "Amaravati",
                        "HouseAndPIN": [
                            [
                                "512",
                                "BenzCircle",
                                "Vijayawada",
                                "Amaravati"
                            ],
                            [
                                "560238",
                                "577034",
                                "678019",
                                "778899"
                            ]
                        ]
                    }
                ]
    },
    {
      "id": "0003",
      "FirstName": "Sourabh",
      "LastName": "Geol",
      "Qualification": "Bachelers",
      "Technology": "AWS",
      "Age": 28,
      "Address": [
                    {
                        "city": "Kolkata",
                        "HouseAndPIN": [
                            [
                                "4453",
                                "west bengal"
                            ],
                            [
                              "710123",
                              "710176"
                            ]
                        ]
                    },
                    {
                        "city": "Sweden",
                        "HouseAndPIN": [
                            [
                                "1CACV",
                                "Gothenberg",
                                "sweden"
                            ],
                            [
                              "DAFEC1A",
                              "DAFED2B",
                              "DAFEE33"
                            ]
                        ]
                    }
                ]
    }
]
"""

dbutils.fs.put("/FileStore/tables/Flatten Nested Array.json", json_sample, True)

# COMMAND ----------

from pyspark.sql.types import *

Schema = StructType([
    StructField("id", StringType(), True),
    StructField("FirstName", StringType(), True),
    StructField("LastName", StringType(), True),
    StructField("Qualification", StringType(), True),
    StructField("Technology", StringType(), True),
    StructField("Age", IntegerType(), True),
    StructField("Address", ArrayType(
      StructType([
        StructField("city", StringType(), True),
        StructField("HouseAndPIN", ArrayType(ArrayType(StringType())), True)
      ])
    ), True)
])

# COMMAND ----------

import json
dfPerson = json.loads(json_sample)
dfPerson = spark.createDataFrame(dfPerson, schema = Schema)
display(dfPerson)
dfPerson.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC - Flatten the “home” Column to Have **“All” the “Array Elements”** in a **“Single Array Column”** for **“Each City”** in the New Row Using the **“flatten()”** Method.

# COMMAND ----------

from pyspark.sql.functions import *

json_dfPerson = dfPerson.select(
                                col("FirstName"),
                                col("LastName"),
                                col("Age"),
                                col("Address.city").alias("City"),
                                flatten(col("Address.HouseAndPIN")).alias("flattenedHouseAndPIN")
                                )
display(json_dfPerson)

# Print the "Schema" of the Created DataFrame
json_dfPerson.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC **EX 01**
# MAGIC
# MAGIC **How to flatten Array of Array using flatten function?**

# COMMAND ----------

from pyspark.sql.functions import col, explode, posexplode, flatten

data = [("Revanth", [["ADF", "Spark", "ADB"], ["ETL", "Devops", None], ["SQL", None]]),
        ("Reshma", [["SSMS", None, "Salesforce"], ["SAP", "ERP", None]]),
        ("Raashi", [["Python" "VB", None], ["C++", "GitHub", "Git"]]),
        ("Krishna", [["SHELL", "DRG"], ["JAVA", None]]),
        ("Sudarshan", None),
        ("Kamal", [])
       ]

columns = ["EmpName", "Technology"]

df = spark.createDataFrame(data=data, schema=columns)
display(df)
df.printSchema()

# COMMAND ----------

display(df.select("EmpName", flatten("Technology").alias('NewTechnology')))

# COMMAND ----------

dfff=df.select("EmpName", flatten("Technology").alias('NewTechnology'))
display(dfff)

# COMMAND ----------

df5 = dfff.select("EmpName", explode("NewTechnology").alias('CoreTechnology'))
display(df5)

# COMMAND ----------

# MAGIC %md
# MAGIC **EX 02**
# MAGIC
# MAGIC **How to flatten Array of Array using Explode function?**

# COMMAND ----------

from pyspark.sql.functions import col, explode, posexplode, flatten

data1 = [("Revanth", [["ADF", "Spark", "ADB"], ["ETL", "Devops", None], ["SQL", None], None]),
        ("Reshma", [["SSMS", None, "Salesforce"], ["SAP", "ERP", None], None]),
        ("Raashi", [["Python" "VB", None], ["C++", "GitHub", "Git"]]),
        ("Krishna", [["SHELL", "DRG"], ["JAVA", None]]),
        ("Sudarshan", None),
        ("Kamal", [])
       ]

columns1 = ["EmpName", "Technology"]

df1 = spark.createDataFrame(data=data1, schema=columns1)
display(df1)
df1.printSchema()

# COMMAND ----------

df2 = df1.select("EmpName", explode("Technology").alias('NewTechnology'))
display(df2)

# COMMAND ----------

df3 = df2.select("EmpName", explode("NewTechnology").alias('CoreTechnology'))
display(df3)
