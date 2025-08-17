# Databricks notebook source
# MAGIC %md
# MAGIC #### **Python Assert**
# MAGIC
# MAGIC - Assert is used to check **if a particular condition is satisfied or not**. 
# MAGIC
# MAGIC - Python provides the assert statement to check if a given **logical expression** is **True or False**.
# MAGIC
# MAGIC - Programe execution **proceeds only if expression is True** and **raises the AssertionError when it is False**.

# COMMAND ----------

# MAGIC %md
# MAGIC      # syntax :::
# MAGIC      assert <condition>, <message>
# MAGIC
# MAGIC - If the condition is **True**, **nothing happens**.
# MAGIC
# MAGIC - If the condition is **False**, **AssertionError will be raised**.

# COMMAND ----------

# MAGIC %md
# MAGIC      .assertEqual(a, b) -->  Checks if a is equal to b, similar to the expression a == b.
# MAGIC
# MAGIC      # Assert that the original message and the received message are the same
# MAGIC      self.assertEqual(json.loads(self.sales_msg), json.loads(d_string), msg="Kafka message is not correctly received")
# MAGIC
# MAGIC      # Assert that the expected schema matches the actual schema
# MAGIC      self.assertEqual(expected_schema, actual_schema, "Schema does not match")

# COMMAND ----------

# DBTITLE 1,Update or Insert Sales data to Target
# sales Columns
sales_cols = meta_columns + ['vehicle_id', 'technology', 'vehicle_type', 'region', 'input_date', 'model_type', 'start_timestamp', 'end_timestamp', 'price', 'delivery_hours', 'base_value', 'vehicle_status', 'market_type', 'delivery_point1', 'delivery_status1']

# sales Columns insert / update to target table
sales_updt_cols = ', '.join([f't.{col} = s.{col}' for col in sales_cols])
sales_insrt_cols = 'INSERT (' + ','.join([f"{i}" for i in sales_cols]) + ') VALUES (' + ','.join([f"s.{i}" for i in sales_cols]) + ')'


def upsertToDelta(microBatchOutputDF, batchId):
    microBatchOutputDF.persist()
    microBatchOutputDF.select(*sales_cols).distinct().createOrReplaceTempView("sales_updates")
    microBatchOutputDF.sparkSession.sql(f"""MERGE INTO `ts_bmw_tvs_unitycatalog-dev`.enriched_veh_sales.tbl_mdt_sales as t \
    USING sales_updates as s \
    ON s.sales_id = t.sales_id\
    WHEN MATCHED THEN UPDATE SET {sales_updt_cols}
    WHEN NOT MATCHED THEN {sales_insrt_cols}
  """)

# COMMAND ----------

# MAGIC %md
# MAGIC **if both columns available, nothing happens**

# COMMAND ----------

# Ensure 'delivery_point' and 'delivery_status' are in sales_cols
assert 'delivery_point' in sales_cols, "delivery_point not in sales_cols"
assert 'delivery_status' in sales_cols, "delivery_status not in sales_cols"

# COMMAND ----------

try:
    # Ensure 'delivery_point' and 'delivery_status' are in sales_cols
    assert 'delivery_point' in sales_cols and 'delivery_status' in sales_cols, "both delivery_point & delivery_status not in sales_cols"
except AssertionError as e:
    print(f"Test failed: {e}")

# COMMAND ----------

# MAGIC %md
# MAGIC **if first column not available, raise error**

# COMMAND ----------

# Ensure 'delivery_point' and 'delivery_status' are in sales_cols
assert 'delivery_point' in sales_cols, "delivery_point not in sales_cols"
assert 'delivery_status' in sales_cols, "delivery_status not in sales_cols"

# COMMAND ----------

# MAGIC %md
# MAGIC **if second column not available, raise error**

# COMMAND ----------

# Ensure 'delivery_point' and 'delivery_status' are in sales_cols
assert 'delivery_point' in sales_cols, "delivery_point not in sales_cols"
assert 'delivery_status' in sales_cols, "delivery_status not in sales_cols"

# COMMAND ----------

# MAGIC %md
# MAGIC **if both columns not available, raise error**

# COMMAND ----------

# Ensure 'delivery_point' and 'delivery_status' are in sales_cols
assert 'delivery_point' in sales_cols and 'delivery_status' in sales_cols, "both delivery_point & delivery_status not in sales_cols"

# COMMAND ----------

try:
    # Ensure 'delivery_point' and 'delivery_status' are in sales_cols
    assert 'delivery_point' in sales_cols and 'delivery_status' in sales_cols, "both delivery_point & delivery_status not in sales_cols"
except AssertionError as e:
    print(f"Test failed: {e}")

# COMMAND ----------

# MAGIC %md
# MAGIC **verbosity**

# COMMAND ----------

# MAGIC %md
# MAGIC      if __name__ == "__main__":
# MAGIC          unittest.main()
# MAGIC
# MAGIC      0 for quiet
# MAGIC      1 for normal
# MAGIC      2 for detailed
# MAGIC
# MAGIC
# MAGIC      if __name__ == "__main__":
# MAGIC      unittest.main(verbosity=2)
# MAGIC
# MAGIC - If you set the **verbosity** level to **2** and this update makes **unittest** generate a **more detailed output** when you run the test module.

# COMMAND ----------

import unittest

class TestSalesStatus(unittest.TestCase):
    # Test to check if the DataFrame is not empty
    def test_table_not_empty(self):
        try:
            # Ensure 'delivery_point' and 'delivery_status' are in sales_cols
            assert 'delivery_point' in sales_cols and 'delivery_status' in sales_cols, "both delivery_point & delivery_status not in sales_cols"
        except AssertionError as e:
            print(f"Test failed : {e}")

if __name__ == '__main__':
    unittest.main(argv=[''], verbosity=0, exit=False)

# COMMAND ----------

import unittest

class TestSalesStatus(unittest.TestCase):
    # Test to check if the DataFrame is not empty
    def test_table_not_empty(self):
        try:
            # Ensure 'delivery_point' and 'delivery_status' are in sales_cols
            assert 'delivery_point' in sales_cols and 'delivery_status' in sales_cols, "both delivery_point & delivery_status not in sales_cols"
        except AssertionError as e:
            print(f"Test failed : {e}")

if __name__ == '__main__':
    unittest.main(argv=[''], verbosity=1, exit=False)

# COMMAND ----------

import unittest

class TestSalesStatus(unittest.TestCase):
    # Test to check if the DataFrame is not empty
    def test_table_not_empty(self):
        try:
            # Ensure 'delivery_point' and 'delivery_status' are in sales_cols
            assert 'delivery_point' in sales_cols and 'delivery_status' in sales_cols, "both delivery_point & delivery_status not in sales_cols"
        except AssertionError as e:
            print(f"Test failed : {e}")

if __name__ == '__main__':
    unittest.main(argv=[''], verbosity=2, exit=False)
