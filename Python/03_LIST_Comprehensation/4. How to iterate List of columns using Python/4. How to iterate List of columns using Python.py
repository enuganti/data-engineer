# Databricks notebook source
target_cols = ['S.No','Target_ID','Target_version_id','sales_id','market_data_id','vehicle_delivery_start_date','vehicle_delivery_end_date','vehicle_delivery_payment_date','vehicle_spread','cluster_monitor','vehicle_price_determination_date','price_status']

# COMMAND ----------

# target_cols Specific columns
target_cols_updt_cols = ', '.join([f't.{col} = s.{col}' for col in target_cols])
target_cols_updt_cols

# COMMAND ----------

[col for col in target_cols]

# COMMAND ----------

['s.{col}' for col in target_cols]

# COMMAND ----------

[f's.{col}' for col in target_cols]

# COMMAND ----------

[f't.{col}' for col in target_cols]

# COMMAND ----------

[f't.{col} = s.{col}' for col in target_cols]