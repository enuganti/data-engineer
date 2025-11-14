# Databricks notebook source
target_cols = ['S.No','Target_ID','Target_version_id','sales_id','market_data_id','vehicle_delivery_start_date','vehicle_delivery_end_date','vehicle_delivery_payment_date','vehicle_spread','cluster_monitor','vehicle_price_determination_date','price_status']

# COMMAND ----------

# target_cols_updt_cols
target_cols_insrt_cols = 'INSERT (' + ','.join([f"{i}" for i in target_cols]) + ') VALUES (' + ','.join([f"s.{i}" for i in target_cols]) + ')'
target_cols_insrt_cols