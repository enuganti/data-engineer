# Databricks notebook source
from pyspark.sql import SparkSession

def sparkSession(app_name):
  spark = SparkSession.builder\
                      .appName(app_name)\
                      .getOrCreate()
  return spark
