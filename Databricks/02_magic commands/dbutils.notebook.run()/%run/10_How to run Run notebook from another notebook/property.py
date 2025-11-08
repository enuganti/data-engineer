# Databricks notebook source
DevConfig = {
    "General" : {
        "min_partitions" : 12,
        "shuffle_partitons" : 36,
        "stream_checkpoint_root" : "/FileStore/tables/Streaming/Stream_checkpoint/"
        },
      "DDLInfo":{
        "RootVolume": "/FileStore/tables/Streaming/",
        "Catalog" : "unitycatalog-dev"
        }
      }
