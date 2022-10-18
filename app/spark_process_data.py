import findspark

findspark.find()
findspark.init()

import pyspark
from pyspark.sql import SparkSession
import pyspark.sql.functions as SparkF
import pyspark.sql.types as SparkT 
import multiprocessing
import numpy as np
import pandas as pd
from app.s3 import S3Client
from app.cassandra_wrapper import CassandraWrapper

cfg = pyspark.SparkConf()
cfg.setMaster(f"local[{multiprocessing.cpu_count()}]")
cfg.setAppName("SparkTest")
cfg.set("spark.eventLog.enabled", False)
cfg.setExecutorEnv(pairs=[("VAR3", "value3"), ("VAR4", "value4")])
cfg.setIfMissing("spark.executor.memory", "1g")

print(cfg.toDebugString())

skipDownload = False # skip downloading from the bucket, used to speed up testing

if not skipDownload:
    s3 = S3Client("^pinterest-data-", "eu-west-2")
    filePaths = s3.get_all_files("tmp")

#context = pyspark.SparkContext(conf=cfg).setLogLevel("DEBUG")
session = SparkSession.builder.getOrCreate()


df = session.read.json("tmp")
df = df.filter(SparkF.col("follower_count").rlike("^[0-9]")) # filters out any invalid follower counts
df = df.filter(SparkF.col("image_src").rlike("^https://.*")) # filter out any row with invalid image url

categ_total_downloads = df.groupBy("category").sum("downloaded")
categ_total_downloads = categ_total_downloads.withColumnRenamed("sum(downloaded)", "downloads")

title_total_downloads = df.groupBy("index").sum("downloaded")
title_categ = df.select("index", "title", "category", "follower_count").distinct()
title_total_downloads = title_total_downloads.sort("index").join(title_categ, on="index", how="left")
title_total_downloads = title_total_downloads.withColumnRenamed("sum(downloaded)", "total_downloads")

cass = CassandraWrapper(["localhost"], "test_keyspace")

cass.insert_dataframe("category_downloads", categ_total_downloads, create_if_missing=True)
cass.insert_dataframe("downloads_by_title", title_total_downloads, create_if_missing=True)