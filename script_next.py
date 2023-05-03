###### TEDx-Load-Aggregate-Model
######

import sys
import json
import pyspark
from pyspark.sql.functions import col, collect_list, array_join,array_distinct

from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job




##### FROM FILES
tedx_dataset_path = "s3://dati-cloud/tedx_dataset.csv"
tedx_dataset_tag="s3://dati-cloud/tags_dataset.csv"
tedx_dataset_next="s3://dati-cloud/watch_next_dataset.csv"
###### READ PARAMETERS
args = getResolvedOptions(sys.argv, ['JOB_NAME'])

##### START JOB CONTEXT AND JOB
sc = SparkContext()


glueContext = GlueContext(sc)
spark = glueContext.spark_session


    
job = Job(glueContext)
job.init(args['JOB_NAME'], args)


#### READ INPUT FILES TO CREATE AN INPUT DATASET
tedx_dataset = spark.read \
    .option("header","true") \
    .option("quote", "\"") \
    .option("escape", "\"") \
    .csv(tedx_dataset_path)
    
tedx_dataset.printSchema()

tedx_tag = spark.read \
    .option("header","true") \
    .option("quote", "\"") \
    .option("escape", "\"") \
    .csv(tedx_dataset_tag)
    
tedx_tag.printSchema()

tedx_next = spark.read \
    .option("header","true") \
    .option("quote", "\"") \
    .option("escape", "\"") \
    .csv(tedx_dataset_next)
    
tedx_next.printSchema()

#### FILTER ITEMS WITH NULL POSTING KEY
##count_items = tedx_dataset.count()
##count_items_null = tedx_dataset.filter("idx is not null").count()

##print(f"Number of items from RAW DATA {count_items}")
##print(f"Number of items from RAW DATA with NOT NULL KEY {count_items_null}")



tags_dataset_agg = tedx_tag.groupBy(col("idx")).agg(collect_list("tag").alias("tags"))
tags_dataset_agg.printSchema()

## READ WATCH NEXT DATASET

#watch_next_dataset = watch_next_dataset.drop("url")
###watch_next_dataset = watch_next_dataset.groupBy(col("idx").alias("idx_ref")).agg(array_distinct(collect_list("watch_next_idx")).alias("watch_next"))
watch_next_dataset = tedx_next.groupBy(col("idx").alias("idx_ref")).agg(array_distinct(collect_list(col("watch_next_idx"))).alias("watch_next"))


# AGGREGATE DATASETS
tedx_dataset_agg = tedx_dataset.join(tags_dataset_agg, tedx_dataset.idx == tags_dataset_agg.idx, "left") \
    .drop(tedx_tag.idx) \
    .select(col("idx").alias("_id"), col("*")) \
    .drop("idx") \

tedx_dataset_agg.printSchema()

tedx_dataset_agg_2 = tedx_dataset_agg.join(watch_next_dataset, tedx_dataset_agg._id == watch_next_dataset.idx_ref, "left") \
    .drop(watch_next_dataset.idx_ref) \
    .select(col("_id"), col("*")) \
    
tedx_dataset_agg_2.printSchema()





mongo_uri = "mongodb+srv://cloud-db:giulia@cluster0.dwvyxyf.mongodb.net"
print(mongo_uri)

write_mongo_options = {
    "uri": mongo_uri,
    "database": "cloud-db",
    "collection": "tedx_data",
    "ssl": "true",
    "ssl.domain_match": "false"}
from awsglue.dynamicframe import DynamicFrame
tedx_dataset_dynamic_frame = DynamicFrame.fromDF(tedx_dataset_agg_2, glueContext, "nested")

glueContext.write_dynamic_frame.from_options(tedx_dataset_dynamic_frame, connection_type="mongodb", connection_options=write_mongo_options)