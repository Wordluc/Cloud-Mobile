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
#Query for RangeCustomers

tags=["Extreme sports","food", "gaming", "humor","internet", "live music", "marketing", "meme", "music","news", "shopping", "sports","students","Tedyouth","Television","Web", "Youth"]

# Filter the id_tags DataFrame to only include rows with the "life" tag
id_tags = tedx_tag.filter(col("tag").isin(tags)).select(col("idx")).distinct()


# Join the tedx_dataset and id_tags DataFrames on the "idx" column, and filter to include only rows with the "life" tag
tedx_dataset_agg = tedx_dataset.join(
    id_tags,
    ["idx"],
    "inner"
).select(
    col("idx").alias("_id"),
    col("*")
).drop("idx","tag")


mongo_uri = "mongodb+srv://cloud-db:giulia@cluster0.dwvyxyf.mongodb.net"
print(mongo_uri)

write_mongo_options = {
    "uri": mongo_uri,
    "database": "cloud-db",
    "collection": "ted_in_fila_afternoon",
    "ssl": "true",
    "ssl.domain_match": "false",
    "deleteBeforeInsert": "true"
}
from awsglue.dynamicframe import DynamicFrame
tedx_dataset_dynamic_frame = DynamicFrame.fromDF(tedx_dataset_agg, glueContext, "nested")

glueContext.write_dynamic_frame.from_options(tedx_dataset_dynamic_frame, connection_type="mongodb", connection_options=write_mongo_options)