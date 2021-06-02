# Import di default
import sys
import json
import pyspark
from pyspark.sql.functions import col, collect_list, array_join

from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

# Inizializzazione del Job Glue, codice di default
# READ PARAMETERS
args = getResolvedOptions(sys.argv, ['JOB_NAME'])

# START JOB CONTEXT AND JOB
sc = SparkContext()

glueContext = GlueContext(sc)
spark = glueContext.spark_session

job = Job(glueContext)
job.init(args['JOB_NAME'], args)
# Fine inizializzazione del Job Glue

# Path del file all'interno del bucket S3
international_days_path = "s3://tedx-app-data/international_days.csv"

# Leggo il file delle giornate internazionali
international_days_dataset = spark.read.option("header","true").csv(international_days_path)

# Count items
count_items = international_days_dataset.count()

# Stampo schema
international_days_dataset.printSchema()
print(f"Number of items from RAW DATA {count_items}")

# Connessione al database
mongo_uri = "mongodb://cluster0-shard-00-00.61qus.mongodb.net:27017,cluster0-shard-00-01.61qus.mongodb.net:27017,cluster0-shard-00-02.61qus.mongodb.net:27017"

write_mongo_options = {
    "uri": mongo_uri,
    "database": "unibg_tedx_2021",
    "collection": "international_days_data",
    "username": "tedx-db-user",
    "password": "vGpz1Ya15Bu1QiJH",
    "ssl": "true",
    "ssl.domain_match": "false"}
from awsglue.dynamicframe import DynamicFrame
international_days_dataset_dynamic_frame = DynamicFrame.fromDF(international_days_dataset, glueContext, "nested")

glueContext.write_dynamic_frame.from_options(international_days_dataset_dynamic_frame, connection_type="mongodb", connection_options=write_mongo_options)
