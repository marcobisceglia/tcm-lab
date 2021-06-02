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

# Path dei file all'interno del bucket S3
tedx_dataset_path = "s3://tedx-app-data/tedx_dataset.csv"
tags_dataset_path = "s3://tedx-app-data/tags_dataset.csv"
watch_next_dataset_path = "s3://tedx-app-data/watch_next_dataset.csv"

# Leggo il file dei tedx
tedx_dataset = spark.read \
    .option("header","true") \
    .option("quote", "\"") \
    .option("escape", "\"") \
    .csv(tedx_dataset_path)
    
# Stampo
tedx_dataset.printSchema()

# Filtro gli elementi con NULL POSTING KEY
# Filter = Where in SQL
count_items = tedx_dataset.count()
count_items_null = tedx_dataset.filter("idx is not null").count()

# Stampo nel log alcune informazioni
print(f"Number of items from RAW DATA {count_items}")
print(f"Number of items from RAW DATA with NOT NULL KEY {count_items_null}")

# Leggo il file tags
tags_dataset = spark.read.option("header","true").csv(tags_dataset_path)

# Leggo il file watch_next
watch_next_dataset = spark.read.option("header","true").csv(watch_next_dataset_path)
count_items = watch_next_dataset.count()

# Rimuovo i duplicati con distinct e i link errati
watch_next_dataset = watch_next_dataset.distinct().where('url != "https://www.ted.com/session/new?context=ted.www%2Fwatch-later"')
count_items_without_duplicates = watch_next_dataset.count()

# Stampo nel log info per rimozione duplicati
print(f"Number of items from RAW DATA {count_items}")
print(f"Number of items from RAW DATA without duplicates and wrong links {count_items_without_duplicates}")

# Raggruppo i due dataset, aggiungendo i watch next nel dataset
watch_next_dataset_agg = watch_next_dataset.groupBy(col("idx").alias("idx_ref")).agg(collect_list("url").alias("watch_next"))
watch_next_dataset_agg.printSchema()
watch_next_dataset_agg = tedx_dataset.join(watch_next_dataset_agg, tedx_dataset.idx == watch_next_dataset_agg.idx_ref, "left") \
    .drop("idx_ref") \
    .select(col("idx").alias("_id"), col("*")) \
    .drop("idx") \

# Stampo
watch_next_dataset_agg.printSchema()

# Raggruppo i due dataset, aggiungendo i tags nel dataset
tags_dataset_agg = tags_dataset.groupBy(col("idx").alias("idx_ref")).agg(collect_list("tag").alias("tags"))
tags_dataset_agg.printSchema()
tedx_dataset_agg = watch_next_dataset_agg.join(tags_dataset_agg, watch_next_dataset_agg._id == tags_dataset_agg.idx_ref, "left") \
    .drop("idx_ref") \

# Stampo
tedx_dataset_agg.printSchema()

# Connessione al database
mongo_uri = "mongodb://cluster0-shard-00-00.61qus.mongodb.net:27017,cluster0-shard-00-01.61qus.mongodb.net:27017,cluster0-shard-00-02.61qus.mongodb.net:27017"

write_mongo_options = {
    "uri": mongo_uri,
    "database": "unibg_tedx_2021",
    "collection": "tedx_data",
    "username": "tedx-db-user",
    "password": "vGpz1Ya15Bu1QiJH",
    "ssl": "true",
    "ssl.domain_match": "false"}
from awsglue.dynamicframe import DynamicFrame
tedx_dataset_dynamic_frame = DynamicFrame.fromDF(tedx_dataset_agg, glueContext, "nested")

glueContext.write_dynamic_frame.from_options(tedx_dataset_dynamic_frame, connection_type="mongodb", connection_options=write_mongo_options)
