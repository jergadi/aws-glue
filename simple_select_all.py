
#Import python modules
from datetime import datetime

#Import pyspark modules
from pyspark.context import SparkContext
from pyspark.sql.functions import *
from pyspark.sql.window import Window

#Import glue modules
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.dynamicframe import DynamicFrame
from awsglue.job import Job

#Initialize contexts and session
spark_context = SparkContext.getOrCreate()
glue_context = GlueContext(spark_context)
session = glue_context.spark_session

#Parameters
s3_write_path = "s3://testbucket/test-file"
db = "testdb"
table = "testtable"

dymc_frame = glue_context.create_dynamic_frame.from_catalog(database = db, table_name = table)

df = dymc_frame.toDF()

df_sel = df.select('*').withColumn("LOAD_DTS", current_timestamp())
        
dynamic_frame_write = DynamicFrame.fromDF(df_sel, glue_context, "dynamic_frame_write")

glue_context.write_dynamic_frame.from_options(
    frame = dynamic_frame_write,
    connection_type = "s3",
    connection_options = {
        "path": s3_write_path,
    },
    format = "parquet"
)