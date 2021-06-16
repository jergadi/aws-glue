
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

rel_list = df.select("id", explode(df.column_with_array).alias('column_name_exploded'))

df_sel = rel_list.select(encode(column_name_exploded.test_column1.cast("string"),"UTF-8").alias('relational_1') 
, encode(column_name_exploded.test_column2.cast("string"), "UTF-8").alias('relational_2')
, lit('this_is_my_source').alias('source_id')).withColumn("timestamp", date_format(current_timestamp(),"MM/dd/yyyy hh:mm:ss"))

dynamic_frame_write = DynamicFrame.fromDF(df_sel, glue_context, "dynamic_frame_write")

glue_context.write_dynamic_frame.from_options(
    frame = dynamic_frame_write,
    connection_type = "s3",
    connection_options = {
        "path": s3_write_path,
    },
    format = "parquet"
)
