# Import python modules
from datetime import datetime

# Import pyspark modules
from pyspark.context import SparkContext
from pyspark.sql.functions import *
from pyspark.sql.window import Window

# Import glue modules
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.dynamicframe import DynamicFrame
from awsglue.job import Job

# Initialize contexts and session
spark_context = SparkContext.getOrCreate()
glue_context = GlueContext(spark_context)
session = glue_context.spark_session

# Parameters
s3_write_path = "s3://kgalife/member"
db = "kgalife"
table = "postgres_landing_member_list"

dymc_frame = glue_context.create_dynamic_frame.from_catalog(database=db, table_name=table)

df = dymc_frame.toDF()

df_sel = df.select( "first_name"
                    , "last_name"
                    , "id_number"
                    , "gender"
                    , to_date(from_unixtime(unix_timestamp("birth_date", "MM/dd/yyyy"), "dd-MM-yyyy"), "dd-MM-yyyy").alias("birth_date")
                    , "title"
                    , hex(sha1(concat(encode(df.id_number.cast("string"),"UTF-8"), encode(df.first_name.cast("string"),"UTF-8"), encode(df.last_name.cast("string"),"UTF-8")))).alias('hash_key')
                    , hex(sha1(concat(encode(df.id_number.cast("string"),"UTF-8"), encode(df.first_name.cast("string"),"UTF-8"), encode(df.last_name.cast("string"),"UTF-8"), encode(df.gender.cast("string"),"UTF-8"), encode(df.birth_date.cast("string"),"UTF-8"), encode(df.title.cast("string"),"UTF-8")))).alias('hash_diff')
                    , lit('landing_member_list').alias("source_table")
                   ).withColumn("load_dts", to_timestamp(date_format(current_timestamp(),"dd-MM-yyyy hh:mm:ss"), "dd-MM-yyyy hh:mm:ss"))

dynamic_frame_write = DynamicFrame.fromDF(df_sel, glue_context, "dynamic_frame_write")

datasink5 = glue_context.write_dynamic_frame.from_catalog(frame = dynamic_frame_write, database = "kgalife", table_name = "postgres_staging_member", transformation_ctx = "datasink5")
