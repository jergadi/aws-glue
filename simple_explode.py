
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

rel_list = df.select("id", "bu_id", explode(df.column_with_array).alias('column_name_exploded'))

df_sel = rel_list.select(hex(sha1(concat(encode(rel_list.id.cast("string"),"UTF-8"), encode(rel_list.bp_person_rel_list.trg_obj_id.value.cast("string"),"UTF-8")))).alias('LNK_BP_PERSON_HK') 
, hex(sha1(encode(rel_list.id.cast("string"), "UTF-8"))).alias('HUB_BP_HK')
, hex(sha1(encode(rel_list.bp_person_rel_list.trg_obj_id.value.cast("string"), "UTF-8"))).alias('HUB_PERSON_HK')
, lit('ACP').alias('LNK_REC_SRC')
, rel_list.bu_id.value.alias('bu_id')).withColumn("LOAD_DTS", date_format(current_timestamp(),"MM/dd/yyyy hh:mm:ss"))

dynamic_frame_write = DynamicFrame.fromDF(df_sel, glue_context, "dynamic_frame_write")

glue_context.write_dynamic_frame.from_options(
    frame = dynamic_frame_write,
    connection_type = "s3",
    connection_options = {
        "path": s3_write_path,
    },
    format = "parquet"
)