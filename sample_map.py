import sys
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from awsglue.transforms import *


args = getResolvedOptions(sys.argv, ["JOB_NAME"])
glue_context= GlueContext(SparkContext.getOrCreate())
job = Job(glue_context)
job.init(args["JOB_NAME"], args)

input_folder = "s3://test-bucket/test-file-folder/"
"""
Sample file layout
{
  "this_id": 123456,
  "channel": 4,
  "my_band": "maestro"
}
"""

def process_channel(rec):
    rec["channel"] = rec["channel"] + 10
    return rec 


inputGDF = glue_context.create_dynamic_frame_from_options(connection_type = "s3", connection_options = {"paths": [input_folder]}, format = "parquet")

mapped_dyF =  Map.apply(frame = inputGDF, f = process_channel)

mapped_dyF.show()

"""
Sample output 
{
  "this_id": 123456,
  "channel": 14,
  "my_band": "maestro"
}
"""
