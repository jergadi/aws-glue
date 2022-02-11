import pyspark
import boto3
from awsglue.context import GlueContext

def main():

   sc = pyspark.SparkContext.getOrCreate()
   glue_context = GlueContext(sc)
   spark = glue_context.spark_session

   sc._jsc.hadoopConfiguration().set("mapred.output.committer.class", "org.apache.hadoop.mapred.FileOutputCommitter")

   sts_connection = boto3.client('sts')
   assume_acct = sts_connection.assume_role(RoleArn="your-assumed-role", RoleSessionName="this-is-a-session-name")

   ACCESS_KEY = assume_acct['Credentials']['AccessKeyId']
   SECRET_KEY = assume_acct['Credentials']['SecretAccessKey']
   SESSION_TOKEN = assume_acct['Credentials']['SessionToken']


   # create service client using the assumed role credentials, e.g. S3
   client = boto3.client(
      's3',
      aws_access_key_id=ACCESS_KEY,
      aws_secret_access_key=SECRET_KEY,
      aws_session_token=SESSION_TOKEN
      )

   def get_files(bucket, prefix):
      result = []
      data = client.list_objects_v2(Bucket=bucket, Prefix=prefix)
      for obj in data.get('Contents'):
         x = obj.get('Key')
         if '.parquet' in x and x not in result:
            result.append(x)
      return result


   def write_data(files, from_bucket, to_bucket):
      for file in files:
         df = spark.read.parquet(f"s3://{from_bucket}/{file}")
         o_file = "s3://" + to_bucket + '/' + file
         #df.write.parquet(o_file)
         print(df)


   bucket='bucket-to-assume'
   prefix = 'prefix-to-assume'

   files = get_files(bucket, prefix)

   write_data(files, bucket, 'bucket to write')


if __name__ == "__main__":
    main()
