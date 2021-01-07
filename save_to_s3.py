import boto3
from io import StringIO

def save_s3(bucket_name, df, file_name, cols):
    bucket = bucket_name # already created on S3
    df = df[[cols]]
    csv_buffer = StringIO()
    df.to_csv(csv_buffer)
    s3_resource = boto3.resource('s3', aws_access_key_id="AKIATWECFJWIOSY5FM53",
                                        aws_secret_access_key= "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")
    s3_resource.Object(bucket, '{}.csv'.format(file_name)).put(Body=csv_buffer.getvalue())