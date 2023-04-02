import io
import boto3

class StorageHelper:
    """
    Amazon S3 Interface
    """

    def __init__(self, bucket_name):
        self.bucket = boto3.client('s3')
        self.bucket_name = bucket_name

    def upload_dataframe(self, df, name):
        csv_buffer = io.StringIO()        
        df.to_csv(csv_buffer, index=False)

        self.bucket.put_object(
            Bucket=self.bucket_name, 
            Key=name, 
            Body=csv_buffer.getvalue()
        )
