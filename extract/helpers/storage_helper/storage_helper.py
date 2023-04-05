import boto3

class StorageHelper:
    """
    Amazon S3 Interface
    """

    def __init__(self, bucket_name: str) -> None:
        self.bucket = boto3.client('s3')
        self.bucket_name = bucket_name

    def upload_to_cloud(self, file_name: str) -> None:
        self.bucket.upload_file(
            file_name,
            self.bucket_name, 
            file_name
        )

    @staticmethod
    def save_to_disk_parquet(df, file_name: str) -> None:
        df.to_parquet(
            path=file_name,
            engine="pyarrow",
            compression="snappy",
            index=True,
        )
