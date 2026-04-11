from airflow.providers.amazon.aws.hooks.s3 import S3Hook

class MinioLoader:
    def __init__(self, minio_conn, bucket_name) -> None:
        self.hook = S3Hook(aws_conn_id=minio_conn)
        self.bucket = bucket_name

    def upload(self, file, file_path, replace=True):
        self.hook.load_string(
            bucket_name=self.bucket,
            replace=replace,
            string_data=file,
            key=file_path
        )