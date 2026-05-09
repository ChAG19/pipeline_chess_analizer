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

    def download(self, file_path):
        file = self.hook.get_key(
            bucket_name=self.bucket,
            key=file_path
        )
        return file
    
    # Проверить существование файла
    def check_file(self, file_path):
        return self.hook.check_for_key(file_path, self.bucket)
    
    # Функция для получения списка файлов
    def get_files_list(self, file_path="") -> str:
        return self.hook.list_keys(self.bucket, prefix=file_path)
