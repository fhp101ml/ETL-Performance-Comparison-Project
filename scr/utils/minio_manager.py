from minio import Minio


class MinioManager:
    def __init__(self, endpoint, access_key, secret_key, secure=True):
        self.client = Minio(endpoint, access_key=access_key, secret_key=secret_key, secure=secure)

    def upload_file(self, bucket_name, object_name, file_path):
        self.client.fput_object(bucket_name, object_name, file_path)
        print(f"Archivo {file_path} subido a {bucket_name}/{object_name}.")

    def download_file(self, bucket_name, object_name, file_path):
        self.client.fget_object(bucket_name, object_name, file_path)
        print(f"Archivo {bucket_name}/{object_name} descargado a {file_path}.")
