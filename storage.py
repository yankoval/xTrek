import os
import boto3
import fnmatch
import logging
from pathlib import Path
from urllib.parse import urlparse
import shutil

logger = logging.getLogger(__name__)

class BaseStorage:
    def list_files(self, path, pattern):
        pass
    def download(self, remote_path, local_path):
        pass
    def upload(self, local_path, remote_path):
        pass
    def mark_processing(self, path):
        pass
    def mark_finished(self, path, delete_source=False):
        pass
    def mark_error(self, path):
        pass
    def exists(self, path):
        pass
    def read_text(self, path):
        pass

class LocalStorage(BaseStorage):
    def list_files(self, path, pattern):
        p = Path(path)
        if not p.exists():
            return []
        return [str(f) for f in p.glob(pattern) if f.is_file() and f.suffix == '.json']

    def download(self, remote_path, local_path):
        if str(remote_path) != str(local_path):
            shutil.copy2(remote_path, local_path)
        return local_path

    def upload(self, local_path, remote_path):
        if str(local_path) != str(remote_path):
            Path(remote_path).parent.mkdir(parents=True, exist_ok=True)
            shutil.copy2(local_path, remote_path)

    def mark_processing(self, path):
        p = Path(path)
        processing_path = p.with_suffix('.processing')
        p.rename(processing_path)
        return str(processing_path)

    def mark_finished(self, path, delete_source=False):
        p = Path(path)
        if delete_source:
            p.unlink()
            return None
        else:
            finished_path = p.with_suffix('.finished')
            if finished_path.exists():
                finished_path.unlink()
            p.rename(finished_path)
            return str(finished_path)

    def mark_error(self, path):
        p = Path(path)
        error_path = p.with_suffix('.error')
        if error_path.exists():
            error_path.unlink()
        p.rename(error_path)
        return str(error_path)

    def exists(self, path):
        return Path(path).exists()

    def read_text(self, path):
        with open(path, 'r', encoding='utf-8') as f:
            return f.read()

class S3Storage(BaseStorage):
    def __init__(self, s3_config):
        self.s3 = boto3.client(
            's3',
            endpoint_url=s3_config.get('endpoint_url', 'https://storage.yandexcloud.net'),
            aws_access_key_id=s3_config.get('aws_access_key_id'),
            aws_secret_access_key=s3_config.get('aws_secret_access_key'),
            region_name=s3_config.get('region_name', 'ru-central1')
        )

    def _parse_s3_url(self, url):
        parsed = urlparse(str(url))
        bucket = parsed.netloc
        key = parsed.path.lstrip('/')
        return bucket, key

    def list_files(self, path, pattern):
        bucket, prefix = self._parse_s3_url(path)
        if prefix and not prefix.endswith('/'):
            prefix += '/'

        files = []
        try:
            paginator = self.s3.get_paginator('list_objects_v2')
            for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
                if 'Contents' in page:
                    for obj in page['Contents']:
                        key = obj['Key']
                        # Фильтруем по паттерну (только имя файла)
                        filename = os.path.basename(key)
                        if fnmatch.fnmatch(filename, pattern):
                            # Проверяем теги
                            if not self._is_processed(bucket, key):
                                files.append(f"s3://{bucket}/{key}")
        except Exception as e:
            logger.error(f"Error listing S3 files: {e}")
        return files

    def _is_processed(self, bucket, key):
        try:
            response = self.s3.get_object_tagging(Bucket=bucket, Key=key)
            tags = {t['Key']: t['Value'] for t in response.get('TagSet', [])}
            return 'status' in tags and tags['status'] in ['processing', 'finished', 'error']
        except Exception:
            return False

    def download(self, remote_path, local_path):
        bucket, key = self._parse_s3_url(remote_path)
        Path(local_path).parent.mkdir(parents=True, exist_ok=True)
        self.s3.download_file(bucket, key, str(local_path))
        return local_path

    def upload(self, local_path, remote_path):
        bucket, key = self._parse_s3_url(remote_path)
        self.s3.upload_file(str(local_path), bucket, key)

    def mark_processing(self, path):
        bucket, key = self._parse_s3_url(path)
        self.s3.put_object_tagging(
            Bucket=bucket,
            Key=key,
            Tagging={'TagSet': [{'Key': 'status', 'Value': 'processing'}]}
        )
        return path

    def mark_finished(self, path, delete_source=False):
        bucket, key = self._parse_s3_url(path)
        if delete_source:
            self.s3.delete_object(Bucket=bucket, Key=key)
            return None
        else:
            self.s3.put_object_tagging(
                Bucket=bucket,
                Key=key,
                Tagging={'TagSet': [{'Key': 'status', 'Value': 'finished'}]}
            )
            return path

    def mark_error(self, path):
        bucket, key = self._parse_s3_url(path)
        self.s3.put_object_tagging(
            Bucket=bucket,
            Key=key,
            Tagging={'TagSet': [{'Key': 'status', 'Value': 'error'}]}
        )
        return path

    def exists(self, path):
        bucket, key = self._parse_s3_url(path)
        try:
            self.s3.head_object(Bucket=bucket, Key=key)
            return True
        except Exception:
            return False

    def read_text(self, path):
        bucket, key = self._parse_s3_url(path)
        response = self.s3.get_object(Bucket=bucket, Key=key)
        return response['Body'].read().decode('utf-8')

def get_storage(path, s3_config=None):
    if str(path).startswith('s3://'):
        return S3Storage(s3_config or {})
    return LocalStorage()
