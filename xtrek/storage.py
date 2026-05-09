import os
import json
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
    def set_tags(self, path, tags):
        pass
    def get_tags(self, path):
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

    def _rename_with_tags(self, old_path, new_path):
        old_p = Path(old_path)
        new_p = Path(new_path)
        old_p.rename(new_p)

        old_tags = old_p.parent / (old_p.name + '.tags')
        new_tags = new_p.parent / (new_p.name + '.tags')
        if old_tags.exists():
            if new_tags.exists():
                new_tags.unlink()
            old_tags.rename(new_tags)

    def mark_processing(self, path):
        p = Path(path)
        processing_path = p.with_suffix('.processing')
        self._rename_with_tags(path, processing_path)
        return str(processing_path)

    def mark_finished(self, path, delete_source=False):
        p = Path(path)
        if delete_source:
            tags_path = p.parent / (p.name + '.tags')
            if tags_path.exists():
                tags_path.unlink()
            p.unlink()
            return None
        else:
            finished_path = p.with_suffix('.finished')
            self._rename_with_tags(path, finished_path)
            return str(finished_path)

    def mark_error(self, path):
        p = Path(path)
        error_path = p.with_suffix('.error')
        self._rename_with_tags(path, error_path)
        return str(error_path)

    def exists(self, path):
        return Path(path).exists()

    def read_text(self, path):
        with open(path, 'r', encoding='utf-8') as f:
            return f.read()

    def set_tags(self, path, tags):
        p = Path(path)
        tags_path = p.parent / (p.name + '.tags')
        current_tags = self.get_tags(path)
        current_tags.update(tags)
        with open(tags_path, 'w', encoding='utf-8') as f:
            json.dump(current_tags, f, ensure_ascii=False, indent=2)

        if 'bufferStatus' in tags:
            new_path = p.with_suffix('.' + tags['bufferStatus'])
            if p.exists():
                self._rename_with_tags(path, new_path)
                return str(new_path)
        return path

    def get_tags(self, path):
        p = Path(path)
        tags = {}
        # Имитация тегов по расширению для обратной совместимости
        if p.suffix == '.processing':
            tags['status'] = 'processing'
        elif p.suffix == '.finished':
            tags['status'] = 'finished'
        elif p.suffix == '.error':
            tags['status'] = 'error'

        tags_path = p.parent / (p.name + '.tags')
        if tags_path.exists():
            try:
                with open(tags_path, 'r', encoding='utf-8') as f:
                    file_tags = json.load(f)
                    tags.update(file_tags)
            except Exception as e:
                logger.error(f"Error reading tags file {tags_path}: {e}")
        return tags

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

    def set_tags(self, path, tags):
        bucket, key = self._parse_s3_url(path)
        # Получаем текущие теги, чтобы не перезаписать их полностью, а обновить
        current_tags = self.get_tags(path)
        current_tags.update(tags)

        tag_set = [{'Key': k, 'Value': str(v)} for k, v in current_tags.items()]
        try:
            self.s3.put_object_tagging(
                Bucket=bucket,
                Key=key,
                Tagging={'TagSet': tag_set}
            )
        except Exception as e:
            logger.error(f"Error setting S3 tags: {e}")
        return path

    def get_tags(self, path):
        bucket, key = self._parse_s3_url(path)
        try:
            response = self.s3.get_object_tagging(Bucket=bucket, Key=key)
            return {t['Key']: t['Value'] for t in response.get('TagSet', [])}
        except Exception as e:
            logger.error(f"Error getting S3 tags: {e}")
            return {}

def get_storage(path, s3_config=None):
    if str(path).startswith('s3://'):
        return S3Storage(s3_config or {})
    return LocalStorage()
