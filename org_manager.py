import json
import os
import uuid
import boto3  # Необходима установка: pip install boto3
import logging
from typing import Dict, List, Optional, Any
from botocore.exceptions import ClientError
from pathlib import Path
from storage import get_storage
from config_loader import load_config

# Настройка логирования
logger = logging.getLogger("OrganizationManager")

"""
API DESCRIPTION:
Система управления данными организаций (Organization Manager) с поддержкой S3.
Интерфейс обеспечивает работу с организациями как с объектами, скрывая детали сериализации.

ОСНОВНЫЕ МЕТОДЫ:
- list(): Получить все организации в виде списка объектов.
- find(inn="..."): Поиск по любому атрибуту (inn, partner_id, connection_id, name).
- save_local(org): Сохранение/обновление одной организации в локальный файл.
- sync_to_s3(bucket, key): Выгрузка всей базы (одним файлом) в S3 хранилище.

STRUCTURE OF DATABASE (DB):
- Тип: NoSQL File-based (JSON).
- Key-Value: Ключом является UUID (org_id).
- Схема данных:
    {
        "UUID": {
            "org_id": "str",
            "name": "str",
            "phone": "str",
            "person": "str",
            "inn": "str|None",
            "partner_id": "str|None",
            "connection_id": "str|None",
            "oms_id": "str|None"
        }
    }
"""

class Organization:
    def __init__(self, name, phone, person, inn=None, partner_id=None, connection_id=None, org_id=None, oms_id=None):
        self.org_id = org_id or str(uuid.uuid4())
        self.name = name
        self.phone = phone
        self.person = person
        self.inn = inn
        self.partner_id = partner_id
        self.connection_id = connection_id
        self.oms_id = oms_id

    def to_dict(self):
        return self.__dict__

class OrganizationManager:
    def __init__(self, storage_dir: str):
        self.storage_dir = storage_dir
        self.organizations: Dict[str, Organization] = {}
        
        if not os.path.exists(self.storage_dir):
            os.makedirs(self.storage_dir)
            
        self.config = load_config()
        self.s3_config = self.config.get('s3_config')
        self.orgs_path = self.config.get('orgs_path')

        if self.orgs_path and self.orgs_path.startswith('s3://'):
            self.storage = get_storage(self.orgs_path, self.s3_config)
            logger.info(f"Инициализирован S3 storage для организаций: {self.orgs_path}")
            self._sync_from_s3()
        else:
            self.storage = None
            logger.info(f"Используется локальное хранилище для организаций: {self.storage_dir}")

        self.sync_from_disk()

    def _sync_from_s3(self):
        if self.storage and self.orgs_path:
            try:
                logger.info(f"Синхронизация организаций из {self.orgs_path}...")
                remote_files = self.storage.list_files(self.orgs_path, "*.json")
                for remote_file in remote_files:
                    filename = os.path.basename(remote_file)
                    local_path = os.path.join(self.storage_dir, filename)
                    self.storage.download(remote_file, local_path)
                logger.info(f"Загружено {len(remote_files)} файлов организаций.")
            except Exception as e:
                logger.error(f"Ошибка синхронизации организаций из S3: {e}")

    def _sync_to_s3(self, local_path: str):
        if self.storage and self.orgs_path:
            try:
                filename = os.path.basename(local_path)
                remote_path = f"{self.orgs_path.rstrip('/')}/{filename}"
                logger.info(f"Выгрузка организации {filename} в {remote_path}...")
                self.storage.upload(local_path, remote_path)
            except Exception as e:
                logger.error(f"Ошибка выгрузки организации в S3: {e}")

    def sync_from_disk(self):
        """Публичный метод для синхронизации памяти с файлами на диске."""
        self.organizations.clear()
        if not os.path.exists(self.storage_dir):
            return

        for filename in os.listdir(self.storage_dir):
            if filename.endswith(".json"):
                path = os.path.join(self.storage_dir, filename)
                try:
                    with open(path, 'r', encoding='utf-8') as f:
                        data = json.load(f)
                        # Обработка если файл - это словарь нескольких организаций
                        if isinstance(data, dict) and not "org_id" in data:
                            for item in data.values():
                                self._add_to_mem(item)
                        else:
                            self._add_to_mem(data)
                except (json.JSONDecodeError, KeyError, Exception) as e:
                    logger.error(f"Ошибка чтения {filename}: {e}")
                    continue

    def _add_to_mem(self, data: dict):
        org = Organization(**data)
        self.organizations[org.org_id] = org

    def list(self) -> List[Organization]:
        """Возвращает список всех объектов организаций."""
        return list(self.organizations.values())

    def find(self, **kwargs) -> Optional[Organization]:
        """Поиск: manager.find(inn='7733154124')"""
        if not kwargs: return None
        attr, value = next(iter(kwargs.items()))
        for org in self.organizations.values():
            if getattr(org, attr, None) == value:
                return org
        return None

    def save_local(self, org: Organization):
        """Сохраняет конкретную организацию в отдельный файл."""
        self.organizations[org.org_id] = org
        path = os.path.join(self.storage_dir, f"{org.org_id}.json")
        with open(path, 'w', encoding='utf-8') as f:
            json.dump(org.to_dict(), f, ensure_ascii=False, indent=4)
        self._sync_to_s3(path)

    def sync_to_s3(self, bucket_name: str, s3_key: str, 
                   region: str = 'ru-central1',
                   endpoint_url: Optional[str] = None):
        """
        Сериализует ВСЮ базу в один JSON и отправляет в S3.
        endpoint_url используется, если у вас частное S3-совместимое хранилище (Selectel, VK и т.д.)
        """
        # Подготовка данных
        full_db = {uid: org.to_dict() for uid, org in self.organizations.items()}
        json_data = json.dumps(full_db, ensure_ascii=False, indent=4)

        try:
            s3_client = boto3.client('s3', region_name=region, endpoint_url=endpoint_url)
            s3_client.put_object(
                Bucket=bucket_name,
                Key=s3_key,
                Body=json_data,
                ContentType='application/json'
            )
            logger.info(f"База синхронизирована с S3: s3://{bucket_name}/{s3_key}")
        except ClientError as e:
            logger.error(f"Ошибка S3: {e}")
        except Exception as e:
            logger.error(f"Непредвиденная ошибка: {e}")

# --- ПРИМЕР ИСПОЛЬЗОВАНИЯ ---
if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    manager = OrganizationManager("./my_orgs")
    
    for org in manager.list():
        print(org.name)
