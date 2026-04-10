import json
import os
import uuid
import boto3  # Необходима установка: pip install boto3
from typing import Dict, List, Optional
from botocore.exceptions import ClientError

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
            "connection_id": "str|None"
        }
    }
"""

class Organization:
    def __init__(self, name, phone, person, inn=None, partner_id=None, connection_id=None, org_id=None):
        self.org_id = org_id or str(uuid.uuid4())
        self.name = name
        self.phone = phone
        self.person = person
        self.inn = inn
        self.partner_id = partner_id
        self.connection_id = connection_id

    def to_dict(self):
        return self.__dict__

class OrganizationManager:
    def __init__(self, storage_dir: str):
        self.storage_dir = storage_dir
        self.organizations: Dict[str, Organization] = {}
        
        if not os.path.exists(self.storage_dir):
            os.makedirs(self.storage_dir)
            
        self.sync_from_disk() # Теперь вызываем этот метод

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
                    print(f"[!] Ошибка чтения {filename}: {e}")
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

    def sync_to_s3(self, bucket_name: str, s3_key: str, 
                   region: str = 'us-east-1', 
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
            print(f"[SUCCESS] База синхронизирована с S3: s3://{bucket_name}/{s3_key}")
        except ClientError as e:
            print(f"[ERROR] Ошибка S3: {e}")
        except Exception as e:
            print(f"[ERROR] Непредвиденная ошибка: {e}")

# --- ПРИМЕР ИСПОЛЬЗОВАНИЯ ---
if __name__ == "__main__":
    manager = OrganizationManager("./my_orgs")
    
    # Добавим Елену Александровну из лога, если её ещё нет
    # if not manager.find(inn="9718180660"):
        # lesnyak = Organization(
            # name="Лесняк Елена Александровна",
            # phone="+70000000000",
            # person="Лесняк Е.А.",
            # inn="9718180660",
            # partner_id="11003862499",
            # connection_id="14000943012"
        # )
        # manager.save_local(lesnyak)
    for org in manager.list():
        print(org.name)

    # Синхронизация с облаком (пример для Selectel или AWS)
    # manager.sync_to_s3(bucket_name='my-project-data', s3_key='backups/orgs_db.json')  