import json
from dataclasses import dataclass, asdict, field
from typing import List, Optional, Dict, Any

# --- Вспомогательный базовый класс ---
class SUZBase:
    def to_dict(self):
        # Исключаем None значения, чтобы JSON был чистым
        return {k: v for k, v in asdict(self).items() if v is not None}
    
    def to_json(self):
        return json.dumps(self.to_dict(), ensure_ascii=False, separators=(',', ':'))

# --- Блок Эмиссии (Заказ) ---

@dataclass
class OrderAttributes(SUZBase):
    productionOrderId: str
    createMethodType: str
    releaseMethodType: str
    paymentType: int
    contactPerson: str

@dataclass
class OrderProduct(SUZBase):
    gtin: str
    quantity: int
    serialNumberType: str
    templateId: int
    cisType: str
    attributes: Dict[str, Any] = field(default_factory=dict)

@dataclass
class EmissionOrder(SUZBase):
    productGroup: str
    attributes: OrderAttributes
    products: List[OrderProduct]

@dataclass
class EmissionOrderreceipts(SUZBase):
    orderId: str
    expectedCompleteTimestamp: int
    omsId: str

# --- Блок получения кодов ---

@dataclass
class CodesBlock(SUZBase):
    codes: List[str]
    omsId: Optional[str] = None
    orderId: Optional[str] = None
    blockId: Optional[str] = None

# --- Блок Отчетов (Нанесение и Ввод в оборот) ---

@dataclass
class UtilisationReport(SUZBase):
    """Отчет о нанесении"""
    participantId: str
    productGroup: str
    usageType: str  # 'VERIFIED'
    sntins: List[str]
    productionDate: Optional[str] = None
    expirationDate: Optional[str] = None

@dataclass
class IntroductionReport(SUZBase):
    """Сообщение о вводе в оборот"""
    participantId: str
    productGroup: str
    sntins: List[str]
    productionDate: str
    # Для разных ТГ могут добавляться доп. поля через dict

# --- Блок Агрегации ---

@dataclass
class AggregationUnit(SUZBase):
    unitSerialNumber: str
    aggregationType: str = "AGGREGATION"
    sntins: List[str] = field(default_factory=list) # Для товаров
    unitSerialNumberList: List[str] = field(default_factory=list) # Для вложенных коробов

@dataclass
class AggregationReport(SUZBase):
    participantId: str
    productGroup: str
    aggregationUnits: List[AggregationUnit]
