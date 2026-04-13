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

# --- Новые модели для работы со статусами и производственными заказами ---

@dataclass
class PasportData(SUZBase):
    Format: str
    LabelLanguage: str
    Manufacturer_inn: str
    Manufacturer_name: str
    Manufacturer_address: str
    Manufacturer_phone: str
    Product_id: str
    Product_article: str
    Product_gtin: str
    Product_ShowArticle: str
    Product_name_part1: str
    Product_name_part2: str
    Product_name_part3: str
    Product_gost: str
    Product_PackInfo: str
    Product_PackQty: str
    Product_PackBarcode: str
    Product_PackIcons1: str
    Product_PackIcons2: str
    Product_ClientBarcode: str
    Batch_id: str
    Batch_number: str
    Batch_BN_1С: str
    Batch_BN_1С_full: str
    Batch_date_production: str
    Batch_date_packing: str
    Batch_date_expired: str
    Batch_date_packing_descr: str
    Batch_date_expired_descr: str
    client_AdditionalInfo: str

@dataclass
class ProductionOrder(SUZBase):
    Article: str
    Gtin: str
    Quantity: str
    PasportData: PasportData

@dataclass
class EmissionOrderStatus(SUZBase):
    omsId: str
    orderId: str
    gtin: str
    bufferStatus: str
    leftInBuffer: int
    totalCodes: int
    unavailableCodes: int
    availableCodes: int
    totalPassed: int
    poolsExhausted: bool
    templateId: Optional[int] = None
