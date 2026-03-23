import os
import sys


sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from suz_api_models import EmissionOrder, OrderAttributes, OrderProduct

# Создаем атрибуты
attr = OrderAttributes(
    productionOrderId="c64b7eb5-4efa-42e8-9310-2c981dd44d03",
    createMethodType="SELF_MADE",
    releaseMethodType="PRODUCTION",
    paymentType=2,
    contactPerson="хТрек 2.5.11.6"
)

# Создаем товар
product = OrderProduct(
    gtin="04630234044646",
    quantity=515,
    serialNumberType="OPERATOR",
    templateId=47,
    cisType="UNIT"
)

# Итоговый заказ
order = EmissionOrder(
    productGroup="chemistry",
    attributes=attr,
    products=[product]
)

print(order.to_json())