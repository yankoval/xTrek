import json
from suz_api_models import EmissionOrder, OrderAttributes, OrderProduct

def test_emission_order_json_serialization():
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

    # Ожидаемая структура
    expected_dict = {
      "attributes": {
        "productionOrderId": "c64b7eb5-4efa-42e8-9310-2c981dd44d03",
        "createMethodType": "SELF_MADE",
        "releaseMethodType": "PRODUCTION",
        "paymentType": 2,
        "contactPerson": "хТрек 2.5.11.6"
      },
      "productGroup": "chemistry",
      "products": [
        {
          "cisType": "UNIT",
          "templateId": 47,
          "serialNumberType": "OPERATOR",
          "quantity": 515,
          "gtin": "04630234044646",
          "attributes": {}
        }
      ]
    }

    # Сравнение структур
    assert order.to_dict() == expected_dict

def test_emission_order_to_json():
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

    json_output = order.to_json()
    data = json.loads(json_output)

    expected_dict = {
      "attributes": {
        "productionOrderId": "c64b7eb5-4efa-42e8-9310-2c981dd44d03",
        "createMethodType": "SELF_MADE",
        "releaseMethodType": "PRODUCTION",
        "paymentType": 2,
        "contactPerson": "хТрек 2.5.11.6"
      },
      "productGroup": "chemistry",
      "products": [
        {
          "cisType": "UNIT",
          "templateId": 47,
          "serialNumberType": "OPERATOR",
          "quantity": 515,
          "gtin": "04630234044646",
          "attributes": {}
        }
      ]
    }

    assert data == expected_dict
