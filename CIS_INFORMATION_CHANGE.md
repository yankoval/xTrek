# Корректировка дат кодов через True API

В `xtrek.create_emission_task_sample` реализована классическая тройка для
документа `CIS_INFORMATION_CHANGE`:

- создание задачи;
- подпись и отправка;
- получение статуса.

Один документ изменяет только один вид сведений: либо `productionDate`, либо
`expirationDate`. Для изменения обеих дат создаются две задачи и отправляются
два отдельных документа.

## Конфигурация storage

```json
{
  "cis-information-change-tasks": "s3://bucket/cisInformationChangeTasks",
  "cis-information-change-receipts": "s3://bucket/cisInformationChangeReceipts",
  "cis-information-changes": "s3://bucket/cisInformationChanges"
}
```

Поддерживаются и snake_case-имена этих трёх параметров.

## Базовая команда с универсальным источником

`SOURCE` может быть полным `s3://` URI или путём локального файла. Чтение
выполняется через `get_storage`. Поддерживаются:

- TXT: один полный код на строку;
- JSON-массив строк;
- JSON-объект с массивом `codes` или `sntins`;
- отчёт оборудования v1 с `readyBox[].productNumbersFull`;
- отчёт оборудования v2 с `readyPallet[].readyBox[].productNumbersFull`.

Изменение даты производства:

```bash
python -m xtrek.create_emission_task_sample \
  --create-cis-information-change s3://bucket/path/codes.json \
  --inn 7701234567 \
  --production-date 2026-08-01 \
  --group chemistry
```

Изменение даты окончания срока годности отдельным документом:

```bash
python -m xtrek.create_emission_task_sample \
  --create-cis-information-change /data/codes.txt \
  --inn 7701234567 \
  --expiration-date 2027-08-01 \
  --group chemistry
```

ID задачи формируется из имени источника и вида даты. Его можно переопределить
через `--cis-task-id`.

## Обёртки

По ID отчёта оборудования v1/v2 (путь строится относительно
`equipment-reports`, а коды извлекаются каноническим pallet-адаптером):

```bash
python -m xtrek.create_emission_task_sample \
  --create-cis-information-change-from-report REPORT_ID \
  --inn 7701234567 \
  --production-date 2026-08-01
```

По ID производственного задания из эмиссии:

```bash
python -m xtrek.create_emission_task_sample \
  --create-cis-information-change-from-emission PRODUCTION_ORDER_ID \
  --expiration-date 2027-08-01
```

Обёртка эмиссии получает `orderId` из `emission_receipts`, читает уже
выгруженный объект `kodes/<orderId>.json`, а при его отсутствии вызывает
существующий `get_emission_kodes`. ИНН берётся из
`PasportData.Manufacturer_inn`; его можно переопределить через `--inn`.

## Подпись, отправка и статус

```bash
python -m xtrek.create_emission_task_sample \
  --send-cis-information-change TASK_ID \
  --group chemistry

python -m xtrek.create_emission_task_sample \
  --cis-information-change-status TASK_ID \
  --group chemistry
```

На отправку формируется оболочка `MANUAL` с типом
`CIS_INFORMATION_CHANGE`. Повторная отправка защищена тем же атомарным lock и
чеком, что и другие True API-документы библиотеки.

## Ограничения True API, учтённые локально

- коды внутри документа уникальны;
- не более 35 000 кодов;
- `participantInn` содержит 10 или 12 цифр;
- дата производства: от текущей даты минус 20 лет до текущей даты плюс 1 день;
- срок годности: от 01.01.1999 до 31.12.2099;
- смешивание `productionDate` и `expirationDate` в одном документе запрещено.

Дополнительные серверные ограничения (товарная группа, тип упаковки и статус
кода) проверяются True API. Согласно официальной документации дата
производства применима к `UNIT`/`BUNDLE` в статусе `APPLIED`/`INTRODUCED`, а
дата окончания срока годности — к допустимым товарным группам и `UNIT` в
статусе `APPLIED`/`INTRODUCED`.

Официальные источники:

- https://docs.crpt.ru/gismt/True_API/ — раздел 4.1.17;
- https://docs.crpt.ru/gismt/Корректировка_сведений_о_кодах/.
