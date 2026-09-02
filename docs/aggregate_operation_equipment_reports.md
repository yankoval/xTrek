# Отчёты оборудования для операций с агрегатами

Имя входного файла без расширения является `taskId`. Рекомендуемый формат имени:
`T-{SSCC/КИЗ/None}-{uuid}.json`.

## Расформирование

```json
{
  "participant_inn": "7701234567",
  "products_list": [
    {"uitu": "000123456789012345"}
  ]
}
```

Обёртка `create_disaggregation_task_from_report()` принимает локальный или S3-путь,
нормализует 18-значный SSCC в представление True API с AI `00` и создаёт файл
задачи с тем же `taskId`.

## Изъятие без расформирования

```json
{
  "participant_inn": "7701234567",
  "reaggregation_type": "REMOVING",
  "uitu": "000123456789012345",
  "uit_uitu_list": [
    {"uit_uitu": "010460000000000021ABC"}
  ]
}
```

Для изъятия вложенного КИТУ вместо `uit_uitu` используется `kitu`. Смешивать эти
поля в одном отчёте нельзя. Обёртка
`create_reaggregation_removing_task_from_report()` нормализует КИТУ и удаляет
криптохвост у `uit_uitu`.

## Проверка и тег `check`

Функции `check_disaggregation_report()` и
`check_reaggregation_removing_report()` в `xtrek.utils` используют:

- `/api/v3/true-api/cises/info` — статус, тип упаковки, владельца и родителя;
- `/api/v3/true-api/cises/aggregated/list` — актуальный первый уровень вложений.

Семантика тега совпадает с существующими отчётами оборудования:

- начальная проверка успешна — `check=""`;
- операция подтверждена состоянием кодов — `check=finished`;
- ошибка структуры или состояния кодов — в `check` записываются отсортированные
  ключи ошибок через `-`;
- ошибка True API возвращается как `api_error`, но тег не меняется, чтобы обработчик
  мог завершиться исключением и повторить обработку события.

Проверка специальных состояний кодов остаётся на стороне True API при обработке
документа: метод `/cises/info` не возвращает это поле.

## Папки и Celery-цепочки

Для каждой стадии сохраняется отдельный путь в `suz_worker_config.json`.
Рекомендуемое соответствие ключей и папок внутреннего бакета:

| Ключ конфигурации | Папка |
|---|---|
| `equipment-reports-disaggregation` | `equipment-reports-disaggregation/` |
| `disaggregation-tasks` | `disaggregationTasks/` |
| `disaggregation-receipts` | `disaggregationReceipts/` |
| `disaggregations` | `disaggregation/` |
| `equipment-reports-reaggregation-removing` | `equipment-reports-reaggregation-removing/` |
| `reaggregation-tasks` | `reaggregationTasks/` |
| `reaggregation-receipts` | `reaggregationReceipts/` |
| `reaggregations` | `reaggregation/` |

Имя JSON-файла без расширения передаётся между всеми стадиями без изменения и
является `taskId`.

Цепочка расформирования:

1. `equipment-reports-disaggregation/` — начальная проверка и
   `create_disaggregation_task_from_report()`;
2. `disaggregationTasks/` — `sign_and_send_disaggregation()`;
3. `disaggregationReceipts/` — `update_disaggregation_status()`;
4. при `CHECKED_OK` повторная проверка исходного отчёта и установка
   `check=finished` только по фактическому состоянию кодов.

Цепочка изъятия устроена так же и использует
`equipment-reports-reaggregation-removing/`, `reaggregationTasks/`,
`reaggregationReceipts/` и функции для `REAGGREGATION REMOVING`.

Промежуточный статус документа повторяется через механизм retry Celery. Ошибка
True API также завершает обработчик исключением и не изменяет `check`.
Бизнес-ошибка проверки кодов сохраняется в `check` и останавливает цепочку без
retry.

## Общий Object Storage trigger

У Object Storage trigger может быть только один `prefix`. У перечисленных папок
нет общего префикса, поэтому единый Celery trigger должен слушать весь внутренний
бакет с фильтром `suffix=.json`.

Связанная Cloud Function обязана проверять `object_id` по точному списку Celery-
папок до отправки сообщения в YMQ. `tasks.process_s3_event` повторно проверяет
путь как второй уровень защиты; неизвестные пути получают `Skipped: No match`.
Это исключает лишние сообщения для `sign/`, файлов токенов, архивов и выходных
папок статусов.

Общий Celery trigger не заменяет специализированные triggers других
потребителей. Отдельно продолжают работать:

- `sign/*.json` и `sign/*.txt` → функция очереди сервиса подписи;
- `equipment-reports/T-*.json` → уведомитель MAX.

Нельзя оставлять одновременно bucket-wide trigger и существующие prefix-триггеры,
направленные в ту же очередь: одно событие будет доставляться дважды. Безопасный
порядок миграции:

1. развернуть версию worker с маршрутами всех старых и новых папок;
2. добавить восемь новых путей в `suz_worker_config.json`;
3. подготовить фильтрующую Cloud Function и общий trigger в неактивном состоянии;
4. зафиксировать время переключения, остановить старые prefix-триггеры,
   направленные в Celery-очередь, и включить общий trigger;
5. проверить по одному тестовому объекту каждой новой стадии и отсутствие
   двойной доставки;
6. отдельно проверить объекты, созданные в коротком окне переключения, и при
   необходимости повторно передать их в очередь.

Триггеры подписи, уведомлений оборудования и других функций в эту миграцию не
входят: отключаются только триггеры, направленные в функцию передачи событий в
Celery-очередь.

Для изолированного тестового worker задаются отдельные `YMQ_QUEUE_URL` и
`YMQ_QUEUE_NAME`. Параметр `internal_bucket` должен быть указан явно: отсутствие
ключа включает промышленное значение по умолчанию.
