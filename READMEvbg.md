# GS1 Russia VbG API

Короткая памятка по модулю `xtrek.vbg`.

## Что делает

`xtrek.vbg` запрашивает сведения о GTIN через GS1 Russia VbG API 3.2.4:

- владелец лицензии GS1;
- ИНН владельца, если GS1 возвращает его;
- GLN владельца;
- описание товара;
- статус карточки и ошибки VbG.

Основная функция:

```python
from xtrek.vbg import get_gtin_info

info = get_gtin_info(["4670167220663", "5449000000996"])
print(info["04670167220663"]["license"]["licenseeINN"])
```

Ключ результата - нормализованный 14-значный GTIN.

## API key

Модуль ищет API key в таком порядке:

1. параметр `token=`;
2. переменная окружения `GS1_VBG_TOKEN`;
3. переменная окружения `GS1_VBG_TOKEN_FILE`;
4. путь из конфига `vbg_api_key_path`;
5. локальный файл `~/.gs1rus-7733154124`.

Для запуска на разных машинах лучше хранить ключ в S3 и указать в `suz_worker_config.json`:

```json
{
  "vbg_api_key_path": "s3://20ab2a0c-2726-4ba1-9c7c-7deae82941ff/secrets/gs1rus-7733154124.key"
}
```

Загрузить локальный ключ в S3:

```bash
suz_worker_config=/Users/ivankiselev/python-projects/suz_worker_config.json \
python -m xtrek.vbg \
  --upload-token-to-s3 s3://20ab2a0c-2726-4ba1-9c7c-7deae82941ff/secrets/gs1rus-7733154124.key \
  --local-token-file ~/.gs1rus-7733154124 \
  4670167220663
```

## CLI примеры

Запросить один GTIN:

```bash
suz_worker_config=/Users/ivankiselev/python-projects/suz_worker_config.json \
python -m xtrek.vbg 4670167220663
```

JSON-вывод:

```bash
suz_worker_config=/Users/ivankiselev/python-projects/suz_worker_config.json \
python -m xtrek.vbg 4670167220663 5449000000996 --json
```

Обновить данные, игнорируя кэш:

```bash
suz_worker_config=/Users/ivankiselev/python-projects/suz_worker_config.json \
python -m xtrek.vbg 4670167220663 --force-refresh
```

Явно указать файл или S3-путь с ключом:

```bash
python -m xtrek.vbg 4670167220663 \
  --token-file s3://20ab2a0c-2726-4ba1-9c7c-7deae82941ff/secrets/gs1rus-7733154124.key
```

## Диагностика в xTrek

Посмотреть VbG-информацию через основной диагностический CLI:

```bash
python -m xtrek.create_emission_task_sample --vbg-gtin-info 4670167220663
```

При обработке входящего задания можно включить VbG-диагностику для UNIT,
которые не входят в `allowed_gtins` и поэтому пропускаются:

```bash
python -m xtrek.create_emission_task_sample \
  --process-task s3://bucket/tasks/task.json \
  --vbg-diagnostics
```

Флаг ничего не меняет в решении "пропустить/обработать"; он только пишет в лог
владельца, ИНН, GLN, бренд и описание из VbG.

## Кэш

По умолчанию ответы кэшируются в:

```text
~/.cache/xtrek/vbg_gtin_cache.json
```

Кэш живет 30 дней. Путь можно переопределить:

```bash
python -m xtrek.vbg 4670167220663 --cache-path /tmp/vbg-cache.json
```
