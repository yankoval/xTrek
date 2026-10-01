# Подпись на Linux: xtrek.sign

Модуль создаёт и проверяет CAdES-BES через официальный `pycades`. Основной сценарий — импорт функций из других модулей, включая обработчики `tasks.py`. Также доступны `python -m xtrek.sign` и команда `xtrek-sign` после установки пакета.

## Совместимость с SignJS

Контракт сверён с `SignJsApp.js` и `CloudSignApp.js` репозитория `yankoval/SignJS`, commit `e22d95edf7bffd2bc5c75d035665c2ac4c9daac2`:

| Вход | Режим CAdES-BES | Функция |
|---|---|---|
| Исходные байты документа JSON или другого формата | Detached | `sign_document` |
| Значение `data` ответа `/auth/key` для True API UUID/JWT и СУЗ | Attached | `sign_token_data` |
| Точный путь и query string подписанного ping СУЗ | Detached | `sign_token_data(..., detached=True)` |
| Файл `.txt` | Attached по умолчанию | `sign_file` |
| Файл `.json` или другое расширение | Detached по умолчанию, как в CloudSignApp | `sign_file` |

Оба варианта SignJS вызывают `SignCades(..., CADES_BES, isDetached)`; отдельных алгоритмов SignHash или XMLDSig в этой версии нет. Новый модуль сохраняет формат результата: Base64 без пробелов и переносов строк, файл `<исходное имя>.sig`. Регистр расширения учитывается, как в SignJS. Режим можно задать явно, независимо от имени файла.

## Среда выполнения

Python 3.9+, Linux, установленный CSP/CAdES и официальный pycades, собранный для используемого Python. На проверенном Debian 11 amd64: CSP 5.0.13003, CAdES 2.0.15003, pycades commit `7c2ac830694d012553599f72d8e964959ebfdff5`.

`pycades` не добавляется как произвольный одноимённый пакет PyPI. Его нужно собрать по [инструкции CryptoPro](https://github.com/CryptoPro/pycades/blob/main/doc/pycades-build.md) с установленными библиотеками и SDK. Обычный импорт `xtrek.sign` не загружает pycades. На Windows/macOS импорт и CLI help работают, а попытка подписи возвращает понятную ошибку о поддержке только Linux.

Сертификат должен находиться в `My` текущего пользователя и иметь закрытый ключ. По умолчанию используется `current_user`; `store_location="local_machine"` выбирает машинное хранилище явно. Автоматического перехода между хранилищами нет. Выбор — по полному SHA-1 thumbprint, совпадение должно быть единственным. Проверка сертификата включена (`CheckCertificate=True`); перед выдачей результата выполняется `VerifyCades`. Для attached дополнительно сравниваются восстановленные байты.

В испытанной установке CSP была активна демолицензия. Постоянный лицензионный режим сервера требует отдельного подтверждения; модуль его не меняет.

## Импорт функций

```python
from xtrek.sign import sign_document, sign_token_data, sign_file

# Настройки владельца ключа задаёт вызывающий код, а не имя документа.
thumbprint = configured_thumbprint
options = {"pin_file": "/run/secrets/signing-pin"}

# Уже подготовленные байты: повторная сериализация JSON недопустима.
signature = sign_document(document_bytes, thumbprint, **options)

# Строка auth/key.data кодируется как UTF-8, НЕ декодируется из Base64.
auth_signature = sign_token_data(challenge["data"], thumbprint, **options)
# Можно передать bytes, если вызывающий код уже определил кодировку.
ping_signature = sign_token_data(exact_ping_path, thumbprint, detached=True, **options)

# Через имеющийся xtrek.storage; результат — путь/URI подписи.
sig_uri = sign_file(
    "s3://bucket/sign/7733154124_document.json",
    thumbprint,
    s3_config=config.get("s3_config"),
    **options,
)
local_sig = sign_file("/srv/sign/7733154124_dataToSign.txt", thumbprint, **options)
```

`sign_bytes(data, thumbprint, detached=True, ...)` — низкоуровневый общий вход. Он и `sign_document` требуют `bytes`; `sign_token_data` принимает `str` или `bytes`. Все функции подписи данных возвращают строку Base64. `sign_file` возвращает место сохранения; параметр `output` позволяет указать иной локальный/S3 путь, в том числе изменить тип хранилища.

`sign_file` использует `storage.download/upload` и временные файлы в закрытом каталоге. Он не вызывает `read_text`, который мог бы удалить BOM или изменить переносы строк. Исходный файл не удаляется и его теги не меняются. Допустимы обычные пути, `file:///...`, `s3://bucket/key`; подписанные HTTP URL и URL произвольных сервисов не поддерживаются. Передаётся конкретный файл в локальной папке или S3-объект, а не сама папка/префикс.

Существующий `.sig` не считается доказательством выполненной работы: создаётся новая подпись, и после успешной криптографической проверки результат записывается по выбранному адресу с заменой файла. При ошибке чтения/подписи/проверки запись не начинается. Ошибка записи возвращается вызывающему коду; подтверждение задания очереди допускается только после успешного возврата. Транзакционность публикации, защита от параллельных задач и повторная доставка остаются ответственностью вызывающего обработчика. Следует использовать уникальный исходник для каждого запроса.

`SigningError` содержит стадию и, если есть, HRESULT. В него не копируются исходные сообщения CSP/S3, данные документа или PIN. Ошибки параметров — `ValueError`/`TypeError`. Отмена через `BaseException` не подавляется. Внутри процесса нативные вызовы сериализуются, объекты CSP создаются заново для каждого вызова. Для Celery удобно использовать отдельные worker-процессы с доступом к нужному пользовательскому хранилищу.

Функции не выпускают токены и не отправляют документы в Честный ЗНАК. Текущие цепочки `tasks.py` и `crpt_auth.py` пока сохраняют свой способ ожидания SignJS: этот коммит добавляет импортируемый модуль, а не автоматически переключает существующих потребителей очереди.

## PIN

В библиотечном вызове разрешён `pin="..."` из секрет-хранилища процесса либо `pin_file`. Одновременное указание запрещено. Для CLI предусмотрен только `--pin-file`, чтобы значение PIN не попадало в командную строку и историю shell.

Файл — UTF-8 с правами только для владельца, например 0600. Один завершающий LF/CRLF удаляется; пробелы PIN сохраняются. Без параметра передаётся пустой PIN для незащищённого контейнера. Для защищённого ключа передавайте его явно, чтобы не зависеть от интерактивного запроса CSP.

## CLI

```sh
# Detached документ; результат записывается рядом в document.json.sig.
xtrek-sign document /srv/sign/document.json --thumbprint "$CERT_THUMBPRINT" \
  --pin-file /run/secrets/signing-pin

# Attached данные авторизации из stdin; Base64 печатается в stdout.
# Вход должен содержать точные байты challenge, без добавленного перевода строки.
python -m xtrek.sign token - --thumbprint "$CERT_THUMBPRINT" \
  --pin-file /run/secrets/signing-pin < challenge.txt

# Режим по расширению .txt/.json, стандартные AWS credentials используются storage.
xtrek-sign file s3://bucket/sign/document.json --thumbprint "$CERT_THUMBPRINT" \
  --pin-file /run/secrets/signing-pin

# Явный режим и другое место результата.
xtrek-sign file /srv/sign/request-path.txt --mode detached \
  --output s3://bucket/results/ping.sig --thumbprint "$CERT_THUMBPRINT" \
  --pin-file /run/secrets/signing-pin
```

`--s3-config FILE` читает JSON-объект настроек `S3Storage` (`endpoint_url`, `region_name`, при необходимости поля AWS credentials). Файл не выводится в лог. Без него применяется существующее стандартное разрешение credentials в boto3. `--store-location local_machine` выбирает машинное хранилище.

Для файлов CLI выводит путь сохранённой подписи. Для stdin без `--output` — Base64 и один терминальный LF; при `--output` — путь результата. `file -` требует явный `--mode attached/detached`, поскольку расширения нет. Успех — код 0, ошибка операции — 1, ошибка синтаксиса argparse — 2.

## Тестирование

Автономные тесты не требуют сертификатов, pycades или сети:

```sh
python -m pytest -q tests/test_sign.py
```

Настоящие криптографические тесты запускаются только при явном указании сертификата на Linux:

```sh
XTREK_SIGN_TEST_THUMBPRINT="$CERT_THUMBPRINT" \
XTREK_SIGN_TEST_PIN_FILE=/run/secrets/signing-pin \
python -m pytest -q tests/test_sign.py tests/test_sign_integration.py
```

Они создают подписи синтетического документа, auth-challenge для UUID/JWT/СУЗ и строки ping; проверяют через `pycades.VerifyCades` и независимый `cryptcp`, отклоняют изменённый документ, проверяют локальное хранение и CLI stdin. При необходимости путь к cryptcp задаётся через `XTREK_SIGN_TEST_CRYPTCP` (по умолчанию `/opt/cprocsp/bin/amd64/cryptcp`). Тесты не выпускают коды или токены и не обращаются к бизнес-API. CSP может обращаться к инфраструктуре проверки цепочки/отзыва сертификата.

S3 проверяется автономно через настоящий адаптер `S3Storage` с подменённым boto3-клиентом: проверяются адреса объектов и неизменность байтов. Реальные операции в S3 не нужны для запуска тестов.

Проверка 01.10.2026: на Debian 11 / Python 3.9.2 с CSP 5.0.13003 и CAdES 2.0.15003 прошли 54 теста (46 автономных + 8 криптографических). Локально прошли 79 автономных и регрессионных тестов вместе с `test_token_purpose_integration.py` и `test_document_send_idempotency.py`; 8 Linux-тестов на macOS ожидаемо пропущены. Действующая установка xTrek на VPS не переключалась: проверка выполнена отдельной копией модуля.
