# GUI File Processor

Утилита для обработки текстовых файлов с возможностью фильтрации строк по заданным паттернам.

## Описание

Программа принимает два входных файла и создает третий, удаляя из второго файла строки, которые содержат подстроки из первого файла. Поддерживает различные режимы кодирования и сериализации.

## Возможности

- 📁 **Фильтрация строк**: Удаление строк из второго файла, содержащих подстроки из первого файла
- 🔍 **Гибкое сравнение**: Поиск подстрок в любой части строки
- ⚙️ **Настраиваемый разделитель**: Возможность указать символ для разделения строк в первом файле
- 📝 **Множественные кодировки**: Поддержка различных кодировок входных и выходных файлов
- 🔤 **Unicode-escape**: Режим преобразования Unicode символов в escape-последовательности
- 🎯 **JSON-сериализация**: Сериализация паттернов поиска по правилам JSON
- 📊 **Подробный вывод**: Режим verbose с детальной информацией о процессе

## Установка

```bash
# Клонирование репозитория
git clone https://github.com/yankoval/xTrek.git
cd gui-file-processor

# Установка зависимостей (опционально, для автоопределения кодировок)
pip install chardet

## Использование
# Базовый синтаксис
bash
python gui_processor.py <file1> <file2> <output> [options]
Основные примеры
bash
# Базовое использование
python gui_processor.py patterns.txt data.txt result.txt

# С JSON-сериализацией
python gui_processor.py patterns.txt data.txt result.txt --json-serialize

# С Unicode-escape
python gui_processor.py patterns.txt data.txt result.txt --unicode-escape

# Подробный вывод
python gui_processor.py patterns.txt data.txt result.txt --verbose
⚙️ Параметры
Параметр	Описание
file1	Файл с паттернами для поиска
file2	Файл для обработки
output	Выходной файл
-s, --separator	Символ разделения (по умолчанию: "93")
--encoding1	Кодировка первого файла
--encoding2	Кодировка второго файла
--output-encoding	Кодировка выходного файла
-u, --unicode-escape	Unicode-escape кодирование вывода
-j, --json-serialize	JSON-сериализация паттернов
-v, --verbose	Подробный вывод
--help	Показать справку
📖 Примеры файлов
patterns.txt:

text
user:admin
password:1234
error
warning]
data.txt:

text
login successful
user:admin logged in
error: connection failed
operation completed
result.txt:

text
login successful
operation completed
🛠 Требования
Python 3.9+

Опционально: chardet для автоопределения кодировок

📄 Лицензия
MIT License

🤝 Поддержка
Сообщения об ошибках и предложения приветствуются через Issues.

## Отчёты

Пакет `xtrek.reports` отделяет получение данных от структуры документа и
оформления. Первый отчёт показывает задания, полученные за календарный день.

Установка поддержки HTML и Markdown:

    pip install "xtrek[reports]"

Установка опционального PDF-рендерера:

    pip install "xtrek[reports-pdf]"

WeasyPrint также требует системные библиотеки, перечисленные в его инструкции
по установке для конкретной операционной системы.

Создание HTML-отчёта для браузера:

    python -m xtrek.reports.tasks \
        --date 2026-08-25 \
        --format html \
        --profile browser \
        --output report.html

Оба отчёта поддерживают диапазон с точностью до минуты вместо `--date`.
Начальная и конечная минуты включаются полностью; время интерпретируется в
часовом поясе `--timezone`:

    python -m xtrek.reports.tasks \
        --from 2026-08-25T09:30 \
        --to 2026-08-26T12:15 \
        --format html \
        --profile browser \
        --output tasks-range.html

    python -m xtrek.reports.tasks-grouped \
        --from 2026-08-25T09:30 \
        --to 2026-08-26T12:15 \
        --format html \
        --profile browser \
        --output tasks-grouped-range.html

Вывод компактного HTML для MAX в stdout:

    python -m xtrek.reports.tasks \
        --date 2026-08-25 \
        --format html \
        --profile messenger

Отчёт за день с группировкой по артикулам (идентификатор отчёта
`tasks-grouped`):

    python -m xtrek.reports.tasks-grouped \
        --date 2026-08-25 \
        --format html \
        --profile browser \
        --output tasks-grouped.html

Для импорта из Python используется имя пакета `xtrek.reports.tasks_grouped`.
Ошибочное прежнее имя `tasks-goupped` оставлено только как совместимый alias.

Для каждого значения `Article` выводятся количество заданий, сумма паспортов
(ярлыков) из `Quantity` и количество кодов. В конце таблицы добавляется общий
итог за выбранный день. В заголовке фиксируются дата отчёта и дата со временем
его создания в часовом поясе `--timezone`. Ниже выводится расшифровка по файлам: локальное время,
ярлыки, коды и полное имя JSON-файла. В профиле `messenger` расшифровка по
умолчанию скрыта, чтобы не превышать лимит сообщения MAX. Управление разделом:

    --details auto   # подробно для browser/printer, кратко для messenger
    --details full   # всегда включать расшифровку
    --details none   # всегда выводить только сводку

Профиль `messenger` не добавляет CSS и рамки. Таблица может сохраняться в
табличном виде через `TableStyle(messenger_layout="table")`. Для MAX и других
клиентов, которые удаляют HTML-таблицы, используйте
`messenger_layout="vertical_table"`: каждая пара «название: значение» будет
отделена символом перевода строки `\n`. Тег `<br>` не используется, поскольку
он не входит в перечень HTML-тегов, поддерживаемых MAX. При отправке результата
через Bot API необходимо передать `format: "html"`. Режимы `cards` и
`key_value` также доступны.

По умолчанию оба отчёта читают долговременный источник Yandex Object Storage:
`s3://20ab2a0c-2726-4ba1-9c7c-7deae82941ff/productionOrders/`. В нём для
одного входящего задания могут находиться основной и производные файлы.
Источник извлекает UUID исходного задания из имени, оставляет самый ранний
JSON для каждого UUID и не включает ручные/ремонтные файлы без UUID. Поэтому
одно полученное задание учитывается ровно один раз. Дата и время задания в
отчёте соответствуют `LastModified` выбранного productionOrder; исходное время
из короткоживущей папки `Задания/` в долговременном объекте не сохраняется.

Параметры `--bucket`, `--prefix`, `--endpoint-url`, `--region` и `--timezone`
можно переопределить. Для префиксов, отличных от `productionOrders/`,
устранение дублей по UUID автоматически отключается. Учётные данные AWS/S3
получает `boto3` стандартным способом.

Таблицы поддерживают декларативные колонки, выравнивание, числовые форматы,
ширину и рамки. Ссылки можно передавать как значения ячеек:

    Table(
        columns=(
            TableColumn("Документ", align="left"),
            TableColumn(
                "Количество",
                align="right",
                number_format=NumberFormat(decimal_places=0),
            ),
        ),
        rows=((Link("Открыть", "https://example.com"), 1234),),
        style=TableStyle(
            outer_border=Border(width=1.5, style="solid"),
            row_border=Border(width=0.5, style="dashed"),
            column_border=Border(width=0.5, style="dashed"),
        ),
    )

Полный набор демонстрационных HTML/Markdown можно пересобрать командой:

    python examples/reports_showcase.py
