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

Вывод компактного HTML для MAX в stdout:

    python -m xtrek.reports.tasks \
        --date 2026-08-25 \
        --format html \
        --profile messenger

Профиль `messenger` не добавляет CSS и рамки. Таблица может сохраняться в
табличном виде через `TableStyle(messenger_layout="table")`. Для MAX и других
клиентов, которые удаляют HTML-таблицы, используйте
`messenger_layout="vertical_table"`: каждая пара «название: значение» будет
отделена явным переносом строки. Режимы `cards` и `key_value` также доступны.

По умолчанию команда читает Yandex Object Storage. Параметры `--bucket`,
`--prefix`, `--endpoint-url`, `--region` и `--timezone` можно переопределить.
Учётные данные AWS/S3 получает `boto3` стандартным способом.

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
