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
Python 3.6+

Опционально: chardet для автоопределения кодировок

📄 Лицензия
MIT License

🤝 Поддержка
Сообщения об ошибках и предложения приветствуются через Issues.