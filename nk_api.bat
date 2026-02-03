@ECHO OFF
REM Запуск nk.py
chcp 65001

REM Добавим путь к текущей папке в PATH чтобы модули импортировались без проблем
set PYTHONPATH=%cd%

REM Установите переменную окружения TRUE_API_TOKEN перед запуском скрипта
REM SET TRUE_API_TOKEN=YOUR_TOKEN_HERE

REM Пример запуска для проверки GTIN из файла
REM python nk.py --file path\to\your\gtins.xlsx --inn YOUR_INN --find-token-by-inn YOUR_INN

python nk.py --file example_gtins.xlsx --inn 1234567890 --find-token-by-inn 1234567890
python trueapi.py --balance --find-token-by-inn 1234567890 --log-file nk.log

REM  Полезные команды
REM  Как отфильтровать текстовый файл подстроками из строк другого файла
REM findstr /I /G:"фильтр.csv" /N "linked_gtins_output.csv"
