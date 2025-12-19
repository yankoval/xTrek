@ECHO OFF
REM Запуск nk.py
chcp 65001

REM Добавим путь к текущей папке в PATH чо бы модули импортировались без проблем
set PYTHONPATH=%cd%
REM Установите переменную окружения TRUE_API_TOKEN перед запуском скрипта
REM SET TRUE_API_TOKEN=
REM SET FIND_TOKEN_BY_INN=7733154124
REM 04630014751849 python  nk.py --file C:\Users\project\Downloads\gtins_ashan.xlsx --inn 7703270067 --find-token-by-inn 9718180660 9723161905
python nk.py --file C:\Users\project\Downloads\gtins_tander.xlsx --inn 2310031475 --find-token-by-inn 7733154124
python nk.py --owngtins --find-token-by-inn 9723161905
python nk.py --linked-gtins --find-token-by-inn 9723161905
python nk.py --owngtins --find-token-by-inn 9718180660
python nk.py --linked-gtins --find-token-by-inn 9718180660
python nk.py --owngtins --find-token-by-inn 7733154124
python -i nk.py --linked-gtins --find-token-by-inn 7733154124

REM  ПОлезные команды
REM  


REM  Как отфильтровать текстовый файл подстроками из строк другого файла
REM findstr /I /G:"фильтр.csv" /N "linked_gtins_20251127_133626.csv"