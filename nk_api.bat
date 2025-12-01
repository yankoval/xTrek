
REM Запуск nk.py
chcp 65001

REM Установите переменную окружения TRUE_API_TOKEN перед запуском скрипта
REM SET TRUE_API_TOKEN=<ВАШ ТОКЕН>

REM 04630014751849 python  nk.py --file C:\Users\project\Downloads\gtins_ashan.xlsx --inn 7703270067
python  nk.py --file C:\Users\project\Downloads\gtins_tander.xlsx --inn 2310031475

REM  ПОлезные команды
REM  


REM  Как отфильтровать текстовый файл подстроками из строк другого файла
REM findstr /I /G:"фильтр.csv" /N "linked_gtins_20251127_133626.csv"