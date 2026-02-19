ECHO OFF
REM Шаг 1 Создаем токены 
REM Шаг 1.0 Получаем случайные данные
REM Фитокосметик
SET conId=90a75021-a56e-415c-b5d1-daabb66002b9
SET INN=7733154124


curl -X GET "https://markirovka.crpt.ru/api/v3/true-api/auth/key" -H "accept: application/json" > %USERPROFILE%\sign_data.json
REM ответ: {"uuid":"f4463cb4-3ff4-41a1-af72-deb2dce91596","data":"VRHZTEQSJUCOEBJRVELQQUWBOASNUC"}

REM Выделяем случайные данные для подписи в файл "%USERPROFILE%\tst\%INN%_dataToSign.txt"
@echo off
powershell -Command "(Get-Content '%USERPROFILE%\sign_data.json' | ConvertFrom-Json).data" > "%USERPROFILE%\tst\%INN%_dataToSign.txt"
REM Шаг 1.1 Подписываем  случайные данные файл %USERPROFILE%\dataToSign.txt с помощью плагина кипто про 
REM https://cryptopro.ru/sites/default/files/products/cades/demopage/cades_bes_file.html
REM Тип подписи CAdES-BES  Вложить серт. конечный Отделенная подпись ВЫКЛ
exit
REM Создаем JSON файл %USERPROFILE%\get_token.json  содержащий: {"uuid":"99ba4df7-3e69-4df7-9251-833b71dbb33a", "data":"MIINrA"  ,    "inn":"7733154124"}
REM  Далее в зависимости от типа токена который хотим получить (для скачивания эмиссии client_token )

REM Для получения JWT токен HONEST_TOKEN (длинный пример: eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJwc ...)
REM Шаг 1.2 Отпавляем подписанный случайные данные в  СУЗ
REM curl -X POST "https://markirovka.crpt.ru/api/v3/true-api/auth/simpleSignIn" ^
     REM -H "accept: application/json" ^
     REM -H "Content-Type: application/json" ^
     REM -d @%USERPROFILE%"\get_jwt_token.json" ^
     REM --verbose > %USERPROFILE%\token_jwt.json

REM Для получения токена аутентификации (client_token пример: 02099dd7-e0ac-4ae0-8807-049e8577d995)
REM 11.2. Метод получения токена аутентификации в СУЗ необходимо иметь connectionID из ЛК/Устройства Для Фито "сервер" 	90a75021-a56e-415c-b5d1-daabb66002b9 Глобал 90a75021-a56e-415c-b5d1-daabb66002b9

curl -X POST "https://markirovka.crpt.ru/api/v3/true-api/auth/simpleSignIn/"%conId% ^
 -H "accept: application/json" ^
 -H "Content-Type: application/json" ^
 -d @%USERPROFILE%"\get_token.json" >  %USERPROFILE%\token_%conId%.json
 
REM Шаг 2 Скачеваем предворительно эметированные коды с помощью библиотеки хТрек  
REM python suz.py --token %token% --omsid 3b1ed9ae-a5d9-4458-9f02-596781bd1e41 --client_token 02099dd7-e0ac-4ae0-8807-049e8577d995  --eorder a107c8be-e084-4497-8fae-8752c7c4d096 --qty=0

REM  ШАГ 3 Преобразовываем полученные данные в CSV
REM  ШАГ 3 Преобразовываем полученные данные в CSV
REM  ШАГ 4 Печатаем  данные CSV на Solmark
REM  ШАГ 5 Делаем отчет о нанесении в ЛК с помощью того же CSV файла 
REM ШАГ 6 Делаем сообщение о вводе в оборот с помощью xlsx шаблона, предворительно очищаем CSV от криптохвостов


pause
