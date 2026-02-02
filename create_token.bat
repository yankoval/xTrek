REM curl -X GET "https://markirovka.crpt.ru/api/v3/true-api/auth/key" -H "accept: application/json"
REM curl -X POST "https://markirovka.crpt.ru/api/v3/true-api/auth/simpleSignIn" -H "accept: application/json" -H "Content-Type: application/json"

REM pause

REM curl -X POST "https://markirovka.crpt.ru/api/v3/true-api/auth/simpleSignIn" ^
     REM -H "accept: application/json" ^
     REM -H "Content-Type: application/json" ^
     REM -d "@C:\Users\project\Scripts\xTrek\sample_data_token_jwt.json" ^
     REM --verbose

REM 11.1. Метод получения токена аутентификации в СУЗ
SET conId=90a75021-a56e-415c-b5d1-daabb66002b9
curl -X POST "https://markirovka.crpt.ru/api/v3/true-api/auth/simpleSignIn/"%conId% ^
 -H "accept: application/json" ^
 -H "Content-Type: application/json" ^
 -d "@C:\Users\project\Scripts\xTrek\sample_data_token_jwt.json"
pause