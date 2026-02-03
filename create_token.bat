@ECHO OFF
REM Скрипт для получения токена

SET conId=YOUR_CONNECTION_ID
curl -X POST "https://markirovka.crpt.ru/api/v3/true-api/auth/simpleSignIn/"%conId% ^
 -H "accept: application/json" ^
 -H "Content-Type: application/json" ^
 -d "@sample_data_token_jwt.json"
pause
