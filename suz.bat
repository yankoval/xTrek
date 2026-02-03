@ECHO OFF
REM Скрипт для работы с СУЗ

python suz.py --create-order --body-file "order.json" ^
	--signature-file "XSignature.txt" ^
	-oid YOUR_OMS_ID ^
	--client_token YOUR_CLIENT_TOKEN ^
	--token YOUR_JWT_TOKEN

pause
