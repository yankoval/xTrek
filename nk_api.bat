chcp 65001
@ -9,10 +9,13 @@ REM SET TRUE_API_TOKEN=
REM SET FIND_TOKEN_BY_INN=7733154124
REM 04630014751849 python  nk.py --file C:\Users\project\Downloads\gtins_ashan.xlsx --inn 7703270067 --find-token-by-inn 9718180660 9723161905
python nk.py --file C:\Users\project\Downloads\gtins_tander.xlsx --inn 2310031475 --find-token-by-inn 7733154124
python trueapi.py --balance --find-token-by-inn 9723161905
python nk.py --owngtins --find-token-by-inn 9723161905
python nk.py --linked-gtins --find-token-by-inn 9723161905
python trueapi.py --balance --find-token-by-inn 9718180660
python nk.py --owngtins --find-token-by-inn 9718180660
python nk.py --linked-gtins --find-token-by-inn 9718180660
python trueapi.py --balance --find-token-by-inn 7733154124
python nk.py --owngtins --find-token-by-inn 7733154124
python -i nk.py --linked-gtins --find-token-by-inn 7733154124

