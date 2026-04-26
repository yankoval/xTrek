import os
import json
import time
import argparse
import uuid
import requests
import urllib3
from pathlib import Path
from .tokens import TokenProcessor

# Отключаем лишние предупреждения в консоли
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

def get_new_token(inn, conid=None, mode='auth', timeout=60):
    """
    Получает новый токен от Честного Знака, взаимодействуя с демоном подписи.
    Возвращает строку токена или None в случае ошибки.
    """
    user_profile = os.environ.get('USERPROFILE', os.path.expanduser('~'))
    work_dir = Path(user_profile) / "tst"
    work_dir.mkdir(parents=True, exist_ok=True)

    unique_id = uuid.uuid4()
    data_to_sign_path = work_dir / f"{inn}_{unique_id}_dataToSign.txt"
    signature_path = work_dir / f"{inn}_{unique_id}_dataToSign.txt.sig"

    token_value = None

    try:
        # --- Шаг 1: Получаем случайные данные ---
        print(f"[*] Requesting auth key from CRPT (SSL Verify: Disabled)...")
        try:
            resp = requests.get("https://markirovka.crpt.ru/api/v3/true-api/auth/key", verify=False)
            resp.raise_for_status()
            auth_data = resp.json()
        except Exception as e:
            print(f"[!] Error getting auth key: {e}")
            return None

        with open(data_to_sign_path, "w", encoding="utf-8") as f:
            f.write(auth_data['data'])

        print(f"[*] Data saved to: {data_to_sign_path}. Waiting for daemon...")

        # --- Шаг 2: Ожидание подписи ---
        start_time = time.time()
        while not signature_path.exists():
            if time.time() - start_time > timeout:
                print("[!] Timeout: Signature file not found.")
                return None
            time.sleep(2)

        time.sleep(0.5)
        print("[+] Signature detected!")

        # --- Шаг 3: Сборка JSON ---
        with open(signature_path, "r", encoding="utf-8") as f:
            signature_body = f.read().strip()

        payload = {
            "uuid": auth_data['uuid'],
            "data": signature_body,
            "inn": inn
        }

        # --- Шаг 4: Отправка ---
        if mode == 'jwt':
            url = "https://markirovka.crpt.ru/api/v3/true-api/auth/simpleSignIn"
        else:
            if not conid:
                print("[!] Error: conid required for 'auth' mode.")
                return None
            url = f"https://markirovka.crpt.ru/api/v3/true-api/auth/simpleSignIn/{conid}"

        try:
            final_resp = requests.post(url, json=payload, verify=False)
            final_resp.raise_for_status()
            token_data = final_resp.json()
            token_value = token_data.get('token')
        except Exception as e:
            print(f"[!] Final request error: {e}")

    finally:
        # Cleanup temporary files
        if data_to_sign_path.exists():
            try:
                data_to_sign_path.unlink()
            except Exception as e:
                print(f"[!] Error deleting data file: {e}")
        if signature_path.exists():
            try:
                signature_path.unlink()
            except Exception as e:
                print(f"[!] Error deleting signature file: {e}")

    return token_value

def main():
    parser = argparse.ArgumentParser(description="CRPT Token Generator")
    parser.add_argument("--inn", required=True, help="ИНН участника")
    parser.add_argument("--conid", help="Connection ID (для обычного токена)")
    parser.add_argument("--mode", choices=['auth', 'jwt'], default='auth',
                        help="Режим: auth (для СУЗ) или jwt (HONEST_TOKEN)")
    parser.add_argument("--timeout", type=int, default=60, help="Тайм-аут ожидания подписи (сек)")

    args = parser.parse_args()

    token_value = get_new_token(inn=args.inn, conid=args.conid, mode=args.mode, timeout=args.timeout)

    if token_value:
        user_profile = os.environ.get('USERPROFILE', os.path.expanduser('~'))
        if args.mode == 'jwt':
            output_file = Path(user_profile) / "token_jwt.json"
        else:
            output_file = Path(user_profile) / f"token_{args.conid}.json"

        # Сохранение в индивидуальный файл
        try:
            with open(output_file, "w", encoding="utf-8") as f:
                json.dump({"token": token_value}, f, indent=4)
            print(f"[+++] Success! Result: {output_file}")
        except Exception as e:
            print(f"[!] Error writing individual token file: {e}")

        # Сохранение в общую базу tokens.json
        try:
            processor = TokenProcessor()
            processor.save_token(token_value, conid=args.conid)
        except Exception as e:
            print(f"[!] Ошибка при сохранении токена в базу: {e}")
    else:
        print("[!] Не удалось получить токен.")

if __name__ == "__main__":
    main()