import os
import json
import time
import argparse
import requests
import urllib3
from pathlib import Path

# Отключаем лишние предупреждения в консоли
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

def main():
    parser = argparse.ArgumentParser(description="CRPT Token Generator")
    parser.add_argument("--inn", required=True, help="ИНН участника")
    parser.add_argument("--conid", help="Connection ID (для обычного токена)")
    parser.add_argument("--mode", choices=['auth', 'jwt'], default='auth', 
                        help="Режим: auth (для СУЗ) или jwt (HONEST_TOKEN)")
    parser.add_argument("--timeout", type=int, default=60, help="Тайм-аут ожидания подписи (сек)")
    
    args = parser.parse_args()

    user_profile = os.environ['USERPROFILE']
    work_dir = Path(user_profile) / "tst"
    work_dir.mkdir(parents=True, exist_ok=True)

    data_to_sign_path = work_dir / f"{args.inn}_dataToSign.txt"
    signature_path = work_dir / f"{args.inn}_dataToSign.txt.sig"
    get_token_json_path = Path(user_profile) / "get_token.json"

    if signature_path.exists():
        signature_path.unlink()

    # --- Шаг 1: Получаем случайные данные ---
    print(f"[*] Requesting auth key from CRPT (SSL Verify: Disabled)...")
    try:
        # verify=False игнорирует ошибку самоподписанного сертификата
        resp = requests.get("https://markirovka.crpt.ru/api/v3/true-api/auth/key", verify=False)
        resp.raise_for_status()
        auth_data = resp.json()
    except Exception as e:
        print(f"[!] Error getting auth key: {e}")
        return

    with open(data_to_sign_path, "w", encoding="utf-8") as f:
        f.write(auth_data['data'])
    
    print(f"[*] Data saved to: {data_to_sign_path}. Waiting for daemon...")

    # --- Шаг 2: Ожидание подписи ---
    start_time = time.time()
    while not signature_path.exists():
        if time.time() - start_time > args.timeout:
            print("[!] Timeout: Signature file not found.")
            return
        time.sleep(2)

    time.sleep(0.5) 
    print("[+] Signature detected!")

    # --- Шаг 3: Сборка JSON ---
    with open(signature_path, "r", encoding="utf-8") as f:
        signature_body = f.read().strip()

    payload = {
        "uuid": auth_data['uuid'],
        "data": signature_body,
        "inn": args.inn
    }

    # --- Шаг 4: Отправка ---
    if args.mode == 'jwt':
        url = "https://markirovka.crpt.ru/api/v3/true-api/auth/simpleSignIn"
        output_file = Path(user_profile) / "token_jwt.json"
    else:
        if not args.conid:
            print("[!] Error: --conid required for 'auth' mode.")
            return
        url = f"https://markirovka.crpt.ru/api/v3/true-api/auth/simpleSignIn/{args.conid}"
        output_file = Path(user_profile) / f"token_{args.conid}.json"

    try:
        final_resp = requests.post(url, json=payload, verify=False)
        final_resp.raise_for_status()
        
        with open(output_file, "w", encoding="utf-8") as f:
            json.dump(final_resp.json(), f, indent=4)
        
        print(f"[+++] Success! Result: {output_file}")
    except Exception as e:
        print(f"[!] Final request error: {e}")

if __name__ == "__main__":
    main()