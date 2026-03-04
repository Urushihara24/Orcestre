import json
import logging
import os
from datetime import datetime
from typing import Optional, Dict, Tuple

import requests

logger = logging.getLogger(__name__)

# OAuth credentials (из epic_api_client.py)
CLIENT_ID = os.getenv("EPIC_CLIENT_ID", "34a02cf8f4414e29b15921876da36f9a").strip()
CLIENT_SECRET = os.getenv("EPIC_CLIENT_SECRET", "daafbccc737745039dffe53d94fc76cf").strip()

# API endpoints
ACCOUNT_BASE = "https://account-public-service-prod.ol.epicgames.com"
FRIENDS_BASE = "https://friends-public-service-prod.ol.epicgames.com"


class DeviceAuthGenerator:
    """Генератор device_auth для аккаунтов."""

    def __init__(self, login: str, password: str, proxy_url: Optional[str] = None):
        """
        Args:
            login: Email или никнейм Epic Games
            password: Пароль аккаунта
            proxy_url: Опционально, URL прокси (http://ip:port или https://ip:port)
        """
        self.login = login
        self.password = password
        self.proxy_url = proxy_url
        self.session = self._create_session()
        self.timeout = 15

    def _create_session(self) -> requests.Session:
        """Создать session с поддержкой прокси."""
        s = requests.Session()
        if self.proxy_url:
            s.proxies = {"http": self.proxy_url, "https": self.proxy_url}
        return s

    def generate(self) -> Tuple[bool, Optional[Dict], str]:
        """
        Основной метод: получить device_auth.

        Returns:
            (success, device_auth_dict, message)

        device_auth_dict = {
            "epic_account_id": "...",
            "device_id": "...",
            "device_secret": "..."
        }
        """
        try:
            logger.info(f"🔐 Начинаю получение device_auth для {self.login}")

            # Шаг 1: Авторизация и получение access_token
            logger.info("1️⃣ Авторизация...")
            token_result = self._get_access_token()
            if not token_result["ok"]:
                return False, None, token_result["message"]

            access_token = token_result["access_token"]
            account_id = token_result.get("account_id")

            logger.info(f"✅ Access token получен (account_id: {account_id})")

            # Шаг 2: Создать/получить device_auth
            logger.info("2️⃣ Генерирую device_auth...")
            device_auth_result = self._create_device_auth(access_token, account_id)
            if not device_auth_result["ok"]:
                return False, None, device_auth_result["message"]

            device_id = device_auth_result["device_id"]
            device_secret = device_auth_result["device_secret"]

            logger.info(f"✅ Device auth создан (device_id: {device_id[:10]}...)")

            result = {
                "epic_account_id": account_id,
                "device_id": device_id,
                "device_secret": device_secret,
            }
            return True, result, "✅ Device auth успешно получен"

        except Exception as e:
            logger.error(f"❌ Ошибка при получении device_auth: {e}")
            return False, None, f"❌ Ошибка: {str(e)}"

    def _get_access_token(self) -> Dict:
        """
        Авторизация через password grant и получение access_token.
        Использует Basic Auth (client_id:client_secret) в заголовке.
        """
        try:
            url = f"{ACCOUNT_BASE}/account/api/oauth/token"

            # Авторизация через Basic client_id:client_secret
            import base64
            basic = base64.b64encode(f"{CLIENT_ID}:{CLIENT_SECRET}".encode()).decode()

            headers = {
                "Authorization": f"Basic {basic}",
                "Content-Type": "application/x-www-form-urlencoded",
                "User-Agent": "EpicGamesClient/2.2 bot-device-auth",
            }

            data = {
                "grant_type": "password",
                "username": self.login,
                "password": self.password,
            }

            logger.debug(f"📤 POST {url}")
            resp = self.session.post(
                url,
                headers=headers,
                data=data,
                timeout=self.timeout,
            )

            if resp.status_code not in (200, 201):
                try:
                    error_data = resp.json() if resp.text else {}
                    error_msg = (
                        error_data.get("errorMessage")
                        or error_data.get("error_description")
                        or resp.text
                        or "Unknown error"
                    )
                except Exception:
                    error_msg = resp.text or "Unknown error"

                if resp.status_code == 401:
                    return {
                        "ok": False,
                        "message": f"❌ Неверные логин/пароль или аккаунт заблокирован. Ответ: {error_msg}",
                    }
                elif resp.status_code == 429:
                    return {
                        "ok": False,
                        "message": f"⏱️ Слишком много попыток. Погоди и попробуй позже. Ответ: {error_msg}",
                    }
                else:
                    return {
                        "ok": False,
                        "message": f"❌ Ошибка авторизации ({resp.status_code}): {error_msg}",
                    }

            data = resp.json()

            return {
                "ok": True,
                "access_token": data.get("access_token"),
                "refresh_token": data.get("refresh_token"),
                "account_id": data.get("account_id"),
            }

        except Exception as e:
            return {
                "ok": False,
                "message": f"❌ Ошибка при авторизации: {str(e)}",
            }


    def _create_device_auth(self, access_token: str, account_id: str) -> Dict:
        """
        Создать device_auth через POST запрос.
        Эндпоинт создаёт новое "устройство" и возвращает deviceId и secret. [web:112]
        """
        try:
            if not account_id:
                return {
                    "ok": False,
                    "message": "❌ account_id missing in auth response",
                }

            url = f"{ACCOUNT_BASE}/account/api/public/account/{account_id}/deviceAuth"

            headers = {
                "Authorization": f"Bearer {access_token}",
                "Content-Type": "application/json",
                "User-Agent": "EpicGamesClient/2.2 bot-device-auth",
            }

            body = {}

            logger.debug(f"📤 POST {url}")
            resp = self.session.post(
                url,
                headers=headers,
                json=body,
                timeout=self.timeout,
            )

            if resp.status_code not in (200, 201):
                try:
                    error_data = resp.json() if resp.text else {}
                    error_msg = (
                        error_data.get("errorMessage")
                        or error_data.get("error_description")
                        or resp.text
                        or "Unknown error"
                    )
                except Exception:
                    error_msg = resp.text or "Unknown error"

                return {
                    "ok": False,
                    "message": f"❌ Ошибка создания device_auth ({resp.status_code}): {error_msg}",
                }

            data = resp.json()

            return {
                "ok": True,
                "device_id": data.get("deviceId"),
                "device_secret": data.get("secret"),
            }

        except Exception as e:
            return {
                "ok": False,
                "message": f"❌ Ошибка при создании device_auth: {str(e)}",
            }

    def close(self):
        """Закрыть session."""
        if hasattr(self, "session"):
            self.session.close()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        self.close()


# ============================================================
# Публичная функция
# ============================================================

def generate_device_auth_for_account(
    login: str,
    password: str,
    proxy_url: Optional[str] = None,
) -> Tuple[bool, Optional[Dict], str]:
    """
    Публичная функция для получения device_auth.

    Usage:
        ok, device_auth, msg = generate_device_auth_for_account(
            login="user@example.com",
            password="pass123",
            proxy_url="http://proxy:8080"
        )
    """
    with DeviceAuthGenerator(login, password, proxy_url) as gen:
        return gen.generate()


# ============================================================
# CLI
# ============================================================

def main_cli():
    """CLI для генерации device_auth."""
    print("\n" + "=" * 60)
    print("🔐 Epic Games Device Auth Generator")
    print("=" * 60)

    login = input("\nЭмейл или никнейм Epic Games: ").strip()
    if not login:
        print("❌ Логин не может быть пустым")
        return

    password = input("Пароль: ").strip()
    if not password:
        print("❌ Пароль не может быть пустым")
        return

    use_proxy = input("\nИспользовать прокси? (y/n): ").strip().lower()
    proxy_url = None
    if use_proxy == "y":
        proxy_url = input("URL прокси (http://ip:port): ").strip()
        if not proxy_url.startswith(("http://", "https://")):
            proxy_url = f"http://{proxy_url}"

    print("\n⏳ Генерирую device_auth (это может занять до 30 сек)...")

    ok, device_auth, msg = generate_device_auth_for_account(login, password, proxy_url)

    print(f"\n{msg}")

    if ok:
        print("\n" + "=" * 60)
        print("✅ Успешно! Вот твои device_auth данные:")
        print("=" * 60)

        output = {
            "login": login,
            "epic_account_id": device_auth["epic_account_id"],
            "device_id": device_auth["device_id"],
            "device_secret": device_auth["device_secret"],
            "generated_at": datetime.now().isoformat(),
        }

        print("\n📋 JSON (для copy-paste в оркестратор):")
        print(json.dumps(output, indent=2, ensure_ascii=False))

        save_to_file = input("\n💾 Сохранить в файл? (y/n): ").strip().lower()
        if save_to_file == "y":
            filename = f"device_auth_{login.split('@')[0]}.json"
            try:
                with open(filename, "w", encoding="utf-8") as f:
                    json.dump(output, f, indent=2, ensure_ascii=False)
                print(f"✅ Сохранено в {filename}")
            except Exception as e:
                print(f"❌ Ошибка при сохранении: {e}")
    else:
        print("\n❌ Не удалось получить device_auth")
        print("   Проверь логин/пароль и попробуй ещё раз")


if __name__ == "__main__":
    main_cli()
