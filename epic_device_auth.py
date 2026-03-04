# epic_device_auth.py
# -*- coding: utf-8 -*-

import asyncio
import json
import os
import platform
import threading
from dataclasses import dataclass
from typing import Any, Dict, Optional, Tuple

import aiohttp


# OAuth app credentials (берём из env; fallback оставлен для обратной совместимости).
SWITCH_TOKEN = os.getenv(
    "EPIC_SWITCH_TOKEN",
    "OThmN2U0MmMyZTNhNGY4NmE3NGViNDNmYmI0MWVkMzk6MGEyNDQ5YTItMDAxYS00NTFlLWFmZWMtM2U4MTI5MDFjNGQ3",
).strip()
ANDROID_TOKEN = os.getenv(
    "EPIC_ANDROID_TOKEN",
    "M2Y2OWU1NmM3NjQ5NDkyYzhjYzI5ZjFhZjA4YThhMTI6YjUxZWU5Y2IxMjIzNGY1MGE2OWVmYTY3ZWY1MzgxMmU=",
).strip()

VERSION = "1.0.0-bot"

# endpoints (как в твоём generator.py)
ACCOUNT_PROD03 = "https://account-public-service-prod03.ol.epicgames.com"
ACCOUNT_PROD = "https://account-public-service-prod.ol.epicgames.com"

_DEFAULT_DEVICE_AUTHS_PATH = "device_auths.json"
_FILE_LOCK = threading.Lock()


@dataclass
class DeviceAuthResult:
    email: str
    display_name: str
    epic_account_id: str
    device_id: str
    device_secret: str
    raw: Dict[str, Any]


class EpicDeviceAuthGenerator:
    """
    2 шага:
      1) create_login_link() -> (url, device_code)
      2) complete_login(device_code) -> DeviceAuthResult
    """

    def __init__(self, poll_interval_sec: int = 10):
        self.poll_interval_sec = max(3, int(poll_interval_sec))
        self.http: Optional[aiohttp.ClientSession] = None
        self.client_access_token: str = ""
        self.user_agent = f"EpicBotDeviceAuth/{VERSION} {platform.system()}/{platform.version()}"

    async def __aenter__(self):
        self.http = aiohttp.ClientSession(headers={"User-Agent": self.user_agent})
        self.client_access_token = await self._get_client_access_token()
        return self

    async def __aexit__(self, exc_type, exc, tb):
        if self.http is not None:
            await self.http.close()
            self.http = None

    async def _get_client_access_token(self) -> str:
        assert self.http is not None
        url = f"{ACCOUNT_PROD}/account/api/oauth/token"
        async with self.http.request(
            method="POST",
            url=url,
            headers={
                "Content-Type": "application/x-www-form-urlencoded",
                "Authorization": f"basic {SWITCH_TOKEN}",
            },
            data={"grant_type": "client_credentials"},
        ) as r:
            data = await r.json()
            if r.status != 200:
                raise RuntimeError(f"client_credentials failed: {r.status} {data}")
            return data["access_token"]

    async def create_login_link(self) -> Tuple[str, str]:
        """
        Возвращает (verification_uri_complete, device_code)
        """
        assert self.http is not None
        url = f"{ACCOUNT_PROD03}/account/api/oauth/deviceAuthorization"
        async with self.http.request(
            method="POST",
            url=url,
            headers={
                "Authorization": f"bearer {self.client_access_token}",
                "Content-Type": "application/x-www-form-urlencoded",
            },
        ) as r:
            data = await r.json()
            if r.status != 200:
                raise RuntimeError(f"deviceAuthorization failed: {r.status} {data}")
            return data["verification_uri_complete"], data["device_code"]

    async def complete_login(self, device_code: str) -> DeviceAuthResult:
        """
        Ждёт, пока ты залогинишься по ссылке, затем создаёт deviceAuth и возвращает его.
        """
        token = await self._wait_device_code(device_code=device_code)
        exchange_code = await self._get_exchange_code(access_token=token["access_token"])
        android_token = await self._exchange_for_android_token(exchange_code=exchange_code)

        account_id = android_token.get("account_id") or android_token.get("accountId")
        access_token = android_token["access_token"]

        profile = await self._get_account_profile(access_token=access_token, account_id=account_id)
        device_auth = await self._create_device_auth(access_token=access_token, account_id=account_id)

        # Нормализуем под твою БД/клиент
        result = DeviceAuthResult(
            email=profile.get("email", ""),
            display_name=profile.get("display_name", ""),
            epic_account_id=device_auth["account_id"],
            device_id=device_auth["device_id"],
            device_secret=device_auth["secret"],
            raw=device_auth,
        )
        return result

    async def _wait_device_code(self, device_code: str) -> Dict[str, Any]:
        assert self.http is not None
        url = f"{ACCOUNT_PROD03}/account/api/oauth/token"

        while True:
            async with self.http.request(
                method="POST",
                url=url,
                headers={
                    "Authorization": f"basic {SWITCH_TOKEN}",
                    "Content-Type": "application/x-www-form-urlencoded",
                },
                data={"grant_type": "device_code", "device_code": device_code},
            ) as r:
                data = await r.json()

                if r.status == 200:
                    return data

                # ожидаем логин
                code = data.get("errorCode") or data.get("error")
                if code in (
                    "errors.com.epicgames.account.oauth.authorization_pending",
                    "authorization_pending",
                    "errors.com.epicgames.not_found",
                    "not_found",
                ):
                    await asyncio.sleep(self.poll_interval_sec)
                    continue

                # “slow_down” иногда встречается в device flow
                if code in ("slow_down", "errors.com.epicgames.account.oauth.slow_down"):
                    await asyncio.sleep(self.poll_interval_sec + 5)
                    continue

                raise RuntimeError(f"device_code polling failed: {r.status} {data}")

    async def _get_exchange_code(self, access_token: str) -> str:
        assert self.http is not None
        url = f"{ACCOUNT_PROD03}/account/api/oauth/exchange"
        async with self.http.request(
            method="GET",
            url=url,
            headers={"Authorization": f"bearer {access_token}"},
        ) as r:
            data = await r.json()
            if r.status != 200:
                raise RuntimeError(f"exchange failed: {r.status} {data}")
            return data["code"]

    async def _exchange_for_android_token(self, exchange_code: str) -> Dict[str, Any]:
        assert self.http is not None
        url = f"{ACCOUNT_PROD03}/account/api/oauth/token"
        async with self.http.request(
            method="POST",
            url=url,
            headers={
                "Authorization": f"basic {ANDROID_TOKEN}",
                "Content-Type": "application/x-www-form-urlencoded",
            },
            data={"grant_type": "exchange_code", "exchange_code": exchange_code},
        ) as r:
            data = await r.json()
            if r.status != 200:
                raise RuntimeError(f"exchange_code->android token failed: {r.status} {data}")
            return data

    async def _create_device_auth(self, access_token: str, account_id: str) -> Dict[str, Any]:
        assert self.http is not None
        url = f"{ACCOUNT_PROD}/account/api/public/account/{account_id}/deviceAuth"
        async with self.http.request(
            method="POST",
            url=url,
            headers={
                "Authorization": f"bearer {access_token}",
                "Content-Type": "application/json",
            },
        ) as r:
            data = await r.json()
            if r.status != 200 and r.status != 201:
                raise RuntimeError(f"deviceAuth create failed: {r.status} {data}")

            return {
                "device_id": data["deviceId"],
                "account_id": data["accountId"],
                "secret": data["secret"],
                "user_agent": data.get("userAgent", ""),
                "created": {
                    "location": (data.get("created") or {}).get("location"),
                    "ip_address": (data.get("created") or {}).get("ipAddress"),
                    "datetime": (data.get("created") or {}).get("dateTime"),
                },
            }

    async def _get_account_profile(self, access_token: str, account_id: str) -> Dict[str, str]:
        assert self.http is not None
        url = f"{ACCOUNT_PROD03}/account/api/public/account/{account_id}"
        async with self.http.request(
            method="GET",
            url=url,
            headers={"Authorization": f"bearer {access_token}"},
        ) as r:
            data = await r.json()
            if r.status != 200:
                raise RuntimeError(f"get account info failed: {r.status} {data}")
            email = data.get("email")
            if not email:
                raise RuntimeError(f"email missing in account info: {data}")
            return {
                "email": str(email),
                "display_name": str(data.get("displayName") or ""),
            }


def append_device_auth_to_file(email: str, device_auth_raw: Dict[str, Any], path: str = _DEFAULT_DEVICE_AUTHS_PATH) -> None:
    """
    Дописывает/обновляет одну запись в общем device_auths.json (ключ = email).
    Thread-safe на уровне процесса.
    """
    with _FILE_LOCK:
        if os.path.isfile(path):
            try:
                with open(path, "r", encoding="utf-8") as fp:
                    current = json.load(fp) or {}
            except Exception:
                current = {}
        else:
            current = {}

        current[email] = device_auth_raw

        with open(path, "w", encoding="utf-8") as fp:
            json.dump(current, fp, sort_keys=False, indent=4, ensure_ascii=False)
