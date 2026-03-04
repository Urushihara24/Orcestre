import logging
import os
import re
import time
import base64
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Any, Dict, Optional, Tuple, Callable
from urllib.parse import quote, urlparse

import requests
from requests.adapters import HTTPAdapter

# ============================================================
# ЛОГИРОВАНИЕ С ФИЛЬТРАЦИЕЙ ЧУВСТВИТЕЛЬНЫХ ДАННЫХ
# ============================================================

class SensitiveDataFilter(logging.Filter):
    """Фильтр для удаления чувствительных данных из логов"""

    SENSITIVE_PATTERNS = [
        (r'password["\']?\s*:\s*["\']?([^"\'\s,}]+)', 'password: ***'),
        (r'Bearer\s+[^\s]+', 'Bearer ***'),
        (r'client_secret["\']?\s*:\s*["\']?([^"\'\s,}]+)', 'client_secret: ***'),
        (r'access_token["\']?\s*:\s*["\']?([^"\'\s,}]+)', 'access_token: ***'),
        (r'refresh_token["\']?\s*:\s*["\']?([^"\'\s,}]+)', 'refresh_token: ***'),
    ]

    def filter(self, record: logging.LogRecord) -> bool:
        message = record.getMessage()
        for pattern, replacement in self.SENSITIVE_PATTERNS:
            message = re.sub(pattern, replacement, message, flags=re.IGNORECASE)
        record.msg = message
        record.args = ()
        return True


logger = logging.getLogger(__name__)
logger.addFilter(SensitiveDataFilter())

# ============================================================
# HELPERS
# ============================================================

def _mask_email(email: str) -> str:
    """Замаскировать email для логирования"""
    if not email:
        return "***"
    if "@" not in email:
        return (email[:3] + "***") if len(email) > 3 else "***"
    local, domain = email.split("@", 1)
    local_m = (local[:3] + "***") if len(local) > 3 else "***"
    dom_head = domain.split(".", 1)[0]
    dom_m = (dom_head[:3] + "***") if len(dom_head) > 3 else "***"
    return f"{local_m}@{dom_m}"


def _now_ts() -> float:
    return datetime.now().timestamp()


def _format_epic_error_details(resp_data: Optional[Dict[str, Any]]) -> str:
    """Собрать краткие детали ошибки Epic для логов/last_error."""
    if not isinstance(resp_data, dict):
        return ""

    parts = []
    for key in ("errorCode", "error", "numericErrorCode", "errorMessage", "error_description"):
        val = resp_data.get(key)
        if val is None or val == "":
            continue
        parts.append(f"{key}={val}")
    return " | ".join(parts)


# ============================================================
# DATA MODELS
# ============================================================

@dataclass
class ProviderResult:
    """Единая структура результата всех операций"""
    ok: bool
    code: str
    message: str
    data: Optional[Dict[str, Any]] = None


# ============================================================
# EPIC GAMES API CLIENT (LONG-LIVED)
# ============================================================

class EpicGamesAPIClient:
    """
    Долго живущий клиент для работы с Epic Games API под задачи бота:
    - хранит логин/пароль и сам переавторизуется при истечении токена
    - password auth + device_auth
    - поиск accountId по displayName
    - отправка friend request (идемпотентно)
    - проверка статуса дружбы через summary
    """

    # Bases
    ACCOUNT_BASE = "https://account-public-service-prod.ol.epicgames.com"
    FRIENDS_BASE = "https://friends-public-service-prod.ol.epicgames.com"

    # OAuth credentials (можно переопределить env, чтобы не хардкодить)
    CLIENT_ID = os.getenv("EPIC_CLIENT_ID", "34a02cf8f4414e29b15921876da36f9a").strip()
    CLIENT_SECRET = os.getenv("EPIC_CLIENT_SECRET", "daafbccc737745039dffe53d94fc76cf").strip()

    # Константы
    TOKEN_EXPIRY_BUFFER = 300  # 5 минут
    MAX_RETRIES = 3
    RETRY_DELAY = 1  # seconds

    def __init__(
        self,
        login: str,
        password: str,
        proxy_url: Optional[str] = None,
        timeout: int = 15,
        max_retries: int = MAX_RETRIES,
        epic_account_id: Optional[str] = None,
        device_id: Optional[str] = None,
        device_secret: Optional[str] = None,
        allow_password_fallback: bool = True,
    ):
        """
        ВАЖНО: Класс хранит логин/пароль в памяти, так что лучше создавать
        по одному клиенту на аккаунт и не шарить между потоками без нужды.
        """
        if not login or not password:
            raise ValueError("login and password are required for long-lived client")

        self._login = login
        self._password = password

        # device_auth (опционально)
        self._epic_account_id = epic_account_id
        self._device_id = device_id
        self._device_secret = device_secret
        self._allow_password_fallback = bool(allow_password_fallback)

        self.proxy_url = proxy_url
        self.timeout = timeout
        self.max_retries = max_retries

        # Валидация прокси URL
        if proxy_url:
            result = urlparse(proxy_url)
            if not result.scheme or result.scheme not in ("http", "https"):  # socks убираем для простоты
                logger.warning(f"⚠️ Неизвестный тип прокси или не поддерживается без PySocks: {result.scheme}")
            if not result.netloc:
                raise ValueError(f"Неверный URL прокси: {proxy_url}")

        self.session = self._create_session()

        # Кэш токена
        self.access_token: Optional[str] = None
        self.refresh_token: Optional[str] = None
        self.token_expires_at: Optional[float] = None
        self.account_id: Optional[str] = None

        logger.info(f"🔧 Epic API Client инициализирован (proxy: {bool(proxy_url)})")

    # --- lifecycle ---

    def __enter__(self) -> "EpicGamesAPIClient":
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        self.close()

    def __del__(self):
        # best-effort: __del__ не гарантируется
        try:
            self.close()
        except Exception as e:
            logger.debug(f"close in __del__ failed: {e}")

    def close(self) -> None:
        """Явно закрыть session и стереть чувствительные данные."""
        self.access_token = None
        self.refresh_token = None
        self.token_expires_at = None
        self.account_id = None
        if getattr(self, "session", None) is not None:
            try:
                self.session.close()
            except Exception as e:
                logger.debug(f"session.close failed: {e}")

    # --- internals ---

    def _create_session(self) -> requests.Session:
        """
        Создать requests.Session.

        Важно: не включаем urllib3 Retry (иначе можно получить двойной retry на POST),
        retry делаем вручную в _make_request.
        """
        s = requests.Session()
        adapter = HTTPAdapter(max_retries=0)
        s.mount("http://", adapter)
        s.mount("https://", adapter)

        if self.proxy_url:
            s.proxies = {"http": self.proxy_url, "https": self.proxy_url}

        return s

    def _is_token_expired(self) -> bool:
        if not self.token_expires_at or not self.access_token:
            return True
        return (self.token_expires_at - _now_ts()) < self.TOKEN_EXPIRY_BUFFER

    def _safe_json_parse(self, response: requests.Response) -> Dict[str, Any]:
        if not response.content:
            return {}
        try:
            return response.json()
        except Exception:
            txt = (response.text or "").strip()
            if not txt:
                return {}
            return {"errorMessage": txt[:400]}

    def _sleep_before_retry(self, resp: Optional[requests.Response], attempt: int) -> None:
        retry_after = None
        if resp is not None:
            ra = resp.headers.get("Retry-After")
            if ra:
                # поддерживаем и секунды, и HTTP-date
                try:
                    retry_after = float(ra)
                except Exception:
                    retry_after = None

        if retry_after is None:
            retry_after = float(self.RETRY_DELAY * (2 ** (attempt - 1)))

        time.sleep(max(0.2, retry_after))

    def _make_request(
        self,
        method: str,
        url: str,
        attempt: int = 1,
        **kwargs
    ) -> Tuple[bool, Optional[Dict[str, Any]], str]:
        """
        Выполнить HTTP запрос с retry и обработкой ошибок.

        Returns:
            (success, data_or_error_json, error_code)
        """
        resp: Optional[requests.Response] = None
        try:
            kwargs.setdefault("timeout", self.timeout)

            resp = self.session.request(method, url, **kwargs)

            if resp.status_code in (200, 201, 204):
                return True, self._safe_json_parse(resp), "ok"

            data = self._safe_json_parse(resp)
            status = resp.status_code

            error_map = {
                400: "bad_request",
                401: "unauthorized",
                403: "forbidden",
                404: "not_found",
                409: "conflict",
                429: "rate_limited",
                500: "server_error",
                502: "bad_gateway",
                503: "service_unavailable",
                504: "gateway_timeout",
            }
            code = error_map.get(status, f"http_{status}")

            # Retry только на 429/5xx
            if status in (429, 500, 502, 503, 504) and attempt < self.max_retries:
                self._sleep_before_retry(resp, attempt)
                return self._make_request(method, url, attempt=attempt + 1, **kwargs)

            return False, data, code

        except requests.Timeout:
            if attempt < self.max_retries:
                self._sleep_before_retry(None, attempt)
                return self._make_request(method, url, attempt=attempt + 1, **kwargs)
            return False, None, "timeout"

        except requests.ConnectionError:
            if attempt < self.max_retries:
                self._sleep_before_retry(None, attempt)
                return self._make_request(method, url, attempt=attempt + 1, **kwargs)
            return False, None, "connection_error"

        except Exception:
            return False, None, "unknown_error"

        finally:
            try:
                if resp is not None:
                    resp.close()
            except Exception as e:
                logger.debug(f"response.close failed: {e}")

    # ============================================================
    # AUTH / TOKEN MANAGEMENT
    # ============================================================

    def _auth_password(self) -> ProviderResult:
        """Авторизация с логином и паролем (внутренняя)."""
        login = self._login
        password = self._password

        if not login or not password:
            return ProviderResult(False, "invalid_credentials", "Login and password are required")

        basic = base64.b64encode(f"{self.CLIENT_ID}:{self.CLIENT_SECRET}".encode("utf-8")).decode("utf-8")
        url = f"{self.ACCOUNT_BASE}/account/api/oauth/token"
        headers = {
            "Authorization": f"Basic {basic}",
            "Content-Type": "application/x-www-form-urlencoded",
            "User-Agent": "EpicGamesClient/2.2-bot-long-lived",
        }
        data = {
            "grant_type": "password",
            "username": login,
            "password": password,
        }

        success, resp_data, error_code = self._make_request("POST", url, headers=headers, data=data)
        details = _format_epic_error_details(resp_data)

        if not success:
            # Epic often blocks password grant for many clients. Treat it as a separate, actionable code.
            if isinstance(resp_data, dict):
                em = str(resp_data.get("errorMessage") or resp_data.get("error_description") or "")
                if "grant type password" in em.lower():
                    message = "Password grant is blocked by Epic for this client. Use device_auth login-link."
                    if details:
                        message = f"{message} ({details})"
                    return ProviderResult(False, "password_grant_blocked", message)
            if error_code in ("unauthorized", "forbidden"):
                message = "Invalid credentials or access denied"
                if details:
                    message = f"{message} ({details})"
                return ProviderResult(False, "auth_failed", message)
            if error_code == "rate_limited":
                message = "Too many login attempts"
                if details:
                    message = f"{message} ({details})"
                return ProviderResult(False, "rate_limited", message)
            if error_code in ("timeout", "connection_error"):
                return ProviderResult(False, error_code, f"Network error: {error_code}")
            message = f"Auth error: {error_code}"
            if details:
                message = f"{message} ({details})"
            return ProviderResult(False, "auth_failed", message)

        if not isinstance(resp_data, dict) or "access_token" not in resp_data:
            return ProviderResult(False, "auth_failed", "Invalid authentication response")

        try:
            self.access_token = str(resp_data["access_token"])
            self.refresh_token = str(resp_data.get("refresh_token", "") or "")
            self.account_id = str(resp_data.get("account_id", "") or "")

            expires_in = int(resp_data.get("expires_in", 28800))
            self.token_expires_at = (datetime.now() + timedelta(seconds=expires_in)).timestamp()

            logger.info(f"✅ Авторизация по паролю успешна: {_mask_email(login)}")

            return ProviderResult(
                True,
                "authenticated",
                "Authentication successful",
                data={
                    "access_token": self.access_token,
                    "refresh_token": self.refresh_token,
                    "account_id": self.account_id,
                    "expires_in": expires_in,
                },
            )
        except Exception:
            return ProviderResult(False, "auth_failed", "Failed to parse authentication response")

    def _auth_device_auth(
        self,
        account_id: str,
        device_id: str,
        device_secret: str,
    ) -> ProviderResult:
        """Авторизация через device_auth."""
        if not account_id or not device_id or not device_secret:
            return ProviderResult(False, "invalid_device_auth", "Device auth is missing")

        basic = base64.b64encode(f"{self.CLIENT_ID}:{self.CLIENT_SECRET}".encode("utf-8")).decode("utf-8")
        url = f"{self.ACCOUNT_BASE}/account/api/oauth/token"
        headers = {
            "Authorization": f"Basic {basic}",
            "Content-Type": "application/x-www-form-urlencoded",
            "User-Agent": "EpicGamesClient/2.2-bot-long-lived",
        }
        data = {
            "grant_type": "device_auth",
            "account_id": account_id,
            "device_id": device_id,
            "secret": device_secret,
        }

        success, resp_data, error_code = self._make_request("POST", url, headers=headers, data=data)
        details = _format_epic_error_details(resp_data)

        if not success:
            if error_code in ("unauthorized", "forbidden"):
                message = "Invalid device_auth or access denied"
                if details:
                    message = f"{message} ({details})"
                return ProviderResult(False, "auth_failed", message)
            if error_code == "rate_limited":
                message = "Too many login attempts"
                if details:
                    message = f"{message} ({details})"
                return ProviderResult(False, "rate_limited", message)
            if error_code in ("timeout", "connection_error"):
                return ProviderResult(False, error_code, f"Network error: {error_code}")
            message = f"Auth error: {error_code}"
            if details:
                message = f"{message} ({details})"
            return ProviderResult(False, "auth_failed", message)

        if not isinstance(resp_data, dict) or "access_token" not in resp_data:
            return ProviderResult(False, "auth_failed", "Invalid authentication response")

        try:
            self.access_token = str(resp_data["access_token"])
            self.refresh_token = str(resp_data.get("refresh_token", "") or "")
            # если API не вернул account_id, используем тот, что в device_auth
            self.account_id = str(resp_data.get("account_id", "") or account_id)

            expires_in = int(resp_data.get("expires_in", 28800))
            self.token_expires_at = (datetime.now() + timedelta(seconds=expires_in)).timestamp()

            logger.info("✅ Авторизация по device_auth успешна")

            return ProviderResult(
                True,
                "authenticated",
                "Authentication successful",
                data={
                    "access_token": self.access_token,
                    "refresh_token": self.refresh_token,
                    "account_id": self.account_id,
                    "expires_in": expires_in,
                },
            )
        except Exception:
            return ProviderResult(False, "auth_failed", "Failed to parse authentication response")

    def ensure_token(self) -> ProviderResult:
        """
        Убедиться, что есть валидный access_token.
        Если токен отсутствует или почти истёк — переавторизуемся.
        Приоритет: device_auth -> пароль.
        """
        if not self._is_token_expired():
            return ProviderResult(
                True,
                "token_ok",
                "Token is valid",
                data={
                    "access_token": self.access_token,
                    "account_id": self.account_id,
                },
            )

        # 1) Если есть device_auth — пробуем его
        if self._epic_account_id and self._device_id and self._device_secret:
            auth = self._auth_device_auth(
                self._epic_account_id,
                self._device_id,
                self._device_secret,
            )
            if auth.ok:
                return auth
            if not self._allow_password_fallback:
                logger.debug(f"device_auth failed (no fallback): {auth.code} {auth.message}")
                return auth
            logger.warning(f"❌ Device auth failed: {auth.code} {auth.message}")

        # 2) Fallback: пароль
        if not self._allow_password_fallback:
            return ProviderResult(False, "auth_failed", "Password fallback disabled")
        auth = self._auth_password()
        if not auth.ok:
            logger.warning(f"❌ Не удалось обновить токен: {auth.code} {auth.message}")
        return auth

    def _with_token_retry(
        self,
        action: Callable[[str, str], ProviderResult]
    ) -> ProviderResult:
        """
        Обёртка: гарантирует токен, при 401/403 пытается один раз переавторизоваться.
        action(access_token, account_id) -> ProviderResult
        """
        # 1) Убедиться, что токен есть
        auth = self.ensure_token()
        if not auth.ok:
            return auth

        access_token = str(auth.data.get("access_token") or "")
        account_id = str(auth.data.get("account_id") or "")
        if not access_token or not account_id:
            return ProviderResult(False, "auth_failed", "Failed to retrieve access token/account id")

        # 2) Первый вызов
        res = action(access_token, account_id)
        if res.code not in ("auth_failed", "unauthorized", "forbidden"):
            return res

        # 3) Если проблема с авторизацией — один раз обновить токен и повторить
        logger.info("♻️ Токен, возможно, истёк. Переавторизация и повтор операции.")
        # Принудительно сбрасываем токен, чтобы ensure_token не вернул тот же "token_ok".
        # Это важно при server-side revoke, когда expires_at ещё не наступил.
        self.access_token = None
        self.token_expires_at = None
        auth2 = self.ensure_token()
        if not auth2.ok:
            return auth2

        access_token = str(auth2.data.get("access_token") or "")
        account_id = str(auth2.data.get("account_id") or "")
        if not access_token or not account_id:
            return ProviderResult(
                False,
                "auth_failed",
                "Failed to retrieve access token/account id (after refresh)",
            )

        return action(access_token, account_id)

    # ============================================================
    # PUBLIC METHODS (API)
    # ============================================================

    def get_account_info(self) -> ProviderResult:
        """Получить информацию об аккаунте текущего клиента"""

        def _impl(access_token: str, account_id: str) -> ProviderResult:
            url = f"{self.ACCOUNT_BASE}/account/api/public/account/{quote(account_id, safe='')}"
            headers = {"Authorization": f"Bearer {access_token}"}

            success, resp_data, error_code = self._make_request("GET", url, headers=headers)

            if not success:
                if error_code in ("unauthorized", "forbidden"):
                    return ProviderResult(False, "auth_failed", "Token invalid or expired")
                return ProviderResult(False, error_code, f"Failed to get account info: {error_code}")

            if not isinstance(resp_data, dict) or not resp_data:
                return ProviderResult(False, "invalid_response", "Empty response")

            return ProviderResult(
                True,
                "account_info",
                "Account info retrieved",
                data={
                    "account_id": resp_data.get("id", ""),
                    "display_name": resp_data.get("displayName", ""),
                    "email": resp_data.get("email", ""),
                },
            )

        return self._with_token_retry(_impl)

    def get_user_by_name(self, username: str) -> ProviderResult:
        """Получить accountId пользователя по displayName"""
        if not username:
            return ProviderResult(False, "invalid_username", "Username is required")

        def _impl(access_token: str, account_id: str) -> ProviderResult:
            url = f"{self.ACCOUNT_BASE}/account/api/public/account/displayName/{quote(username, safe='')}"
            headers = {"Authorization": f"Bearer {access_token}"}

            success, resp_data, error_code = self._make_request("GET", url, headers=headers)

            if not success:
                if error_code == "not_found":
                    return ProviderResult(False, "user_not_found", f"User '{username}' not found")
                if error_code in ("unauthorized", "forbidden"):
                    return ProviderResult(False, "auth_failed", "Token invalid or expired")
                return ProviderResult(False, error_code, f"Failed to get user: {error_code}")

            if not isinstance(resp_data, dict) or "id" not in resp_data:
                return ProviderResult(False, "invalid_response", "Invalid user data in response")

            return ProviderResult(
                True,
                "user_found",
                "User found",
                data={
                    "user_id": resp_data.get("id", ""),
                    "display_name": resp_data.get("displayName", ""),
                },
            )

        return self._with_token_retry(_impl)

    def send_friend_request(self, target_id: str) -> ProviderResult:
        """Отправить заявку в друзья (idempotent под бота)"""
        if not target_id:
            return ProviderResult(False, "invalid_target", "Target ID is required")

        def _impl(access_token: str, my_account_id: str) -> ProviderResult:
            url = (
                f"{self.FRIENDS_BASE}/friends/api/v1/"
                f"{quote(my_account_id, safe='')}/friends/{quote(target_id, safe='')}"
            )
            headers = {"Authorization": f"Bearer {access_token}"}

            success, resp_data, error_code = self._make_request("POST", url, headers=headers)

            if not success:
                # Идемпотентность: если уже друзья/уже отправлено, считаем как "request_sent"
                if error_code in ("conflict", "bad_request"):
                    return ProviderResult(
                        True,
                        "request_sent",
                        "Friend request already sent or already friends",
                        data={"target_id": target_id, "note": "idempotent_success"},
                    )

                if error_code in ("unauthorized", "forbidden"):
                    return ProviderResult(False, "auth_failed", "Token invalid/expired or access denied")

                if error_code == "rate_limited":
                    return ProviderResult(False, "rate_limited", "Rate limited while sending request")

                msg = ""
                if isinstance(resp_data, dict):
                    msg = (
                        resp_data.get("errorMessage")
                        or resp_data.get("error_description")
                        or resp_data.get("error")
                        or ""
                    )
                return ProviderResult(
                    False,
                    error_code,
                    msg or f"Send failed: {error_code}",
                    data=resp_data if isinstance(resp_data, dict) else None,
                )

            logger.info(f"✅ Friend request sent: {target_id}")
            return ProviderResult(
                True,
                "request_sent",
                "Friend request sent successfully",
                data={"target_id": target_id},
            )

        return self._with_token_retry(_impl)

    def get_friend_status(self, target_id: str) -> ProviderResult:
        """
        Проверить статус через summary:
        - friends -> accepted
        - outgoing/incoming -> pending
        - нигде нет -> rejected
        """
        if not target_id:
            return ProviderResult(False, "invalid_target", "Target ID is required")

        def _impl(access_token: str, my_account_id: str) -> ProviderResult:
            url = f"{self.FRIENDS_BASE}/friends/api/v1/{quote(my_account_id, safe='')}/summary"
            headers = {"Authorization": f"Bearer {access_token}"}

            success, resp_data, error_code = self._make_request("GET", url, headers=headers)

            if not success:
                if error_code in ("unauthorized", "forbidden"):
                    return ProviderResult(False, "auth_failed", "Token invalid/expired or access denied")
                return ProviderResult(False, error_code, f"Failed to check status: {error_code}")

            if not isinstance(resp_data, dict):
                return ProviderResult(False, "invalid_response", "Invalid summary response")

            def _contains(lst: Any) -> bool:
                if not isinstance(lst, list):
                    return False
                for item in lst:
                    if isinstance(item, dict) and (
                        item.get("accountId") == target_id
                        or item.get("account_id") == target_id
                    ):
                        return True
                return False

            friends = resp_data.get("friends")
            outgoing = resp_data.get("outgoing")
            incoming = resp_data.get("incoming")

            if _contains(friends):
                return ProviderResult(
                    True,
                    "accepted",
                    "Status checked",
                    data={"status": "ACCEPTED", "target_id": target_id},
                )
            if _contains(outgoing) or _contains(incoming):
                return ProviderResult(
                    True,
                    "pending",
                    "Status checked",
                    data={"status": "PENDING", "target_id": target_id},
                )

            return ProviderResult(
                True,
                "rejected",
                "Status checked",
                data={"status": "NOT_FRIENDS", "target_id": target_id},
            )

        return self._with_token_retry(_impl)

    def _delete_friend_link(self, target_id: str) -> ProviderResult:
        """
        Удалить связь friend/outgoing/incoming.
        Epic использует один DELETE endpoint для:
        - удаления из друзей
        - отмены исходящей заявки
        - отклонения входящей заявки
        """
        if not target_id:
            return ProviderResult(False, "invalid_target", "Target ID is required")

        def _impl(access_token: str, my_account_id: str) -> ProviderResult:
            url = (
                f"{self.FRIENDS_BASE}/friends/api/v1/"
                f"{quote(my_account_id, safe='')}/friends/{quote(target_id, safe='')}"
            )
            headers = {"Authorization": f"Bearer {access_token}"}

            success, resp_data, error_code = self._make_request("DELETE", url, headers=headers)
            if success:
                return ProviderResult(
                    True,
                    "friend_link_deleted",
                    "Friend link deleted",
                    data={"target_id": target_id},
                )

            # Идемпотентность: если связи уже нет, считаем успешным результатом.
            if error_code in ("not_found", "conflict", "bad_request"):
                return ProviderResult(
                    True,
                    "friend_link_deleted",
                    "Friend link already absent",
                    data={"target_id": target_id, "note": "idempotent_success"},
                )

            if error_code in ("unauthorized", "forbidden"):
                return ProviderResult(False, "auth_failed", "Token invalid/expired or access denied")
            if error_code == "rate_limited":
                return ProviderResult(False, "rate_limited", "Rate limited while deleting friend link")
            return ProviderResult(False, error_code, f"Delete friend link failed: {error_code}")

        return self._with_token_retry(_impl)

    def cancel_friend_request(self, target_id: str) -> ProviderResult:
        """Отозвать исходящую заявку (идемпотентно)."""
        return self._delete_friend_link(target_id)

    def remove_friend(self, target_id: str) -> ProviderResult:
        """Удалить пользователя из друзей (идемпотентно)."""
        return self._delete_friend_link(target_id)

    def verify_account_health(self) -> ProviderResult:
        """Проверить здоровье аккаунта (для долго живущего клиента)"""

        def _impl(access_token: str, account_id: str) -> ProviderResult:
            url = f"{self.ACCOUNT_BASE}/account/api/public/account/{quote(account_id, safe='')}"
            headers = {"Authorization": f"Bearer {access_token}"}
            success, resp_data, error_code = self._make_request("GET", url, headers=headers)

            if not success:
                if error_code in ("unauthorized", "forbidden"):
                    return ProviderResult(False, "account_banned", "Account banned or token invalid")
                return ProviderResult(False, "account_error", f"Failed to retrieve account info: {error_code}")

            if not isinstance(resp_data, dict) or not resp_data:
                return ProviderResult(False, "invalid_response", "Failed to retrieve account information")

            logger.info(f"✅ Account health check passed: {_mask_email(self._login)}")
            return ProviderResult(
                True,
                "account_healthy",
                "Account is healthy",
                data={
                    "account_id": resp_data.get("id", ""),
                    "display_name": resp_data.get("displayName", ""),
                    "email": resp_data.get("email", ""),
                },
            )

        return self._with_token_retry(_impl)


# ============================================================
# CONVENIENCE FUNCTIONS (обертки)
# ============================================================

def send_friend_request(
    login: str,
    password: str,
    target_username: str,
    proxy_url: Optional[str] = None,
) -> ProviderResult:
    """Одноразовая convenience-функция поверх долго живущего клиента (password-only)."""
    if not all([login, password, target_username]):
        return ProviderResult(False, "missing_params", "login, password, target_username are required")

    with EpicGamesAPIClient(login=login, password=password, proxy_url=proxy_url) as client:
        user = client.get_user_by_name(target_username)
        if not user.ok:
            return user
        if not user.data or "user_id" not in user.data:
            return ProviderResult(False, "invalid_response", "Failed to retrieve target user ID")
        return client.send_friend_request(str(user.data["user_id"]))


def verify_account_health(
    login: str,
    password: str,
    proxy_url: Optional[str] = None,
) -> ProviderResult:
    """Проверить здоровье аккаунта (password-only convenience-функция)."""
    if not login or not password:
        return ProviderResult(False, "missing_params", "login and password are required")

    with EpicGamesAPIClient(login=login, password=password, proxy_url=proxy_url) as client:
        return client.verify_account_health()


# --- новые обёртки с device_auth ---

def send_friend_request_with_device(
    login: str,
    password: str,
    target_username: str,
    proxy_url: Optional[str],
    epic_account_id: Optional[str],
    device_id: Optional[str],
    device_secret: Optional[str],
) -> ProviderResult:
    """Отправка заявки с возможностью использовать device_auth (если есть)."""
    if not all([login, password, target_username]):
        return ProviderResult(False, "missing_params", "login, password, target_username are required")

    with EpicGamesAPIClient(
        login=login,
        password=password,
        proxy_url=proxy_url,
        epic_account_id=epic_account_id or "",
        device_id=device_id or "",
        device_secret=device_secret or "",
        allow_password_fallback=False,
    ) as client:
        user = client.get_user_by_name(target_username)
        if not user.ok:
            return user
        if not user.data or "user_id" not in user.data:
            return ProviderResult(False, "invalid_response", "Failed to retrieve target user ID")
        return client.send_friend_request(str(user.data["user_id"]))


def check_friend_status_with_device(
    login: str,
    password: str,
    target_username: str,
    proxy_url: Optional[str],
    epic_account_id: Optional[str],
    device_id: Optional[str],
    device_secret: Optional[str],
) -> ProviderResult:
    """Проверка статуса заявки с опцией device_auth."""
    if not all([login, password, target_username]):
        return ProviderResult(False, "missing_params", "login, password, target_username are required")

    with EpicGamesAPIClient(
        login=login,
        password=password,
        proxy_url=proxy_url,
        epic_account_id=epic_account_id or "",
        device_id=device_id or "",
        device_secret=device_secret or "",
        allow_password_fallback=False,
    ) as client:
        user = client.get_user_by_name(target_username)
        if not user.ok:
            return user
        if not user.data or "user_id" not in user.data:
            return ProviderResult(False, "invalid_response", "Failed to retrieve target user ID")
        return client.get_friend_status(str(user.data["user_id"]))


def verify_account_health_with_device(
    login: str,
    password: str,
    proxy_url: Optional[str],
    epic_account_id: Optional[str],
    device_id: Optional[str],
    device_secret: Optional[str],
) -> ProviderResult:
    """Проверка здоровья аккаунта с учётом device_auth."""
    if not login or not password:
        return ProviderResult(False, "missing_params", "login and password are required")

    with EpicGamesAPIClient(
        login=login,
        password=password,
        proxy_url=proxy_url,
        epic_account_id=epic_account_id or "",
        device_id=device_id or "",
        device_secret=device_secret or "",
        allow_password_fallback=False,
    ) as client:
        return client.verify_account_health()


def cancel_friend_request_with_device(
    login: str,
    password: str,
    target_username: str,
    proxy_url: Optional[str],
    epic_account_id: Optional[str],
    device_id: Optional[str],
    device_secret: Optional[str],
) -> ProviderResult:
    """Отозвать исходящую заявку с опцией device_auth."""
    if not all([login, password, target_username]):
        return ProviderResult(False, "missing_params", "login, password, target_username are required")

    with EpicGamesAPIClient(
        login=login,
        password=password,
        proxy_url=proxy_url,
        epic_account_id=epic_account_id or "",
        device_id=device_id or "",
        device_secret=device_secret or "",
        allow_password_fallback=False,
    ) as client:
        user = client.get_user_by_name(target_username)
        if not user.ok:
            return user
        if not user.data or "user_id" not in user.data:
            return ProviderResult(False, "invalid_response", "Failed to retrieve target user ID")
        return client.cancel_friend_request(str(user.data["user_id"]))


def remove_friend_with_device(
    login: str,
    password: str,
    target_username: str,
    proxy_url: Optional[str],
    epic_account_id: Optional[str],
    device_id: Optional[str],
    device_secret: Optional[str],
) -> ProviderResult:
    """Удалить из друзей с опцией device_auth."""
    if not all([login, password, target_username]):
        return ProviderResult(False, "missing_params", "login, password, target_username are required")

    with EpicGamesAPIClient(
        login=login,
        password=password,
        proxy_url=proxy_url,
        epic_account_id=epic_account_id or "",
        device_id=device_id or "",
        device_secret=device_secret or "",
        allow_password_fallback=False,
    ) as client:
        user = client.get_user_by_name(target_username)
        if not user.ok:
            return user
        if not user.data or "user_id" not in user.data:
            return ProviderResult(False, "invalid_response", "Failed to retrieve target user ID")
        return client.remove_friend(str(user.data["user_id"]))


# ============================================================
# MAIN (минимальный self-test)
# ============================================================

if __name__ == "__main__":
    # Пример (не запускай с реальными кредами в публичном окружении)
    # export EPIC_LOGIN="email"
    # export EPIC_PASSWORD="pass"
    login = os.getenv("EPIC_LOGIN", "").strip()
    password = os.getenv("EPIC_PASSWORD", "").strip()
    target = os.getenv("EPIC_TARGET", "").strip()

    if not login or not password:
        print("Set EPIC_LOGIN and EPIC_PASSWORD env vars for self-test.")
        raise SystemExit(1)

    with EpicGamesAPIClient(login=login, password=password) as client:
        r = client.verify_account_health()
        print("verify_account_health:", r)

        if target:
            user = client.get_user_by_name(target)
            print("get_user_by_name:", user)
            if user.ok and user.data and "user_id" in user.data:
                rid = client.send_friend_request(user.data["user_id"])
                print("send_friend_request:", rid)
                st = client.get_friend_status(user.data["user_id"])
                print("get_friend_status:", st)
