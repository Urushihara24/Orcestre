# Multi Friends Epic Bot

Telegram bot for managing a pool of Epic Games accounts and sending friend requests to campaign targets.

<div align="center">

[![Python](https://img.shields.io/badge/Python-3.x-3776AB?style=for-the-badge&logo=python&logoColor=white)](https://www.python.org/)
[![Telegram](https://img.shields.io/badge/Telegram-Bot-26A5E4?style=for-the-badge&logo=telegram&logoColor=white)](https://core.telegram.org/bots)
[![SQLAlchemy](https://img.shields.io/badge/SQLAlchemy-ORM-D71F00?style=for-the-badge&logo=python&logoColor=white)](https://www.sqlalchemy.org/)
[![PostgreSQL](https://img.shields.io/badge/PostgreSQL-Database-4169E1?style=for-the-badge&logo=postgresql&logoColor=white)](https://www.postgresql.org/)
[![Docker](https://img.shields.io/badge/Docker-Compose-2496ED?style=for-the-badge&logo=docker&logoColor=white)](https://www.docker.com/)
[![pytest](https://img.shields.io/badge/pytest-Tested-0A9EDC?style=for-the-badge&logo=pytest&logoColor=white)](https://pytest.org/)

</div>

## 1. What the bot does

- imports sender accounts from `.xlsx/.txt/.csv`;
- connects accounts through Epic OAuth and stores `device_auth` in the database;
- manages targets/campaigns with independent settings;
- imports and edits recipient nicknames inside each target;
- schedules friend-request tasks inside configured time windows with jitter;
- supports recheck and sending missing requests;
- can revoke outgoing requests and remove users from friends;
- shows statistics by target, nickname and sender accounts for a specific nickname;
- supports forced send cycles from a selected account or a random account;
- supports restricted `auth-operator` access for account authorization only.

## 2. Target logic model

Each target has two primary parameters:

1. `На ник` — how many sender accounts per day should cover each recipient nickname.
2. `Алгоритм отправки`:
   - `sender_first` — one sender processes every nickname before moving to the next sender;
   - `target_first` — one nickname is covered by senders before moving to the next nickname.

Additional settings:

- `Джиттер` — random delay between send tasks;
- `Окна` — time windows when the target is active;
- `Аккаунты на recheck/сутки` — number of sender accounts participating in daily rechecks;
- `Ежедневный повтор` — repeat target coverage every day;
- `Порядок отправителей` — sender order by ID or randomized.

Timezone is always `Europe/Moscow`.

> The bot UI is currently Russian, so button and setting labels are kept exactly as they appear in Telegram.

## 3. Bot menu

### 3.1 Main menu

- `👥 Аккаунты`
- `🎯 Цели`
- `⚙️ Настройки`
- `🔧 Управление`
- `📊 Статистика`
- `⚠️ Диагностика`

### 3.2 Accounts

- `📥 Импорт файлов`
- `📋 Список аккаунтов`
- `🔐 Авторизация Epic (ссылка)`
- `✅ Проверить аккаунты`
- `🔄 Обновить ники Epic`
- `📝 Массовая смена ников`
- `📊 Статус смены ников`
- `➕ Добавить аккаунт`
- `➖ Удалить аккаунт`
- `◀️ Аккаунты / ▶️ Аккаунты`
- `🔎 Поиск аккаунтов`

### 3.3 Targets

Top level:

- `🗂️ Менеджер целей`
- `📊 Статистика целей`
- `▶️ Запустить все цели`
- `⏸️ Остановить все цели`
- `⛔ Остановить операцию`

Manager:

- `➕ Добавить цель`
- `📋 Список целей`
- `🎯 Выбрать цель`

Selected-target screen:

- `✏️ Редактировать цель`
- `📊 Статистика цели`
- `👥 Ники`
- `🚀 Отправка`
- `🧹 Операции`

`👥 Ники` section:

- `📥 Импорт ников`
- `📋 Ники цели`
- `📄 Статусы по никам`
- `👀 Отправители по нику`
- `◀️ Страница / ▶️ Страница`
- `🔎 Поиск ников`
- `➕ Добавить ник`
- `➖ Удалить ник`

`🚀 Отправка` section:

- `🚀 Распределить ники`
- `⚡ Форс-цикл с аккаунта`
- `🎲 Форс-цикл (рандом)`
- `▶️ Запустить цель`
- `⏸️ Остановить цель`

`🧹 Операции` section:

- `🔍 Проверить в друзьях`
- `🔁 Дослать отсутствующих`
- `↩️ Отозвать заявки`
- `🗑️ Удалить из друзей`
- `⛔ Остановить операцию`
- `🗑️ Удалить цель`

### 3.4 Target editing

- `🎯 На ник`
- `⏱️ Джиттер`
- `🕐 Окна`
- `🔁 Аккаунты на recheck`
- `📅 Ежедневный повтор`
- `🔀 Алгоритм отправки`
- `🎲 Порядок отправителей`
- `📋 Параметры цели`

### 3.5 Settings

- `🛡️ API лимиты` per account
- `📌 Прокси`
- `🧯 Новые заявки` on/off
- `♻️ Только recheck` on/off
- `👤 Доступ auth`

### 3.6 Operations

- `▶️ Тик (1 раз)`
- `⏸️ Стоп обработки`
- `▶️ Старт обработки`
- `📍 Статус обработки`
- `📤 Экспорт`

## 4. Reading target statistics

The `📊 Статистика цели` screen shows:

- `Новых заявок отправлено сегодня (DONE send_request)` — successfully completed friend-request sends during the current day;
- `Уникальных аккаунтов отправителей сегодня` — number of unique sender accounts that completed at least one send today;
- `Закрыто пар аккаунт→ник` — progress of covered sender-account → recipient-nickname pairs;
- `Осталось пар до полного покрытия` — number of pairs still missing for full coverage.

These are different metrics. For example, 21 sends today and 21 unique senders can happen to match, but they do not have to.

## 5. Meaning of the `🔁 Дослать отсутствующих` response

After the missing-request operation starts, the bot reports:

- `Всего пар отправитель→ник в цели`;
- `Уже покрыто (accepted/pending)`;
- `Поставлено в очередь send_request`;
- `Не поставлено сейчас (активная задача/нет auth)`;
- `Осталось к доотправке на сейчас`.

This makes it possible to see what was actually scheduled and what is still blocked by current state.

## 6. Input formats

### 6.1 Time windows

- `24/7`
- or:
  - `days=1,2,3,4,5 from=12:00 to=20:00`
  - `days=6,7 from=10:00 to=18:00`

`days` uses ISO weekdays (`1` = Monday, `7` = Sunday). Overnight windows are supported.

### 6.2 API limits

Format:

- `min_interval_sec hourly_limit daily_limit`

Example:

- `40 40 500`

### 6.3 Add an account

- `login:password`

### 6.4 Bulk nickname changes

Use a two-column file:

- column 1: `login/email`
- column 2: `new nickname`

Supported formats: `.xlsx/.txt/.csv`.

Nickname constraints:

- characters: `A-Za-z0-9_`
- length: `3..16`

If a nickname is already taken, the bot tries similar variants. If the Epic cooldown has not expired, the task moves to `failed` with a reason.

## 7. Context-sensitive file import

- In `Аккаунты`, imported files are treated as account imports.
- In `Массовая смена ников`, imported files are treated as nickname-change tasks.
- In `Цели` / `Ники`, imported files are treated as recipient-nickname imports.

## 8. Environment variables

Minimum variables:

- `TELEGRAM_BOT_TOKEN`
- `ADMIN_TELEGRAM_ID` or `ADMIN_TELEGRAM_IDS`
- `DB_URL`
- `EPIC_CLIENT_ID`
- `EPIC_CLIENT_SECRET`
- `EPIC_SWITCH_TOKEN`
- `EPIC_ANDROID_TOKEN`

Key flags:

- `DRY_RUN=1/0`
- `SEND_REQUESTS_ENABLED=1/0`
- `APP_MODE=all|bot|worker|scheduler`
- `LIST_PAGE_SIZE`, `TARGETS_PAGE_SIZE`, `SENDERS_PAGE_SIZE`

See `.env.example`.

## 9. Running the application

### 9.1 Local

```bash
cd <project-path>
python3 -m venv .venv
./.venv/bin/pip install -r requirements.txt
cp .env.example .env
./.venv/bin/python main.py
```

### 9.2 Docker

With Docker Compose V2:

```bash
docker compose -f docker-compose.prod.yml up -d --build
docker compose -f docker-compose.prod.yml ps
docker compose -f docker-compose.prod.yml logs --tail=200 app
```

With legacy `docker-compose` V1:

```bash
docker-compose -f docker-compose.prod.yml up -d --build
docker-compose -f docker-compose.prod.yml ps
docker-compose -f docker-compose.prod.yml logs --tail=200 app
```

## 10. Diagnostics

```bash
cd <project-path>
./.venv/bin/python tools/healthcheck.py
./.venv/bin/python tools/audit_queue.py
./.venv/bin/python tools/prod_doctor.py --allow-empty
```

## 11. Tests

```bash
cd <project-path>
PYTHONPATH=<project-path> ./.venv/bin/pytest -q
```

## 12. Common problems

### 12.1 Friend requests are not being sent

Check that:

- the target is running;
- processing is enabled with `▶️ Старт обработки`;
- `DRY_RUN=0`;
- `SEND_REQUESTS_ENABLED=1`;
- sender accounts have `device_auth`;
- the current time is inside the target's active windows;
- accounts have not reached API limits.

### 12.2 `can't parse entities`

Key bot screens are sent as plain text. If this error repeats, inspect user-provided content for unescaped special characters.

### 12.3 `No space left on device`

Free disk space and restart the Compose stack.

## 13. Notes

- A default target is not created automatically.
- UI numbering `№N` is ordinal and does not equal the database `id`.
- The database is the source of truth for `device_auth`.
