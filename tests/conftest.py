import importlib.util
import sys
from pathlib import Path

import pytest

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

MAIN_PATH = PROJECT_ROOT / "main.py"

def _purge_modules():
    # main.py imports these at import-time; purge to force DB_URL reload.
    for mod in [
        "db_models",
        "epic_api_client",
        "epic_device_auth",
        "device_auth_jobs",
        "device_auth_generator",
    ]:
        sys.modules.pop(mod, None)


@pytest.fixture()
def fresh_main(tmp_path, monkeypatch):
    """
    Import main.py into a fresh module namespace with a dedicated SQLite DB file.
    DRY_RUN=1 so no real Epic calls happen.
    """
    db_path = tmp_path / "test.db"
    monkeypatch.setenv("DRY_RUN", "1")
    monkeypatch.setenv("TELEGRAM_BOT_TOKEN", "123456:TEST_TOKEN")
    monkeypatch.setenv("ADMIN_TELEGRAM_ID", "1")
    monkeypatch.setenv("DB_URL", f"sqlite:///{db_path}")
    monkeypatch.setenv("DB_AUTO_INIT_ON_IMPORT", "0")
    monkeypatch.setenv("MAX_TASKS_PER_TICK", "50")
    monkeypatch.setenv("PROCESS_TICK_SECONDS", "1")
    monkeypatch.setenv("DEFAULT_SEND_JITTER_MIN_SEC", "0")
    monkeypatch.setenv("DEFAULT_SEND_JITTER_MAX_SEC", "0")

    _purge_modules()

    module_name = f"main_pytest_{id(db_path)}"
    spec = importlib.util.spec_from_file_location(module_name, str(MAIN_PATH))
    module = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    spec.loader.exec_module(module)
    module.init_db_schema(run_migrations=True)
    return module
