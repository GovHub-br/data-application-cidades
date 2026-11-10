"""Shared test setup. Import roots live in pyproject.toml ([tool.pytest.ini_options])."""

import importlib
import sys

# Always set, never setdefault: a real .env pulled in by load_dotenv() (e.g.
# scripts/conversao/config.py) would otherwise point the tests at the real lake and DB.
TEST_ENV = {
    "MINIO_ENDPOINT": "minio:9000",
    "MINIO_ACCESS_KEY": "teste",
    "MINIO_SECRET_KEY": "teste",
    "MINIO_BUCKET": "teste",
    "DB_DW_HOST_MCID": "postgres",
    "DB_DW_USER_MCID": "teste",
    "DB_DW_PASSWORD_MCID": "teste",
    "DB_DW_DBNAME_MCID": "teste",
    "LAKE_LOCAL_ARTIFACTS": "0",
}


def fresh_import(monkeypatch, module, *packages, **env):
    """Import `module` from scratch under TEST_ENV plus `env`.

    The lake packages copy the environment at import time, so every test that changes
    it needs a new import of `module` and of `packages` (top-level names).
    """
    for key, value in {**TEST_ENV, **env}.items():
        monkeypatch.setenv(key, value)
    stale = {module.split(".")[0], *packages}
    for name in [m for m in sys.modules if m.split(".")[0] in stale]:
        del sys.modules[name]
    return importlib.import_module(module)
