import pytest

from transit import settings

PG_VARS = ("POSTGRES_HOST", "POSTGRES_PORT", "POSTGRES_DB", "POSTGRES_USER", "POSTGRES_PASSWORD")


@pytest.fixture(autouse=True)
def isolated_env(monkeypatch):
    """Start every test with no relevant env vars, no .env loading, and empty caches."""
    for var in (*PG_VARS, "KAFKA_BOOTSTRAP_SERVERS", "STM_API_KEY"):
        monkeypatch.delenv(var, raising=False)
    monkeypatch.setattr(settings, "load_dotenv", lambda: None)
    settings.get_postgres_settings.cache_clear()
    settings.get_kafka_settings.cache_clear()
    yield
    settings.get_postgres_settings.cache_clear()
    settings.get_kafka_settings.cache_clear()


def test_postgres_settings_do_not_need_stm_key(monkeypatch):
    monkeypatch.setenv("POSTGRES_DB", "transit")
    monkeypatch.setenv("POSTGRES_USER", "u")
    monkeypatch.setenv("POSTGRES_PASSWORD", "p@ss word")

    pg = settings.get_postgres_settings()

    assert pg.host == "localhost" and pg.port == 5432
    assert "p@ss word" not in repr(pg)  # password hidden from repr
    assert "dbname=transit" in pg.conninfo
    assert "password='p@ss word'" in pg.conninfo  # special chars quoted


def test_missing_postgres_var_raises_clear_error():
    with pytest.raises(RuntimeError, match="POSTGRES_DB"):
        settings.get_postgres_settings()


def test_kafka_defaults_to_localhost():
    assert settings.get_kafka_settings().bootstrap_servers == "localhost:9092"


def test_api_key_required():
    with pytest.raises(RuntimeError, match="STM_API_KEY"):
        settings.get_api_key("STM_API_KEY")
