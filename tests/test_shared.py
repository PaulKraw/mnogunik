"""
tests/test_shared.py — Тесты общих утилит монорепозитория.
"""

import os
import sqlite3
import tempfile

import pytest


class TestConfig:
    def test_root_dir_not_empty(self):
        from shared.config import ROOT_DIR
        assert ROOT_DIR != ""

    def test_hostname(self):
        from shared.config import HOSTNAME
        assert isinstance(HOSTNAME, str)
        assert len(HOSTNAME) > 0

    def test_spreadsheet_id(self):
        from shared.config import SPREADSHEET_ID
        assert len(SPREADSHEET_ID) > 10


class TestLogger:
    def test_write_log_to_file(self, tmp_path):
        from shared.logger import write_log
        log_file = str(tmp_path / "test.log")
        write_log("test message", log_file=log_file)
        content = open(log_file).read()
        assert "test message" in content

    def test_reset_log(self, tmp_path):
        from shared.logger import reset_log
        log_file = str(tmp_path / "test.log")
        with open(log_file, "w") as f:
            f.write("old content")
        reset_log(log_file)
        assert open(log_file).read() == ""


class TestGoogleSheets:
    def test_make_csv_url(self):
        from shared.google_sheets import make_csv_url
        url = "https://docs.google.com/spreadsheets/d/ABC123/edit?gid=789"
        result = make_csv_url(url)
        assert "ABC123" in result
        assert "gid=789" in result
        assert "export?format=csv" in result

    def test_make_csv_url_hash(self):
        from shared.google_sheets import make_csv_url
        url = "https://docs.google.com/spreadsheets/d/XYZ/edit#gid=42"
        result = make_csv_url(url)
        assert "gid=42" in result

    def test_make_csv_url_invalid(self):
        from shared.google_sheets import make_csv_url
        with pytest.raises(ValueError):
            make_csv_url("https://example.com")


class TestAvitoApi:
    def test_get_ozon_headers(self):
        from shared.avito_api import get_ozon_headers
        headers = get_ozon_headers()
        assert "Client-Id" in headers
        assert "Api-Key" in headers
        assert headers["Content-Type"] == "application/json"


class TestDb:
    def test_get_connection(self):
        from shared.db import get_connection
        with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
            path = f.name
        try:
            conn = get_connection(path)
            assert isinstance(conn, sqlite3.Connection)
            conn.close()
        finally:
            os.unlink(path)

    def test_execute_create_and_fetch(self):
        from shared.db import get_connection, fetch_all
        with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
            path = f.name
        try:
            conn = get_connection(path)
            conn.execute("CREATE TABLE test (id INTEGER, name TEXT)")
            conn.execute("INSERT INTO test VALUES (1, 'hello')")
            conn.commit()
            conn.close()

            rows = fetch_all("SELECT * FROM test", db_path=path)
            assert len(rows) == 1
            assert rows[0]["name"] == "hello"
        finally:
            os.unlink(path)

    def test_init_db(self, tmp_path):
        from shared.db import get_connection, init_db
        db_path = str(tmp_path / "test.db")
        # Минимальная схема для теста
        schema = str(tmp_path / "schema.sql")
        with open(schema, "w") as f:
            f.write("CREATE TABLE IF NOT EXISTS test_table (id INTEGER PRIMARY KEY);")

        init_db(schema_path=schema, db_path=db_path)

        conn = get_connection(db_path)
        cursor = conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name='test_table'"
        )
        assert cursor.fetchone() is not None
        conn.close()
