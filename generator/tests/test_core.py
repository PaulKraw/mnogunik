"""
tests/test_core.py — Минимальные тесты ядра mnogunik.
"""

import re
import pandas as pd
import pytest


class TestReplaceSynonyms:
    """Тесты replace_synonyms."""

    def test_simple_choice(self):
        from generator.core.text import replace_synonyms

        text = "$sin{ A || B || C $sin}"
        result = replace_synonyms(text)
        assert result in ("A", "B", "C")

    def test_nested(self):
        from generator.core.text import replace_synonyms

        text = "$sin{ $sin{ 1 || 2 $sin} || X $sin}"
        result = replace_synonyms(text)
        assert result in ("1", "2", "X")

    def test_no_synonyms(self):
        from generator.core.text import replace_synonyms

        text = "Обычный текст без синонимов"
        assert replace_synonyms(text) == text

    def test_multiple(self):
        from generator.core.text import replace_synonyms

        text = "$sin{ A || B $sin} и $sin{ X || Y $sin}"
        result = replace_synonyms(text)
        assert "$sin" not in result
        assert " и " in result


class TestReplaceGrandValues:
    """Тесты replace_grand_values."""

    def test_simple(self):
        from generator.core.prices import replace_grand_values

        df = pd.DataFrame({"val": ["grand(10, 20, 5)"]})
        result = replace_grand_values(df)
        assert int(result.at[0, "val"]) in [10, 15, 20]

    def test_no_grand(self):
        from generator.core.prices import replace_grand_values

        df = pd.DataFrame({"val": ["hello"]})
        result = replace_grand_values(df)
        assert result.at[0, "val"] == "hello"

    def test_numeric_passthrough(self):
        from generator.core.prices import replace_grand_values

        df = pd.DataFrame({"val": [42]})
        result = replace_grand_values(df)
        assert result.at[0, "val"] == 42


class TestHelpers:
    """Тесты утилит."""

    def test_random_code_length(self):
        from generator.utils.helpers import generate_random_code

        code = generate_random_code(8)
        assert len(code) == 8
        assert code.isdigit()

    def test_hex_color(self):
        from generator.utils.helpers import generate_random_hex_color

        color = generate_random_hex_color()
        assert re.match(r"^#[0-9A-F]{6}$", color)

    def test_format_time(self):
        from generator.utils.helpers import format_execution_time

        text = format_execution_time(125.5)
        assert "2 минуты" in text
        assert "секунд" in text

    def test_natural_sort(self):
        from generator.utils.helpers import natural_sort_key

        items = ["img10.jpg", "img2.jpg", "img1.jpg"]
        result = sorted(items, key=natural_sort_key)
        assert result == ["img1.jpg", "img2.jpg", "img10.jpg"]


class TestGoogleSheets:
    """Тесты преобразования URL."""

    def test_make_csv_url(self):
        from shared.google_sheets import make_csv_url

        url = "https://docs.google.com/spreadsheets/d/1abc123/edit?gid=456#gid=456"
        result = make_csv_url(url)
        assert "export?format=csv" in result
        assert "1abc123" in result
        assert "gid=456" in result

    def test_invalid_url(self):
        from shared.google_sheets import make_csv_url

        with pytest.raises(ValueError):
            make_csv_url("https://example.com/bad-url")
