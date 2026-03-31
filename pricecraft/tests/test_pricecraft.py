"""
tests/test_pricecraft.py — Тесты ядра pricecraft.
"""

import pandas as pd
import pytest


class TestGetFixedHash:
    def test_consistent(self):
        from pricecraft.modules.get_config import get_fixed_hash
        h1 = get_fixed_hash("test_string", 8)
        h2 = get_fixed_hash("test_string", 8)
        assert h1 == h2

    def test_prefix(self):
        from pricecraft.modules.get_config import get_fixed_hash
        h = get_fixed_hash("abc", 10, prefix="pf_")
        assert h.startswith("pf_")
        assert len(h) == 3 + 10  # prefix + hash

    def test_short_input(self):
        from pricecraft.modules.get_config import get_fixed_hash
        assert get_fixed_hash("x") == "x"

    def test_spaces(self):
        from pricecraft.modules.get_config import get_fixed_hash
        assert get_fixed_hash(" ") == "_"


class TestInitDf:
    def test_basic(self):
        from pricecraft.modules.get_config import init_conf_df
        data = [["col1", "col2", "col3"], ["a", "b", "c"], ["d", "e", "f"]]
        df = pd.DataFrame(data)
        result = init_conf_df(df)
        assert list(result.columns) == ["col1", "col2", "col3"]
        assert len(result) == 2


class TestGetHashTable:
    def test_creates_pairs(self):
        from pricecraft.modules.get_config import getHashTable
        data = {
            "CPU": ["", "GPU1", "GPU2"],
            "i5": ["i5", "100", None],
            "i7": ["i7", None, "200"],
        }
        df = pd.DataFrame(data)
        df.columns = ["CPU", "GPU1", "GPU2"]
        result = getHashTable(df)
        assert len(result) == 2
        assert "conf" in result.columns
        assert "Процессор_ar" in result.columns


class TestApplyMarketplaceMapping:
    def test_mapping_applied(self):
        from pricecraft.modules.get_config import apply_marketplace_mapping
        df = pd.DataFrame({"color": ["red", "blue"], "size": ["M", "L"]})
        mapping = pd.DataFrame({
            "marketplace": ["ozon", "ozon"],
            "source_param_name": ["color", "color"],
            "source_value": ["red", "blue"],
            "target_param_name": ["Цвет", "Цвет"],
            "target_value": ["Красный", "Синий"],
        })
        result = apply_marketplace_mapping(df, mapping, "ozon")
        assert "Цвет" in result.columns
        assert result.loc[0, "Цвет"] == "Красный"
        assert result.loc[1, "Цвет"] == "Синий"

    def test_wrong_marketplace_ignored(self):
        from pricecraft.modules.get_config import apply_marketplace_mapping
        df = pd.DataFrame({"x": ["a"]})
        mapping = pd.DataFrame({
            "marketplace": ["wb"],
            "source_param_name": ["x"],
            "source_value": ["a"],
            "target_param_name": ["y"],
            "target_value": ["b"],
        })
        result = apply_marketplace_mapping(df, mapping, "ozon")
        assert "y" not in result.columns or result.loc[0, "y"] == ""


class TestGetnameSbopk:
    def test_returns_string(self):
        from pricecraft.modules.get_config import getname_sbopk
        row = {
            "Видеокарта*": "RTX 3060",
            "Процессор*": "i5-12400F",
            "Число ядер процессора": "6",
            "Частота процессора, ГГц": "2.5",
            "Оперативная память*": "16 ГБ",
            "Диск ГБ": "512",
            "Кейс название": "3RGB",
        }
        name = getname_sbopk(row)
        assert "ULTRAFPS" in name
        assert "RTX 3060" in name

    def test_missing_keys(self):
        from pricecraft.modules.get_config import getname_sbopk
        assert getname_sbopk({}) == "Игровой компьютер"
