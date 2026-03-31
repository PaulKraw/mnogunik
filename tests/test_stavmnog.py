"""
tests/test_stavmnog.py — Тесты модуля stavmnog.
"""

import pytest


class TestCalcBid:
    def test_zero_ctr_few_clicks(self):
        from stavmnog.utils.formulas import calc_bid
        assert calc_bid(100, 500, 200, ctr=0, clicks=5, leads=0) == 100

    def test_zero_ctr_many_clicks(self):
        from stavmnog.utils.formulas import calc_bid
        result = calc_bid(100, 500, 200, ctr=0, clicks=15, leads=0)
        assert result == 50  # ceil(100/2)

    def test_low_ctr(self):
        from stavmnog.utils.formulas import calc_bid
        assert calc_bid(100, 500, 200, ctr=0.02, clicks=50, leads=3) == 5.0

    def test_no_leads(self):
        from stavmnog.utils.formulas import calc_bid
        assert calc_bid(100, 500, 200, ctr=0.05, clicks=50, leads=0) == 100

    def test_one_lead(self):
        from stavmnog.utils.formulas import calc_bid
        result = calc_bid(100, 500, 200, ctr=0.05, clicks=50, leads=1)
        assert result == 140  # max(100, round(200*0.7))

    def test_many_leads(self):
        from stavmnog.utils.formulas import calc_bid
        assert calc_bid(100, 500, 200, ctr=0.05, clicks=50, leads=5) == 200

    def test_max_cap(self):
        from stavmnog.utils.formulas import calc_bid
        assert calc_bid(100, 150, 200, ctr=0.05, clicks=50, leads=5) == 150

    def test_all_none(self):
        from stavmnog.utils.formulas import calc_bid
        assert calc_bid(None, None, None, ctr=0, clicks=0, leads=0) == 0


class TestSafeDiv:
    def test_normal(self):
        from stavmnog.utils.formulas import safe_div
        assert safe_div(10, 3) == round(10 / 3, 4)

    def test_zero(self):
        from stavmnog.utils.formulas import safe_div
        assert safe_div(10, 0) == 0.0


class TestDeltaPct:
    def test_increase(self):
        from stavmnog.utils.formulas import delta_pct
        assert delta_pct(150, 100) == 50.0

    def test_decrease(self):
        from stavmnog.utils.formulas import delta_pct
        assert delta_pct(50, 100) == -50.0

    def test_zero_base(self):
        from stavmnog.utils.formulas import delta_pct
        assert delta_pct(100, 0) == 0.0


class TestColLetter:
    def test_a(self):
        from stavmnog.utils.formulas import col_letter
        assert col_letter(1) == "A"

    def test_z(self):
        from stavmnog.utils.formulas import col_letter
        assert col_letter(26) == "Z"

    def test_aa(self):
        from stavmnog.utils.formulas import col_letter
        assert col_letter(27) == "AA"


class TestExtractSheetId:
    def test_full_url(self):
        from stavmnog.scripts.export_stats import _extract_sheet_id
        url = "https://docs.google.com/spreadsheets/d/18XoFtoLE3askqCfKWTf4U1pmnJs-r22QlRKD_BfjydM/edit?gid=0"
        assert _extract_sheet_id(url) == "18XoFtoLE3askqCfKWTf4U1pmnJs-r22QlRKD_BfjydM"

    def test_plain_id(self):
        from stavmnog.scripts.export_stats import _extract_sheet_id
        assert _extract_sheet_id("ABC123def") == "ABC123def"
