"""Tests for MT940 source.

These tests verify the basic functionality of the MT940 source module.
"""

import datetime
import os
import tempfile
from decimal import Decimal

import pytest

pytest.importorskip("mt940")

# Direct imports from mt940_source without beancount dependencies for parsing tests
import sys
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(__file__))))

from beancount_import.source.mt940_source import (
    PekaoAdapter,
    NestBankAdapter,
    UniversalAdapter,
    Field86Data,
    parse_mt940_file,
    _generate_transaction_id,
    _detect_file_encoding,
)


class TestPekaoAdapter:
    """Test Bank Pekao :86: field parsing."""

    @pytest.fixture
    def adapter(self):
        return PekaoAdapter()

    def test_przelew_internet(self, adapter):
        raw = "^00PRZELEW INTERNET^20Op ata za ifirma.pl^3012406960^32IFIRMA SA ul. Grabiszy ska^33241G, 53-234 Wroc aw 12406^34000^3874124069601570000000550657^62960"
        result = adapter.parse(raw)
        
        assert result.transaction_type == "PRZELEW INTERNET"
        assert result.title == "Op ata za ifirma.pl"
        assert "IFIRMA SA" in result.counterparty
        assert result.counterparty_iban == "74124069601570000000550657"

    def test_przekaz_euro_krajowy(self, adapter):
        raw = "^00PRZEKAZ EURO-KRAJOWY^20Przelew srodkow^3012404315^32DAVENTI DAWID SZWAJCA STARO^33MIEJSKA 6/10D KATOWICE 40-0^34000^3885124043151978001138516368"
        result = adapter.parse(raw)
        
        assert result.transaction_type == "PRZEKAZ EURO-KRAJOWY"
        assert result.title == "Przelew srodkow"
        assert "DAVENTI DAWID SZWAJCA" in result.counterparty
        assert result.counterparty_iban == "85124043151978001138516368"

    def test_transakcja_karta(self, adapter):
        raw = "^00TRANSAKCJA KART  P ATNICZ ^32PP *baselinker.com     Wroc^33law        PL 26723985 8914^34000^6263 *********0003986"
        result = adapter.parse(raw)
        
        assert "TRANSAKCJA" in result.transaction_type
        assert "baselinker.com" in result.counterparty
        assert result.card_number == "63 *********0003986"

    def test_fx_rates(self, adapter):
        raw = "^00PRZEKAZ EURO-KRAJOWY^20Przelew srodkow^51PLN00001,000000^52EUR00004,362700"
        result = adapter.parse(raw)
        
        assert result.fx_rate_from == "PLN00001,000000"
        assert result.fx_rate_to == "EUR00004,362700"


class TestNestBankAdapter:
    """Test Nest Bank :86: field parsing."""

    @pytest.fixture
    def adapter(self):
        return NestBankAdapter()

    def test_przelewy_przychodzace(self, adapter):
        raw = "<00Przelewy przychodzace<101625670045<20\"Przelew srodkow\"<21<22<23<27SZWAJCA DAWID<28<29ul. KS. J. NYGI 1A/15 41400 MYSLOW<3015600013<312001525640000001<32<3842156000132001525640000001<60ICE<63REF25/02/07/243872/1"
        result = adapter.parse(raw)
        
        assert result.transaction_type == "Przelewy przychodzace"
        assert result.title == '"Przelew srodkow"'
        assert "SZWAJCA DAWID" in result.counterparty
        assert result.counterparty_iban == "42156000132001525640000001"
        assert result.reference == "REF25/02/07/243872/1"

    def test_platnosci_karta(self, adapter):
        raw = "<00Platnosci karta<101626571701<20\"APPLE.COM/BILL APPLE.COM/BIL Nr k<21arty ...8107 5,00PLN\"<22<23<27<28<29<30<31<32<38<60<63REF25/02/10/82023/1"
        result = adapter.parse(raw)
        
        assert result.transaction_type == "Platnosci karta"
        assert "APPLE.COM" in result.title
        assert result.reference == "REF25/02/10/82023/1"

    def test_przelewy_wychodzace(self, adapter):
        raw = "<00Przelewy wychodzace<101625691344<20\"Przelew\"<21<22<23<27DAWID SZWAJCA<28<29KS. J. NYGI 1A m. 15<3018701045<312078100264020002<32<3841187010452078100264020002<6041400 MYSLOWICE<63REF25/02/07/253663/1"
        result = adapter.parse(raw)
        
        assert result.transaction_type == "Przelewy wychodzace"
        assert result.title == '"Przelew"'
        assert "DAWID SZWAJCA" in result.counterparty
        assert "MYSLOWICE" in result.counterparty  # <60> field included


class TestUniversalAdapter:
    """Test auto-detection of separator format."""

    @pytest.fixture
    def adapter(self):
        return UniversalAdapter()

    def test_detects_pekao_format(self, adapter):
        raw = "^00PRZELEW INTERNET^20Test"
        result = adapter.parse(raw)
        assert result.transaction_type == "PRZELEW INTERNET"

    def test_detects_nestbank_format(self, adapter):
        raw = "<00Przelewy przychodzace<20Test"
        result = adapter.parse(raw)
        assert result.transaction_type == "Przelewy przychodzace"

    def test_fallback_for_unknown_format(self, adapter):
        raw = "Some random text"
        result = adapter.parse(raw)
        assert result.title == raw


class TestEncodingDetection:
    """Test file encoding auto-detection."""

    def test_utf8_file(self):
        with tempfile.NamedTemporaryFile(mode='w', suffix='.sta', delete=False, encoding='utf-8') as f:
            f.write(":20:Test\n:25:PL12345\n")
            temp_path = f.name
        
        try:
            encoding = _detect_file_encoding(temp_path)
            assert encoding == 'utf-8'
        finally:
            os.unlink(temp_path)

    def test_iso8859_file(self):
        with tempfile.NamedTemporaryFile(mode='wb', suffix='.sta', delete=False) as f:
            # Write Polish characters in ISO-8859-2
            content = ":20:Test\n:25:PL12345\n:86:Przelew środków\n"
            f.write(content.encode('iso-8859-2'))
            temp_path = f.name
        
        try:
            encoding = _detect_file_encoding(temp_path)
            assert encoding in ('utf-8', 'iso-8859-2')  # Either is acceptable
        finally:
            os.unlink(temp_path)


class TestTransactionIdGeneration:
    """Test unique transaction ID generation."""

    def test_id_format(self):
        from beancount_import.source.mt940_source import Mt940Transaction
        
        txn = Mt940Transaction(
            value_date=datetime.date(2025, 1, 15),
            entry_date=None,
            amount=Decimal("-50.00"),
            status='D',
            transaction_code='NTRF',
            customer_reference='NONREF',
            bank_reference='12345',
            extra_details='25/01/15/123',
            transaction_details='Test',
        )
        
        result = _generate_transaction_id("PL12345", txn)
        assert result.startswith("mt940:")
        assert len(result) == 17  # "mt940:" + 12 char hash

    def test_same_txn_same_id(self):
        from beancount_import.source.mt940_source import Mt940Transaction
        
        txn1 = Mt940Transaction(
            value_date=datetime.date(2025, 1, 15),
            entry_date=None,
            amount=Decimal("-50.00"),
            status='D',
            transaction_code='NTRF',
            customer_reference='NONREF',
            bank_reference='12345',
            extra_details='25/01/15/123',
            transaction_details='Test',
        )
        
        txn2 = Mt940Transaction(
            value_date=datetime.date(2025, 1, 15),
            entry_date=None,
            amount=Decimal("-50.00"),
            status='D',
            transaction_code='NTRF',
            customer_reference='NONREF',
            bank_reference='12345',
            extra_details='25/01/15/123',
            transaction_details='Test',
        )
        
        id1 = _generate_transaction_id("PL12345", txn1)
        id2 = _generate_transaction_id("PL12345", txn2)
        assert id1 == id2


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
