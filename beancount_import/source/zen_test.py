"""Tests for Zen source.

These tests verify the basic functionality of the Zen source module.
Full integration tests require actual Zen CSV statements.

To run tests with your own CSV statements:
1. Place CSV files in testdata/source/zen/
2. Run: pytest beancount_import/source/zen_test.py -v
"""

import datetime
import os
import tempfile
from decimal import Decimal

import pytest

from . import zen


class TestZenDateParsing:
    """Test parsing of Zen date formats."""

    def test_standard_format(self):
        result = zen.parse_zen_date("1 Jan 2025")
        assert result == datetime.date(2025, 1, 1)

    def test_two_digit_day(self):
        result = zen.parse_zen_date("28 Feb 2025")
        assert result == datetime.date(2025, 2, 28)

    def test_december(self):
        result = zen.parse_zen_date("31 Dec 2024")
        assert result == datetime.date(2024, 12, 31)

    def test_iso_format(self):
        result = zen.parse_zen_date("2025-01-15")
        assert result == datetime.date(2025, 1, 15)

    def test_invalid_date_raises(self):
        with pytest.raises(ValueError):
            zen.parse_zen_date("invalid")


class TestZenAmountParsing:
    """Test parsing of Zen amount formats."""

    def test_positive_amount(self):
        assert zen.parse_zen_amount("1000.00") == Decimal("1000.00")

    def test_negative_amount(self):
        assert zen.parse_zen_amount("-15.00") == Decimal("-15.00")

    def test_simple_amount(self):
        assert zen.parse_zen_amount("45.99") == Decimal("45.99")

    def test_empty_string(self):
        assert zen.parse_zen_amount("") == Decimal("0")

    def test_whitespace(self):
        assert zen.parse_zen_amount("  100.50  ") == Decimal("100.50")


class TestCounterpartyExtraction:
    """Test extraction of counterparty info from description."""

    def test_card_payment_with_location(self):
        desc = "STARBUCKS              POL,POL CARD: MASTERCARD *7492"
        counterparty, address, iban, card = zen._extract_counterparty_info(desc, "Card payment")
        assert counterparty == "STARBUCKS"
        assert address == "POL"
        assert card == "7492"

    def test_card_payment_complex_merchant(self):
        desc = "GOOGLE*ADS3573692684              IRL,IRL CARD: MASTERCARD *7492"
        counterparty, address, iban, card = zen._extract_counterparty_info(desc, "Card payment")
        assert counterparty == "GOOGLE*ADS3573692684"
        assert address == "IRL"
        assert card == "7492"

    def test_incoming_transfer(self):
        desc = "ZEN.COM UAB,   ZEN account top-up, Card **6671 "
        counterparty, address, iban, card = zen._extract_counterparty_info(desc, "Incoming transfer")
        assert counterparty == "ZEN.COM UAB"

    def test_outgoing_transfer_with_iban(self):
        desc = "Dawid Szwajca,  PL Proce PL42156000132001525640000001"
        counterparty, address, iban, card = zen._extract_counterparty_info(desc, "Outgoing transfer")
        assert counterparty == "Dawid Szwajca"
        assert iban == "PL42156000132001525640000001"

    def test_cashback(self):
        desc = "CASHBACK aliexpress 5e2b632c-b61a-7bcf-b216-0194afcff75d 4.0% 20250129Z"
        counterparty, address, iban, card = zen._extract_counterparty_info(desc, "Cashback")
        assert counterparty == "aliexpress"

    def test_cashback_refund(self):
        desc = "STORNO CASHBACK ALIEXPRESS.COM 10582ab7-0e0d-7997-a6e5-01950be55d5d 4.0% 20250215Z"
        counterparty, address, iban, card = zen._extract_counterparty_info(desc, "Cashback Refund")
        assert counterparty == "ALIEXPRESS.COM"


class TestParseCSV:
    """Test CSV parsing."""

    def test_parse_simple_csv(self):
        csv_content = """PLN monthly statement
Generated: 6 Jan 2026
Date: 1 Jan 2025 - 31 Jan 2025

Account owner
DAWID ZBIGNIEW SZWAJCA
Kwiatowa 29G/3
41-400 Mysłowice PL

Account details
"Local IBAN: "
"Local BIC/SWIFT: "
Global IBAN: GB72TCCL04140411776433
Global BIC/SWIFT: TCCLGB3L
Currency: PLN

Total income:,11185.68,PLN
Opening balance:,759.28,PLN
Total outcome:,-11360.47,PLN
Closing balance:,584.49,PLN


Transactions:
Date,Transaction type,Description,Settlement amount,Settlement currency,Original amount,Original currency,Currency rate,Fee description,Fee amount,Fee currency,Balance
1 Jan 2025,Card payment,"ORANGE FLEX              POL,POL CARD: MASTERCARD *7492",-15.00,PLN,-15.00,PLN,1.0,Fee for processing transaction,,,744.28
2 Jan 2025,Incoming transfer,"ZEN.COM UAB,   ZEN account top-up ",1000.00,PLN,1000.00,PLN,1.0,Fee for processing transaction,,,1744.28

This is a computer-generated document.
"""
        with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False, encoding='utf-8') as f:
            f.write(csv_content)
            temp_path = f.name

        try:
            statement = zen.parse_csv(temp_path)
            
            assert statement is not None
            assert statement.iban == "GB72TCCL04140411776433"
            assert statement.currency == "PLN"
            assert statement.period_start == datetime.date(2025, 1, 1)
            assert statement.period_end == datetime.date(2025, 1, 31)
            assert statement.opening_balance == Decimal("759.28")
            assert statement.closing_balance == Decimal("584.49")
            assert len(statement.transactions) == 2
            
            # First transaction
            txn1 = statement.transactions[0]
            assert txn1.date == datetime.date(2025, 1, 1)
            assert txn1.transaction_type == "Card payment"
            assert txn1.settlement_amount == Decimal("-15.00")
            assert txn1.counterparty == "ORANGE FLEX"
            assert txn1.counterparty_address == "POL"
            assert txn1.card_number == "7492"
            
            # Second transaction
            txn2 = statement.transactions[1]
            assert txn2.date == datetime.date(2025, 1, 2)
            assert txn2.transaction_type == "Incoming transfer"
            assert txn2.settlement_amount == Decimal("1000.00")
            assert txn2.counterparty == "ZEN.COM UAB"
        finally:
            os.unlink(temp_path)


class TestGenerateTransactionId:
    """Test transaction ID generation."""

    def test_id_format(self):
        txn = zen.ZenTransaction(
            date=datetime.date(2025, 1, 15),
            transaction_type="Card payment",
            description="Test",
            settlement_amount=Decimal("-50.00"),
            settlement_currency="PLN",
            original_amount=Decimal("-50.00"),
            original_currency="PLN",
            currency_rate=Decimal("1.0"),
            fee_description="",
            fee_amount=None,
            fee_currency=None,
            balance_after=Decimal("200.00"),
            line_number=25,
        )
        
        result = zen._generate_transaction_id("GB72TCCL04140411776433", txn)
        # Hash format: zen:{12-char-hash}
        assert result.startswith("zen:")
        assert len(result) == 16  # "zen:" + 12 char hash


class TestZenSource:
    """Test ZenSource class functionality."""

    def test_source_name(self):
        """Test that source has correct name."""
        with tempfile.TemporaryDirectory() as tmpdir:
            source = zen.ZenSource(
                directory=tmpdir,
                default_account='Assets:Bank:Zen',
                log_status=lambda x: None,
            )
            assert source.name == 'zen'

    def test_requires_account_config(self):
        """Test that source requires account configuration."""
        with tempfile.TemporaryDirectory() as tmpdir:
            with pytest.raises(ValueError, match="requires either"):
                zen.ZenSource(
                    directory=tmpdir,
                    log_status=lambda x: None,
                )

    def test_loading_csv_files(self):
        """Test loading CSV files from directory."""
        csv_content = """PLN monthly statement
Generated: 6 Jan 2026
Date: 1 Jan 2025 - 31 Jan 2025

Account owner
TEST USER

Account details
Global IBAN: GB72TCCL04140411776433
Currency: PLN

Opening balance:,100.00,PLN
Closing balance:,50.00,PLN


Transactions:
Date,Transaction type,Description,Settlement amount,Settlement currency,Original amount,Original currency,Currency rate,Fee description,Fee amount,Fee currency,Balance
1 Jan 2025,Card payment,"TEST MERCHANT              POL,POL CARD: MASTERCARD *1234",-50.00,PLN,-50.00,PLN,1.0,Fee for processing transaction,,,50.00

This is a computer-generated document.
"""
        with tempfile.TemporaryDirectory() as tmpdir:
            # Create year subdirectory
            year_dir = os.path.join(tmpdir, "2025")
            os.makedirs(year_dir)
            
            # Write CSV file
            csv_path = os.path.join(year_dir, "2025-01.csv")
            with open(csv_path, 'w', encoding='utf-8') as f:
                f.write(csv_content)
            
            # Create source
            source = zen.ZenSource(
                directory=tmpdir,
                default_account='Assets:Bank:Zen:PLN',
                log_status=lambda x: None,
            )
            
            assert len(source.statements) == 1
            assert len(source.transactions) == 1
            
            statement, txn = source.transactions[0]
            assert statement.iban == "GB72TCCL04140411776433"
            assert txn.settlement_amount == Decimal("-50.00")
            assert txn.counterparty == "TEST MERCHANT"

    def test_is_posting_cleared(self):
        """Test posting cleared detection."""
        from beancount.core.data import Posting
        from beancount.core.amount import Amount
        from beancount.core.number import D

        with tempfile.TemporaryDirectory() as tmpdir:
            source = zen.ZenSource(
                directory=tmpdir,
                default_account='Assets:Bank:Zen',
                log_status=lambda x: None,
            )

            # Posting with source_ref is cleared
            cleared_posting = Posting(
                account='Assets:Bank:Zen',
                units=Amount(D('100'), 'PLN'),
                cost=None,
                price=None,
                flag=None,
                meta={'source_ref': 'zen:GB123:25:2025-01-15:-50.00'},
            )
            assert source.is_posting_cleared(cleared_posting) is True

            # Posting without metadata is not cleared
            uncleared_posting = Posting(
                account='Assets:Bank:Zen',
                units=Amount(D('100'), 'PLN'),
                cost=None,
                price=None,
                flag=None,
                meta=None,
            )
            assert source.is_posting_cleared(uncleared_posting) is False


class TestFxPairing:
    """Test FX transaction pairing logic."""

    def test_fx_pair_detection(self):
        """Test that FX pairs are correctly detected from matching transactions."""
        # Create two CSV files with matching FX transactions
        pln_csv = """PLN monthly statement
Generated: 6 Jan 2026
Date: 1 Mar 2025 - 31 Mar 2025

Account owner
TEST USER

Account details
Global IBAN: GB72TCCL04140411776433
Currency: PLN

Opening balance:,100.00,PLN
Closing balance:,71.86,PLN


Transactions:
Date,Transaction type,Description,Settlement amount,Settlement currency,Original amount,Original currency,Currency rate,Fee description,Fee amount,Fee currency,Balance
9 Mar 2025,Exchange money,Currency exchange transaction,-28.14,PLN,-6.74,EUR,0.239517,Fee for processing transaction,,,71.86

This is a computer-generated document.
"""
        eur_csv = """EUR monthly statement
Generated: 6 Jan 2026
Date: 1 Mar 2025 - 31 Mar 2025

Account owner
TEST USER

Account details
Global IBAN: GB72TCCL04140411776433
Currency: EUR

Opening balance:,0.00,EUR
Closing balance:,6.74,EUR


Transactions:
Date,Transaction type,Description,Settlement amount,Settlement currency,Original amount,Original currency,Currency rate,Fee description,Fee amount,Fee currency,Balance
9 Mar 2025,Exchange money,Currency exchange transaction,6.74,EUR,28.14,PLN,0.239517,Fee for processing transaction,,,6.74

This is a computer-generated document.
"""
        with tempfile.TemporaryDirectory() as tmpdir:
            year_dir = os.path.join(tmpdir, "2025")
            os.makedirs(year_dir)
            
            with open(os.path.join(year_dir, "2025-03-PLN-1234.csv"), 'w', encoding='utf-8') as f:
                f.write(pln_csv)
            with open(os.path.join(year_dir, "2025-03-EUR-5678.csv"), 'w', encoding='utf-8') as f:
                f.write(eur_csv)
            
            source = zen.ZenSource(
                directory=tmpdir,
                account_map={
                    'GB72TCCL04140411776433_PLN': 'Assets:Zen:PLN',
                    'GB72TCCL04140411776433_EUR': 'Assets:Zen:EUR',
                },
                log_status=lambda x: None,
            )
            
            pairs, src_keys, tgt_keys = source._find_fx_pairs()
            
            assert len(pairs) == 1
            pair = pairs[0]
            assert pair.date == datetime.date(2025, 3, 9)
            assert pair.source_txn.settlement_amount == Decimal("-28.14")
            assert pair.target_txn.settlement_amount == Decimal("6.74")
            assert pair.is_reversal is False

    def test_fx_transaction_narration(self):
        """Test that FX transactions have correct narration format."""
        pln_csv = """PLN monthly statement
Generated: 6 Jan 2026
Date: 1 Mar 2025 - 31 Mar 2025

Account owner
TEST USER

Account details
Global IBAN: GB72TCCL04140411776433
Currency: PLN

Opening balance:,100.00,PLN
Closing balance:,71.86,PLN


Transactions:
Date,Transaction type,Description,Settlement amount,Settlement currency,Original amount,Original currency,Currency rate,Fee description,Fee amount,Fee currency,Balance
9 Mar 2025,Exchange money,Currency exchange transaction,-28.14,PLN,-6.74,EUR,0.239517,Fee for processing transaction,,,71.86

This is a computer-generated document.
"""
        eur_csv = """EUR monthly statement
Generated: 6 Jan 2026
Date: 1 Mar 2025 - 31 Mar 2025

Account owner
TEST USER

Account details
Global IBAN: GB72TCCL04140411776433
Currency: EUR

Opening balance:,0.00,EUR
Closing balance:,6.74,EUR


Transactions:
Date,Transaction type,Description,Settlement amount,Settlement currency,Original amount,Original currency,Currency rate,Fee description,Fee amount,Fee currency,Balance
9 Mar 2025,Exchange money,Currency exchange transaction,6.74,EUR,28.14,PLN,0.239517,Fee for processing transaction,,,6.74

This is a computer-generated document.
"""
        with tempfile.TemporaryDirectory() as tmpdir:
            year_dir = os.path.join(tmpdir, "2025")
            os.makedirs(year_dir)
            
            with open(os.path.join(year_dir, "2025-03-PLN-1234.csv"), 'w', encoding='utf-8') as f:
                f.write(pln_csv)
            with open(os.path.join(year_dir, "2025-03-EUR-5678.csv"), 'w', encoding='utf-8') as f:
                f.write(eur_csv)
            
            source = zen.ZenSource(
                directory=tmpdir,
                account_map={
                    'GB72TCCL04140411776433_PLN': 'Assets:Zen:PLN',
                    'GB72TCCL04140411776433_EUR': 'Assets:Zen:EUR',
                },
                log_status=lambda x: None,
            )
            
            pairs, _, _ = source._find_fx_pairs()
            pair = pairs[0]
            
            txn = source._make_fx_transaction(pair, 'Assets:Zen:PLN', 'Assets:Zen:EUR')
            
            assert txn.narration == "FX - PLN → EUR"
            assert txn.payee == "Zen"

    def test_fx_per_unit_price(self):
        """Test that FX transactions have correct per-unit price calculation."""
        pln_csv = """PLN monthly statement
Generated: 6 Jan 2026
Date: 1 Mar 2025 - 31 Mar 2025

Account owner
TEST USER

Account details
Global IBAN: GB72TCCL04140411776433
Currency: PLN

Opening balance:,100.00,PLN
Closing balance:,71.86,PLN


Transactions:
Date,Transaction type,Description,Settlement amount,Settlement currency,Original amount,Original currency,Currency rate,Fee description,Fee amount,Fee currency,Balance
9 Mar 2025,Exchange money,Currency exchange transaction,-28.14,PLN,-6.74,EUR,0.239517,Fee for processing transaction,,,71.86

This is a computer-generated document.
"""
        eur_csv = """EUR monthly statement
Generated: 6 Jan 2026
Date: 1 Mar 2025 - 31 Mar 2025

Account owner
TEST USER

Account details
Global IBAN: GB72TCCL04140411776433
Currency: EUR

Opening balance:,0.00,EUR
Closing balance:,6.74,EUR


Transactions:
Date,Transaction type,Description,Settlement amount,Settlement currency,Original amount,Original currency,Currency rate,Fee description,Fee amount,Fee currency,Balance
9 Mar 2025,Exchange money,Currency exchange transaction,6.74,EUR,28.14,PLN,0.239517,Fee for processing transaction,,,6.74

This is a computer-generated document.
"""
        with tempfile.TemporaryDirectory() as tmpdir:
            year_dir = os.path.join(tmpdir, "2025")
            os.makedirs(year_dir)
            
            with open(os.path.join(year_dir, "2025-03-PLN-1234.csv"), 'w', encoding='utf-8') as f:
                f.write(pln_csv)
            with open(os.path.join(year_dir, "2025-03-EUR-5678.csv"), 'w', encoding='utf-8') as f:
                f.write(eur_csv)
            
            source = zen.ZenSource(
                directory=tmpdir,
                account_map={
                    'GB72TCCL04140411776433_PLN': 'Assets:Zen:PLN',
                    'GB72TCCL04140411776433_EUR': 'Assets:Zen:EUR',
                },
                log_status=lambda x: None,
            )
            
            pairs, _, _ = source._find_fx_pairs()
            pair = pairs[0]
            
            txn = source._make_fx_transaction(pair, 'Assets:Zen:PLN', 'Assets:Zen:EUR')
            
            # First posting: source (PLN debit)
            assert txn.postings[0].units.number == Decimal("-28.14")
            assert txn.postings[0].units.currency == "PLN"
            assert txn.postings[0].price is None
            
            # Second posting: target (EUR credit) with per-unit price
            assert txn.postings[1].units.number == Decimal("6.74")
            assert txn.postings[1].units.currency == "EUR"
            assert txn.postings[1].price is not None
            assert txn.postings[1].price.currency == "PLN"
            # Per-unit price should be 28.14 / 6.74 ≈ 4.175...
            expected_per_unit = Decimal("28.14") / Decimal("6.74")
            assert abs(txn.postings[1].price.number - expected_per_unit) < Decimal("0.0001")


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
