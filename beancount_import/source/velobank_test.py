"""Tests for velobank source.

These tests verify the basic functionality of the VeloBank source module.
Full integration tests require actual VeloBank PDF statements which are
user-specific and cannot be included in the repository.

To run tests with your own PDF statements:
1. Place PDF files in testdata/source/velobank/
2. Run: pytest beancount_import/source/velobank_test.py -v
"""

import datetime
import os
from decimal import Decimal

import pytest

from . import velobank


class TestPolishAmountParsing:
    """Test parsing of Polish-formatted amounts."""

    def test_positive_amount(self):
        assert velobank.parse_polish_amount("1 500,00") == Decimal("1500.00")

    def test_negative_amount(self):
        assert velobank.parse_polish_amount("-8 706,73") == Decimal("-8706.73")

    def test_simple_amount(self):
        assert velobank.parse_polish_amount("45,00") == Decimal("45.00")

    def test_large_amount(self):
        assert velobank.parse_polish_amount("20 000,00") == Decimal("20000.00")

    def test_negative_simple(self):
        assert velobank.parse_polish_amount("-45,00") == Decimal("-45.00")


class TestPolishDateParsing:
    """Test parsing of Polish-formatted dates."""

    def test_dot_separator(self):
        result = velobank.parse_polish_date("2024.01.15")
        assert result == datetime.date(2024, 1, 15)

    def test_dash_separator(self):
        result = velobank.parse_polish_date("2024-01-15")
        assert result == datetime.date(2024, 1, 15)


class TestFormatDetection:
    """Test automatic format detection."""

    def test_old_format_detection(self):
        text = "Wyciąg z rachunku nr 1/2024 za okres 2024.01.01 - 2024.01.31"
        assert velobank.detect_format(text) == 'old'

    def test_new_format_detection(self):
        text = "VeloBank S.A.\nWyciąg nr 7718519\nWyciąg za okres od 2024.12.01"
        assert velobank.detect_format(text) == 'new'


class TestTransactionTypeExtraction:
    """Test extraction of transaction types from descriptions."""

    def test_outgoing_external_transfer(self):
        result = velobank._extract_transaction_type(
            "Przelew wychodzący zewnętrzny do John Doe")
        assert result == "Przelew wychodzący zewnętrzny"

    def test_incoming_internal_transfer(self):
        result = velobank._extract_transaction_type(
            "Przelew przychodzący wewnętrzny od XYZ")
        assert result == "Przelew przychodzący wewnętrzny"

    def test_card_operation(self):
        result = velobank._extract_transaction_type(
            "Operacja kartą 5375 xxxx xxxx 1234 na kwotę 50,00 PLN")
        assert result == "Operacja kartą"

    def test_generic_description(self):
        result = velobank._extract_transaction_type("NETFLIX subscription")
        assert result == "NETFLIX subscription"


class TestVelobankSource:
    """Test VelobankSource class functionality."""

    def test_source_name(self):
        """Test that source has correct name."""
        # Create source with empty directory (no PDFs to load)
        import tempfile
        with tempfile.TemporaryDirectory() as tmpdir:
            source = velobank.VelobankSource(
                directory=tmpdir,
                assets_account='Assets:Bank:VeloBank',
                log_status=lambda x: None,
            )
            assert source.name == 'velobank'
            assert source.assets_account == 'Assets:Bank:VeloBank'

    def test_is_posting_cleared(self):
        """Test posting cleared detection."""
        from beancount.core.data import Posting
        from beancount.core.amount import Amount
        from beancount.core.number import D
        import tempfile

        with tempfile.TemporaryDirectory() as tmpdir:
            source = velobank.VelobankSource(
                directory=tmpdir,
                assets_account='Assets:Bank:VeloBank',
                log_status=lambda x: None,
            )

            # Posting with velobank_statement is cleared
            cleared_posting = Posting(
                account='Assets:Bank:VeloBank',
                units=Amount(D('100'), 'PLN'),
                cost=None,
                price=None,
                flag=None,
                meta={'velobank_statement': '1/2024:1:abc123'},
            )
            assert source.is_posting_cleared(cleared_posting) is True

            # Posting without metadata is not cleared
            uncleared_posting = Posting(
                account='Assets:Bank:VeloBank',
                units=Amount(D('100'), 'PLN'),
                cost=None,
                price=None,
                flag=None,
                meta=None,
            )
            assert source.is_posting_cleared(uncleared_posting) is False

