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

            # Posting with source_ref is cleared
            cleared_posting = Posting(
                account='Assets:Bank:VeloBank',
                units=Amount(D('100'), 'PLN'),
                cost=None,
                price=None,
                flag=None,
                meta={'source_ref': '1/2024:1:abc123'},
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


class TestCreditCardHtmlParsing:
    """Test parsing of credit card HTML statements."""

    def test_extract_credit_card_transaction_type_card_payment(self):
        """Test card payment type extraction."""
        result = velobank._extract_credit_card_transaction_type(
            "Operacja kartą 5130 xxxx xxxx 6220 na kwotę 100,00 PLN w MERCHANT, CITY, POL")
        assert result == "Card payment"

    def test_extract_credit_card_transaction_type_card_refund(self):
        """Test card refund type extraction."""
        result = velobank._extract_credit_card_transaction_type(
            "Zwrot operacji kartą 5130 xxxx xxxx 6220 na kwotę 50,00 PLN w MERCHANT")
        assert result == "Card refund"

    def test_extract_credit_card_transaction_type_card_fee(self):
        """Test card fee type extraction."""
        result = velobank._extract_credit_card_transaction_type(
            "Opłata za obsługę karty nr 5130 XXX X XXXX 6220 za miesiąc 2024-12 (Obciążenie)")
        assert result == "Card fee"

    def test_extract_credit_card_transaction_type_repayment(self):
        """Test credit card repayment type extraction."""
        result = velobank._extract_credit_card_transaction_type(
            "spłata karty kredytowej (Uznanie)")
        assert result == "Credit card repayment"

    def test_html_parser_statement_metadata(self):
        """Test HTML parser extracts statement metadata correctly."""
        html_content = '''
        <html>
        <h1>Wyciąg z rachunku karty kredytowej numer 5/2025<br/>
        za okres rozliczeniowy 2025.05.01 - 2025.05.31</h1>
        <table>
            <tr class="border_bottom"><td>NA RACHUNEK KARTY KREDYTOWEJ</td>
                <td class="rightal">62 1560 0013 0000 0200 0537 1656</td></tr>
        </table>
        </html>
        '''
        parser = velobank.CreditCardHTMLParser()
        parser.feed(html_content)
        
        assert parser.statement_id == '5/2025'
        assert parser.period_start == datetime.date(2025, 5, 1)
        assert parser.period_end == datetime.date(2025, 5, 31)
        assert parser.account_iban == 'PL62156000130000020005371656'

    def test_html_parser_transactions(self):
        """Test HTML parser extracts transactions correctly."""
        html_content = '''
        <html>
        <table id="operacje" class="rach_log">
            <tbody>
                <tr class="border_bottom">
                    <td class="data">2025.08.11</td>
                    <td class="data">2025.08.11</td>
                    <td class="opis">Opłata za obsługę karty (Obciążenie)</td>
                    <td class="liczba">5,00 PLN</td>
                </tr>
                <tr class="border_bottom">
                    <td class="data">2025.08.13</td>
                    <td class="data">2025.08.13</td>
                    <td class="opis">spłata karty kredytowej (Uznanie)</td>
                    <td class="liczba">-3 120,68 PLN</td>
                </tr>
            </tbody>
        </table>
        </html>
        '''
        parser = velobank.CreditCardHTMLParser()
        parser.feed(html_content)
        
        assert len(parser.transactions) == 2
        
        # First transaction - card fee
        assert parser.transactions[0]['booking_date'] == datetime.date(2025, 8, 11)
        assert parser.transactions[0]['amount'] == Decimal('5.00')
        
        # Second transaction - repayment
        assert parser.transactions[1]['booking_date'] == datetime.date(2025, 8, 13)
        assert parser.transactions[1]['amount'] == Decimal('-3120.68')

    def test_html_parser_skips_summary_rows(self):
        """Test HTML parser skips summary/balance rows."""
        html_content = '''
        <html>
        <table id="operacje" class="rach_log">
            <tbody>
                <tr class="border_bottom">
                    <td></td><td></td>
                    <td><span class="stift">SALDO POCZĄTKOWE (ZADŁUŻENIE)</span></td>
                    <td class="numcol">1000,00 PLN</td>
                </tr>
                <tr class="border_bottom">
                    <td class="data">2025.08.11</td>
                    <td class="data">2025.08.11</td>
                    <td class="opis">Valid transaction</td>
                    <td class="liczba">5,00 PLN</td>
                </tr>
                <tr class="border_bottom">
                    <td></td><td></td>
                    <td><span class="stift">SALDO KOŃCOWE (ZADŁUŻENIE)</span></td>
                    <td class="numcol">1005,00 PLN</td>
                </tr>
            </tbody>
        </table>
        </html>
        '''
        parser = velobank.CreditCardHTMLParser()
        parser.feed(html_content)
        
        # Only the valid transaction should be parsed
        assert len(parser.transactions) == 1
        assert parser.transactions[0]['description'] == 'Valid transaction'
