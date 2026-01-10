"""Tests for Revolut CSV/PDF source."""

import datetime
import os
import tempfile
from decimal import Decimal
from unittest import mock

import pytest

from beancount_import.source import revolut


class TestParseRevolutDate:
    """Tests for parse_revolut_date function."""
    
    def test_datetime_format(self):
        """Parse datetime with timestamp."""
        result = revolut.parse_revolut_date('2025-01-02 02:19:20')
        assert result == datetime.date(2025, 1, 2)
    
    def test_date_only_format(self):
        """Parse date without time."""
        result = revolut.parse_revolut_date('2025-01-02')
        assert result == datetime.date(2025, 1, 2)
    
    def test_invalid_format(self):
        """Invalid date raises ValueError."""
        with pytest.raises(ValueError):
            revolut.parse_revolut_date('invalid')


class TestParseRevolutAmount:
    """Tests for parse_revolut_amount function."""
    
    def test_negative_amount(self):
        """Parse negative amount."""
        result = revolut.parse_revolut_amount('-15.00')
        assert result == Decimal('-15.00')
    
    def test_positive_amount(self):
        """Parse positive amount."""
        result = revolut.parse_revolut_amount('1000.00')
        assert result == Decimal('1000.00')
    
    def test_empty_string(self):
        """Empty string returns ZERO."""
        result = revolut.parse_revolut_amount('')
        assert result == Decimal('0')


class TestDetectCsvFormat:
    """Tests for detect_csv_format function."""
    
    def test_credit_card_format(self):
        """Detect credit card CSV format."""
        with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False) as f:
            f.write('Type,Started Date,Completed Date,Description,Amount,Fee,Balance\n')
            f.flush()
            result = revolut.detect_csv_format(f.name)
        os.unlink(f.name)
        assert result == 'creditcard'
    
    def test_account_format(self):
        """Detect regular account CSV format."""
        with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False) as f:
            f.write('Type,Product,Started Date,Completed Date,Description,Amount,Fee,Currency,State,Balance\n')
            f.flush()
            result = revolut.detect_csv_format(f.name)
        os.unlink(f.name)
        assert result == 'account'


class TestParseCreditCardCsv:
    """Tests for parse_credit_card_csv function."""
    
    def test_parse_basic_transactions(self):
        """Parse credit card CSV with transactions."""
        csv_content = """Type,Started Date,Completed Date,Description,Amount,Fee,Balance
TRANSFER,2025-01-02 02:19:45,2025-01-02 02:19:45,Credit card repayment,137.69,0.00,0.00
CARD_PAYMENT,2025-01-02 18:24:10,2025-01-03 16:46:55,Allegro,-1.00,0.00,-1.00
"""
        with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False) as f:
            f.write(csv_content)
            f.flush()
            result = revolut.parse_credit_card_csv(f.name)
        os.unlink(f.name)
        
        assert len(result.transactions) == 2
        assert result.currency == 'PLN'
        assert result.account_type == 'creditcard'
        
        # Check first transaction
        txn = result.transactions[0]
        assert txn.transaction_type == 'TRANSFER'
        assert txn.description == 'Credit card repayment'
        assert txn.amount == Decimal('137.69')
        assert txn.completed_date == datetime.date(2025, 1, 2)


class TestParseAccountCsv:
    """Tests for parse_account_csv function."""
    
    def test_parse_multi_currency(self):
        """Parse account CSV with multiple currencies."""
        csv_content = """Type,Product,Started Date,Completed Date,Description,Amount,Fee,Currency,State,Balance
Card Payment,Current,2025-01-06 08:09:49,2025-01-06 12:58:39,Trading 212,-627.36,0.00,PLN,COMPLETED,224.44
Transfer,Current,2025-10-08 20:19:48,2025-10-08 20:19:48,Exchanged to EUR,23.36,0.00,EUR,COMPLETED,23.36
"""
        with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False) as f:
            f.write(csv_content)
            f.flush()
            result = revolut.parse_account_csv(f.name)
        os.unlink(f.name)
        
        assert len(result) == 2  # Two currency statements
        
        pln_stmt = next(s for s in result if s.currency == 'PLN')
        eur_stmt = next(s for s in result if s.currency == 'EUR')
        
        assert len(pln_stmt.transactions) == 1
        assert len(eur_stmt.transactions) == 1
        
        pln_txn = pln_stmt.transactions[0]
        assert pln_txn.description == 'Trading 212'
        assert pln_txn.amount == Decimal('-627.36')


class TestMatchCsvWithPdf:
    """Tests for match_csv_with_pdf function."""
    
    def test_match_by_date_and_description(self):
        """Match CSV transactions with PDF data."""
        # Create CSV statement
        csv_txn = revolut.RevolutTransaction(
            transaction_type='Card Payment',
            started_date=datetime.date(2025, 1, 6),
            completed_date=datetime.date(2025, 1, 6),
            description='Trading 212',
            amount=Decimal('-627.36'),
            fee=Decimal('0'),
            balance_after=Decimal('224.44'),
            currency='PLN',
            product='Current',
            state='COMPLETED',
            line_number=2,
        )
        csv_stmt = revolut.CsvStatementInfo(
            filename='test.csv',
            account_type='personal',
            currency='PLN',
            transactions=[csv_txn],
        )
        
        # Create PDF section
        pdf_txn = revolut.PdfTransactionInfo(
            date=datetime.date(2025, 1, 6),
            description='Trading 212',
            amount=None,
            iban='LT133250085489069781',
            card_number='516794******6712',
            counterparty_address='Trading 212, London, 7NA',
            exchange_rate='1.00 PLN = $0.24',
            original_currency='USD',
            original_amount='151.05',
        )
        pdf_section = revolut.PdfCurrencySection(
            currency='PLN',
            ibans=['LT133250085489069781'],
            transactions=[pdf_txn],
        )
        
        # Match
        revolut.match_csv_with_pdf([csv_stmt], {'PLN': pdf_section})
        
        # Verify enrichment
        assert csv_txn.iban == 'LT133250085489069781'
        assert csv_txn.card_number == '516794******6712'
        assert csv_txn.counterparty_address == 'Trading 212, London, 7NA'
        assert csv_txn.exchange_rate == '1.00 PLN = $0.24'


class TestRevolutSource:
    """Tests for RevolutSource class."""
    
    def test_requires_account_map(self):
        """Source requires account_map."""
        with pytest.raises(TypeError):
            revolut.RevolutSource(
                directory='/tmp/nonexistent',
                log_status=lambda x: None,
            )
    
    def test_empty_account_map_raises(self):
        """Empty account_map raises ValueError."""
        with pytest.raises(ValueError):
            revolut.RevolutSource(
                directory='/tmp/nonexistent',
                account_map={},
                log_status=lambda x: None,
            )
    
    def test_get_account_for_id(self):
        """Test account mapping."""
        with tempfile.TemporaryDirectory() as tmpdir:
            source = revolut.RevolutSource(
                directory=tmpdir,
                account_map={'personal_PLN': 'Assets:Revolut:PLN'},
                log_status=lambda x: None,
            )
            
            assert source._get_account_for_id('personal_PLN') == 'Assets:Revolut:PLN'
            assert source._get_account_for_id('unknown_EUR') is None
