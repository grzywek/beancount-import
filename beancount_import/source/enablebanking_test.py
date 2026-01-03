"""Tests for EnableBanking source."""

import datetime
import json
import os
import tempfile
from decimal import Decimal

import pytest

from beancount_import.source.enablebanking import (
    EnableBankingSource,
    EnableBankingTransaction,
    _parse_decimal,
    _parse_date,
    _parse_transaction,
    _generate_transaction_id,
)


class TestParseDecimal:
    def test_valid_decimal(self):
        assert _parse_decimal("100.50") == Decimal("100.50")
        assert _parse_decimal("1000") == Decimal("1000")
    
    def test_none_returns_none(self):
        assert _parse_decimal(None) is None
    
    def test_empty_string_returns_none(self):
        assert _parse_decimal("") is None


class TestParseDate:
    def test_iso_date(self):
        result = _parse_date("2024-01-15")
        assert result == datetime.date(2024, 1, 15)
    
    def test_iso_datetime_truncated(self):
        result = _parse_date("2024-01-15T10:30:00")
        assert result == datetime.date(2024, 1, 15)
    
    def test_none_returns_none(self):
        assert _parse_date(None) is None


class TestParseTransaction:
    def test_basic_debit_transaction(self):
        txn_data = {
            "entry_reference": "202512281122",
            "transaction_amount": {
                "currency": "PLN",
                "amount": "100.00"
            },
            "credit_debit_indicator": "DBIT",
            "status": "BOOK",
            "booking_date": "2025-12-28",
            "remittance_information": ["Test payment"],
        }
        
        txn = _parse_transaction(txn_data, "PL123_PLN", "mBank")
        
        assert txn is not None
        assert txn.entry_reference == "202512281122"
        assert txn.amount == Decimal("-100.00")  # DBIT = negative
        assert txn.currency == "PLN"
        assert txn.booking_date == datetime.date(2025, 12, 28)
        assert txn.bank == "mBank"
    
    def test_credit_transaction(self):
        txn_data = {
            "entry_reference": "202512221120",
            "transaction_amount": {
                "currency": "PLN",
                "amount": "500.00"
            },
            "credit_debit_indicator": "CRDT",
            "status": "BOOK",
            "booking_date": "2025-12-22",
            "debtor": {
                "name": "JOHN DOE",
            },
            "remittance_information": ["Salary"],
        }
        
        txn = _parse_transaction(txn_data, "PL456_PLN", "Pekao")
        
        assert txn is not None
        assert txn.amount == Decimal("500.00")  # CRDT = positive
        assert txn.debtor_name == "JOHN DOE"
    
    def test_missing_entry_reference_returns_none(self):
        txn_data = {
            "transaction_amount": {"currency": "PLN", "amount": "100.00"},
            "credit_debit_indicator": "DBIT",
            "booking_date": "2025-01-01",
        }
        
        assert _parse_transaction(txn_data, "PL123_PLN", "bank") is None
    
    def test_bank_transaction_code(self):
        txn_data = {
            "entry_reference": "abc123",
            "transaction_amount": {"currency": "PLN", "amount": "50.00"},
            "credit_debit_indicator": "DBIT",
            "booking_date": "2025-01-01",
            "bank_transaction_code": {
                "code": "CARD_PAYMENT",
            },
        }
        
        txn = _parse_transaction(txn_data, "PL123_PLN", "Revolut")
        
        assert txn is not None
        assert txn.bank_transaction_code == "CARD_PAYMENT"


class TestGenerateTransactionId:
    def test_id_includes_bank_and_reference(self):
        txn = EnableBankingTransaction(
            entry_reference="12345",
            amount=Decimal("100"),
            currency="PLN",
            credit_debit_indicator="DBIT",
            booking_date=datetime.date(2025, 1, 1),
            transaction_date=None,
            value_date=None,
            status="BOOK",
            creditor_name=None,
            creditor_iban=None,
            debtor_name=None,
            debtor_iban=None,
            remittance_information=[],
            bank_transaction_code=None,
            balance_after=None,
            account_id="PL123_PLN",
            bank="mBank",
        )
        
        result = _generate_transaction_id(txn)
        assert result == "mBank:12345"


class TestEnableBankingSource:
    @pytest.fixture
    def test_data_dir(self):
        """Create a temporary directory with test JSON files."""
        with tempfile.TemporaryDirectory() as tmpdir:
            # Create mbank subdirectory
            mbank_dir = os.path.join(tmpdir, "mbank")
            os.makedirs(mbank_dir)
            
            # Create accounts.json
            accounts_data = {
                "accounts": [
                    {
                        "account_id": {"iban": "PL11111111111111111111111111", "other": None},
                        "currency": "PLN",
                        "name": "Test Account",
                        "product": "Checking",
                    }
                ],
                "aspsp_id": "mBank",
            }
            with open(os.path.join(mbank_dir, "accounts.json"), "w") as f:
                json.dump(accounts_data, f)
            
            # Create transactions file
            transactions_data = {
                "account_id": "PL11111111111111111111111111_PLN",
                "transactions": [
                    {
                        "entry_reference": "txn001",
                        "transaction_amount": {"currency": "PLN", "amount": "100.00"},
                        "credit_debit_indicator": "DBIT",
                        "status": "BOOK",
                        "booking_date": "2025-01-15",
                        "remittance_information": ["Test payment"],
                        "balance_after_transaction": {"currency": "PLN", "amount": "900.00"},
                    },
                    {
                        "entry_reference": "txn002",
                        "transaction_amount": {"currency": "PLN", "amount": "50.00"},
                        "credit_debit_indicator": "CRDT",
                        "status": "BOOK",
                        "booking_date": "2025-01-16",
                        "debtor": {"name": "JOHN DOE"},
                        "remittance_information": ["Incoming transfer"],
                        "balance_after_transaction": {"currency": "PLN", "amount": "950.00"},
                    },
                ],
            }
            with open(os.path.join(mbank_dir, "transactions_PL11111111111111111111111111_PLN.json"), "w") as f:
                json.dump(transactions_data, f)
            
            yield tmpdir
    
    def test_requires_account_config(self):
        """Test that source requires account_map or default_account."""
        with tempfile.TemporaryDirectory() as tmpdir:
            with pytest.raises(ValueError, match="requires either"):
                EnableBankingSource(data_directory=tmpdir, log_status=lambda x: None)
    
    def test_loads_accounts(self, test_data_dir):
        source = EnableBankingSource(
            data_directory=test_data_dir,
            default_account="Assets:Bank",
            log_status=lambda x: None,
        )
        
        assert len(source.accounts) == 1
        assert source.accounts[0].iban == "PL11111111111111111111111111"
        assert source.accounts[0].bank == "mBank"
    
    def test_loads_transactions(self, test_data_dir):
        source = EnableBankingSource(
            data_directory=test_data_dir,
            default_account="Assets:Bank",
            log_status=lambda x: None,
        )
        
        assert len(source.transactions) == 2
        
        # First transaction (debit)
        assert source.transactions[0].amount == Decimal("-100.00")
        assert source.transactions[0].entry_reference == "txn001"
        
        # Second transaction (credit)
        assert source.transactions[1].amount == Decimal("50.00")
        assert source.transactions[1].debtor_name == "JOHN DOE"
    
    def test_account_map(self, test_data_dir):
        source = EnableBankingSource(
            data_directory=test_data_dir,
            account_map={
                "PL11111111111111111111111111_PLN": "Assets:mBank:Checking",
            },
            default_account="Assets:Unknown",
            log_status=lambda x: None,
        )
        
        # Mapped account
        result = source._get_account_for_id("PL11111111111111111111111111_PLN")
        assert result == "Assets:mBank:Checking"
        
        # Unknown account (fallback)
        result = source._get_account_for_id("UNKNOWN_PLN")
        assert result == "Assets:Unknown"
    
    def test_make_transaction_debit(self, test_data_dir):
        source = EnableBankingSource(
            data_directory=test_data_dir,
            default_account="Assets:Bank",
            log_status=lambda x: None,
        )
        
        txn = source.transactions[0]  # Debit transaction
        beancount_txn = source._make_transaction(txn, "Assets:Bank")
        
        assert beancount_txn.date == datetime.date(2025, 1, 15)
        assert beancount_txn.narration == "Test payment"
        assert len(beancount_txn.postings) == 2
        
        # First posting (bank account)
        assert beancount_txn.postings[0].account == "Assets:Bank"
        assert beancount_txn.postings[0].units.number == Decimal("-100.00")
        
        # Second posting (FIXME)
        assert beancount_txn.postings[1].account == "Expenses:FIXME"
        assert beancount_txn.postings[1].units.number == Decimal("100.00")
    
    def test_make_transaction_credit(self, test_data_dir):
        source = EnableBankingSource(
            data_directory=test_data_dir,
            default_account="Assets:Bank",
            log_status=lambda x: None,
        )
        
        txn = source.transactions[1]  # Credit transaction
        beancount_txn = source._make_transaction(txn, "Assets:Bank")
        
        assert beancount_txn.date == datetime.date(2025, 1, 16)
        assert beancount_txn.payee == "JOHN DOE"  # Debtor as payee for incoming
        assert beancount_txn.postings[0].units.number == Decimal("50.00")


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
