"""Tests for Trading 212 file-based source."""

import csv
import datetime
import tempfile
import json
import os
from decimal import Decimal
from unittest import mock

import pytest

from beancount_import.source.trading212 import (
    Trading212Source,
    Trading212DataError,
    ApiOrder,
    ApiDividend,
    ApiTransaction,
    ApiPosition,
    ApiAccountSummary,
    CsvTransaction,
    _ticker_to_symbol,
    _parse_iso_datetime,
    _parse_date,
)


class TestTickerToSymbol:
    def test_simple_ticker_usd(self):
        # US stock - returns base symbol
        assert _ticker_to_symbol("AAPL_US_EQ", "USD") == "AAPL"
    
    def test_simple_ticker_no_currency(self):
        # Without currency, should work like before
        assert _ticker_to_symbol("AAPL_US_EQ") == "AAPL"
    
    def test_ticker_without_suffix(self):
        assert _ticker_to_symbol("AAPL") == "AAPL"
    
    def test_complex_ticker(self):
        assert _ticker_to_symbol("VWCE_EAM_EQ") == "VWCE"
    
    def test_london_exchange_base_symbol(self):
        # London exchange - by default returns base symbol (exchange suffix stripped)
        assert _ticker_to_symbol("WTAIl_EQ") == "WTAI"
        assert _ticker_to_symbol("BAl_EQ") == "BA"
    
    def test_include_exchange_suffix(self):
        # With include_exchange=True, adds exchange suffix only when known
        assert _ticker_to_symbol("WTAIl_EQ", include_exchange=True) == "WTAI-LSE"
        assert _ticker_to_symbol("BAl_EQ", include_exchange=True) == "BA-LSE"
        # US stocks don't have a known exchange suffix - returns base symbol
        assert _ticker_to_symbol("AAPL_US_EQ", include_exchange=True) == "AAPL"
        assert _ticker_to_symbol("IBM_US_EQ", include_exchange=True) == "IBM"
    
    def test_preserve_lowercase_in_symbol(self):
        # Should not strip if the whole symbol has mixed case
        assert _ticker_to_symbol("ABCdef") == "ABCdef"


class TestParseDatetime:
    def test_iso_datetime(self):
        result = _parse_iso_datetime("2024-01-15T10:30:00Z")
        assert result == datetime.datetime(2024, 1, 15, 10, 30, 0, tzinfo=datetime.timezone.utc)
    
    def test_iso_datetime_with_offset(self):
        result = _parse_iso_datetime("2024-01-15T10:30:00+02:00")
        assert result is not None
        assert result.year == 2024
    
    def test_none_input(self):
        assert _parse_iso_datetime(None) is None
    
    def test_empty_string(self):
        assert _parse_iso_datetime("") is None


class TestParseDate:
    def test_iso_date(self):
        result = _parse_date("2024-01-15")
        assert result == datetime.date(2024, 1, 15)
    
    def test_iso_datetime_truncated(self):
        result = _parse_date("2024-01-15T10:30:00Z")
        assert result == datetime.date(2024, 1, 15)
    
    def test_none_input(self):
        assert _parse_date(None) is None


class TestTrading212Source:
    @pytest.fixture
    def test_data_dir(self):
        """Create a temporary directory with test JSON files."""
        with tempfile.TemporaryDirectory() as tmpdir:
            # Create minimal JSON files for testing
            with open(os.path.join(tmpdir, "orders.json"), "w") as f:
                json.dump({"items": []}, f)
            with open(os.path.join(tmpdir, "pending_orders.json"), "w") as f:
                json.dump({"items": []}, f)
            with open(os.path.join(tmpdir, "dividends.json"), "w") as f:
                json.dump({"items": []}, f)
            with open(os.path.join(tmpdir, "transactions.json"), "w") as f:
                json.dump({"items": []}, f)
            with open(os.path.join(tmpdir, "positions.json"), "w") as f:
                json.dump({"items": []}, f)
            with open(os.path.join(tmpdir, "account_summary.json"), "w") as f:
                json.dump({
                    "account_id": 12345,
                    "currency": "EUR",
                    "cash_available": "1000.00",
                    "cash_in_pies": "0",
                    "cash_reserved": "0", 
                    "investments_value": "5000.00",
                    "total_value": "6000.00",
                }, f)
            yield tmpdir
    
    def test_data_directory_required(self):
        """Test that data_directory is required and must exist."""
        with pytest.raises(Trading212DataError, match="does not exist"):
            Trading212Source(directory="/nonexistent/path")

    def _csv_txn(self, **overrides):
        values = dict(
            action="Market buy",
            time=datetime.datetime(2026, 4, 13, 11, 55, 2),
            isin="US0378331005",
            ticker="AAPL",
            name="Apple",
            notes=None,
            transaction_id="txn-001",
            num_shares=Decimal("3"),
            price_per_share=Decimal("219.3775203200"),
            price_currency="USD",
            exchange_rate=Decimal("1.00000000"),
            result=None,
            result_currency=None,
            currency="USD",
            total=Decimal("658.13"),
            withholding_tax=None,
            withholding_tax_currency=None,
            charge_amount=None,
            charge_currency=None,
            deposit_fee=None,
            deposit_fee_currency=None,
            stamp_duty=None,
            stamp_duty_currency=None,
            transaction_fee=None,
            transaction_fee_currency=None,
            finra_fee=None,
            finra_fee_currency=None,
            source_file="/tmp/export.csv",
        )
        values.update(overrides)
        return CsvTransaction(**values)
    
    def test_get_symbol_account(self, test_data_dir):
        source = Trading212Source(
            directory=test_data_dir,
            investment_account="Assets:Trading212",
        )
        assert source._get_symbol_account("AAPL_US_EQ") == "Assets:Trading212:AAPL"
    
    def test_make_order_transaction_buy(self, test_data_dir):
        source = Trading212Source(
            directory=test_data_dir,
            cash_trade_account="Assets:Trading212:Cash",
            investment_account="Assets:Trading212",
        )
        
        order = ApiOrder(
            order_id=12345,
            ticker="AAPL_US_EQ",
            isin="US0378331005",
            name="Apple Inc",
            side="BUY",
            status="FILLED",
            quantity=Decimal("10"),
            filled_quantity=Decimal("10"),
            filled_price=Decimal("150.00"),
            filled_at=datetime.datetime(2024, 1, 15, 10, 30, 0),
            created_at=datetime.datetime(2024, 1, 15, 10, 29, 0),
            currency="USD",
            account_currency="EUR",
            fx_rate=Decimal("0.92"),
            net_value=Decimal("1380.00"),
            realized_pnl=None,
            taxes=[],
        )
        
        txn = source._make_order_transaction(order)
        
        assert txn.date == datetime.date(2024, 1, 15)
        assert txn.payee == "TRADING 212"
        assert "Buy" in txn.narration
        assert len(txn.postings) == 2
        
        # First posting should be the stock
        stock_posting = txn.postings[0]
        assert stock_posting.account == "Assets:Trading212:AAPL"
        assert stock_posting.units.number == Decimal("10")
        assert stock_posting.units.currency == "AAPL"
        
        # Second posting should be cash
        cash_posting = txn.postings[1]
        assert cash_posting.account == "Assets:Trading212:Cash"
        assert cash_posting.units.number == Decimal("-1380.00")
    
    def test_make_dividend_transaction(self, test_data_dir):
        source = Trading212Source(
            directory=test_data_dir,
            cash_trade_account="Assets:Trading212:Cash",
            dividend_income_account="Income:Dividends",
        )
        
        dividend = ApiDividend(
            reference="div-123",
            ticker="AAPL_US_EQ",
            isin="US0378331005",
            name="Apple Inc",
            paid_on=datetime.date(2024, 3, 15),
            quantity=Decimal("10"),
            amount=Decimal("12.50"),
            amount_per_share=Decimal("1.25"),
            currency="EUR",
            ticker_currency="USD",
            dividend_type="ORDINARY",
        )
        
        txn = source._make_dividend_transaction(dividend)
        
        assert txn.date == datetime.date(2024, 3, 15)
        assert "Dividend" in txn.narration
        assert len(txn.postings) == 2
        
        # First posting should be cash received
        cash_posting = txn.postings[0]
        assert cash_posting.account == "Assets:Trading212:Cash"
        assert cash_posting.units.number == Decimal("12.50")
        
        # Second posting should be income
        income_posting = txn.postings[1]
        assert income_posting.account == "Income:Dividends:AAPL"
        assert income_posting.units.number == Decimal("-12.50")
    
    def test_make_deposit_transaction(self, test_data_dir):
        source = Trading212Source(
            directory=test_data_dir,
            cash_vault_account="Assets:Trading212:Cash",
            transfer_account="Assets:Bank:Checking",
        )
        
        txn_api = ApiTransaction(
            reference="txn-456",
            transaction_type="DEPOSIT",
            amount=Decimal("1000.00"),
            currency="EUR",
            date_time=datetime.datetime(2024, 1, 1, 12, 0, 0),
        )
        
        txn = source._make_cash_transaction(txn_api)
        
        assert txn.date == datetime.date(2024, 1, 1)
        assert txn.narration == "Deposit"
        assert "fixme" not in txn.tags  # DEPOSIT should not have fixme
        assert len(txn.postings) == 2
        
        # First posting should be cash increase
        cash_posting = txn.postings[0]
        assert cash_posting.units.number == Decimal("1000.00")
        
        # Second posting should be transfer account
        transfer_posting = txn.postings[1]
        assert transfer_posting.account == "Assets:Bank:Checking"
        assert transfer_posting.units.number == Decimal("-1000.00")
    
    def test_make_unknown_transaction_has_fixme_tag(self, test_data_dir):
        source = Trading212Source(
            directory=test_data_dir,
            cash_trade_account="Assets:Trading212:Cash",
            transfer_account="Assets:Bank:Checking",
        )
        
        txn_api = ApiTransaction(
            reference="txn-789",
            transaction_type="UNKNOWN_TYPE",
            amount=Decimal("500.00"),
            currency="EUR",
            date_time=datetime.datetime(2024, 1, 1, 12, 0, 0),
        )
        
        txn = source._make_cash_transaction(txn_api)
        
        assert txn.date == datetime.date(2024, 1, 1)
        assert "fixme" in txn.tags  # Unknown type should have fixme
    
    def test_csv_only_transactions_includes_stock_distribution(self, test_data_dir):
        """Test that _get_csv_only_transactions includes stock distributions."""
        source = Trading212Source(
            directory=test_data_dir,
            cash_trade_account="Assets:Trading212:Cash",
            investment_account="Assets:Trading212",
        )
        
        # Create mock CSV transactions
        from beancount_import.source.trading212 import CsvTransaction
        
        source._csv_transactions = [
            CsvTransaction(
                action="Market buy",
                time=datetime.datetime(2024, 1, 1, 10, 0, 0),
                isin="US0378331005",
                ticker="AAPL_US_EQ",
                name="Apple Inc",
                notes=None,
                transaction_id="txn-001",
                num_shares=Decimal("10"),
                price_per_share=Decimal("150.00"),
                price_currency="USD",
                exchange_rate=Decimal("1"),
                result=None,
                result_currency=None,
                currency="USD",
                total=Decimal("1500.00"),
                withholding_tax=None,
                withholding_tax_currency=None,
                charge_amount=None,
                charge_currency=None,
                deposit_fee=None,
                deposit_fee_currency=None,
                stamp_duty=None,
                stamp_duty_currency=None,
                transaction_fee=None,
                transaction_fee_currency=None,
                finra_fee=None,
                finra_fee_currency=None,
            ),
            CsvTransaction(
                action="Lending interest",
                time=datetime.datetime(2024, 1, 2, 10, 0, 0),
                isin=None,
                ticker=None,
                name=None,
                notes="Share lending interest",
                transaction_id="lending-001",
                num_shares=None,
                price_per_share=None,
                price_currency=None,
                exchange_rate=None,
                result=None,
                result_currency=None,
                currency="USD",
                total=Decimal("0.05"),
                withholding_tax=None,
                withholding_tax_currency=None,
                charge_amount=None,
                charge_currency=None,
                deposit_fee=None,
                deposit_fee_currency=None,
                stamp_duty=None,
                stamp_duty_currency=None,
                transaction_fee=None,
                transaction_fee_currency=None,
                finra_fee=None,
                finra_fee_currency=None,
            ),
            CsvTransaction(
                action="Stock distribution",
                time=datetime.datetime(2024, 2, 15, 12, 0, 0),
                isin="US42227T1051",
                ticker="HCWC_US_EQ",
                name="Healthy Choice Wellness",
                notes=None,
                transaction_id="dist-001",
                num_shares=Decimal("2.87"),
                price_per_share=Decimal("0"),
                price_currency="USD",
                exchange_rate=None,
                result=None,
                result_currency=None,
                currency="USD",
                total=Decimal("0"),
                withholding_tax=None,
                withholding_tax_currency=None,
                charge_amount=None,
                charge_currency=None,
                deposit_fee=None,
                deposit_fee_currency=None,
                stamp_duty=None,
                stamp_duty_currency=None,
                transaction_fee=None,
                transaction_fee_currency=None,
                finra_fee=None,
                finra_fee_currency=None,
            ),
        ]
        
        csv_only = source._get_csv_only_transactions()
        
        # Should include lending interest and stock distribution, but not market buy
        assert len(csv_only) == 2
        actions = {t.action for t in csv_only}
        assert "Lending interest" in actions
        assert "Stock distribution" in actions
        assert "Market buy" not in actions

    def test_make_csv_currency_conversion_transaction(self, test_data_dir):
        """Test that currency conversion generates correct postings."""
        source = Trading212Source(
            directory=test_data_dir,
            cash_vault_account="Assets:Broker:Trading212:USD:Vault",
            cash_trade_account="Assets:Broker:Trading212:USD:Trade",
            fx_income_account="Income:FX:Trading212",
            fees_account="Expenses:Fees:Trading:Trading212",
        )

        # Mock account summary to provide base currency
        source._account_summary = ApiAccountSummary(
            account_id=12345,
            currency="USD",
            cash_available=Decimal("1000.00"),
            cash_in_pies=Decimal("0"),
            cash_reserved=Decimal("0"),
            investments_value=Decimal("5000.00"),
            total_value=Decimal("6000.00"),
        )

        from beancount_import.source.trading212 import CsvTransaction

        csv_txn = CsvTransaction(
            action="Currency conversion",
            time=datetime.datetime(2026, 2, 27, 15, 36, 23),
            isin=None,
            ticker=None,
            name=None,
            notes="2366.00 EUR -> 2794.55 USD",
            transaction_id="HYN2V2FQ7RMDGX47",
            num_shares=None,
            price_per_share=None,
            price_currency=None,
            exchange_rate=None,
            result=None,
            result_currency=None,
            currency="USD",
            total=Decimal("-4.19"),
            withholding_tax=None,
            withholding_tax_currency=None,
            charge_amount=None,
            charge_currency=None,
            deposit_fee=None,
            deposit_fee_currency=None,
            stamp_duty=None,
            stamp_duty_currency=None,
            transaction_fee=None,
            transaction_fee_currency=None,
            finra_fee=None,
            finra_fee_currency=None,
            conversion_from_amount=Decimal("2366.00"),
            conversion_from_currency="EUR",
            conversion_to_amount=Decimal("2794.55"),
            conversion_to_currency="USD",
            conversion_fee=Decimal("-4.19"),
            conversion_fee_currency="USD",
            source_file="/tmp/test.csv",
        )

        txn = source._make_csv_currency_conversion_transaction(csv_txn)

        assert txn.date == datetime.date(2026, 2, 27)
        assert txn.payee == "TRADING 212"
        assert "Currency Conversion" in txn.narration
        assert "EUR" in txn.narration
        assert "USD" in txn.narration

        # Should have 4 postings: EUR out, USD in, fee, FX auto-balance
        assert len(txn.postings) == 4

        # First posting: EUR out of EUR vault (currency-specific)
        eur_posting = txn.postings[0]
        assert eur_posting.account == "Assets:Broker:Trading212:EUR:Vault"
        assert eur_posting.units.number == Decimal("-2366.00")
        assert eur_posting.units.currency == "EUR"

        # Second posting: USD into USD vault
        usd_posting = txn.postings[1]
        assert usd_posting.account == "Assets:Broker:Trading212:USD:Vault"
        assert usd_posting.units.number == Decimal("2794.55")
        assert usd_posting.units.currency == "USD"

        # Third posting: fee
        fee_posting = txn.postings[2]
        assert fee_posting.account == "Expenses:Fees:Trading:Trading212"
        assert fee_posting.units.number == Decimal("4.19")
        assert fee_posting.units.currency == "USD"

        # Fourth posting: FX income auto-balance
        fx_posting = txn.postings[3]
        assert fx_posting.account == "Income:FX:Trading212"
        assert fx_posting.units is None  # Auto-balanced

    def test_make_csv_dividend_adjustment_without_ticker(self, test_data_dir):
        """Dividend adjustments without instrument data should not try to resolve a stock symbol."""
        source = Trading212Source(
            directory=test_data_dir,
            cash_trade_account="Assets:Trading212:Cash",
            dividend_income_account="Income:Dividends",
        )

        csv_txn = self._csv_txn(
            action="Dividend adjustment",
            isin=None,
            ticker=None,
            name=None,
            notes="2025 US dividends withholding tax adjustment.",
            transaction_id="019d632c-1c34-72cb-a2af-3f06547aca23",
            num_shares=None,
            price_per_share=None,
            price_currency=None,
            result=None,
            result_currency=None,
            total=Decimal("0.01"),
        )

        txn = source._make_csv_dividend_transaction(csv_txn)

        assert txn.narration == "Dividend adjustment"
        assert txn.postings[0].account == "Assets:Trading212:Cash"
        assert txn.postings[0].units.number == Decimal("0.01")
        assert txn.postings[1].account == "Income:Dividends"
        assert txn.postings[1].units is None

    def test_make_csv_transfer_out_transaction(self, test_data_dir):
        source = Trading212Source(
            directory=test_data_dir,
            investment_account="Assets:Trading212:Holdings",
            transfer_account="Assets:Transfers:Trading212",
        )

        csv_txn = self._csv_txn(action="Transfer out")

        txn = source._make_csv_security_transfer_transaction(csv_txn)

        assert txn.narration == "Transfer out - AAPL"
        assert len(txn.postings) == 2
        stock_posting = txn.postings[0]
        assert stock_posting.account == "Assets:Trading212:Holdings:AAPL"
        assert stock_posting.units.number == Decimal("-3")
        assert stock_posting.units.currency == "AAPL"
        assert stock_posting.cost.number_per is None
        assert txn.postings[1].account == "Assets:Transfers:Trading212"
        assert txn.postings[1].units is None

    def test_make_csv_transfer_in_transaction(self, test_data_dir):
        source = Trading212Source(
            directory=test_data_dir,
            investment_account="Assets:Trading212:Holdings",
            transfer_account="Assets:Transfers:Trading212",
        )

        csv_txn = self._csv_txn(
            action="Transfer in",
            isin="US88160R1014",
            ticker="TSLA",
            name="Tesla",
            num_shares=Decimal("22"),
            price_per_share=Decimal("186.6900000000"),
            total=Decimal("4107.18"),
        )

        txn = source._make_csv_security_transfer_transaction(csv_txn)

        assert txn.narration == "Transfer in - TSLA"
        stock_posting = txn.postings[0]
        assert stock_posting.account == "Assets:Trading212:Holdings:TSLA"
        assert stock_posting.units.number == Decimal("22")
        assert stock_posting.units.currency == "TSLA"
        assert stock_posting.cost.number_per == Decimal("186.6900000000")
        assert stock_posting.cost.currency == "USD"
        assert txn.postings[1].account == "Assets:Transfers:Trading212"

    def test_example_csv_actions_are_supported(self):
        csv_path = os.path.join(
            os.path.dirname(os.path.dirname(os.path.dirname(__file__))),
            "examples",
            "data",
            "trading212",
            "export_2026-03-22_to_2026-04-30.csv",
        )
        with open(csv_path, newline="", encoding="utf-8") as f:
            actions = {row["Action"] for row in csv.DictReader(f)}

        supported_actions = {
            "Currency conversion",
            "Deposit",
            "Interest on cash",
            "Lending interest",
            "Market buy",
            "Market sell",
            "Stop buy",
            "Transfer in",
            "Transfer out",
        }
        unsupported_actions = {
            action for action in actions
            if action not in supported_actions and not action.startswith("Dividend")
        }

        assert unsupported_actions == set()


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
