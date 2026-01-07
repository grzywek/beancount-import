"""Zen (ZEN.COM UAB) CSV bank statement source.

Data format
===========

This source imports transactions from Zen monthly CSV statements. Download
CSV statements from the Zen web interface and store them in a directory
structure organized by year.

Directory structure:
    zen/
      2025/
        2025-01-PLN.csv
        2025-01-USD.csv
        2025-02-PLN.csv
        ...

Specifying the source to beancount_import
=========================================

Within your Python script for invoking beancount_import, use an expression like:

    # Multi-currency configuration (recommended):
    dict(
        module='beancount_import.source.zen',
        data_directory='/path/to/zen',
        account_map={
            'GB72TCCL04140411776433_PLN': 'Assets:Zen:PLN',
            'GB72TCCL04140411776433_USD': 'Assets:Zen:USD',
            'GB72TCCL04140411776433_EUR': 'Assets:Zen:EUR',
            # Format: IBAN_CURRENCY -> Beancount account
        },
    )

    # Or with default_account fallback:
    dict(
        module='beancount_import.source.zen',
        data_directory='/path/to/zen',
        account_map={
            'GB72TCCL04140411776433_PLN': 'Assets:Zen:PLN',
        },
        default_account='Assets:Zen:Unknown',  # For unmapped currencies
    )

Imported transaction format
===========================

Transactions are generated in the following form:

    2025-01-02 * "STARBUCKS" "Card payment"
      Assets:Zen:PLN     -15.00 PLN
        source_ref: "zen:GB72TCCL04140411776433:25:2025-01-02:-15.00"
        source_bank: "Zen"
        transaction_type: "Card payment"
        counterparty: "STARBUCKS"
        counterparty_address: "POL"
      Expenses:FIXME          15.00 PLN

The `source_ref` metadata field is used to match transactions and avoid duplicates.
"""

import collections
import csv
import datetime
import io
import os
import re
from dataclasses import dataclass, field
from decimal import Decimal, InvalidOperation
from typing import Dict, List, Optional, Tuple

from beancount.core.data import Balance, Document, Posting, Transaction, EMPTY_SET
from beancount.core.flags import FLAG_OKAY
from beancount.core.number import D, ZERO
from beancount.core.amount import Amount

from . import ImportResult, Source, SourceResults, InvalidSourceReference
from ..matching import FIXME_ACCOUNT
from ..journal_editor import JournalEditor


# Metadata keys (standardized across all bank sources)
SOURCE_REF_KEY = 'source_ref'  # Unique transaction reference
SOURCE_BANK_KEY = 'source_bank'  # Bank name
TRANSACTION_TYPE_KEY = 'transaction_type'  # Transaction type
COUNTERPARTY_KEY = 'counterparty'  # Counterparty name
COUNTERPARTY_ADDRESS_KEY = 'counterparty_address'  # Counterparty address/location
COUNTERPARTY_IBAN_KEY = 'counterparty_iban'  # Counterparty IBAN
ACCOUNT_IBAN_KEY = 'account_iban'  # Own account IBAN
TITLE_KEY = 'title'  # Transaction title
CARD_NUMBER_KEY = 'card_number'  # Last 4 digits of card
ORIGINAL_AMOUNT_KEY = 'original_amount'  # Original amount if different currency
ORIGINAL_CURRENCY_KEY = 'original_currency'  # Original currency
CURRENCY_RATE_KEY = 'currency_rate'  # Exchange rate
SOURCE_DOC_KEY = 'document'  # Link to source document file (clickable in fava)


# Transaction types in Zen CSV
TRANSACTION_TYPES = {
    'Card payment',
    'Card refund',
    'Incoming transfer',
    'Outgoing transfer',
    'Exchange money',
    'Cashback',
    'Cashback Refund',
}


# Pattern to match files that already have a 4-digit suffix before extension
SUFFIX_PATTERN = re.compile(r'-\d{4}(\.[^.]+)?$')


def ensure_file_has_suffix(filepath: str) -> str:
    """Ensure file has a 4-digit suffix, renaming it if needed.
    
    If the file doesn't have a suffix like '-1234', generate a random one
    and physically rename the file on disk.
    
    Args:
        filepath: Full path to the file.
        
    Returns:
        The new filepath (with suffix) or original if already had one.
    """
    import random
    
    basename = os.path.basename(filepath)
    
    # Check if file already has a 4-digit suffix
    if SUFFIX_PATTERN.search(basename):
        return filepath  # Already has suffix
    
    # Generate new filename with suffix
    base, ext = os.path.splitext(filepath)
    suffix = random.randint(1000, 9999)
    new_filepath = f"{base}-{suffix}{ext}"
    
    # Physically rename the file
    try:
        os.rename(filepath, new_filepath)
        return new_filepath
    except OSError as e:
        # If rename fails (permissions, etc.), return original
        print(f"Warning: could not rename {filepath} to {new_filepath}: {e}")
        return filepath


def parse_zen_date(text: str) -> datetime.date:
    """Parse Zen date format: "1 Jan 2025" or "28 Feb 2025".
    
    Args:
        text: Date string in Zen format.
        
    Returns:
        datetime.date object.
        
    Raises:
        ValueError: If date cannot be parsed.
    """
    text = text.strip()
    try:
        return datetime.datetime.strptime(text, "%d %b %Y").date()
    except ValueError:
        pass
    # Try alternative formats
    try:
        return datetime.datetime.strptime(text, "%Y-%m-%d").date()
    except ValueError:
        raise ValueError(f"Cannot parse date: {text}")


def parse_zen_amount(text: str) -> Decimal:
    """Parse Zen amount format: "-15.00" or "1000.00".
    
    Args:
        text: Amount string.
        
    Returns:
        Decimal representing the amount.
    """
    text = text.strip()
    if not text:
        return ZERO
    try:
        return D(text)
    except InvalidOperation:
        return ZERO


def _extract_counterparty_info(description: str, txn_type: str) -> Tuple[Optional[str], Optional[str], Optional[str], Optional[str]]:
    """Extract counterparty info from description.
    
    Handles formats:
    - Card payment: "MERCHANT LOCATION              COUNTRY,COUNTRY CARD: MASTERCARD *7492"
    - Transfer: "ZEN.COM UAB,   ZEN account top-up, Card **6671 "
    - Outgoing transfer: "Dawid Szwajca,  PL Proce PL42156000132001525640000001"
    - Cashback: "CASHBACK aliexpress 5e2b632c-b61a-7bcf-b216-0194afcff75d 4.0% 20250129Z"
    
    Returns:
        Tuple of (counterparty, address, iban, card_number)
    """
    counterparty = None
    address = None
    iban = None
    card_number = None
    
    if not description:
        return counterparty, address, iban, card_number
    
    # Extract card number from description
    card_match = re.search(r'CARD:\s*MASTERCARD\s*\*(\d{4})', description)
    if card_match:
        card_number = card_match.group(1)
    
    # Card payment format: "MERCHANT LOCATION              COUNTRY,COUNTRY CARD: MASTERCARD *7492"
    if txn_type in ('Card payment', 'Card refund'):
        # Split by comma
        parts = description.split(',', 1)
        if len(parts) >= 1:
            merchant_part = parts[0].strip()
            # Merchant name is before the long whitespace
            # Pattern: "MERCHANT              LOCATION"
            match = re.match(r'^(.+?)\s{2,}(.+)$', merchant_part)
            if match:
                counterparty = match.group(1).strip()
                address = match.group(2).strip()
            else:
                counterparty = merchant_part
    
    # Transfer format: "Counterparty,  Title IBAN"
    elif txn_type in ('Incoming transfer', 'Outgoing transfer'):
        parts = description.split(',', 1)
        if len(parts) >= 1:
            counterparty = parts[0].strip()
        if len(parts) >= 2:
            remainder = parts[1].strip()
            # Look for IBAN (starts with country code, 2 letters + digits)
            iban_match = re.search(r'((?:PL|GB|DE|LT|FR)[A-Z0-9]{10,32})', remainder)
            if iban_match:
                iban = iban_match.group(1)
    
    # Cashback format: "CASHBACK merchant uuid percentage date"
    elif txn_type in ('Cashback', 'Cashback Refund'):
        # Try to extract merchant name
        # Format: "CASHBACK merchant-name uuid 4.0% dateZ" or "STORNO CASHBACK ..."
        desc_upper = description.upper()
        if desc_upper.startswith('CASHBACK ') or desc_upper.startswith('STORNO CASHBACK '):
            # Remove prefix
            if desc_upper.startswith('STORNO CASHBACK '):
                remainder = description[16:].strip()
            else:
                remainder = description[9:].strip()
            # The merchant name is before the UUID (8-4-4-4-12 hex pattern)
            uuid_match = re.search(r'\s+[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}', remainder, re.IGNORECASE)
            if uuid_match:
                counterparty = remainder[:uuid_match.start()].strip()
            else:
                # No UUID, take first word as merchant
                parts = remainder.split(None, 1)
                if parts:
                    counterparty = parts[0]
    
    # Exchange money format: "Currency exchange transaction" or similar
    elif txn_type == 'Exchange money':
        # Usually just a description, no counterparty
        pass
    
    return counterparty, address, iban, card_number


@dataclass
class ZenTransaction:
    """Represents a parsed transaction from the CSV statement."""
    date: datetime.date
    transaction_type: str
    description: str
    settlement_amount: Decimal
    settlement_currency: str
    original_amount: Decimal
    original_currency: str
    currency_rate: Decimal
    fee_description: str
    fee_amount: Optional[Decimal]
    fee_currency: Optional[str]
    balance_after: Decimal
    line_number: int
    # Parsed from description
    counterparty: Optional[str] = None
    counterparty_address: Optional[str] = None
    counterparty_iban: Optional[str] = None
    card_number: Optional[str] = None


@dataclass
class StatementInfo:
    """Metadata about a parsed statement."""
    filename: str
    iban: str
    currency: str
    period_start: Optional[datetime.date]
    period_end: Optional[datetime.date]
    opening_balance: Optional[Decimal]
    closing_balance: Optional[Decimal]
    transactions: List[ZenTransaction] = field(default_factory=list)


def parse_csv(path: str) -> Optional[StatementInfo]:
    """Parse a Zen CSV statement file.
    
    Args:
        path: Path to the CSV file.
        
    Returns:
        StatementInfo with parsed data, or None on error.
    """
    try:
        with open(path, 'r', encoding='utf-8') as f:
            content = f.read()
    except Exception as e:
        print(f"zen: error reading {path}: {e}")
        return None
    
    # Handle Windows line endings
    content = content.replace('\r\n', '\n').replace('\r', '\n')
    lines = content.split('\n')
    
    # Parse header section
    iban = None
    currency = None
    period_start = None
    period_end = None
    opening_balance = None
    closing_balance = None
    transactions_start = None
    
    for i, line in enumerate(lines):
        line = line.strip()
        
        # Find IBAN
        if line.startswith('Global IBAN:'):
            iban = line.split(':', 1)[1].strip()
        
        # Find currency
        elif line.startswith('Currency:'):
            currency = line.split(':', 1)[1].strip()
        
        # Find period (Date: 1 Jan 2025 - 31 Jan 2025)
        elif line.startswith('Date:'):
            date_part = line.split(':', 1)[1].strip()
            date_match = re.match(r'(.+?)\s*-\s*(.+)', date_part)
            if date_match:
                try:
                    period_start = parse_zen_date(date_match.group(1).strip())
                    period_end = parse_zen_date(date_match.group(2).strip())
                except ValueError:
                    pass
        
        # Find opening balance (Opening balance:,759.28,PLN)
        elif 'Opening balance:' in line:
            parts = line.split(',')
            if len(parts) >= 2:
                opening_balance = parse_zen_amount(parts[1])
        
        # Find closing balance (Closing balance:,584.49,PLN)
        elif 'Closing balance:' in line:
            parts = line.split(',')
            if len(parts) >= 2:
                closing_balance = parse_zen_amount(parts[1])
        
        # Find transactions start
        elif line == 'Transactions:':
            transactions_start = i + 1
            break
    
    if not iban:
        print(f"zen: could not find IBAN in {path}")
        return None
    
    if transactions_start is None:
        print(f"zen: could not find Transactions: line in {path}")
        return None
    
    # Parse transactions using csv module
    transactions = []
    csv_content = '\n'.join(lines[transactions_start:])
    
    reader = csv.DictReader(io.StringIO(csv_content))
    
    for line_num, row in enumerate(reader, start=transactions_start + 2):
        # Skip empty rows
        if not row.get('Date'):
            continue
        
        # Skip footer rows
        date_str = row.get('Date', '').strip()
        if not date_str or date_str.startswith('This is a computer'):
            continue
        
        try:
            date = parse_zen_date(date_str)
        except ValueError:
            continue
        
        txn_type = row.get('Transaction type', '').strip()
        description = row.get('Description', '').strip()
        settlement_amount = parse_zen_amount(row.get('Settlement amount', ''))
        settlement_currency = row.get('Settlement currency', 'PLN').strip()
        original_amount = parse_zen_amount(row.get('Original amount', ''))
        original_currency = row.get('Original currency', 'PLN').strip()
        currency_rate = parse_zen_amount(row.get('Currency rate', '1.0'))
        fee_description = row.get('Fee description', '').strip()
        fee_amount_str = row.get('Fee amount', '').strip()
        fee_amount = parse_zen_amount(fee_amount_str) if fee_amount_str else None
        fee_currency = row.get('Fee currency', '').strip() or None
        balance_after = parse_zen_amount(row.get('Balance', ''))
        
        # Extract counterparty info from description
        counterparty, address, iban_from_desc, card_number = _extract_counterparty_info(description, txn_type)
        
        txn = ZenTransaction(
            date=date,
            transaction_type=txn_type,
            description=description,
            settlement_amount=settlement_amount,
            settlement_currency=settlement_currency,
            original_amount=original_amount,
            original_currency=original_currency,
            currency_rate=currency_rate,
            fee_description=fee_description,
            fee_amount=fee_amount,
            fee_currency=fee_currency,
            balance_after=balance_after,
            line_number=line_num,
            counterparty=counterparty,
            counterparty_address=address,
            counterparty_iban=iban_from_desc,
            card_number=card_number,
        )
        transactions.append(txn)
    
    return StatementInfo(
        filename=path,  # Full path to the CSV file
        iban=iban,
        currency=currency or 'PLN',
        period_start=period_start,
        period_end=period_end,
        opening_balance=opening_balance,
        closing_balance=closing_balance,
        transactions=transactions,
    )


def _generate_transaction_id(iban: str, txn: ZenTransaction) -> str:
    """Generate a unique ID for a transaction.
    
    Uses date, amount and balance_after - these are always stable and unique.
    
    Args:
        iban: Account IBAN.
        txn: The Zen transaction.
        
    Returns:
        A unique identifier string.
    """
    import hashlib
    # Use only stable, unique values: date + amount + balance_after
    data = f"{txn.date}:{txn.settlement_amount}:{txn.balance_after}"
    hash_value = hashlib.md5(data.encode()).hexdigest()[:12]
    # Format: zen:{hash}
    return f"zen:{hash_value}"


def get_info(filename: str) -> dict:
    """Create info dict for import result."""
    return dict(
        type='text/csv',
        filename=filename,
    )


class ZenSource(Source):
    """Zen CSV transaction source."""

    def __init__(
        self,
        directory: str,
        account_map: Optional[Dict[str, str]] = None,
        default_account: Optional[str] = None,
        **kwargs,
    ) -> None:
        """Initialize the Zen source.

        Args:
            directory: Directory containing year subdirectories with CSV files.
            account_map: Dictionary mapping account_id (IBAN_CURRENCY) to Beancount account.
            default_account: Fallback account when account not in account_map.
            **kwargs: Additional arguments passed to Source.
        """
        super().__init__(**kwargs)
        self.data_directory = directory
        
        self.account_map: Dict[str, str] = account_map or {}
        self.default_account = default_account
        
        if not self.default_account and not self.account_map:
            raise ValueError(
                "ZenSource requires either 'account_map' or "
                "'default_account' to be specified."
            )

        # Store loaded data
        self.statements: List[StatementInfo] = []
        self.transactions: List[Tuple[StatementInfo, ZenTransaction]] = []
        
        # Load all data
        self._load_all_data()

    def _load_all_data(self) -> None:
        """Load all data from directory."""
        if not os.path.isdir(self.data_directory):
            self.log_status(f'zen: directory not found: {self.data_directory}')
            return
        
        # Walk through all subdirectories looking for CSV files
        for root, dirs, files in os.walk(self.data_directory):
            for filename in sorted(files):
                if not filename.endswith('.csv'):
                    continue
                
                path = os.path.join(root, filename)
                
                # Ensure file has unique suffix, renaming if needed
                path = ensure_file_has_suffix(path)
                
                statement = parse_csv(path)
                
                if statement:
                    self.statements.append(statement)
                    for txn in statement.transactions:
                        self.transactions.append((statement, txn))
        
        self.log_status(
            f'zen: loaded {len(self.statements)} statements, '
            f'{len(self.transactions)} transactions'
        )

    def _get_account_for_id(self, account_id: str) -> Optional[str]:
        """Get the Beancount account for a given account_id.
        
        Returns None if account is not mapped and no default_account is set.
        """
        if account_id in self.account_map:
            return self.account_map[account_id]
        return self.default_account

    def _get_all_accounts(self) -> set:
        """Get all accounts used by this source."""
        accounts = set(self.account_map.values())
        if self.default_account:
            accounts.add(self.default_account)
        return accounts

    def get_example_key_value_pairs(
        self,
        transaction: Transaction,
        posting: Posting,
    ) -> dict:
        """Extract key-value pairs for account prediction."""
        result = {}
        if posting.meta is None:
            return result
        
        def maybe_add_key(key: str) -> None:
            value = posting.meta.get(key)
            if value is not None:
                result[key] = value
        
        maybe_add_key(TRANSACTION_TYPE_KEY)
        maybe_add_key(COUNTERPARTY_KEY)
        maybe_add_key(TITLE_KEY)

        return result

    def is_posting_cleared(self, posting: Posting) -> bool:
        """Check if a posting is cleared."""
        if posting.meta is None:
            return False
        return SOURCE_REF_KEY in posting.meta

    def prepare(
        self,
        journal: JournalEditor,
        results: SourceResults,
    ) -> None:
        """Prepare import results from loaded transactions."""
        all_accounts = self._get_all_accounts()

        # Build set of already-matched transaction IDs
        matched_ids: Dict[str, List[Tuple[Transaction, Posting]]] = {}

        for entry in journal.all_entries:
            if not isinstance(entry, Transaction):
                continue
            for posting in entry.postings:
                if posting.meta is None:
                    continue
                if posting.account not in all_accounts:
                    continue
                ref = posting.meta.get(SOURCE_REF_KEY)
                if ref is not None:
                    matched_ids.setdefault(ref, []).append((entry, posting))

        # Track for balance assertions
        balances_by_account: Dict[str, List[Tuple[datetime.date, Decimal, str]]] = {}
        
        # Process all transactions
        valid_ids = set()
        for statement, txn in self.transactions:
            account_id = f"{statement.iban}_{statement.currency}"
            txn_id = _generate_transaction_id(statement.iban, txn)
            valid_ids.add(txn_id)

            existing = matched_ids.get(txn_id)
            if existing is not None:
                if len(existing) > 1:
                    results.add_invalid_reference(
                        InvalidSourceReference(len(existing) - 1, existing))
            else:
                # Create new transaction
                target_account = self._get_account_for_id(account_id)
                if target_account is None:
                    continue
                beancount_txn = self._make_transaction(statement, txn, target_account)
                results.add_pending_entry(
                    ImportResult(
                        date=txn.date,
                        entries=[beancount_txn],
                        info=get_info(statement.filename),
                    ))
            
            # Track balance for assertions
            target_account = self._get_account_for_id(account_id)
            if target_account:
                if target_account not in balances_by_account:
                    balances_by_account[target_account] = []
                balances_by_account[target_account].append(
                    (txn.date, txn.balance_after, txn.settlement_currency)
                )

        # Generate monthly balance assertions
        today = datetime.date.today()
        current_year_month = (today.year, today.month)
        
        for account, balance_list in balances_by_account.items():
            # Sort by date
            balance_list.sort(key=lambda x: x[0])
            
            # Group by year-month and keep last balance
            monthly_balances: Dict[Tuple[int, int], Tuple[Decimal, str]] = {}
            for date, balance, currency in balance_list:
                year_month = (date.year, date.month)
                monthly_balances[year_month] = (balance, currency)
            
            # Generate balance assertion for each completed month
            for (year, month), (balance, currency) in monthly_balances.items():
                if (year, month) == current_year_month:
                    continue
                
                # Balance date is the 1st of the next month
                if month == 12:
                    balance_date = datetime.date(year + 1, 1, 1)
                else:
                    balance_date = datetime.date(year, month + 1, 1)
                
                results.add_pending_entry(
                    ImportResult(
                        date=balance_date,
                        entries=[
                            Balance(
                                date=balance_date,
                                meta=None,
                                account=account,
                                amount=Amount(balance, currency),
                                tolerance=None,
                                diff_amount=None,
                            )
                        ],
                        info={'type': 'balance', 'source': 'zen'},
                    ))

        # Check for invalid references
        for ref, postings in matched_ids.items():
            if ref not in valid_ids:
                results.add_invalid_reference(
                    InvalidSourceReference(len(postings), postings))

        # Generate Document directives for source files
        # Files already have unique suffix from ensure_file_has_suffix during load
        for statement in self.statements:
            if not statement.transactions:
                continue
            
            account_id = f"{statement.iban}_{statement.currency}"
            target_account = self._get_account_for_id(account_id)
            if target_account is None:
                continue
            
            # Find max transaction date from this statement
            max_date = max(txn.date for txn in statement.transactions)
            
            # Use absolute path - SourceResults will convert to relative if needed
            results.add_pending_entry(
                ImportResult(
                    date=max_date,
                    entries=[
                        Document(
                            meta=None,
                            date=max_date,
                            account=target_account,
                            filename=statement.filename,  # Absolute path
                            tags=EMPTY_SET,
                            links=EMPTY_SET,
                        )
                    ],
                    info=dict(
                        type='text/csv',
                        filename=os.path.basename(statement.filename),
                    ),
                ))

        # Register all accounts
        for account in all_accounts:
            results.add_account(account)

    def _make_transaction(
        self, 
        statement: StatementInfo,
        txn: ZenTransaction, 
        target_account: str,
    ) -> Transaction:
        """Create a Beancount Transaction from a Zen transaction."""
        txn_id = _generate_transaction_id(statement.iban, txn)

        # Build metadata
        meta = collections.OrderedDict([
            (SOURCE_REF_KEY, txn_id),
            (SOURCE_BANK_KEY, 'Zen'),
        ])
        
        # Add account IBAN
        meta[ACCOUNT_IBAN_KEY] = statement.iban
        
        # Add transaction type
        if txn.transaction_type:
            meta[TRANSACTION_TYPE_KEY] = txn.transaction_type
        
        # Add counterparty info
        if txn.counterparty:
            meta[COUNTERPARTY_KEY] = txn.counterparty
        if txn.counterparty_address:
            meta[COUNTERPARTY_ADDRESS_KEY] = txn.counterparty_address
        if txn.counterparty_iban:
            meta[COUNTERPARTY_IBAN_KEY] = txn.counterparty_iban
        if txn.card_number:
            meta[CARD_NUMBER_KEY] = txn.card_number
        
        # Add original currency info if different
        if txn.original_currency != txn.settlement_currency:
            meta[ORIGINAL_AMOUNT_KEY] = str(txn.original_amount)
            meta[ORIGINAL_CURRENCY_KEY] = txn.original_currency
            meta[CURRENCY_RATE_KEY] = str(txn.currency_rate)
        
        # Add title if description differs from counterparty
        if txn.description and txn.description != txn.counterparty:
            meta[TITLE_KEY] = txn.description
        
        # Add link to source document (only filename, not full path)
        meta[SOURCE_DOC_KEY] = os.path.basename(statement.filename)

        # Determine payee and narration
        payee = txn.counterparty or 'Zen'
        narration = txn.transaction_type or txn.description or 'Transaction'
        
        # Create postings
        amount = Amount(txn.settlement_amount, txn.settlement_currency)
        neg_amount = Amount(-txn.settlement_amount, txn.settlement_currency)

        return Transaction(
            meta=None,
            date=txn.date,
            flag=FLAG_OKAY,
            payee=payee,
            narration=narration,
            tags=EMPTY_SET,
            links=EMPTY_SET,
            postings=[
                Posting(
                    account=target_account,
                    units=amount,
                    cost=None,
                    price=None,
                    flag=None,
                    meta=meta,
                ),
                Posting(
                    account=FIXME_ACCOUNT,
                    units=neg_amount,
                    cost=None,
                    price=None,
                    flag=None,
                    meta=None,
                ),
            ],
        )

    @property
    def name(self) -> str:
        """Return the source name."""
        return 'zen'


def load(spec: dict, log_status) -> ZenSource:
    """Load the Zen source.

    Args:
        spec: Configuration dictionary.
        log_status: Logging function.

    Returns:
        Configured ZenSource instance.
    """
    return ZenSource(log_status=log_status, **spec)
