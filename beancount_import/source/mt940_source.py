"""MT940 bank statement source.

Data format
===========

This source imports transactions from MT940/MT942 SWIFT bank statement files.
MT940 is a standard format used by many banks for account statements.

Uses the `mt940` library (https://github.com/wolph/mt940) for parsing, with
custom extensions for bank-specific formats.

Directory structure:
    mt940/
      bankpekao-daventi/
        2025/
          daventi-31.01.2025.txt
          daventi-28.02.2025.txt
      nestbank-nestkonto/
        20250101-20260120-Nest.sta

Specifying the source to beancount_import
=========================================

Within your Python script for invoking beancount_import, use an expression like:

    dict(
        module='beancount_import.source.mt940_source',
        accounts=[
            dict(
                directory='/path/to/mt940/bankpekao-daventi',
                account='Assets:Bank:BankPekao:PLN',
                bank='pekao',
            ),
            dict(
                directory='/path/to/mt940/nestbank-nestkonto',
                account='Assets:Bank:Nest:PLN',
                bank='nestbank',
            ),
        ],
    )

Imported transaction format
===========================

Transactions are generated in the following form:

    2025-01-29 * "DAVENTI DAWID SZWAJCA" "PRZEKAZ EURO-KRAJOWY"
      Assets:Bank:BankPekao:PLN     -10.00 PLN
        source_ref: "mt940:PL431240..."
        source_bank: "Bank Pekao"
        transaction_type: "PRZEKAZ EURO-KRAJOWY"
        counterparty: "DAVENTI DAWID SZWAJCA"
        counterparty_iban: "PL85124043151978001138516368"
        title: "Przelew środków"
      Expenses:FIXME          10.00 PLN

Supported banks
===============

- **Bank Pekao** (`bank='pekao'`): Uses `^XX` separators in :86: field
- **Nest Bank** (`bank='nestbank'`): Uses `<XX>` separators in :86: field
- **Universal** (`bank='universal'`): Auto-detects separator format
"""

import datetime
import hashlib
import os
import re
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from decimal import Decimal
from typing import Dict, List, Optional, Tuple

import mt940
import mt940.models
import mt940.tags

from beancount.core.data import Balance, Posting, Transaction, EMPTY_SET
from beancount.core.flags import FLAG_OKAY
from beancount.core.number import D, ZERO
from beancount.core.amount import Amount

from . import ImportResult, Source, SourceResults, InvalidSourceReference
from ..matching import FIXME_ACCOUNT
from ..journal_editor import JournalEditor


# Metadata keys (standardized across all bank sources)
SOURCE_REF_KEY = 'source_ref'
SOURCE_BANK_KEY = 'source_bank'
TRANSACTION_TYPE_KEY = 'transaction_type'
COUNTERPARTY_KEY = 'counterparty'
COUNTERPARTY_IBAN_KEY = 'counterparty_iban'
TITLE_KEY = 'title'
ACCOUNT_IBAN_KEY = 'account_iban'
SOURCE_DOC_KEY = 'document'





# =============================================================================
# Custom MT940 tag for Bank Pekao's non-standard :61: format
# =============================================================================

class StatementPekao(mt940.tags.Statement):
    """Custom Statement tag for Bank Pekao's extended :61: format.
    
    Bank Pekao uses a non-standard :61: format with an additional timestamp:
    :61:2501290130231740DN000000000010,00N767NONREF//122251T606402830
         ~~~~~~ ~~~~ ~~~~~~
         YYMMDD MMDD HHMMSS <- extra timestamp!
    
    Standard format is:
    :61:YYMMDD[MMDD][D/C][Amount][N]TypeReference//BankRef
    """
    
    # Use string pattern - base class compiles it
    pattern = r'''
        (?P<year>\d{2})
        (?P<month>\d{2})
        (?P<day>\d{2})
        (?P<entry_month>\d{2})?
        (?P<entry_day>\d{2})?
        (?P<timestamp>\d{6})?  # Pekao-specific: HHMMSS timestamp
        (?P<status>R?[DC])
        (?P<funds_code>[A-Z])?
        [\n ]?
        (?P<amount>[\d,]{1,15})
        (?P<id>[A-Z][A-Z0-9 ]{3})?
        (?P<customer_reference>((?!//)[^\n]){0,16})
        (//(?P<bank_reference>.{0,23}))?
        (\n?(?P<extra_details>.{0,34}))?
        $
        '''


# =============================================================================
# Field :86: Adapters - Parse bank-specific transaction details
# =============================================================================

@dataclass
class Field86Data:
    """Parsed data from :86: field."""
    transaction_type: Optional[str] = None
    title: Optional[str] = None
    counterparty: Optional[str] = None
    counterparty_iban: Optional[str] = None
    bank_code: Optional[str] = None
    account_number: Optional[str] = None
    card_number: Optional[str] = None
    reference: Optional[str] = None
    fx_rate_from: Optional[str] = None
    fx_rate_to: Optional[str] = None
    raw: str = ""


class Field86Adapter(ABC):
    """Base class for parsing :86: field."""
    
    @abstractmethod
    def parse(self, raw: str) -> Field86Data:
        """Parse :86: content into structured fields."""
        pass


class PekaoAdapter(Field86Adapter):
    """Bank Pekao :86: format with ^XX separators.
    
    Fields:
    - ^00: Transaction type
    - ^20-21: Title
    - ^30: Bank code
    - ^32-34: Counterparty name and address
    - ^38: Counterparty IBAN
    - ^51: FX rate from (e.g., PLN00001,000000)
    - ^52: FX rate to (e.g., EUR00004,362700)
    - ^62: Card number or additional info
    """
    
    def parse(self, raw: str) -> Field86Data:
        data = Field86Data(raw=raw)
        
        # Split by ^
        fields = self._split_fields(raw)
        
        data.transaction_type = fields.get('00')
        
        # Title: concatenate 20, 21, 22, 23
        title_parts = []
        for key in ['20', '21', '22', '23']:
            if key in fields and fields[key]:
                title_parts.append(fields[key])
        if title_parts:
            data.title = ' '.join(title_parts)
        
        data.bank_code = fields.get('30')
        
        # Counterparty: concatenate 32, 33, 34
        cp_parts = []
        for key in ['32', '33', '34']:
            if key in fields and fields[key] and fields[key] != '000':
                cp_parts.append(fields[key])
        if cp_parts:
            data.counterparty = ' '.join(cp_parts)
        
        data.counterparty_iban = fields.get('38')
        data.fx_rate_from = fields.get('51')
        data.fx_rate_to = fields.get('52')
        data.card_number = fields.get('62')
        
        return data
    
    def _split_fields(self, raw: str) -> Dict[str, str]:
        """Split ^XX formatted string into dict of field_code -> value."""
        fields = {}
        # Pattern: ^XX followed by content until next ^XX or end
        pattern = re.compile(r'\^(\d{2})([^^]*)')
        for match in pattern.finditer(raw):
            code = match.group(1)
            value = match.group(2).strip()
            # Remove newlines and clean up
            value = ' '.join(value.split())
            fields[code] = value
        return fields


class NestBankAdapter(Field86Adapter):
    """Nest Bank :86: format with <XX> separators.
    
    Fields:
    - <00>: Transaction type (e.g., "Przelewy przychodzace")
    - <10>: Transaction ID
    - <20-23>: Title
    - <27-29>: Counterparty name and address
    - <30>: Bank code
    - <31>: Account number
    - <38>: IBAN/BBAN
    - <60>: Additional address
    - <63>: Reference (e.g., REF25/02/07/243872/1)
    """
    
    def parse(self, raw: str) -> Field86Data:
        data = Field86Data(raw=raw)
        
        fields = self._split_fields(raw)
        
        data.transaction_type = fields.get('00')
        
        # Title: concatenate 20, 21, 22, 23
        title_parts = []
        for key in ['20', '21', '22', '23']:
            if key in fields and fields[key]:
                title_parts.append(fields[key])
        if title_parts:
            data.title = ' '.join(title_parts)
        
        data.bank_code = fields.get('30')
        data.account_number = fields.get('31')
        
        # Counterparty: concatenate 27, 28, 29, 60
        cp_parts = []
        for key in ['27', '28', '29', '60']:
            if key in fields and fields[key]:
                cp_parts.append(fields[key])
        if cp_parts:
            data.counterparty = ' '.join(cp_parts)
        
        data.counterparty_iban = fields.get('38')
        
        # Reference: parse from <63>REFxxx format
        ref = fields.get('63', '')
        if ref.startswith('REF'):
            data.reference = ref
        
        return data
    
    def _split_fields(self, raw: str) -> Dict[str, str]:
        """Split <XX> formatted string into dict of field_code -> value."""
        fields = {}
        # Pattern: <XX> followed by content until next <XX> or end
        pattern = re.compile(r'<(\d{2})>?([^<]*)')
        for match in pattern.finditer(raw):
            code = match.group(1)
            value = match.group(2).strip()
            # Remove newlines and clean up
            value = ' '.join(value.split())
            fields[code] = value
        return fields


class UniversalAdapter(Field86Adapter):
    """Auto-detect and parse :86: field format."""
    
    def __init__(self):
        self._pekao = PekaoAdapter()
        self._nestbank = NestBankAdapter()
    
    def parse(self, raw: str) -> Field86Data:
        # Detect format by separator
        if '^' in raw:
            return self._pekao.parse(raw)
        elif '<' in raw:
            return self._nestbank.parse(raw)
        else:
            # Fallback - just return raw
            return Field86Data(raw=raw, title=raw)


# Adapter registry
ADAPTERS: Dict[str, Field86Adapter] = {
    'pekao': PekaoAdapter(),
    'nestbank': NestBankAdapter(),
    'universal': UniversalAdapter(),
}

# Bank display names
BANK_NAMES: Dict[str, str] = {
    'pekao': 'Bank Pekao',
    'nestbank': 'Nest Bank',
    'universal': 'MT940',
}


# =============================================================================
# Data structures
# =============================================================================

@dataclass
class Mt940Transaction:
    """Represents a parsed transaction from MT940 statement."""
    value_date: datetime.date
    entry_date: Optional[datetime.date]
    amount: Decimal
    status: str  # 'C' or 'D' for credit/debit
    transaction_code: str  # e.g., "NTRF", "NMSC"
    customer_reference: str
    bank_reference: str
    extra_details: str
    transaction_details: str  # Raw :86: field
    # Parsed from :86:
    transaction_type: Optional[str] = None
    title: Optional[str] = None
    counterparty: Optional[str] = None
    counterparty_iban: Optional[str] = None
    reference: Optional[str] = None


@dataclass
class Mt940Statement:
    """Represents a parsed MT940 statement."""
    filename: str
    account_iban: str
    statement_number: str
    currency: str
    opening_balance: Decimal
    opening_date: datetime.date
    closing_balance: Decimal
    closing_date: datetime.date
    transactions: List[Mt940Transaction] = field(default_factory=list)


def _detect_file_encoding(filepath: str) -> str:
    """Auto-detect file encoding."""
    # Try common encodings
    encodings = ['utf-8', 'iso-8859-2', 'windows-1250', 'cp1250']
    
    for encoding in encodings:
        try:
            with open(filepath, 'r', encoding=encoding) as f:
                content = f.read()
                # Check if content looks valid (no replacement characters)
                if '\ufffd' not in content:
                    return encoding
        except (UnicodeDecodeError, UnicodeError):
            continue
    
    return 'utf-8'  # Fallback


def parse_mt940_file(
    filepath: str,
    bank: str = 'universal',
) -> List[Mt940Statement]:
    """Parse an MT940 file and return list of statements.
    
    Args:
        filepath: Path to the MT940 file.
        bank: Bank identifier for :86: parsing.
        
    Returns:
        List of Mt940Statement objects.
    """
    adapter = ADAPTERS.get(bank, ADAPTERS['universal'])
    
    # Detect encoding
    encoding = _detect_file_encoding(filepath)
    
    # Read file content
    with open(filepath, 'r', encoding=encoding) as f:
        data = f.read()
    
    # Configure mt940 parser
    if bank == 'pekao':
        # Use custom Statement tag for Pekao
        transactions = mt940.models.Transactions(
            tags={
                StatementPekao.id: StatementPekao(),
            }
        )
    else:
        transactions = mt940.models.Transactions()
    
    # Parse
    try:
        transactions.parse(data)
    except Exception as e:
        print(f"mt940_source: error parsing {filepath}: {e}")
        return []
    
    statements: List[Mt940Statement] = []
    
    # Build statement lookup - group transactions by statement number
    current_stmt = None
    current_txns: List[Mt940Transaction] = []
    
    # The mt940 library returns Transactions as a collection of individual transactions
    # We need to reconstruct statements from the parsed data
    
    # Get statement-level data from the transactions object
    stmt_data = transactions.data if hasattr(transactions, 'data') else {}
    
    for txn in transactions:
        txn_data = txn.data
        
        # Parse dates
        value_date = txn_data.get('date')
        if isinstance(value_date, mt940.models.Date):
            value_date = datetime.date(value_date.year, value_date.month, value_date.day)
        
        entry_date = txn_data.get('entry_date')
        if isinstance(entry_date, mt940.models.Date):
            entry_date = datetime.date(entry_date.year, entry_date.month, entry_date.day)
        
        # Parse amount
        amount_obj = txn_data.get('amount')
        if hasattr(amount_obj, 'amount'):
            amount = D(str(amount_obj.amount))
        else:
            amount = ZERO
        
        # Get status and adjust sign
        status = txn_data.get('status', 'C')
        if status in ('D', 'RD'):
            amount = -abs(amount)
        else:
            amount = abs(amount)
        
        # Parse :86: field
        raw_86 = txn_data.get('transaction_details', '')
        field86_data = adapter.parse(raw_86)
        
        mt940_txn = Mt940Transaction(
            value_date=value_date,
            entry_date=entry_date,
            amount=amount,
            status=status,
            transaction_code=txn_data.get('id', ''),
            customer_reference=txn_data.get('customer_reference', ''),
            bank_reference=txn_data.get('bank_reference', ''),
            extra_details=txn_data.get('extra_details', ''),
            transaction_details=raw_86,
            transaction_type=field86_data.transaction_type,
            title=field86_data.title,
            counterparty=field86_data.counterparty,
            counterparty_iban=field86_data.counterparty_iban,
            reference=field86_data.reference,
        )
        current_txns.append(mt940_txn)
    
    # Extract statement-level info from the transactions object
    # The mt940 library stores this in the statement wrapper
    if hasattr(transactions, 'statements'):
        for stmt in transactions.statements:
            stmt_d = stmt.data
            
            # Account IBAN
            account_id = stmt_d.get('account_identification', '')
            if account_id.startswith('/'):
                account_id = account_id[1:]
            
            # Opening balance
            opening = stmt_d.get('opening_balance') or stmt_d.get('final_opening_balance')
            if opening:
                opening_balance = D(str(opening.amount.amount)) if hasattr(opening.amount, 'amount') else ZERO
                opening_date_obj = opening.date
                if isinstance(opening_date_obj, mt940.models.Date):
                    opening_date = datetime.date(opening_date_obj.year, opening_date_obj.month, opening_date_obj.day)
                else:
                    opening_date = datetime.date.today()
                currency = str(opening.amount.currency) if hasattr(opening.amount, 'currency') else 'PLN'
            else:
                opening_balance = ZERO
                opening_date = datetime.date.today()
                currency = 'PLN'
            
            # Closing balance
            closing = stmt_d.get('closing_balance') or stmt_d.get('final_closing_balance')
            if closing:
                closing_balance = D(str(closing.amount.amount)) if hasattr(closing.amount, 'amount') else ZERO
                closing_date_obj = closing.date
                if isinstance(closing_date_obj, mt940.models.Date):
                    closing_date = datetime.date(closing_date_obj.year, closing_date_obj.month, closing_date_obj.day)
                else:
                    closing_date = datetime.date.today()
            else:
                closing_balance = ZERO
                closing_date = datetime.date.today()
            
            # Statement number
            stmt_number = stmt_d.get('statement_number', '')
            
            # Get transactions for this statement
            stmt_txns = [t for t in current_txns]  # For now, all transactions
            
            statements.append(Mt940Statement(
                filename=filepath,
                account_iban=account_id,
                statement_number=str(stmt_number),
                currency=currency,
                opening_balance=opening_balance,
                opening_date=opening_date,
                closing_balance=closing_balance,
                closing_date=closing_date,
                transactions=stmt_txns,
            ))
            current_txns = []  # Reset for next statement
    
    # Fallback: if no statements structure, create one from collected transactions
    if not statements and current_txns:
        statements.append(Mt940Statement(
            filename=filepath,
            account_iban=stmt_data.get('account_identification', '').lstrip('/'),
            statement_number='1',
            currency='PLN',
            opening_balance=ZERO,
            opening_date=datetime.date.today(),
            closing_balance=ZERO,
            closing_date=datetime.date.today(),
            transactions=current_txns,
        ))
    
    return statements


def _generate_transaction_id(account_iban: str, txn: Mt940Transaction) -> str:
    """Generate a unique ID for a transaction."""
    # Use date + amount + reference for uniqueness
    data = f"{txn.value_date}:{txn.amount}:{txn.extra_details or txn.customer_reference}"
    hash_value = hashlib.md5(data.encode()).hexdigest()[:12]
    return f"mt940:{hash_value}"


def get_info(filename: str) -> dict:
    """Create info dict for import result."""
    return dict(
        type='application/mt940',
        filename=filename,
    )


# =============================================================================
# Source class
# =============================================================================

class Mt940Source(Source):
    """MT940 bank statement source."""
    
    def __init__(
        self,
        accounts: List[dict],
        **kwargs,
    ) -> None:
        """Initialize the MT940 source.
        
        Args:
            accounts: List of account configurations, each containing:
                - directory: Path to MT940 files
                - account: Beancount account name
                - bank: Bank identifier ('pekao', 'nestbank', 'universal')
            **kwargs: Additional arguments passed to Source.
        """
        super().__init__(**kwargs)
        self.accounts_config = accounts
        
        # Store loaded data
        self.statements: List[Tuple[Mt940Statement, str, str]] = []  # (stmt, account, bank)
        self.transactions: List[Tuple[Mt940Statement, Mt940Transaction, str, str]] = []  # (stmt, txn, account, bank)
        
        # Load all data
        self._load_all_data()
    
    @property
    def name(self) -> str:
        return 'mt940'
    
    def _load_all_data(self) -> None:
        """Load all data from configured directories."""
        for config in self.accounts_config:
            directory = config['directory']
            account = config['account']
            bank = config.get('bank', 'universal')
            
            if not os.path.isdir(directory):
                self.log_status(f'mt940_source: directory not found: {directory}')
                continue
            
            # Walk through all subdirectories
            for root, dirs, files in os.walk(directory):
                for filename in sorted(files):
                    # Accept common MT940 extensions
                    if not any(filename.endswith(ext) for ext in ['.sta', '.txt', '.mt940', '.940']):
                        continue
                    
                    path = os.path.join(root, filename)
                    

                    
                    # Parse file
                    parsed_statements = parse_mt940_file(path, bank)
                    
                    for stmt in parsed_statements:
                        self.statements.append((stmt, account, bank))
                        for txn in stmt.transactions:
                            self.transactions.append((stmt, txn, account, bank))
        
        self.log_status(
            f'mt940_source: loaded {len(self.statements)} statements, '
            f'{len(self.transactions)} transactions'
        )
    
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
    
    def _make_transaction(
        self,
        stmt: Mt940Statement,
        txn: Mt940Transaction,
        account: str,
        bank: str,
    ) -> Transaction:
        """Create a Beancount transaction from MT940 data."""
        txn_id = _generate_transaction_id(stmt.account_iban, txn)
        
        # Determine payee and narration
        payee = txn.counterparty or None
        narration = txn.transaction_type or txn.title or "MT940 Transaction"
        
        # Build metadata
        meta = {
            SOURCE_REF_KEY: txn_id,
            SOURCE_BANK_KEY: BANK_NAMES.get(bank, 'MT940'),
        }
        
        if txn.transaction_type:
            meta[TRANSACTION_TYPE_KEY] = txn.transaction_type
        if txn.counterparty:
            meta[COUNTERPARTY_KEY] = txn.counterparty
        if txn.counterparty_iban:
            meta[COUNTERPARTY_IBAN_KEY] = txn.counterparty_iban
        if txn.title:
            meta[TITLE_KEY] = txn.title
        meta[ACCOUNT_IBAN_KEY] = stmt.account_iban
        
        # Link to source document
        meta[SOURCE_DOC_KEY] = stmt.filename
        
        # Create postings
        amount_obj = Amount(txn.amount, stmt.currency)
        
        posting = Posting(
            account=account,
            units=amount_obj,
            cost=None,
            price=None,
            flag=None,
            meta=meta,
        )
        
        # FIXME posting for balancing
        fixme_posting = Posting(
            account=FIXME_ACCOUNT,
            units=Amount(-txn.amount, stmt.currency),
            cost=None,
            price=None,
            flag=None,
            meta=None,
        )
        
        return Transaction(
            meta={
                'filename': '<mt940_source>',
                'lineno': 0,
            },
            date=txn.value_date,
            flag=FLAG_OKAY,
            payee=payee,
            narration=narration,
            tags=EMPTY_SET,
            links=EMPTY_SET,
            postings=[posting, fixme_posting],
        )
    
    def prepare(
        self,
        journal: JournalEditor,
        results: SourceResults,
    ) -> None:
        """Prepare import results from loaded transactions."""
        # Get all accounts we manage
        all_accounts = {config['account'] for config in self.accounts_config}
        
        # Build set of already-matched transaction IDs
        matched_ids: Dict[str, List[Tuple[Transaction, Posting]]] = {}
        
        for entry in journal.all_entries:
            if isinstance(entry, Transaction):
                for posting in entry.postings:
                    if posting.meta is None:
                        continue
                    if posting.account not in all_accounts:
                        continue
                    ref = posting.meta.get(SOURCE_REF_KEY)
                    if ref is not None:
                        matched_ids.setdefault(ref, []).append((entry, posting))
        
        # Track for balance assertions
        balances_by_account: Dict[str, Tuple[datetime.date, Decimal, str]] = {}
        
        # Track valid IDs
        valid_ids = set()
        
        # Process transactions
        for stmt, txn, account, bank in self.transactions:
            txn_id = _generate_transaction_id(stmt.account_iban, txn)
            valid_ids.add(txn_id)
            
            existing = matched_ids.get(txn_id)
            if existing is not None:
                if len(existing) > 1:
                    results.add_invalid_reference(
                        InvalidSourceReference(len(existing) - 1, existing))
            else:
                # Create new transaction
                beancount_txn = self._make_transaction(stmt, txn, account, bank)
                results.add_pending_entry(
                    ImportResult(
                        date=txn.value_date,
                        entries=[beancount_txn],
                        info=get_info(stmt.filename),
                    ))
            
            # Track latest balance for this account
            balances_by_account[account] = (stmt.closing_date, stmt.closing_balance, stmt.currency)
        
        # Generate balance assertions
        for account, (date, balance, currency) in balances_by_account.items():
            balance_entry = Balance(
                meta={
                    'filename': '<mt940_source>',
                    'lineno': 0,
                },
                account=account,
                amount=Amount(balance, currency),
                tolerance=None,
                diff_amount=None,
                date=date + datetime.timedelta(days=1),
            )
            results.add_pending_entry(
                ImportResult(
                    date=date,
                    entries=[balance_entry],
                    info=get_info('<balance>'),
                ))
        
        # Report invalid references
        for ref, entries in matched_ids.items():
            if ref not in valid_ids and ref.startswith('mt940:'):
                results.add_invalid_reference(
                    InvalidSourceReference(len(entries), entries))


def load(spec: dict, log_status: callable) -> Mt940Source:
    """Load the MT940 source from specification."""
    return Mt940Source(
        accounts=spec['accounts'],
        log_status=log_status,
    )
