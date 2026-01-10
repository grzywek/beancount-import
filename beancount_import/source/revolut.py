"""Revolut CSV/PDF bank statement source.

Data format
===========

This source imports transactions from Revolut CSV statements and enriches them
with additional metadata from PDF statements. CSV files are the primary data
source, while PDF files provide supplementary information like IBANs, exchange
rates, card numbers, and merchant addresses.

Directory structure:
    revolut/
      personal/
        account-statement_2025-01-01_2025-12-31_en_xxxxx.csv
        account-statement_2025-01-01_2025-12-31_en_xxxxx.pdf
      creditcard/
        2025-01-31_statement.csv
        2025-01-31_statement.pdf
      pro/
        account-statement_2025-01-01_2025-12-31_en_xxxxx.csv
        account-statement_2025-01-01_2025-12-31_en_xxxxx.pdf

CSV Formats
===========

Credit Card CSV (7 columns):
    Type,Started Date,Completed Date,Description,Amount,Fee,Balance

Regular Account CSV (10 columns):
    Type,Product,Started Date,Completed Date,Description,Amount,Fee,Currency,State,Balance

Specifying the source to beancount_import
=========================================

    dict(
        module='beancount_import.source.revolut',
        directory='/path/to/revolut',
        account_map={
            'personal_PLN': 'Assets:Revolut:Personal:PLN',
            'personal_EUR': 'Assets:Revolut:Personal:EUR',
            'personal_USD': 'Assets:Revolut:Personal:USD',
            'creditcard_PLN': 'Liabilities:Revolut:CreditCard',
            'pro_USD': 'Assets:Revolut:Pro:USD',
        },
        default_account='Assets:Revolut:Unknown',
    )

Imported transaction format
===========================

Transactions are generated with metadata from both CSV and PDF:

    2025-01-06 * "Trading 212" "Card Payment"
      Assets:Revolut:PLN     -627.36 PLN
        source_ref: "revolut:..."
        source_bank: "Revolut"
        transaction_type: "Card Payment"
        counterparty: "Trading 212"
        counterparty_address: "London, 7NA"
        card_number: "6712"
        original_currency: "USD"
        original_amount: "151.05"
        exchange_rate: "1.00 PLN = $0.24"
        account_iban: "LT133250085489069781"
      Expenses:FIXME          627.36 PLN
"""

import collections
import csv
import datetime
import hashlib
import io
import os
import re
import subprocess
from dataclasses import dataclass, field
from decimal import Decimal, InvalidOperation
from typing import Dict, List, Optional, Tuple, Set

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
COUNTERPARTY_ADDRESS_KEY = 'counterparty_address'
ACCOUNT_IBAN_KEY = 'account_iban'
ACCOUNT_IBAN_PL_KEY = 'account_iban_pl'
CARD_NUMBER_KEY = 'card_number'
ORIGINAL_AMOUNT_KEY = 'original_amount'
ORIGINAL_CURRENCY_KEY = 'original_currency'
EXCHANGE_RATE_KEY = 'exchange_rate'
SOURCE_DOC_KEY = 'document'

# Pattern to match files that already have a 4-digit suffix before extension
SUFFIX_PATTERN = re.compile(r'-\d{4}(\.[^.]+)?$')


def ensure_file_has_suffix(filepath: str) -> str:
    """Ensure file has a 4-digit suffix, renaming it if needed."""
    import random
    
    basename = os.path.basename(filepath)
    
    if SUFFIX_PATTERN.search(basename):
        return filepath
    
    base, ext = os.path.splitext(filepath)
    suffix = random.randint(1000, 9999)
    new_filepath = f"{base}-{suffix}{ext}"
    
    try:
        os.rename(filepath, new_filepath)
        return new_filepath
    except OSError as e:
        print(f"Warning: could not rename {filepath} to {new_filepath}: {e}")
        return filepath


def parse_revolut_date(text: str) -> datetime.date:
    """Parse Revolut date format: '2025-01-02 02:19:20'.
    
    Args:
        text: Date string in Revolut format.
        
    Returns:
        datetime.date object.
    """
    text = text.strip()
    # Format: 2025-01-02 02:19:20
    try:
        return datetime.datetime.strptime(text, "%Y-%m-%d %H:%M:%S").date()
    except ValueError:
        pass
    # Try date only
    try:
        return datetime.datetime.strptime(text, "%Y-%m-%d").date()
    except ValueError:
        raise ValueError(f"Cannot parse date: {text}")


def parse_revolut_amount(text: str) -> Decimal:
    """Parse Revolut amount format: '-15.00' or '1000.00'."""
    text = text.strip()
    if not text:
        return ZERO
    try:
        return D(text)
    except InvalidOperation:
        return ZERO


@dataclass
class RevolutTransaction:
    """Represents a parsed transaction from CSV."""
    transaction_type: str
    started_date: datetime.date
    completed_date: Optional[datetime.date]
    description: str
    amount: Decimal
    fee: Decimal
    balance_after: Decimal
    currency: str
    product: Optional[str]  # For regular accounts
    state: Optional[str]  # For regular accounts
    line_number: int
    # PDF enrichment fields
    iban: Optional[str] = None
    iban_pl: Optional[str] = None
    card_number: Optional[str] = None
    counterparty_address: Optional[str] = None
    original_amount: Optional[str] = None
    original_currency: Optional[str] = None
    exchange_rate: Optional[str] = None


@dataclass
class CsvStatementInfo:
    """Metadata about a parsed CSV statement."""
    filename: str
    account_type: str  # 'personal', 'creditcard', 'pro'
    currency: str
    transactions: List[RevolutTransaction] = field(default_factory=list)


@dataclass
class PdfTransactionInfo:
    """Supplementary data from PDF for a single transaction."""
    date: datetime.date
    description: str
    amount: Optional[Decimal]
    iban: Optional[str] = None
    iban_pl: Optional[str] = None
    card_number: Optional[str] = None
    counterparty_address: Optional[str] = None
    original_amount: Optional[str] = None
    original_currency: Optional[str] = None
    exchange_rate: Optional[str] = None


@dataclass
class PdfCurrencySection:
    """A currency section within a PDF (e.g., PLN Statement, EUR Statement)."""
    currency: str
    ibans: List[str] = field(default_factory=list)
    transactions: List[PdfTransactionInfo] = field(default_factory=list)


def extract_pdf_text(pdf_path: str) -> str:
    """Extract text from PDF using pdftotext."""
    try:
        result = subprocess.run(
            ['pdftotext', '-layout', pdf_path, '-'],
            capture_output=True,
            text=True,
            check=True,
        )
        return result.stdout
    except subprocess.CalledProcessError as e:
        raise RuntimeError(f"pdftotext failed for {pdf_path}: {e.stderr}")
    except FileNotFoundError:
        raise RuntimeError("pdftotext not found. Please install poppler-utils.")


def parse_pdf(pdf_path: str) -> Dict[str, PdfCurrencySection]:
    """Parse PDF statement for supplementary transaction data.
    
    Returns:
        Dictionary mapping currency code to PdfCurrencySection.
    """
    text = extract_pdf_text(pdf_path)
    lines = text.split('\n')
    
    sections: Dict[str, PdfCurrencySection] = {}
    current_section: Optional[PdfCurrencySection] = None
    
    # Patterns
    currency_header_pattern = re.compile(r'^\s*([A-Z]{3})\s+Statement\s*$')
    iban_pattern = re.compile(r'IBAN\s+([A-Z]{2}[A-Z0-9]{10,32})')
    date_pattern = re.compile(r'^([A-Z][a-z]{2}\s+\d{1,2},\s+\d{4})')
    card_pattern = re.compile(r'Card:\s*(\d{6}\*+\d{4})')
    rate_pattern = re.compile(r'Revolut Rate\s+(.+?)\s*\(ECB')
    to_pattern = re.compile(r'To:\s*(.+)')
    from_pattern = re.compile(r'From:\s*(.+)')
    
    i = 0
    while i < len(lines):
        line = lines[i]
        
        # Check for currency section header
        header_match = currency_header_pattern.search(line)
        if header_match:
            currency = header_match.group(1)
            current_section = PdfCurrencySection(currency=currency)
            sections[currency] = current_section
            i += 1
            continue
        
        # Extract IBAN
        iban_match = iban_pattern.search(line)
        if iban_match and current_section:
            iban = iban_match.group(1)
            if iban not in current_section.ibans:
                current_section.ibans.append(iban)
        
        # Look for transaction lines (start with date)
        date_match = date_pattern.match(line.strip())
        if date_match and current_section:
            # Parse transaction header line
            try:
                date_str = date_match.group(1)
                txn_date = datetime.datetime.strptime(date_str, "%b %d, %Y").date()
            except ValueError:
                i += 1
                continue
            
            # Extract description (between date and first amount)
            # Format: "Jan 6, 2025             Trading 212                  627.36 PLN"
            rest_of_line = line.strip()[len(date_str):].strip()
            
            # Find description by looking for amount pattern at the end
            # Amounts look like: "627.36 PLN", "2,043.44 PLN", "$151.05"
            amount_pattern = re.compile(r'\s+[\d,]+\.\d{2}\s+[A-Z]{3}\s*$|\s+[\$€£][\d,]+\.?\d*\s*$')
            amount_match = amount_pattern.search(rest_of_line)
            
            if amount_match:
                # Description is everything before the amount
                description = rest_of_line[:amount_match.start()].strip()
            else:
                # No amount found, try to extract first meaningful words
                # Skip lines that are just amounts or empty
                words = rest_of_line.split()
                # Take words until we hit what looks like an amount
                desc_words = []
                for word in words:
                    if re.match(r'^[\d,]+\.\d{2}$', word) or re.match(r'^[\$€£]', word):
                        break
                    desc_words.append(word)
                description = ' '.join(desc_words)
            
            # Create transaction info
            txn_info = PdfTransactionInfo(
                date=txn_date,
                description=description,
                amount=None,
            )
            
            # Set IBANs from section
            if current_section.ibans:
                # Prefer LT IBAN as main, PL as secondary
                for iban in current_section.ibans:
                    if iban.startswith('LT'):
                        txn_info.iban = iban
                    elif iban.startswith('PL'):
                        txn_info.iban_pl = iban
                if not txn_info.iban and current_section.ibans:
                    txn_info.iban = current_section.ibans[0]
            
            # Look at following lines for more details
            j = i + 1
            while j < min(i + 5, len(lines)):
                detail_line = lines[j]
                
                card_match = card_pattern.search(detail_line)
                if card_match:
                    txn_info.card_number = card_match.group(1)
                
                rate_match = rate_pattern.search(detail_line)
                if rate_match:
                    txn_info.exchange_rate = rate_match.group(1).strip()
                    # Extract original amount (usually after the rate line)
                    # Format like: $151.05
                    orig_amount_match = re.search(r'\$([0-9,.]+)|€([0-9,.]+)|£([0-9,.]+)', detail_line)
                    if orig_amount_match:
                        for g in orig_amount_match.groups():
                            if g:
                                txn_info.original_amount = g
                                break
                        # Determine original currency from symbol
                        if '$' in detail_line:
                            txn_info.original_currency = 'USD'
                        elif '€' in detail_line:
                            txn_info.original_currency = 'EUR'
                        elif '£' in detail_line:
                            txn_info.original_currency = 'GBP'
                
                to_match = to_pattern.search(detail_line)
                if to_match:
                    txn_info.counterparty_address = to_match.group(1).strip()
                
                from_match = from_pattern.search(detail_line)
                if from_match and not txn_info.counterparty_address:
                    txn_info.counterparty_address = from_match.group(1).strip()
                
                # Stop if we hit another date line
                if date_pattern.match(detail_line.strip()):
                    break
                
                j += 1
            
            current_section.transactions.append(txn_info)
        
        i += 1
    
    return sections


def detect_csv_format(path: str) -> str:
    """Detect CSV format based on header.
    
    Returns:
        'creditcard' or 'account'
    """
    with open(path, 'r', encoding='utf-8') as f:
        header = f.readline().strip()
    
    if header.startswith('Type,Started Date,Completed Date,Description,Amount,Fee,Balance'):
        return 'creditcard'
    elif header.startswith('Type,Product,'):
        return 'account'
    else:
        raise ValueError(f"Unknown CSV format: {header}")


def parse_credit_card_csv(path: str, account_type: str = 'creditcard') -> CsvStatementInfo:
    """Parse credit card CSV (7 columns).
    
    Format: Type,Started Date,Completed Date,Description,Amount,Fee,Balance
    """
    transactions: List[RevolutTransaction] = []
    
    with open(path, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        
        for line_num, row in enumerate(reader, start=2):
            if not row.get('Started Date'):
                continue
            
            try:
                started_date = parse_revolut_date(row['Started Date'])
            except ValueError:
                continue
            
            completed_date = None
            if row.get('Completed Date'):
                try:
                    completed_date = parse_revolut_date(row['Completed Date'])
                except ValueError:
                    pass
            
            txn = RevolutTransaction(
                transaction_type=row.get('Type', '').strip(),
                started_date=started_date,
                completed_date=completed_date,
                description=row.get('Description', '').strip(),
                amount=parse_revolut_amount(row.get('Amount', '')),
                fee=parse_revolut_amount(row.get('Fee', '')),
                balance_after=parse_revolut_amount(row.get('Balance', '')),
                currency='PLN',  # Credit card is always PLN
                product=None,
                state=None,
                line_number=line_num,
            )
            transactions.append(txn)
    
    return CsvStatementInfo(
        filename=path,
        account_type=account_type,
        currency='PLN',
        transactions=transactions,
    )


def parse_account_csv(path: str, account_type: str = 'personal') -> List[CsvStatementInfo]:
    """Parse regular account CSV (10 columns).
    
    Format: Type,Product,Started Date,Completed Date,Description,Amount,Fee,Currency,State,Balance
    
    Returns:
        List of CsvStatementInfo, one per currency found in the CSV.
    """
    transactions_by_currency: Dict[str, List[RevolutTransaction]] = {}
    
    with open(path, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        
        for line_num, row in enumerate(reader, start=2):
            if not row.get('Started Date'):
                continue
            
            try:
                started_date = parse_revolut_date(row['Started Date'])
            except ValueError:
                continue
            
            completed_date = None
            if row.get('Completed Date'):
                try:
                    completed_date = parse_revolut_date(row['Completed Date'])
                except ValueError:
                    pass
            
            currency = row.get('Currency', 'PLN').strip()
            
            txn = RevolutTransaction(
                transaction_type=row.get('Type', '').strip(),
                started_date=started_date,
                completed_date=completed_date,
                description=row.get('Description', '').strip(),
                amount=parse_revolut_amount(row.get('Amount', '')),
                fee=parse_revolut_amount(row.get('Fee', '')),
                balance_after=parse_revolut_amount(row.get('Balance', '')),
                currency=currency,
                product=row.get('Product', '').strip() or None,
                state=row.get('State', '').strip() or None,
                line_number=line_num,
            )
            
            transactions_by_currency.setdefault(currency, []).append(txn)
    
    result = []
    for currency, txns in transactions_by_currency.items():
        result.append(CsvStatementInfo(
            filename=path,
            account_type=account_type,
            currency=currency,
            transactions=txns,
        ))
    
    return result


def match_csv_with_pdf(
    csv_statements: List[CsvStatementInfo],
    pdf_sections: Dict[str, PdfCurrencySection],
) -> None:
    """Enrich CSV transactions with PDF data by matching.
    
    Matches are made by: date, description similarity, amount (when available).
    Modifies csv_statements in place.
    """
    for csv_stmt in csv_statements:
        pdf_section = pdf_sections.get(csv_stmt.currency)
        if not pdf_section:
            continue
        
        # Build lookup of PDF transactions
        pdf_by_date: Dict[datetime.date, List[PdfTransactionInfo]] = {}
        for pdf_txn in pdf_section.transactions:
            pdf_by_date.setdefault(pdf_txn.date, []).append(pdf_txn)
        
        for csv_txn in csv_stmt.transactions:
            # Use completed_date for matching (that's what PDF shows)
            match_date = csv_txn.completed_date or csv_txn.started_date
            
            pdf_candidates = pdf_by_date.get(match_date, [])
            
            # Find best match by description
            best_match = None
            best_score = 0
            
            for pdf_txn in pdf_candidates:
                # Simple matching: check if description starts with same word
                csv_desc_first = csv_txn.description.split()[0].lower() if csv_txn.description else ''
                pdf_desc_words = pdf_txn.description.split() if pdf_txn.description else []
                pdf_desc_first = pdf_desc_words[0].lower() if pdf_desc_words else ''
                
                if csv_desc_first and pdf_desc_first and csv_desc_first == pdf_desc_first:
                    score = 1
                    if score > best_score:
                        best_score = score
                        best_match = pdf_txn
            
            if best_match:
                # Enrich CSV transaction with PDF data
                csv_txn.iban = best_match.iban
                csv_txn.iban_pl = best_match.iban_pl
                csv_txn.card_number = best_match.card_number
                csv_txn.counterparty_address = best_match.counterparty_address
                csv_txn.original_amount = best_match.original_amount
                csv_txn.original_currency = best_match.original_currency
                csv_txn.exchange_rate = best_match.exchange_rate


def _generate_transaction_id(account_type: str, currency: str, txn: RevolutTransaction) -> str:
    """Generate unique transaction ID."""
    data = f"{account_type}:{currency}:{txn.completed_date or txn.started_date}:{txn.amount}:{txn.balance_after}"
    hash_value = hashlib.md5(data.encode()).hexdigest()[:12]
    return f"revolut:{hash_value}"


def get_info(filename: str) -> dict:
    """Create info dict for import result."""
    return dict(
        type='text/csv',
        filename=filename,
    )


class RevolutSource(Source):
    """Revolut CSV/PDF transaction source."""

    def __init__(
        self,
        directory: str,
        account_map: Dict[str, str],
        **kwargs,
    ) -> None:
        """Initialize the Revolut source.

        Args:
            directory: Directory containing subdirectories with CSV/PDF files.
            account_map: Dictionary mapping account_id (type_currency) to Beancount account.
                         Example: {'personal_PLN': 'Assets:Revolut:PLN'}
        """
        super().__init__(**kwargs)
        self.data_directory = directory
        self.account_map: Dict[str, str] = account_map
        
        if not self.account_map:
            raise ValueError(
                "RevolutSource requires 'account_map' to be specified."
            )

        self.statements: List[CsvStatementInfo] = []
        self.transactions: List[Tuple[CsvStatementInfo, RevolutTransaction]] = []
        
        self._load_all_data()

    def _load_all_data(self) -> None:
        """Load all data from directory."""
        if not os.path.isdir(self.data_directory):
            self.log_status(f'revolut: directory not found: {self.data_directory}')
            return
        
        # Collect all CSV and PDF files
        csv_files: List[Tuple[str, str]] = []  # (path, account_type)
        pdf_files: List[str] = []
        
        for root, dirs, files in os.walk(self.data_directory):
            # Determine account type from directory name
            rel_path = os.path.relpath(root, self.data_directory)
            path_parts = rel_path.split(os.sep)
            account_type = path_parts[0] if path_parts and path_parts[0] != '.' else 'unknown'
            
            for filename in sorted(files):
                filepath = os.path.join(root, filename)
                
                if filename.endswith('.csv'):
                    filepath = ensure_file_has_suffix(filepath)
                    csv_files.append((filepath, account_type))
                elif filename.endswith('.pdf'):
                    pdf_files.append(filepath)
        
        # Parse all PDFs first to build supplementary data
        all_pdf_sections: Dict[str, PdfCurrencySection] = {}
        for pdf_path in pdf_files:
            try:
                sections = parse_pdf(pdf_path)
                for currency, section in sections.items():
                    if currency not in all_pdf_sections:
                        all_pdf_sections[currency] = section
                    else:
                        # Merge transactions
                        all_pdf_sections[currency].transactions.extend(section.transactions)
                        for iban in section.ibans:
                            if iban not in all_pdf_sections[currency].ibans:
                                all_pdf_sections[currency].ibans.append(iban)
            except Exception as e:
                self.log_status(f'revolut: error parsing PDF {pdf_path}: {e}')
        
        # Parse all CSVs
        for csv_path, account_type in csv_files:
            try:
                fmt = detect_csv_format(csv_path)
                if fmt == 'creditcard':
                    stmt = parse_credit_card_csv(csv_path, account_type)
                    self.statements.append(stmt)
                else:
                    stmts = parse_account_csv(csv_path, account_type)
                    self.statements.extend(stmts)
            except Exception as e:
                self.log_status(f'revolut: error parsing CSV {csv_path}: {e}')
        
        # Enrich CSV transactions with PDF data
        match_csv_with_pdf(self.statements, all_pdf_sections)
        
        # Build transaction list
        for stmt in self.statements:
            for txn in stmt.transactions:
                self.transactions.append((stmt, txn))
        
        self.log_status(
            f'revolut: loaded {len(self.statements)} statements, '
            f'{len(self.transactions)} transactions'
        )

    def _get_account_for_id(self, account_id: str) -> Optional[str]:
        """Get Beancount account for account_id. Returns None if not mapped."""
        return self.account_map.get(account_id)

    def _get_all_accounts(self) -> set:
        """Get all accounts used by this source."""
        return set(self.account_map.values())

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

        return result

    def is_posting_cleared(self, posting: Posting) -> bool:
        """Check if posting is cleared."""
        if posting.meta is None:
            return False
        return SOURCE_REF_KEY in posting.meta

    def _make_transaction(
        self,
        statement: CsvStatementInfo,
        txn: RevolutTransaction,
        target_account: str,
    ) -> Transaction:
        """Create Beancount transaction from Revolut transaction."""
        # Determine payee and narration
        payee = txn.description
        narration = txn.transaction_type
        
        # Build metadata
        account_id = f"{statement.account_type}_{txn.currency}"
        txn_id = _generate_transaction_id(statement.account_type, txn.currency, txn)
        
        meta = collections.OrderedDict()
        meta[SOURCE_REF_KEY] = txn_id
        meta[SOURCE_BANK_KEY] = 'Revolut'
        
        if txn.transaction_type:
            meta[TRANSACTION_TYPE_KEY] = txn.transaction_type
        
        if txn.description:
            meta[COUNTERPARTY_KEY] = txn.description
        
        if txn.counterparty_address:
            meta[COUNTERPARTY_ADDRESS_KEY] = txn.counterparty_address
        
        if txn.card_number:
            # Extract last 4 digits
            card_last4 = txn.card_number[-4:] if len(txn.card_number) >= 4 else txn.card_number
            meta[CARD_NUMBER_KEY] = card_last4
        
        if txn.original_amount:
            meta[ORIGINAL_AMOUNT_KEY] = txn.original_amount
        
        if txn.original_currency:
            meta[ORIGINAL_CURRENCY_KEY] = txn.original_currency
        
        if txn.exchange_rate:
            meta[EXCHANGE_RATE_KEY] = txn.exchange_rate
        
        if txn.iban:
            meta[ACCOUNT_IBAN_KEY] = txn.iban
        
        if txn.iban_pl:
            meta[ACCOUNT_IBAN_PL_KEY] = txn.iban_pl
        
        # Link to source document
        meta[SOURCE_DOC_KEY] = os.path.basename(statement.filename)
        
        # Create postings
        units = Amount(txn.amount, txn.currency)
        posting = Posting(
            account=target_account,
            units=units,
            cost=None,
            price=None,
            flag=None,
            meta=meta,
        )
        
        # FIXME posting for other side
        fixme_units = Amount(-txn.amount, txn.currency)
        fixme_posting = Posting(
            account=FIXME_ACCOUNT,
            units=fixme_units,
            cost=None,
            price=None,
            flag=None,
            meta=None,
        )
        
        # Transaction date
        txn_date = txn.completed_date or txn.started_date
        
        return Transaction(
            meta=collections.OrderedDict([
                ('filename', statement.filename),
                ('lineno', txn.line_number),
            ]),
            date=txn_date,
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
        
        valid_ids = set()

        # Process all transactions
        for statement, txn in self.transactions:
            account_id = f"{statement.account_type}_{txn.currency}"
            txn_id = _generate_transaction_id(statement.account_type, txn.currency, txn)
            valid_ids.add(txn_id)

            existing = matched_ids.get(txn_id)
            if existing is not None:
                if len(existing) > 1:
                    results.add_invalid_reference(
                        InvalidSourceReference(len(existing) - 1, existing))
            else:
                target_account = self._get_account_for_id(account_id)
                if target_account is None:
                    continue
                    
                beancount_txn = self._make_transaction(statement, txn, target_account)
                results.add_pending_entry(
                    ImportResult(
                        date=txn.completed_date or txn.started_date,
                        entries=[beancount_txn],
                        info=get_info(statement.filename),
                    ))
                
                # Track balance
                balances_by_account.setdefault(target_account, []).append(
                    (txn.completed_date or txn.started_date, txn.balance_after, txn.currency)
                )

        # Generate balance assertions
        for account, balances in balances_by_account.items():
            if not balances:
                continue
            
            # Get latest balance per date
            latest_by_date: Dict[datetime.date, Tuple[Decimal, str]] = {}
            for date, balance, currency in balances:
                latest_by_date[date] = (balance, currency)
            
            # Add balance assertions for the latest date
            latest_date = max(latest_by_date.keys())
            balance_amount, currency = latest_by_date[latest_date]
            
            balance_entry = Balance(
                meta=collections.OrderedDict([
                    ('filename', '<revolut>'),
                    ('lineno', 0),
                ]),
                date=latest_date + datetime.timedelta(days=1),
                account=account,
                amount=Amount(balance_amount, currency),
                tolerance=None,
                diff_amount=None,
            )
            results.add_pending_entry(
                ImportResult(
                    date=latest_date + datetime.timedelta(days=1),
                    entries=[balance_entry],
                    info=get_info('<balance>'),
                ))

        # Check for invalid references
        for ref, entries in matched_ids.items():
            if ref not in valid_ids and ref.startswith('revolut:'):
                results.add_invalid_reference(
                    InvalidSourceReference(0, entries))


def load(spec: dict, log_status):
    """Load the Revolut source."""
    return RevolutSource(log_status=log_status, **spec)
