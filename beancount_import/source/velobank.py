"""VeloBank (Poland) PDF bank statement source.

Data format
===========

This source imports transactions from VeloBank PDF bank statements. Download
PDF statements from the VeloBank online banking portal and store them in a
directory structure.

You might have a directory structure like:

    financial/
      data/
        velobank/
          2023/
            statement_01.pdf
            statement_02.pdf
          2024/
            statement_01.pdf
            ...

The PDF statements contain:
- Account holder information
- Account number (IBAN)
- Statement period
- Opening/closing balance
- Transaction list with dates, descriptions, amounts, and running balance

This module uses `pdftotext` command-line tool for PDF parsing. Make sure
poppler-utils is installed on your system.

Specifying the source to beancount_import
=========================================

Within your Python script for invoking beancount_import, you might use an
expression like the following to specify the velobank source:

    # Simple single-account configuration:
    dict(module='beancount_import.source.velobank',
         directory=os.path.join(journal_dir, 'data', 'velobank'),
         assets_account='Assets:Bank:VeloBank',
    )

    # Multi-account configuration (different IBANs -> different accounts):
    dict(module='beancount_import.source.velobank',
         directory=os.path.join(journal_dir, 'data', 'velobank'),
         account_map={
             'PL42156000132001525640000001': 'Assets:Bank:VeloBank:Osobisty',
             'PL42156000132001525640000002': 'Assets:Bank:VeloBank:Oszczednosciowy',
         },
         default_account='Assets:Bank:VeloBank:Other',  # fallback for unknown IBANs
    )

where `journal_dir` refers to the financial/ directory.

Format Detection
================

VeloBank has changed their statement format over time. This module automatically
detects and handles:

1. Old format (2018 - early 2024): Column order is transaction date, booking
   date, description, amount, balance. Multi-line descriptions.

2. New format (late 2024+): Column order is booking date, transaction date,
   description, amount, balance. Simpler single-line descriptions.

Imported transaction format
===========================

Transactions are generated in the following form:

    2024-01-02 * "Przelew przychodzący zewnętrzny"
      Assets:Bank:VeloBank     48.00 PLN
        date: 2024-01-02
        velobank_statement: "1/2024"
        velobank_type: "Przelew przychodzący zewnętrzny"
        velobank_counterparty: "KALWIK RADOSŁAW"
        velobank_title: "YouTube premium + Google One - 3 msc"
      Expenses:FIXME          -48.00 PLN

The `velobank_statement` metadata field is used to associate transactions in the
Beancount journal with the source PDF statement.

For account prediction, the `velobank_type`, `velobank_counterparty`, and
`velobank_title` metadata fields are used as features.
"""

import collections
import datetime
import glob
import hashlib
import os
import re
import subprocess
from html.parser import HTMLParser
from typing import Dict, List, NamedTuple, Optional, Tuple, Union

from beancount.core.data import Balance, Document, Posting, Transaction, EMPTY_SET, Open
from beancount.core.flags import FLAG_OKAY
from beancount.core.number import D, ZERO
from beancount.core.amount import Amount

from . import ImportResult, Source, SourceResults, InvalidSourceReference
from ..matching import FIXME_ACCOUNT
from ..journal_editor import JournalEditor


# Metadata keys (standardized across all bank sources)
SOURCE_REF_KEY = 'source_ref'  # Unique transaction reference
TRANSACTION_TYPE_KEY = 'transaction_type'  # Transaction type
COUNTERPARTY_KEY = 'counterparty'  # Counterparty name
TITLE_KEY = 'title'  # Transaction title
COUNTERPARTY_IBAN_KEY = 'counterparty_iban'  # Counterparty IBAN (with country prefix)
COUNTERPARTY_BBAN_KEY = 'counterparty_bban'  # Counterparty BBAN (without country prefix)
ACCOUNT_IBAN_KEY = 'account_iban'  # Own account IBAN
CARD_NUMBER_KEY = 'card_number'  # Card number for card operations
TRANSACTION_DATE_KEY = 'transaction_date'  # Value/transaction date (when money moved)
BOOKING_DATE_KEY = 'booking_date'  # Booking date (when bank recorded it)
COUNTERPARTY_ADDRESS_KEY = 'counterparty_address'  # Counterparty address
SOURCE_BANK_KEY = 'source_bank'  # Bank name for this source
# Tax payment metadata keys
TAX_NIP_KEY = 'tax_nip'  # NIP identifier for tax payments
TAX_SYMBOL_KEY = 'tax_symbol'  # Tax symbol (e.g., VAT-7K, PIT-28)
TAX_PERIOD_KEY = 'tax_period'  # Settlement period (e.g., 24K01)
TAX_OBLIGATION_KEY = 'tax_obligation'  # Obligation identification
TAX_PAYER_KEY = 'tax_payer'  # Obligor/payer data
SOURCE_DOC_KEY = 'document'  # Link to source document file (clickable in fava)

# Currency
DEFAULT_CURRENCY = 'PLN'


# Polish to English transaction type translations
# This is the closed catalog of known transaction types
TRANSACTION_TYPE_TRANSLATIONS = {
    'Przelew przychodzący zewnętrzny': 'Incoming external transfer',
    'Przelew przychodzący wewnętrzny': 'Incoming internal transfer',
    'Przelew wychodzący zewnętrzny': 'Outgoing external transfer',
    'Przelew wychodzący wewnętrzny': 'Outgoing internal transfer',
    'Przelew do Urzędu Skarbowego': 'Tax office payment',
    'Operacja kartą': 'Card payment',
    'Spłata kredytu': 'Loan repayment',
    'Przeksięgowanie kredytu': 'Loan transfer',
    'Wypłata BLIK': 'BLIK withdrawal',
    'BLIK': 'BLIK payment',
    'Przelew na rachunek własny': 'Transfer to own account',
    'Przelew z rachunku własnego': 'Transfer from own account',
    'Zlecenie stałe': 'Standing order',
    'Polecenie zapłaty': 'Direct debit',
    'Wpłata gotówkowa': 'Cash deposit',
    'Wypłata gotówkowa': 'Cash withdrawal',
    'Kapitalizacja odsetek': 'Interest capitalization',
    'Opłata': 'Fee',
    'Prowizja': 'Commission',
}

# Set of known transaction types (Polish) for validation
KNOWN_TRANSACTION_TYPES = set(TRANSACTION_TYPE_TRANSLATIONS.keys())


def _get_transaction_type_if_known(polish_type: str) -> Optional[str]:
    """Get English transaction type if the Polish type is known.
    
    Only returns a value if the type is in the closed catalog.
    This prevents transaction titles from being used as types.

    Args:
        polish_type: The Polish transaction type string.

    Returns:
        English translation if known, None otherwise.
    """
    if not polish_type:
        return None
    
    # Try exact match first
    if polish_type in TRANSACTION_TYPE_TRANSLATIONS:
        return TRANSACTION_TYPE_TRANSLATIONS[polish_type]
    
    # Try prefix match (e.g., "Operacja kartą 5375..." -> "Card payment")
    for polish, english in TRANSACTION_TYPE_TRANSLATIONS.items():
        if polish_type.startswith(polish):
            return english
    
    return None


def _translate_transaction_type(polish_type: str) -> str:
    """Translate Polish transaction type to English.

    Args:
        polish_type: The Polish transaction type.

    Returns:
        English translation if available, otherwise the original Polish type.
    """
    result = _get_transaction_type_if_known(polish_type)
    return result if result else polish_type


def _split_name_and_address(text: str) -> Tuple[str, Optional[str]]:
    """Split counterparty name from address.
    
    Handles various patterns:
    - "NAME ul. ADDRESS" (with space)
    - "NAMEul. ADDRESS" (without space - PDF extraction issue)
    - "NAME,ul. ADDRESS" (with comma)
    - "NAME, UL. ADDRESS" (comma and space)
    - "NAME,,ADDRESS" (double comma separator)
    
    Address indicators: ul./Ul./UL., al./Al./AL., pl./Pl./PL.
    
    Args:
        text: Full text containing name and possibly address.
        
    Returns:
        Tuple of (name, address). Address is None if not found.
    """
    if not text:
        return '', None
    
    text = text.strip()
    
    # First try double-comma separator (VeloBank's common pattern)
    if ',,' in text:
        parts = text.split(',,', 1)
        return parts[0].strip(), parts[1].strip() if len(parts) > 1 else None
    
    # Try to split at address indicators (ul./Ul./UL./al./Al./AL./pl./Pl./PL.)
    # Handle both: 'NAME ul.' and 'NAMEul.' (PDF extraction without space)
    match = re.match(
        r'^(.+?)(?:\s+|,\s*|(?=[Uu][Ll]\.|[Aa][Ll]\.|[Pp][Ll]\.))([Uu][Ll]\.|[Aa][Ll]\.|[Pp][Ll]\.)(.*)$', 
        text
    )
    if match:
        name = match.group(1).strip().rstrip(',')
        address = (match.group(2) + match.group(3)).strip()
        return name, address if address else None
    
    # Try comma separator (simple split)
    if ',' in text:
        parts = text.split(',', 1)
        name = parts[0].strip()
        address = parts[1].strip() if len(parts) > 1 else None
        return name, address
    
    # No address found
    return text, None


class RawTransaction(NamedTuple):
    """Represents a parsed transaction from the PDF statement."""
    transaction_date: datetime.date
    booking_date: datetime.date
    description: str
    amount: D
    balance_after: D
    statement_id: str
    line_number: int
    filename: str
    # Optional parsed fields
    transaction_type: Optional[str] = None
    counterparty: Optional[str] = None
    counterparty_iban: Optional[str] = None
    counterparty_address: Optional[str] = None
    title: Optional[str] = None
    card_number: Optional[str] = None
    # Tax payment fields
    tax_nip: Optional[str] = None
    tax_symbol: Optional[str] = None
    tax_period: Optional[str] = None
    tax_payer: Optional[str] = None


class StatementInfo(NamedTuple):
    """Metadata about a parsed statement."""
    filename: str
    statement_id: str
    account_iban: str
    period_start: datetime.date
    period_end: datetime.date
    opening_balance: Optional[D]
    transactions: List[RawTransaction]


def parse_polish_amount(text: str) -> D:
    """Parse a Polish-formatted amount string.

    Handles formats like:
    - "-8 706,73" (with space as thousand separator)
    - "1 500,00"
    - "-45,00"
    - "20 000,00"
    - "-20 000,00 PLN" (with currency suffix)
    - "650--12.99" (malformed: two columns merged, take second part)

    Args:
        text: Amount string in Polish format.

    Returns:
        Decimal representing the amount.
    """
    # Remove any leading/trailing whitespace
    text = text.strip()
    
    # Remove currency suffix (PLN, EUR, etc.)
    text = re.sub(r'\s*(PLN|EUR|USD|GBP|CHF)\s*$', '', text, flags=re.IGNORECASE)

    # Remove thousands separators (spaces or non-breaking spaces) FIRST
    text = re.sub(r'[\s\u00a0]+', '', text)

    # Handle malformed amounts where two columns merged with double dash
    # e.g., "650--12,99" should be parsed as "-12,99"
    # This happens when phone numbers like "650-253-0000" get captured with amounts
    if '--' in text:
        text = '-' + text.split('--')[-1]

    # Replace comma with period for decimal
    text = text.replace(',', '.')

    return D(text)


def parse_polish_date(text: str) -> datetime.date:
    """Parse a Polish-formatted date string.

    Handles formats like:
    - "2024.01.02" (YYYY.MM.DD)
    - "2024-01-02" (YYYY-MM-DD)
    - "01.02.2024" (DD.MM.YYYY)

    Args:
        text: Date string.

    Returns:
        datetime.date object.
    """
    text = text.strip()
    # Handle both . and - separators
    if '.' in text:
        # Check if it's DD.MM.YYYY or YYYY.MM.DD
        parts = text.split('.')
        if len(parts[0]) == 4:
            # YYYY.MM.DD format
            return datetime.datetime.strptime(text, '%Y.%m.%d').date()
        else:
            # DD.MM.YYYY format
            return datetime.datetime.strptime(text, '%d.%m.%Y').date()
    else:
        return datetime.datetime.strptime(text, '%Y-%m-%d').date()


def extract_pdf_text(pdf_path: str) -> str:
    """Extract text from a PDF file using pdftotext.

    Args:
        pdf_path: Path to the PDF file.

    Returns:
        Extracted text content.

    Raises:
        RuntimeError: If pdftotext fails or is not available.
    """
    try:
        # Use pdftotext WITH -layout to preserve column positions in tables
        # VeloBank statements have tabular format where dates, descriptions, 
        # amounts and balances are in separate columns. Without -layout flag,
        # the columns get interleaved causing transaction data to be lost.
        result = subprocess.run(
            ['pdftotext', '-layout', pdf_path, '-'],
            capture_output=True,
            text=True,
            check=True,
        )
        return result.stdout
    except subprocess.CalledProcessError as e:
        raise RuntimeError(f'pdftotext failed for {pdf_path}: {e.stderr}')
    except FileNotFoundError:
        raise RuntimeError(
            'pdftotext not found. Please install poppler-utils.')


def detect_format(text: str) -> str:
    """Detect the statement format version.

    Args:
        text: Extracted text from PDF.

    Returns:
        'old', 'new', or 'history' format identifier.
    """
    # "Historia rachunku" format - account history export (late 2024+)
    # Has "Historia rachunku" header and uses transaction layout similar to new format
    if 'Historia rachunku' in text:
        return 'history'

    # New format has header starting with "VeloBank S.A." and different layout
    if 'VeloBank S.A.' in text and 'Wyciąg nr ' in text:
        return 'new'

    # Old format has "Wyciąg z rachunku" in the header
    if 'Wyciąg z rachunku' in text:
        return 'old'

    # Default to old format
    return 'old'


# ============================================================================
# HTML Credit Card Statement Parsing
# ============================================================================

class CreditCardHTMLParser(HTMLParser):
    """HTML parser for VeloBank credit card statements.
    
    Extracts:
    - Statement metadata (statement_id, period, account_iban)
    - Transactions from both main table and per-card breakdown tables
    """
    
    def __init__(self):
        super().__init__()
        self.statement_id = ''
        self.period_start = None
        self.period_end = None
        self.account_iban = ''
        self.transactions = []
        self.closing_balance = None  # Closing balance from statement
        
        # Parser state
        self._in_h1 = False
        self._in_table = False
        self._table_id = None
        self._in_tbody = False
        self._in_tr = False
        self._in_td = False
        self._td_class = ''
        self._td_data = []
        self._current_row = []
        self._current_text = ''
        self._collecting_iban = False
        
    def handle_starttag(self, tag, attrs):
        attrs_dict = dict(attrs)
        
        if tag == 'h1':
            self._in_h1 = True
            self._current_text = ''
        elif tag == 'table':
            self._in_table = True
            self._table_id = attrs_dict.get('id', '')
        elif tag == 'tbody':
            self._in_tbody = True
        elif tag == 'tr':
            self._in_tr = True
            self._current_row = []
        elif tag == 'td':
            self._in_td = True
            self._td_class = attrs_dict.get('class', '')
            self._current_text = ''
            
    def handle_endtag(self, tag):
        if tag == 'h1':
            self._in_h1 = False
            self._parse_h1_content(self._current_text)
        elif tag == 'table':
            self._in_table = False
            self._table_id = None
        elif tag == 'tbody':
            self._in_tbody = False
        elif tag == 'tr':
            self._in_tr = False
            if self._current_row and self._in_tbody:
                self._process_row(self._current_row)
        elif tag == 'td':
            self._in_td = False
            if self._in_tr:
                self._current_row.append({
                    'class': self._td_class,
                    'text': self._current_text.strip()
                })
                # Check for IBAN in "NA RACHUNEK KARTY KREDYTOWEJ" row
                if 'rightal' in self._td_class and not self.account_iban:
                    text = self._current_text.strip()
                    # Look for IBAN pattern (26 digits with spaces)
                    iban_match = re.search(r'(\d{2}[\s\d]{24,})', text)
                    if iban_match:
                        self.account_iban = 'PL' + re.sub(r'\s', '', iban_match.group(1))
                        
    def handle_data(self, data):
        if self._in_h1 or self._in_td:
            self._current_text += data
            
    def _parse_h1_content(self, text):
        """Extract statement number and period from h1 content."""
        # Pattern: "Wyciąg z rachunku karty kredytowej numer X/YYYY"
        stmt_match = re.search(r'numer\s+(\d+/\d{4})', text)
        if stmt_match:
            self.statement_id = stmt_match.group(1)
            
        # Pattern: "za okres rozliczeniowy YYYY.MM.DD - YYYY.MM.DD"
        period_match = re.search(
            r'okres rozliczeniowy\s+(\d{4}\.\d{2}\.\d{2})\s*-\s*(\d{4}\.\d{2}\.\d{2})',
            text
        )
        if period_match:
            self.period_start = parse_polish_date(period_match.group(1))
            self.period_end = parse_polish_date(period_match.group(2))
            
    def _process_row(self, row):
        """Process a table row and extract transaction data if valid."""
        if len(row) < 3:
            return
            
        row_text = ' '.join(cell['text'] for cell in row)
        
        # Extract closing balance from "SALDO KOŃCOWE" row
        if 'SALDO KOŃCOWE' in row_text:
            for cell in row:
                cell_class = cell['class']
                cell_text = cell['text']
                if 'numcol' in cell_class or 'liczba' in cell_class:
                    try:
                        # For credit cards, the balance shown is positive but represents debt
                        # In Beancount, Liabilities accounts have negative balance for debt
                        # So a debt of 100 PLN = -100 PLN in Beancount
                        balance_value = parse_polish_amount(cell_text)
                        # Negate: positive debt in statement = negative liability balance
                        self.closing_balance = -balance_value
                    except (ValueError, TypeError):
                        pass
            return
        
        # Skip other summary/balance rows
        skip_patterns = [
            'SALDO POCZĄTKOWE',
            'SUMA TRANSAKCJI DLA KARTY',
            'ŁĄCZNA SUMA TRANSAKCJI',
            'DATA KSIĘGOWANIA',  # header row
        ]
        if any(pattern in row_text for pattern in skip_patterns):
            return
            
        # Extract fields based on row structure
        # Expected: [booking_date, txn_date, description, amount]
        booking_date_text = ''
        txn_date_text = ''
        description = ''
        amount_text = ''
        
        for cell in row:
            cell_class = cell['class']
            cell_text = cell['text']
            
            if 'data' in cell_class and not booking_date_text:
                booking_date_text = cell_text
            elif 'data' in cell_class and booking_date_text and not txn_date_text:
                txn_date_text = cell_text
            elif 'opis' in cell_class:
                description = cell_text
            elif 'liczba' in cell_class or 'numcol' in cell_class:
                amount_text = cell_text
                
        # Validate: need at least dates and amount
        if not booking_date_text or not amount_text:
            return
            
        # Parse dates
        try:
            booking_date = parse_polish_date(booking_date_text)
            txn_date = parse_polish_date(txn_date_text) if txn_date_text else booking_date
        except (ValueError, AttributeError):
            return
            
        # Parse amount
        try:
            amount = parse_polish_amount(amount_text)
        except (ValueError, TypeError):
            return
            
        # Create transaction
        self.transactions.append({
            'booking_date': booking_date,
            'transaction_date': txn_date,
            'description': description,
            'amount': amount,
        })


def parse_credit_card_html(html_path: str) -> StatementInfo:
    """Parse a VeloBank credit card HTML statement.
    
    Args:
        html_path: Path to the HTML file.
        
    Returns:
        StatementInfo containing parsed data.
    """
    with open(html_path, 'r', encoding='utf-8') as f:
        content = f.read()
        
    parser = CreditCardHTMLParser()
    parser.feed(content)
    
    filename = os.path.basename(html_path)
    
    # Get closing balance (default to 0 if not found)
    closing_balance = parser.closing_balance if parser.closing_balance is not None else D('0')
    
    # Calculate running balances by working backwards from closing balance
    # For credit cards: amounts are positive for charges, negative for payments/credits
    # The balance_after represents the liability balance (negative = debt)
    # We need to work backwards: for each transaction, the balance before = balance after - amount
    # Since amounts in HTML are: positive = charge (increases debt), negative = payment (reduces debt)
    # But for Liabilities: charges should be NEGATIVE (credit to liability = increase debt)
    # and payments should be POSITIVE (debit to liability = reduce debt)
    # The HTML shows charges as positive, so we need to NEGATE amounts for Beancount
    
    # First pass: convert amounts for Beancount (negate them for liability accounting)
    parsed_txns = []
    for txn in parser.transactions:
        # Negate amount: HTML positive (charge) -> Beancount negative (credit to liability)
        beancount_amount = -txn['amount']
        parsed_txns.append({
            'booking_date': txn['booking_date'],
            'transaction_date': txn['transaction_date'],
            'description': txn['description'],
            'amount': beancount_amount,
        })
    
    # Sort transactions by booking_date for proper running balance calculation
    # HTML may have transactions grouped by card, not chronologically
    parsed_txns.sort(key=lambda x: (x['booking_date'], x['transaction_date']))
    
    # Calculate balance_after for each transaction (working backwards)
    # Start from closing balance and work backwards
    running_balance = closing_balance
    balances = []
    for txn in reversed(parsed_txns):
        balances.insert(0, running_balance)
        # balance_before = balance_after - amount
        running_balance = running_balance - txn['amount']
    
    # Convert parsed transactions to RawTransaction objects
    transactions = []
    for i, (txn, balance) in enumerate(zip(parsed_txns, balances), 1):
        description = txn['description']
        amount = txn['amount']
        
        # Extract transaction type
        transaction_type = _extract_credit_card_transaction_type(description)
        
        # Extract counterparty and location for card operations
        counterparty = None
        counterparty_address = None
        card_number = None
        
        if 'Operacja kartą' in description or 'Zwrot operacji kartą' in description:
            counterparty, counterparty_address = _extract_card_merchant(description)
            card_number = _extract_card_number(description)
        elif 'Opłata za obsługę karty' in description:
            # Extract card number from fee description
            card_number = _extract_card_number(description)
        
        transactions.append(RawTransaction(
            transaction_date=txn['transaction_date'],
            booking_date=txn['booking_date'],
            description=description,
            amount=amount,
            balance_after=balance,
            statement_id=parser.statement_id or 'html',
            line_number=i,
            filename=html_path,
            transaction_type=transaction_type,
            counterparty=counterparty,
            counterparty_iban=None,
            counterparty_address=counterparty_address,
            title=None,
            card_number=card_number,
        ))
    
    return StatementInfo(
        filename=html_path,
        statement_id=parser.statement_id or filename.replace('.html', ''),
        account_iban=parser.account_iban,
        period_start=parser.period_start or datetime.date.today(),
        period_end=parser.period_end or datetime.date.today(),
        opening_balance=running_balance,  # The calculated opening balance
        transactions=transactions,
    )


def _extract_credit_card_transaction_type(description: str) -> Optional[str]:
    """Extract transaction type from credit card statement description.
    
    Args:
        description: Transaction description text.
        
    Returns:
        Transaction type string.
    """
    description_lower = description.lower()
    
    if 'zwrot operacji kartą' in description_lower:
        return 'Card refund'
    elif 'operacja kartą' in description_lower:
        return 'Card payment'
    elif 'opłata za obsługę karty' in description_lower:
        return 'Card fee'
    elif 'spłata karty kredytowej' in description_lower:
        return 'Credit card repayment'
    
    return _extract_transaction_type(description)


def _extract_fields_from_continuation_text(text: str) -> dict:
    """Extract IBAN, counterparty, address, and title from continuation text.
    
    Uses keyword-based splitting to handle any line arrangement.
    Keywords: Z rachunku, Na rachunek, Prowadzonego na rzecz, Odbiorca, Nadawca, Tytułem, Tytuł
    
    Args:
        text: Combined continuation text (all lines joined with spaces).
        
    Returns:
        Dict with keys: 'counterparty_iban', 'counterparty', 'counterparty_address', 'title'
    """
    result = {
        'counterparty_iban': None,
        'counterparty': None,
        'counterparty_address': None,
        'title': None,
    }
    
    if not text:
        return result
    
    # Normalize whitespace
    text = re.sub(r'\s+', ' ', text).strip()
    
    # Extract IBAN/BBAN - digits after "Z rachunku:" or "Na rachunek:"
    iban_match = re.search(
        r'(?:Z rachunku|Na rachunek)[:\s]*([A-Z]{0,2}\s*[\d\s]{15,}?)(?=\s*(?:Prowadzon|Odbiorca|Nadawca|Tytułem|Tytuł|$))',
        text, re.IGNORECASE
    )
    if iban_match:
        result['counterparty_iban'] = re.sub(r'[^\d]', '', iban_match.group(1))
    else:
        # Fallback: if text starts with IBAN-like digits (20+ chars), extract them
        # This handles tax payments where description ends with "na rachunek :" and
        # continuation starts directly with account number
        standalone_iban = re.match(r'^([\d\s]{20,})(?:\s+\D|$)', text)
        if standalone_iban:
            result['counterparty_iban'] = re.sub(r'[^\d]', '', standalone_iban.group(1))
    
    # Extract counterparty and address from Prowadzonego/Odbiorca/Nadawca section
    cp_match = re.search(
        r'(?:Prowadzonego na rzecz|Prowadzony na rzecz|Odbiorca|Nadawca)[:\s]*(.+?)(?=\s*(?:Tytułem|Tytuł)|$)',
        text, re.IGNORECASE
    )
    if cp_match:
        cp_text = cp_match.group(1).strip()
        
        # Split at ,, or address indicators (UL., ul., AL., al., postal code XX-XXX)
        # Also handle NO comma case - everything before first address indicator
        name_addr_match = re.match(
            r'^(.+?)(?:,,|,\s*(?:UL\.|ul\.|Ul\.|AL\.|al\.|Al\.)|\s+(?:UL\.|ul\.|Ul\.|AL\.|al\.|Al\.)|,\s*\d{2}-\d{3})',
            cp_text
        )
        if name_addr_match:
            result['counterparty'] = name_addr_match.group(1).strip().rstrip(',')
            # Extract address - everything after the name
            addr_start = name_addr_match.end(1)
            addr = cp_text[addr_start:].strip().lstrip(',').strip()
            if addr:
                result['counterparty_address'] = addr
        else:
            # No address indicator found - full text is counterparty name (possibly with trailing address)
            # Try to split at first comma followed by uppercase location
            parts = cp_text.split(',', 1)
            result['counterparty'] = parts[0].strip()
            if len(parts) > 1 and parts[1].strip():
                result['counterparty_address'] = parts[1].strip()
    
    # Normalize counterparty - fix PDF line-break artifacts
    if result['counterparty']:
        # Remove space before lowercase letter (PDF line break artifact)
        result['counterparty'] = re.sub(r' ([a-ząćęłńóśźż])', r'\1', result['counterparty'])
        result['counterparty'] = re.sub(r'\s+', ' ', result['counterparty']).strip()
    
    # Extract title - everything after Tytułem/Tytuł
    title_match = re.search(r'(?:Tytułem|Tytuł)[:\s]*(.+?)$', text, re.IGNORECASE)
    if title_match:
        result['title'] = title_match.group(1).strip()
    
    # Fallback: if no keywords matched at all, use entire text as title
    # This handles cases like "Automatyczna spłata karty kredytowe 5371656" 
    # with no Z rachunku/Tytułem/Odbiorca keywords
    if not any([result['counterparty_iban'], result['counterparty'], result['title']]):
        if text and not re.search(r'(?:Z rachunku|Na rachunek|Prowadzon|Odbiorca|Nadawca|Tytułem|Tytuł)', text, re.IGNORECASE):
            result['title'] = text
    
    return result


def parse_old_format(text: str, filename: str) -> StatementInfo:
    """Parse a statement in the old format (2018 - early 2024) or Historia rachunku.

    Args:
        text: Extracted text from PDF.
        filename: Source filename for reference.

    Returns:
        StatementInfo containing parsed data.
    """
    lines = text.split('\n')

    # Extract statement ID and period
    statement_id = ''
    period_start = None
    period_end = None
    account_iban = ''
    opening_balance = None

    # Look for header pattern: "Wyciąg z rachunku nr X/YYYY za okres YYYY.MM.DD - YYYY.MM.DD"
    header_pattern = re.compile(
        r'Wyciąg z rachunku(?:\s+nr)?\s+(\d+/\d{4})\s+za okres\s+(\d{4}\.\d{2}\.\d{2})\s*-\s*(\d{4}\.\d{2}\.\d{2})'
    )
    
    # Historia rachunku format: "Za okres od DD.MM.YYYY do DD.MM.YYYY"
    history_period_pattern = re.compile(
        r'Za okres od\s+(\d{2}\.\d{2}\.\d{4})\s+do\s+(\d{2}\.\d{2}\.\d{4})'
    )

    # Look for account number
    iban_pattern = re.compile(r'(?:NUMER RACHUNKU|Numer rachunku)[:\s]*([\d\s]{20,})')

    for line in lines:
        # Try to extract header (old format)
        header_match = header_pattern.search(line)
        if header_match:
            statement_id = header_match.group(1)
            period_start = parse_polish_date(header_match.group(2))
            period_end = parse_polish_date(header_match.group(3))
        
        # Try to extract period (Historia rachunku format: DD.MM.YYYY)
        if not period_start:
            history_match = history_period_pattern.search(line)
            if history_match:
                period_start = datetime.datetime.strptime(history_match.group(1), '%d.%m.%Y').date()
                period_end = datetime.datetime.strptime(history_match.group(2), '%d.%m.%Y').date()

        # Try to extract IBAN
        iban_match = iban_pattern.search(line)
        if iban_match:
            account_iban = re.sub(r'\s+', '', iban_match.group(1))
            # Normalize: ensure IBAN has PL prefix (old format doesn't include it)
            if account_iban and not account_iban.startswith('PL'):
                account_iban = 'PL' + account_iban

    # Parse transactions
    transactions = _parse_old_format_transactions(
        lines, statement_id, filename)

    return StatementInfo(
        filename=filename,
        statement_id=statement_id,
        account_iban=account_iban,
        period_start=period_start or datetime.date.today(),
        period_end=period_end or datetime.date.today(),
        opening_balance=opening_balance,
        transactions=transactions,
    )


def _parse_old_format_transactions(
    lines: List[str],
    statement_id: str,
    filename: str,
) -> List[RawTransaction]:
    """Parse transactions from old format statement lines.

    The old format has columns:
    DATA TRANSAKCJI | DATA KSIĘGOWANIA | OPIS TRANSAKCJI | KWOTA TRANSAKCJI | SALDO PO TRANSAKCJI

    Transactions can span multiple lines, with continuation lines for:
    - Counterparty account number
    - Counterparty name
    - Transaction title
    
    Supports both YYYY.MM.DD (old format) and DD.MM.YYYY (Historia rachunku) date formats.
    Supports amounts with optional PLN suffix.
    """
    transactions = []

    # Pattern to match transaction start line
    # Two dates (YYYY.MM.DD or DD.MM.YYYY) followed by description and amount columns
    # Amounts may have optional PLN suffix
    date_pattern = r'(?:\d{4}\.\d{2}\.\d{2}|\d{2}\.\d{2}\.\d{4})'
    amount_pattern = r'[-\s\d]+[,]\d{2}(?:\s*PLN)?'
    txn_start_pattern = re.compile(
        rf'^({date_pattern})\s+({date_pattern})\s+(.+?)\s+({amount_pattern})\s+({amount_pattern})\s*$'
    )

    # Patterns for continuation lines
    # IBAN pattern: "Na rachunek:" or "Z rachunku:" followed by account number (digits with spaces)
    iban_line_pattern = re.compile(
        r'(?:Na rachunek|Z rachunku)[:\s]*([\dPL][\d\s]{10,})', re.IGNORECASE)
    # Recipient name pattern: "Odbiorca:" or "Nadawca:" followed by name
    recipient_name_pattern = re.compile(
        r'(?:Odbiorca|Nadawca)[:\s]*(.*)', re.IGNORECASE)
    counterparty_pattern = re.compile(
        r'(?:Prowadzony na rzecz|Prowadzonego na rzecz)[:\s]*(.*)', re.IGNORECASE)
    # Title patterns: both "Tytułem:" (old format) and "Tytuł:" (Historia rachunku)
    title_pattern = re.compile(r'(?:Tytułem|Tytuł)[:\s]*(.*)', re.IGNORECASE)

    i = 0
    line_number = 0
    while i < len(lines):
        line = lines[i]
        line_number = i + 1

        match = txn_start_pattern.match(line.strip())
        if match:
            txn_date = parse_polish_date(match.group(1))
            booking_date = parse_polish_date(match.group(2))
            description = match.group(3).strip()
            amount = parse_polish_amount(match.group(4))
            balance = parse_polish_amount(match.group(5))

            # Collect continuation lines
            counterparty = None
            counterparty_iban = None
            counterparty_address = None
            title = None

            i += 1
            # Phase 1: Collect all continuation lines into one string
            continuation_lines = []
            while i < len(lines):
                cont_line = lines[i].strip()

                # Stop if we hit another transaction
                if txn_start_pattern.match(cont_line):
                    break
                
                # Stop at date patterns (next transaction on different page)  
                if re.match(r'^(?:\d{4}\.\d{2}\.\d{2}|\d{2}\.\d{2}\.\d{4})', cont_line):
                    break
                
                # Skip empty lines
                if not cont_line:
                    i += 1
                    continue
                
                # Stop at footer patterns - don't collect these
                is_footer = (
                    cont_line.startswith('Dokument wygenerowany') or
                    'VeloBank S.A.' in cont_line or
                    'Saldo' in cont_line or
                    re.match(r'^(DATA|KWOTA|OPIS|SALDO)', cont_line, re.IGNORECASE) or
                    'Obciążenia' in cont_line or
                    'Uznania' in cont_line or
                    'Środki zgromadzone' in cont_line or
                    'Bankowym Funduszu' in cont_line or
                    'Funduszu Gwarancyjnym' in cont_line or
                    'systemie gwarantowania' in cont_line or
                    'Arkuszu Informacyjnym' in cont_line or
                    'Sąd Rejonowy dla m.st.' in cont_line or  # Only footer (bank registration), not counterparty
                    'Nie wymaga podpisu' in cont_line or
                    'www.bfg.pl' in cont_line or
                    'w całości opłaconym' in cont_line or
                    'Dz. U.' in cont_line or
                    'Dz.U.' in cont_line or
                    re.search(r'KRS \d+', cont_line) or
                    re.search(r'NIP \d+', cont_line) or
                    re.search(r'REGON \d+', cont_line) or
                    re.search(r'kapitale.*zł', cont_line, re.IGNORECASE)
                )
                
                if is_footer:
                    break
                
                # Collect this continuation line
                continuation_lines.append(cont_line)
                i += 1
            
            # Phase 2: Extract fields from collected text using keyword-based splitting
            continuation_text = ' '.join(continuation_lines)
            fields = _extract_fields_from_continuation_text(continuation_text)
            
            counterparty = fields['counterparty']
            counterparty_iban = fields['counterparty_iban']
            counterparty_address = fields['counterparty_address']
            title = fields['title']

            # For card operations, extract merchant name as counterparty
            if not counterparty and description.startswith('Operacja kartą'):
                counterparty, card_location = _extract_card_merchant(description)
                if card_location and not counterparty_address:
                    counterparty_address = card_location

            # Normalize counterparty - fix PDF line-break artifacts
            # "ZTM Dzial Ko ntroli Bi" -> "ZTM Dzial Kontroli Bi"
            if counterparty:
                # Remove space before lowercase letter (PDF line break artifact)
                counterparty = re.sub(r' ([a-ząćęłńóśźż])', r'\1', counterparty)
                # Collapse multiple spaces
                counterparty = re.sub(r'\s+', ' ', counterparty).strip()

            transactions.append(RawTransaction(
                transaction_date=txn_date,
                booking_date=booking_date,
                description=description,
                amount=amount,
                balance_after=balance,
                statement_id=statement_id,
                line_number=line_number,
                filename=filename,
                transaction_type=_extract_transaction_type(description),
                counterparty=counterparty,
                counterparty_iban=counterparty_iban,
                counterparty_address=counterparty_address,
                title=title,
                card_number=_extract_card_number(description),
            ))
        else:
            i += 1

    return transactions


def parse_new_format(text: str, filename: str) -> StatementInfo:
    """Parse a statement in the new format (late 2024+).

    Args:
        text: Extracted text from PDF.
        filename: Source filename for reference.

    Returns:
        StatementInfo containing parsed data.
    """
    lines = text.split('\n')

    # Extract statement ID and period
    statement_id = ''
    period_start = None
    period_end = None
    account_iban = ''
    opening_balance = None

    # Look for statement number: "Wyciąg nr 7718519"
    statement_pattern = re.compile(r'Wyciąg nr\s+(\d+)')

    # Look for period - two formats:
    # 1. "Wyciąg za okres od 2024.12.01 do 2024.12.31" (YYYY.MM.DD)
    # 2. "Za okres od 01.05.2025 do 31.05.2025" (DD.MM.YYYY - Historia rachunku format)
    period_pattern = re.compile(
        r'Wyciąg za okres od\s+(\d{4}\.\d{2}\.\d{2})\s+do\s+(\d{4}\.\d{2}\.\d{2})'
    )
    period_pattern_history = re.compile(
        r'Za okres od\s+(\d{2}\.\d{2}\.\d{4})\s+do\s+(\d{2}\.\d{2}\.\d{4})'
    )

    # Look for IBAN - two formats:
    # 1. "IBAN: PL 42 1560 0013 2001 5256 4000 0001"
    # 2. "NUMER RACHUNKU: 42 1560 0013 2001 5256 4000 0001" (Historia rachunku format)
    iban_pattern = re.compile(r'IBAN[:\s]*(PL[\s\d]+)')
    account_pattern = re.compile(r'NUMER RACHUNKU[:\s]*([\d\s]{20,})')

    # Look for opening balance: "Saldo początkowe ... -6 961,43"
    opening_pattern = re.compile(r'Saldo początkowe\s+([-\s\d,]+)')

    for line in lines:
        # Try to extract statement number
        stmt_match = statement_pattern.search(line)
        if stmt_match:
            statement_id = stmt_match.group(1)

        # Try to extract period (new format: YYYY.MM.DD)
        period_match = period_pattern.search(line)
        if period_match:
            period_start = parse_polish_date(period_match.group(1))
            period_end = parse_polish_date(period_match.group(2))
        
        # Try to extract period (history format: DD.MM.YYYY)
        if not period_start:
            history_match = period_pattern_history.search(line)
            if history_match:
                # Parse DD.MM.YYYY format
                period_start = datetime.datetime.strptime(
                    history_match.group(1), '%d.%m.%Y').date()
                period_end = datetime.datetime.strptime(
                    history_match.group(2), '%d.%m.%Y').date()

        # Try to extract IBAN (standard format)
        iban_match = iban_pattern.search(line)
        if iban_match:
            account_iban = re.sub(r'\s+', '', iban_match.group(1))
        
        # Try to extract IBAN (Historia rachunku format: NUMER RACHUNKU)
        if not account_iban:
            account_match = account_pattern.search(line)
            if account_match:
                raw_iban = re.sub(r'\s+', '', account_match.group(1))
                # Add PL prefix if not present
                account_iban = 'PL' + raw_iban if not raw_iban.startswith('PL') else raw_iban

        # Try to extract opening balance
        opening_match = opening_pattern.search(line)
        if opening_match:
            try:
                opening_balance = parse_polish_amount(opening_match.group(1))
            except Exception:
                pass

    # Parse transactions
    transactions = _parse_new_format_transactions(
        lines, statement_id, filename, opening_balance)

    return StatementInfo(
        filename=filename,
        statement_id=statement_id,
        account_iban=account_iban,
        period_start=period_start or datetime.date.today(),
        period_end=period_end or datetime.date.today(),
        opening_balance=opening_balance,
        transactions=transactions,
    )


def _parse_new_format_continuation(
    lines: List[str],
    start_idx: int,
    max_lines: int = 10,
    expect_counterparty_first: bool = False,
) -> Tuple[Optional[str], Optional[str], Optional[str], Optional[str], Optional[str], Optional[str]]:
    """Parse continuation lines in new format to extract IBAN, counterparty, title, and address.
    
    Args:
        lines: All lines from the statement.
        start_idx: Index to start searching from (line after transaction).
        max_lines: Maximum number of lines to search.
        expect_counterparty_first: If True, the main transaction line ended with "Odbiorca:"
            or "Nadawca:", so the first continuation line is the counterparty name.
    
    Returns:
        Tuple of (counterparty_iban, counterparty_name, counterparty_address, title, card_merchant, card_location).
    """
    counterparty_iban = None
    counterparty_name = None
    counterparty_address = None
    title = None
    
    # Flags to track multi-line extraction
    expect_iban = False  # Next line after "Z rachunku:" might be IBAN
    expect_counterparty = expect_counterparty_first  # Collecting counterparty name parts
    counterparty_parts = []
    
    # For card operations: collect continuation lines to extract merchant info
    card_continuation_text = []
    
    # Track if previous line ended with "Prowadzony" (split across lines)
    pending_prowadzony = False
    pending_iban_line = None  # Store line that had IBAN + "Prowadzony"
    
    j = start_idx
    while j < len(lines) and j < start_idx + max_lines:
        line = lines[j].strip()
        
        # Stop if we hit another transaction line (starts with date) - check BEFORE collecting
        if re.match(r'^\d{4}\.\d{2}\.\d{2}', line):
            break
        
        # Collect non-empty lines for card merchant extraction
        if line:
            card_continuation_text.append(line)
        
        # Skip page markers
        if 'Strona ' in line and ' z ' in line:
            j += 1
            continue
        
        # Skip footer lines
        if 'Obroty WN' in line or 'Obroty MA' in line or 'Saldo końcowe' in line:
            break
        
        # Handle split "Prowadzony / na rzecz:" pattern
        if pending_prowadzony and line.lower().startswith('na rzecz'):
            # This line is continuation of "Prowadzony na rzecz:"
            # Extract counterparty name after "na rzecz:"
            cp_match = re.search(r'na rzecz[:\s]*(.*)', line, re.IGNORECASE)
            if cp_match:
                rest = cp_match.group(1).strip()
                if rest:
                    # Check if Tytułem is in same line
                    if 'Tytułem' in rest:
                        parts = re.split(r'\s*Tytułem[:\s]*', rest, maxsplit=1)
                        counterparty_name_raw = parts[0].strip()
                        if counterparty_name_raw:
                            counterparty_name, counterparty_address = _extract_name_and_address_from_text(counterparty_name_raw)
                        if len(parts) > 1 and parts[1].strip():
                            title = parts[1].strip()
                        expect_counterparty = False  # Already finalized
                    else:
                        counterparty_parts.append(rest)
                        expect_counterparty = True
            pending_prowadzony = False
            j += 1
            continue
        
        # Check for IBAN on this line or if we're expecting IBAN from previous line
        if not counterparty_iban:
            # Check if line has "Z rachunku:" or "Na rachunek:" with IBAN on same line
            iban_match = re.search(r'(?:Z rachunku|Na rachunek)[\s:]*([A-Z]{0,2}\s*[\d\s]{20,})', line, re.IGNORECASE)
            if iban_match:
                counterparty_iban = re.sub(r'\s+', '', iban_match.group(1))
            # Check if line is just "Z rachunku:" - IBAN on next line
            elif re.search(r'(?:Z rachunku|Na rachunek)\s*:?\s*$', line, re.IGNORECASE):
                expect_iban = True
            # Check if previous line indicated IBAN follows
            elif expect_iban and re.match(r'^[A-Z]{0,2}\s*[\d\s]{20,}$', line):
                counterparty_iban = re.sub(r'\s+', '', line)
                expect_iban = False
            # Standalone IBAN line (just digits with spaces, 20+ chars)
            elif re.match(r'^[\d\s]{20,}$', line):
                counterparty_iban = re.sub(r'\s+', '', line)
            # IBAN at start of line followed by Nadawca/Odbiorca/Prowadzony (new format)
            # Handle both complete "Prowadzony na rzecz:" and split "Prowadzony" + "na rzecz:" on next line
            else:
                # Try complete pattern first
                iban_before_cp = re.match(r'^((?:PL)?[\d\s]{20,})\s+(?:Nadawca|Odbiorca|Prowadzony na rzecz|Prowadzonego na rzecz)[:\s]', line, re.IGNORECASE)
                if iban_before_cp:
                    counterparty_iban = re.sub(r'[^\d]', '', iban_before_cp.group(1))
                else:
                    # Try split pattern - line ends with "Prowadzony" or "Prowadzonego"
                    iban_split = re.match(r'^((?:PL)?[\d\s]{20,})\s+(Prowadzony|Prowadzonego)\s*$', line, re.IGNORECASE)
                    if iban_split:
                        counterparty_iban = re.sub(r'[^\d]', '', iban_split.group(1))
                        pending_prowadzony = True
        
        # Check for counterparty name
        if not counterparty_name:
            # Check for "Prowadzonego na rzecz:" pattern - name follows (complete on one line)
            cp_match = re.search(r'(?:Prowadzony na rzecz|Prowadzonego na rzecz|Odbiorca|Nadawca)[:\s]*(.*)', line, re.IGNORECASE)
            if cp_match:
                rest = cp_match.group(1).strip()
                if rest:
                    counterparty_parts.append(rest)
                expect_counterparty = True
            elif expect_counterparty:
                # Collecting counterparty parts until we hit Tytułem or empty
                if 'Tytułem' in line:
                    # Line contains Tytułem - extract counterparty before it
                    before_title = re.split(r'\s*Tytułem', line, maxsplit=1)[0].strip()
                    if before_title:
                        counterparty_parts.append(before_title)
                    # Done collecting - join parts and extract name + address
                    if counterparty_parts:
                        full_text = ' '.join(counterparty_parts)
                        counterparty_name, counterparty_address = _extract_name_and_address_from_text(full_text)
                    expect_counterparty = False
                elif not line:
                    # Empty line - finalize counterparty
                    if counterparty_parts:
                        full_text = ' '.join(counterparty_parts)
                        counterparty_name, counterparty_address = _extract_name_and_address_from_text(full_text)
                    expect_counterparty = False
                else:
                    counterparty_parts.append(line)
        
        # Try to extract title or append to existing title
        if not title:
            title_match = re.search(r'Tytułem[:\s]*(.*)', line, re.IGNORECASE)
            if title_match:
                title = title_match.group(1).strip()
        elif title and line and not re.match(r'^\d{4}\.\d{2}\.\d{2}', line):
            # Append continuation to title (if line is not a new transaction, footer, or keyword)
            if not any(kw in line for kw in ['Strona ', 'Obroty', 'Saldo', 'Tytułem', 'Z rachunku', 'Na rachunek', 'Prowadzon', 'Nadawca', 'Odbiorca']):
                # Only append if it looks like title continuation (not IBAN or similar)
                if not re.match(r'^[A-Z]{0,2}[\d\s]{20,}$', line):
                    title = title + ' ' + line
        
        j += 1
    
    # Finalize counterparty if still collecting
    if expect_counterparty and counterparty_parts and not counterparty_name:
        full_text = ' '.join(counterparty_parts)
        counterparty_name, counterparty_address = _extract_name_and_address_from_text(full_text)
    
    # Extract card merchant from "PLN w MERCHANT, LOCATION" pattern
    card_merchant = None
    card_location = None
    if card_continuation_text:
        combined = ' '.join(card_continuation_text)
        # Pattern: "20,00 PLN w APPLE.COM/BILL , CORK, IRL" or "... PLN w MERCHANT, CITY, COUNTRY"
        card_match = re.search(r'PLN\s+w\s+(.+?)(?:\s*$)', combined, re.IGNORECASE)
        if card_match:
            card_full = card_match.group(1).strip()
            # Split by comma - first is merchant, rest is location
            parts = [p.strip() for p in card_full.split(',') if p.strip()]
            if parts:
                card_merchant = parts[0]
                card_location = ', '.join(parts[1:]) if len(parts) > 1 else None
    
    return counterparty_iban, counterparty_name, counterparty_address, title, card_merchant, card_location


def _extract_name_and_address_from_text(text: str) -> Tuple[Optional[str], Optional[str]]:
    """Extract counterparty name and address from combined text.
    
    Handles patterns like:
    - "Cashbill eComm www.maxdtf.pl 40-082 Katowice"
    - "JAN KOWALSKI,UL. DŁUGA 15, 00-001 WARSZAWA"
    - "FIRMA XYZ,,ul. Krótka 1"
    
    Args:
        text: Combined text from counterparty lines.
        
    Returns:
        Tuple of (name, address). Address is None if not found.
    """
    if not text:
        return None, None
    
    text = text.strip()
    
    # First try double-comma separator (VeloBank's common pattern)
    if ',,' in text:
        parts = text.split(',,', 1)
        name = parts[0].strip().rstrip(',')
        address = parts[1].strip() if len(parts) > 1 else None
        # Normalize spaces in name
        name = re.sub(r'\s+', ' ', name)
        return name, address
    
    # Try to find address indicators: www., ul./UL./Ul., al./AL., pl./PL., postal code pattern
    # Pattern: name ends before www., ul., al., pl., or postal code (XX-XXX)
    addr_pattern = r'^(.+?)(?:\s+(?:www\.|[Uu][Ll]\.|[Aa][Ll]\.|[Pp][Ll]\.|\d{2}-\d{3}).*|,\s*(?:[Uu][Ll]\.|[Aa][Ll]\.|[Pp][Ll]\.).*|$)'
    match = re.match(addr_pattern, text)
    
    if match:
        name = match.group(1).strip().rstrip(',')
        # Get the rest as address if there's more
        if len(name) < len(text):
            address_start = len(name)
            # Find where address actually starts (skip commas/spaces)
            while address_start < len(text) and text[address_start] in ', \t':
                address_start += 1
            if address_start < len(text):
                address = text[address_start:].strip()
            else:
                address = None
        else:
            address = None
        
        # Normalize spaces in name
        name = re.sub(r'\s+', ' ', name)
        return name, address
    
    # Fallback: split at first comma
    if ',' in text:
        parts = text.split(',', 1)
        name = parts[0].strip()
        address = parts[1].strip() if len(parts) > 1 else None
        name = re.sub(r'\s+', ' ', name)
        return name, address
    
    # No address found - just normalize and return
    name = re.sub(r'\s+', ' ', text)
    return name, None


def _parse_new_format_transactions(
    lines: List[str],
    statement_id: str,
    filename: str,
    opening_balance: Optional[D] = None,
) -> List[RawTransaction]:
    """Parse transactions from new format statement lines.

    The new format has columns:
    Data księgowania | Data transakcji | Opis transakcji | Kwota transakcji | Saldo po transakcji

    Note: Column order is reversed compared to old format (booking date first).
    In the new format, amounts don't have explicit +/- signs, so we need to infer
    the sign from balance changes.
    """
    raw_transactions = []

    # Pattern to match transaction line with description
    # Booking date, transaction date, description, amount (abs), balance
    txn_pattern = re.compile(
        r'^(\d{4}\.\d{2}\.\d{2})\s+(\d{4}\.\d{2}\.\d{2})\s+(.+?)\s+([-\s\d]+[,]\d{2})\s+([-\s\d]+[,]\d{2})\s*$'
    )
    
    # Pattern to match transaction line WITHOUT description (page break case)
    # Just dates and two amounts at the end
    txn_no_desc_pattern = re.compile(
        r'^(\d{4}\.\d{2}\.\d{2})\s+(\d{4}\.\d{2}\.\d{2})\s+([-\s\d]+[,]\d{2})\s+([-\s\d]+[,]\d{2})\s*$'
    )

    i = 0
    while i < len(lines):
        line_stripped = lines[i].strip()

        # Skip header/footer lines
        if 'Saldo początkowe' in line_stripped:
            i += 1
            continue
        if 'Data księgowania' in line_stripped:
            i += 1
            continue
        if 'Obroty WN' in line_stripped or 'Obroty MA' in line_stripped:
            i += 1
            continue
        if 'Saldo końcowe' in line_stripped:
            i += 1
            continue
        if 'Strona ' in line_stripped and ' z ' in line_stripped:
            i += 1
            continue

        # Try matching transaction with description first
        match = txn_pattern.match(line_stripped)
        is_valid_match = False
        
        if match:
            description = match.group(3).strip()
            
            # Check if "description" contains letters (not just digits from amount)
            if re.search(r'[a-zA-ZąćęłńóśźżĄĆĘŁŃÓŚŹŻ]', description):
                try:
                    booking_date = parse_polish_date(match.group(1))
                    txn_date = parse_polish_date(match.group(2))
                    amount_abs = abs(parse_polish_amount(match.group(4)))
                    balance = parse_polish_amount(match.group(5))
                except (ValueError, AttributeError):
                    # Malformed line - skip it
                    i += 1
                    continue
                
                # Skip zero-amount transactions
                if amount_abs != ZERO:
                    # Clean up description - extract just the type part
                    description = description.replace('|', ' ').strip()
                    description = re.sub(r'\s+', ' ', description)
                    
                    # Check if description ends with "Odbiorca:" or "Nadawca:" - counterparty on next line
                    expects_counterparty = bool(re.search(r'(Odbiorca|Nadawca)[:\s]*$', description, re.IGNORECASE))
                    
                    # Remove trailing "Z rachunku:" or "Na rachunek:" etc
                    description = re.sub(r'\s*(Z rachunku|Na rachunek|Odbiorca|Nadawca)[:\s]*$', '', description, flags=re.IGNORECASE)
                    
                    # Parse continuation lines for additional fields
                    iban, counterparty, counterparty_addr, title, card_merchant, card_location = _parse_new_format_continuation(
                        lines, i + 1, expect_counterparty_first=expects_counterparty)
                    
                    # For card operations, use card merchant info if available
                    if description.startswith('Operacja kartą') and not counterparty and card_merchant:
                        counterparty = card_merchant
                    
                    raw_transactions.append({
                        'transaction_date': txn_date,
                        'booking_date': booking_date,
                        'description': description,
                        'amount_abs': amount_abs,
                        'balance_after': balance,
                        'line_number': i + 1,
                        'counterparty_iban': iban,
                        'counterparty': counterparty,
                        'counterparty_address': counterparty_addr,
                        'title': title,
                        'card_merchant': card_merchant,
                        'card_location': card_location,
                    })
                    is_valid_match = True
        
        if is_valid_match:
            i += 1
            continue
        
        # Try matching transaction WITHOUT description (page break case)
        no_desc_match = txn_no_desc_pattern.match(line_stripped)
        if no_desc_match:
            booking_date = parse_polish_date(no_desc_match.group(1))
            txn_date = parse_polish_date(no_desc_match.group(2))
            amount_abs = abs(parse_polish_amount(no_desc_match.group(3)))
            balance = parse_polish_amount(no_desc_match.group(4))
            
            # Look for description in following lines (after page break marker)
            description = None
            j = i + 1
            while j < len(lines) and j < i + 15:  # Look up to 15 lines ahead
                next_line = lines[j].strip()
                
                # Skip page markers and empty lines
                if not next_line or ('Strona ' in next_line and ' z ' in next_line):
                    j += 1
                    continue
                
                # Found description - look for transaction type keywords
                if any(kw in next_line for kw in [
                    'Przelew przychodzący', 'Przelew wychodzący', 
                    'Operacja kartą', 'BLIK', 'Spłata'
                ]):
                    description = next_line
                    break
                j += 1
            
            if description and amount_abs != ZERO:
                # Clean up description - extract just the type part
                description = description.replace('|', ' ').strip()
                description = re.sub(r'\s+', ' ', description)
                
                # Check if description ends with "Odbiorca:" or "Nadawca:" - counterparty on next line
                expects_counterparty = bool(re.search(r'(Odbiorca|Nadawca)[:\s]*$', description, re.IGNORECASE))
                
                # Remove trailing "Z rachunku:" or "Na rachunek:" etc
                description = re.sub(r'\s*(Z rachunku|Na rachunek|Odbiorca|Nadawca)[:\s]*$', '', description, flags=re.IGNORECASE)
                
                # Parse continuation lines for additional fields (start from where we found description)
                iban, counterparty, counterparty_addr, title, card_merchant, card_location = _parse_new_format_continuation(
                    lines, j, expect_counterparty_first=expects_counterparty)
                
                # For card operations, use card merchant info if available
                if description.startswith('Operacja kartą') and not counterparty and card_merchant:
                    counterparty = card_merchant
                
                raw_transactions.append({
                    'transaction_date': txn_date,
                    'booking_date': booking_date,
                    'description': description,
                    'amount_abs': amount_abs,
                    'balance_after': balance,
                    'line_number': i + 1,
                    'counterparty_iban': iban,
                    'counterparty': counterparty,
                    'counterparty_address': counterparty_addr,
                    'title': title,
                    'card_merchant': card_merchant,
                    'card_location': card_location,
                })
        
        i += 1

    # Now infer amount signs by comparing sequential balances
    # First, sort transactions by line order (as they appear in statement)
    # We'll use the balance to determine if amount was positive or negative
    transactions = []
    prev_balance = opening_balance

    for raw in raw_transactions:
        amount_abs = raw['amount_abs']
        balance = raw['balance_after']

        # Determine sign: if balance increased, amount is positive; else negative
        if prev_balance is not None and amount_abs != ZERO:
            expected_if_positive = prev_balance + amount_abs
            expected_if_negative = prev_balance - amount_abs
            
            # Check which matches the actual balance (with some tolerance for rounding)
            diff_pos = abs(expected_if_positive - balance)
            diff_neg = abs(expected_if_negative - balance)
            
            if diff_neg < diff_pos:
                amount = -amount_abs
            else:
                amount = amount_abs
        else:
            # Can't determine sign - keep as positive (will be corrected later)
            amount = amount_abs

        # Get counterparty - from parsed data or extract from card operation
        counterparty = raw.get('counterparty')
        counterparty_address = raw.get('counterparty_address') or raw.get('card_location')
        
        # For card operations without counterparty, try card_merchant first, then description extraction
        if not counterparty and raw['description'].startswith('Operacja kartą'):
            counterparty = raw.get('card_merchant')
            if not counterparty:
                counterparty, card_addr = _extract_card_merchant(raw['description'])
                if not counterparty_address and card_addr:
                    counterparty_address = card_addr
        
        # Normalize whitespace in counterparty name (but don't remove valid spaces!)
        if counterparty:
            counterparty = re.sub(r'\s+', ' ', counterparty).strip()

        transactions.append(RawTransaction(
            transaction_date=raw['transaction_date'],
            booking_date=raw['booking_date'],
            description=raw['description'],
            amount=amount,
            balance_after=balance,
            statement_id=statement_id,
            line_number=raw['line_number'],
            filename=filename,
            transaction_type=_extract_transaction_type(raw['description']),
            counterparty=counterparty,
            counterparty_iban=raw.get('counterparty_iban'),
            counterparty_address=counterparty_address,
            title=raw.get('title') or raw['description'],
            card_number=_extract_card_number(raw['description']),
        ))
        
        prev_balance = balance

    return transactions


def _extract_card_merchant(description: str) -> Tuple[Optional[str], Optional[str]]:
    """Extract merchant name and location from card operation description.

    Args:
        description: Transaction description like
            "Operacja kartą 5375 xxxx xxxx 3459 na kwotę 90,00 PLN w SAB-MAR, KATOWICE, POL"
            or "Operacja kartą ... w LIDL GEN. ZIET KA, Myslowice,"

    Returns:
        Tuple of (merchant_name, location) or (None, None) if not a card operation.
    """
    # Find the part after "w " - format is "MERCHANT, CITY, COUNTRY"
    match = re.search(r'\bw\s+(.+?)(?:\s*$)', description)
    if match:
        full_text = match.group(1).strip()
        # Split by comma - first is merchant, rest is location
        parts = [p.strip() for p in full_text.split(',') if p.strip()]
        if parts:
            merchant = parts[0]
            location = ', '.join(parts[1:]) if len(parts) > 1 else None
            return merchant, location
    
    return None, None


def _extract_card_number(description: str) -> Optional[str]:
    """Extract card number (last 4 digits) from card operation description.

    Args:
        description: Transaction description like
            "Operacja kartą 5375 xxxx xxxx 1335 na kwotę 90,00 PLN w ..."

    Returns:
        Last 4 digits of card number (e.g., "1335") or None if not a card operation.
    """
    if not description.startswith('Operacja kartą'):
        return None
    
    # Pattern: "Operacja kartą XXXX xxxx xxxx YYYY" where YYYY are last 4 digits
    # Card number format: 4 digits, space, xxxx, space, xxxx, space, 4 digits
    match = re.search(r'Operacja kartą\s+(\d{4})\s+xxxx\s+xxxx\s+(\d{4})', description)
    if match:
        # Return full masked card number for reference
        return f"{match.group(1)}xxxxxx{match.group(2)}"
    
    return None


def _extract_transaction_type(description: str) -> str:
    """Extract transaction type from description.

    Args:
        description: Transaction description text.

    Returns:
        Extracted transaction type.
    """
    # Common transaction types
    types = [
        'Przelew przychodzący zewnętrzny',
        'Przelew przychodzący wewnętrzny',
        'Przelew wychodzący zewnętrzny',
        'Przelew wychodzący wewnętrzny',
        'Operacja kartą',
        'Spłata kredytu',
        'Przeksięgowanie kredytu',
        'Wypłata BLIK',
        'BLIK',
    ]

    for t in types:
        if description.startswith(t):
            return t

    # Return first few words as type
    words = description.split()[:3]
    return ' '.join(words)


def parse_pdf_statement(pdf_path: str) -> StatementInfo:
    """Parse a VeloBank PDF statement.

    Uses pdftotext with -layout flag to preserve column positions.
    Detects format and delegates to appropriate parser.

    Args:
        pdf_path: Path to the PDF file.

    Returns:
        StatementInfo containing all parsed data.
    """
    text = extract_pdf_text(pdf_path)
    filename = pdf_path  # Full path to the PDF file
    
    # Detect format and use appropriate parser
    fmt = detect_format(text)
    
    if fmt == 'history':
        # History format uses DD.MM.YYYY dates - old format parser now supports both
        return parse_old_format(text, filename)
    elif fmt == 'new':
        return parse_new_format(text, filename)
    else:
        # Old format (2018-2024)
        return parse_old_format(text, filename)


# Parser states
STATE_LOOKING_FOR_DATE = 0
STATE_LOOKING_FOR_TXN_DATE = 1
STATE_COLLECTING_DESCRIPTION = 2
STATE_LOOKING_FOR_BALANCE = 3


def _is_date_line(line: str) -> bool:
    """Check if line contains only a date in YYYY.MM.DD or DD.MM.YYYY format."""
    return bool(re.match(r'^(\d{4}\.\d{2}\.\d{2}|\d{2}\.\d{2}\.\d{4})$', line))


def _is_amount_line(line: str) -> bool:
    """Check if line contains only an amount (like -1 234,56 or 56,78 or -20 000,00 PLN)."""
    return bool(re.match(r'^-?[\d\s]+,\d{2}(\s*PLN)?$', line))


def _parse_transactions_nolayout(
    lines: List[str],
    statement_id: str,
    filename: str,
) -> List[RawTransaction]:
    """Parse transactions from non-layout pdftotext output.

    Structure:
        [booking_date YYYY.MM.DD]
        [txn_date YYYY.MM.DD]
        [description lines...]
        [amount]
        [balance]

    Args:
        lines: List of text lines.
        statement_id: Statement identifier.
        filename: PDF filename.

    Returns:
        List of parsed transactions.
    """
    transactions = []
    state = STATE_LOOKING_FOR_DATE
    txn_number = 0

    booking_date = None
    txn_date = None
    description_lines = []
    amount = None

    for line_num, line in enumerate(lines):
        if not line:
            continue

        # Skip header/footer content
        if any(skip in line for skip in [
            'DATA', 'TRANSAKCJI', 'KSIĘGOWANIA', 'KWOTA', 'SALDO',
            'Obroty WN', 'Obroty MA', 'Saldo końcowe',
            'Data i godzina:', 'Strona', 'VeloBank S.A.',
            'Kod BIC', 'RACHUNEK PROWADZONY', 'NUMER RACHUNKU',
            'OPROCENTOWANIE', 'Wyciąg', 'Pakiet:', 'Waluta rachunku',
            'Numer ewidencyjny', 'Bankowym Funduszu Gwarancyjnym',
            'www.bfg.pl', 'art.7 ustawy', 'Dokument wygenerowany',
        ]):
            continue

        if state == STATE_LOOKING_FOR_DATE:
            if _is_date_line(line):
                # First date in PDF is transaction date
                txn_date = parse_polish_date(line)
                state = STATE_LOOKING_FOR_TXN_DATE

        elif state == STATE_LOOKING_FOR_TXN_DATE:
            if _is_date_line(line):
                # Second date in PDF is booking date
                booking_date = parse_polish_date(line)
                description_lines = []
                state = STATE_COLLECTING_DESCRIPTION
            elif not _is_amount_line(line):
                # Might be continuation of previous description, reset
                state = STATE_LOOKING_FOR_DATE

        elif state == STATE_COLLECTING_DESCRIPTION:
            if _is_amount_line(line):
                amount = parse_polish_amount(line)
                state = STATE_LOOKING_FOR_BALANCE
            elif not _is_date_line(line):
                description_lines.append(line)

        elif state == STATE_LOOKING_FOR_BALANCE:
            if _is_amount_line(line):
                balance = parse_polish_amount(line)
                
                # Build transaction - parse lines individually
                txn_number += 1
                full_description = ' '.join(description_lines)
                
                # Extract metadata from individual lines (preserving structure)
                txn_type = None
                counterparty = None
                counterparty_iban = None
                counterparty_address = None
                title = None
                card_number = None
                # Tax payment fields
                tax_nip = None
                tax_symbol = None
                tax_period = None
                tax_payer = None
                
                # Define keywords that start new sections
                keywords = ['Z rachunku:', 'Na rachunek:', 'Prowadzon', 'Nadawca:', 'Odbiorca:', 'Tytułem:', 'Tytuł:']
                
                # Build sections - each section is (keyword, content_lines)
                sections = []
                current_section = None
                current_lines = []
                
                for i, desc_line in enumerate(description_lines):
                    # First line is always transaction type
                    if i == 0:
                        txn_type = _extract_transaction_type(desc_line)
                        # For card operations, mark section and include first line
                        if 'Operacja kartą' in desc_line:
                            card_number = _extract_card_number(desc_line)
                            current_section = 'CARD'
                            current_lines = [desc_line]  # Include first line for merchant extraction
                        # Check if first line also contains Z rachunku: or Na rachunek:
                        elif 'Z rachunku:' in desc_line or 'z rachunku:' in desc_line.lower():
                            current_section = 'Z rachunku:'
                            current_lines = [desc_line]
                        elif 'Na rachunek:' in desc_line or 'na rachunek:' in desc_line.lower():
                            current_section = 'Na rachunek:'
                            current_lines = [desc_line]
                        # Tax payment - special format
                        elif 'Urzędu Skarbowego' in desc_line:
                            current_section = 'TAX'
                            current_lines = [desc_line]
                        continue
                    
                    # Check if line starts a new section
                    found_keyword = None
                    for kw in keywords:
                        if kw.lower() in desc_line.lower():
                            found_keyword = kw
                            break
                    
                    if found_keyword:
                        # Save previous section
                        if current_section:
                            sections.append((current_section, current_lines))
                        # Start new section
                        current_section = found_keyword
                        current_lines = [desc_line]
                    else:
                        # Continuation of current section
                        current_lines.append(desc_line)
                
                # Save last section
                if current_section and current_lines:
                    sections.append((current_section, current_lines))
                
                # Process sections
                for section_kw, section_lines in sections:
                    section_text = ' '.join(section_lines)
                    
                    # IBAN - check for Z rachunku: or Na rachunek: sections
                    if 'rachunk' in section_kw.lower():  # matches rachunku and rachunek
                        # First, look for standalone line that looks like an account number
                        for line in section_lines:
                            # Check for line that is mostly digits (account number)
                            cleaned = re.sub(r'\s+', '', line)
                            if re.match(r'^\d{20,26}$', cleaned):
                                counterparty_iban = cleaned[:26]
                                break
                        # Fallback to regex search in joined text
                        if not counterparty_iban:
                            iban_match = re.search(r'(\d[\d\s]{15,})', section_text)
                            if iban_match:
                                counterparty_iban = re.sub(r'\s+', '', iban_match.group(1))[:26]
                    
                    # Prowadzonego na rzecz - comma separated
                    elif 'prowadzon' in section_kw.lower():
                        pnr_match = re.search(r'Prowadzon(?:y|ego|e) na rzecz:\s*(.+)', section_text, re.IGNORECASE)
                        if pnr_match:
                            full_text = pnr_match.group(1).strip()
                            # First comma separates name from address
                            parts = full_text.split(',', 1)
                            counterparty = parts[0].strip()
                            if len(parts) > 1:
                                counterparty_address = parts[1].strip().lstrip(',').strip()
                    
                    # Nadawca/Odbiorca - first line is name, rest is address
                    elif section_kw.lower() in ['nadawca:', 'odbiorca:']:
                        on_match = re.search(r'(?:Nadawca|Odbiorca):\s*(.+)', section_lines[0], re.IGNORECASE)
                        if on_match:
                            counterparty = on_match.group(1).strip().rstrip(',')
                        # Remaining lines are address
                        if len(section_lines) > 1:
                            counterparty_address = ' '.join(line.strip() for line in section_lines[1:])
                    
                    # Tytułem or Tytuł
                    elif 'tytułem' in section_kw.lower() or 'tytuł:' in section_kw.lower():
                        title_match = re.search(r'(?:Tytułem|Tytuł):\s*(.+)', section_text, re.IGNORECASE)
                        if title_match:
                            title = title_match.group(1).strip()
                    
                    # Card operation - extract merchant and location from all lines
                    elif section_kw == 'CARD':
                        # Combine all lines (first has merchant, rest is location continuation)
                        combined_text = ' '.join(line.strip() for line in section_lines)
                        counterparty, counterparty_address = _extract_card_merchant(combined_text)
                    
                    # Tax payment - parse special fields
                    elif section_kw == 'TAX':
                        section_text = ' '.join(section_lines)
                        # Extract IBAN from "na rachunek : XX XXXX XXXX..."
                        iban_match = re.search(r'na rachunek\s*:\s*(\d[\d\s]{15,})', section_text, re.IGNORECASE)
                        if iban_match:
                            counterparty_iban = re.sub(r'\s+', '', iban_match.group(1))[:26]
                        # Extract NIP
                        nip_match = re.search(r'Identyfikator:\s*(\d+)', section_text)
                        if nip_match:
                            tax_nip = nip_match.group(1)
                        # Extract tax symbol (VAT-7K, PIT-28, etc.)
                        symbol_match = re.search(r'Symbol:\s*([A-Z0-9-]+)', section_text)
                        if symbol_match:
                            tax_symbol = symbol_match.group(1)
                        # Extract period
                        period_match = re.search(r'Okres rozliczenia:\s*(\S+)', section_text)
                        if period_match:
                            tax_period = period_match.group(1)
                        # Extract payer data
                        payer_match = re.search(r'Dane zobowiązanego:\s*(.+?)(?:$|Symbol|Identyf)', section_text)
                        if payer_match:
                            tax_payer = payer_match.group(1).strip()
                        # Set counterparty as "Urząd Skarbowy"
                        counterparty = 'Urząd Skarbowy'

                transactions.append(RawTransaction(
                    transaction_date=txn_date,
                    booking_date=booking_date,
                    description=full_description,
                    amount=amount,
                    balance_after=balance,
                    statement_id=statement_id,
                    line_number=txn_number,
                    filename=filename,
                    transaction_type=txn_type,
                    counterparty=counterparty,
                    counterparty_iban=counterparty_iban,
                    counterparty_address=counterparty_address,
                    title=title,
                    card_number=card_number,
                    tax_nip=tax_nip,
                    tax_symbol=tax_symbol,
                    tax_period=tax_period,
                    tax_payer=tax_payer,
                ))
                
                # Reset for next transaction
                state = STATE_LOOKING_FOR_DATE
            elif _is_date_line(line):
                # New transaction started, save current as incomplete
                state = STATE_LOOKING_FOR_TXN_DATE
                booking_date = parse_polish_date(line)

    return transactions


def _extract_counterparty_info(description: str) -> Tuple[Optional[str], Optional[str], Optional[str]]:
    """Extract counterparty name, IBAN, and address from description.

    Old format (2018-2023) patterns:
    - "Na rachunek: XXXXXX Prowadzony na rzecz: NAME, ADDRESS Tytułem: TITLE"
    - "Z rachunku: XXXXXX Prowadzonego na rzecz: NAME, ADDRESS Tytułem: TITLE"
    
    New format (2024+) patterns:
    - "Na rachunek: XXXXXX Odbiorca: NAME ul. ADDRESS Tytułem: TITLE"
    - "Z rachunku: XXXXXX Nadawca: NAME ul. ADDRESS Tytułem: TITLE"

    Args:
        description: Full transaction description.

    Returns:
        Tuple of (counterparty_name, counterparty_iban, counterparty_address).
    """
    counterparty = None
    counterparty_iban = None
    counterparty_address = None

    # Extract IBAN - look for account number (BBAN) after "Z rachunku:" or "Na rachunek:"
    # PDF contains BBAN (26 digits), we convert to IBAN by adding "PL" prefix
    iban_match = re.search(
        r'(?:Z rachunku|Na rachunek):\s*(\d[\d\s]{15,})',
        description,
        re.IGNORECASE
    )
    if iban_match:
        bban = re.sub(r'\s+', '', iban_match.group(1))
        # Take only first 26 digits (BBAN) and convert to IBAN
        if len(bban) > 26:
            bban = bban[:26]
        counterparty_iban = 'PL' + bban

    # Extract counterparty name and address
    # Strategy: find the counterparty section, then split name from address
    
    # Pattern to extract full counterparty section (name + address) before Tytułem:
    section_patterns = [
        r'Prowadzon(?:y|ego|e) na rzecz:\s*(.+?)(?:\s+Tytułem:|\s*$)',
        r'Nadawca:\s*(.+?)(?:\s+Tytułem:|\s*$)',
        r'Odbiorca:\s*(.+?)(?:\s+Tytułem:|\s*$)',
    ]
    
    for pattern in section_patterns:
        match = re.search(pattern, description, re.IGNORECASE)
        if match:
            full_section = match.group(1).strip()
            
            # Split by address indicators (ul., UL, AL, aleja, etc.)
            addr_match = re.search(r'^(.+?)\s+([Uu][Ll]\.?\s+.+|[Aa][Ll]\.?\s+.+|[Aa]leja\s+.+)$', full_section)
            if addr_match:
                counterparty = addr_match.group(1).strip().rstrip(',')
                counterparty_address = addr_match.group(2).strip()
            else:
                # Try comma split - name before first comma, rest is address
                parts = full_section.split(',', 1)
                counterparty = parts[0].strip()
                if len(parts) > 1:
                    counterparty_address = parts[1].strip()
            
            # Skip if counterparty looks like it captured Tytułem
            if counterparty and counterparty.startswith('Tytułem'):
                counterparty = None
                counterparty_address = None
                continue
            
            if counterparty:
                break

    # For card operations, extract merchant as counterparty if not found
    if not counterparty and 'Operacja kartą' in description:
        counterparty, counterparty_address = _extract_card_merchant(description)

    return counterparty, counterparty_iban, counterparty_address


def _extract_title(description: str) -> Optional[str]:
    """Extract transaction title from description.

    Pattern: "Tytułem: <title>"
    
    Both old and new formats use "Tytułem:" keyword.

    Args:
        description: Full transaction description.

    Returns:
        Transaction title or None.
    """
    match = re.search(r'Tytułem:\s*(.+)', description, re.IGNORECASE)
    if match:
        title = match.group(1).strip()
        return title if title else None
    return None


def get_velobank_account_map(accounts: Dict[str, Open]) -> Dict[str, str]:
    """Build a mapping from IBAN to account name.

    Args:
        accounts: Dictionary of Open directives.

    Returns:
        Dictionary mapping IBAN to account name.
    """
    result = {}
    for entry in accounts.values():
        if entry.meta:
            iban = entry.meta.get(COUNTERPARTY_BBAN_KEY)
            if iban:
                result[iban] = entry.account
    return result


def _generate_transaction_id(txn: RawTransaction) -> str:
    """Generate a unique ID for a transaction.

    For PDF statements: uses booking_date, amount, and balance_after.
    These are unique because balance_after is a running balance.
    
    For HTML credit card statements: also includes line_number because
    multiple transactions can have same date/amount/balance when the
    running balance is calculated (not from source).

    Args:
        txn: The raw transaction.

    Returns:
        A unique identifier string.
    """
    # Build hash from stable values
    parts = [
        str(txn.booking_date),
        str(txn.amount),
        str(txn.balance_after),
    ]
    
    # For HTML credit card statements, add line_number for uniqueness
    # (multiple transactions can have same date/amount/calculated balance)
    if txn.filename.endswith('.html'):
        parts.append(str(txn.line_number))
    
    data = ':'.join(parts)
    hash_value = hashlib.md5(data.encode()).hexdigest()[:12]
    # Format: velobank:{hash}
    return f"velobank:{hash_value}"


def get_info(txn: RawTransaction) -> dict:
    """Create info dict for import result.

    Args:
        txn: The raw transaction.

    Returns:
        Dictionary with file info.
    """
    return dict(
        type='application/pdf',
        filename=txn.filename,
        line=txn.line_number,
    )


class VelobankSource(Source):
    """VeloBank PDF statement source."""

    def __init__(
        self,
        directory: str,
        assets_account: Optional[str] = None,
        account_map: Optional[Dict[str, str]] = None,
        default_account: Optional[str] = None,
        **kwargs,
    ) -> None:
        """Initialize the VeloBank source.

        Args:
            directory: Directory containing PDF statements.
            assets_account: Legacy single account (deprecated, use account_map).
            account_map: Dictionary mapping IBAN to Beancount account.
            default_account: Fallback account when IBAN not in account_map.
            **kwargs: Additional arguments passed to Source.
        """
        super().__init__(**kwargs)
        self.directory = directory
        
        # Support both legacy single-account and new multi-account configuration
        self.account_map: Dict[str, str] = account_map or {}
        self.default_account = default_account or assets_account
        
        if not self.default_account and not self.account_map:
            raise ValueError(
                "VelobankSource requires either 'assets_account', 'account_map', "
                "or 'default_account' to be specified."
            )

        # Load all PDF statements
        self.statements: List[StatementInfo] = []
        self._load_statements()

    def _load_statements(self) -> None:
        """Load and parse all PDF and HTML statements from the directory."""
        pdf_files = []
        html_files = []

        # Recursively find all PDF and HTML files
        for root, dirs, files in os.walk(self.directory):
            for filename in files:
                filepath = os.path.join(root, filename)
                if filename.lower().endswith('.pdf'):
                    pdf_files.append(filepath)
                elif filename.lower().endswith('.html'):
                    html_files.append(filepath)

        # Sort files for consistent ordering
        pdf_files.sort()
        html_files.sort()

        # Load PDF statements
        for pdf_path in pdf_files:
            try:

                
                self.log_status(f'velobank: loading {pdf_path}')
                statement = parse_pdf_statement(pdf_path)
                self.statements.append(statement)
            except Exception as e:
                self.log_status(f'velobank: error loading {pdf_path}: {e}')
        
        # Load HTML credit card statements
        for html_path in html_files:
            try:
                # Check if it's a credit card statement
                with open(html_path, 'r', encoding='utf-8') as f:
                    content = f.read()
                if 'karty kredytowej' not in content.lower():
                    continue  # Skip non-credit-card HTML files
                    

                
                self.log_status(f'velobank: loading HTML {html_path}')
                statement = parse_credit_card_html(html_path)
                self.statements.append(statement)
            except Exception as e:
                self.log_status(f'velobank: error loading {html_path}: {e}')

    def get_example_key_value_pairs(
        self,
        transaction: Transaction,
        posting: Posting,
    ) -> dict:
        """Extract key-value pairs for account prediction.

        Args:
            transaction: The transaction.
            posting: The posting to extract features from.

        Returns:
            Dictionary of feature key-value pairs.
        """
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
        """Check if a posting is cleared.

        A posting is cleared if it has the velobank_statement metadata.

        Args:
            posting: The posting to check.

        Returns:
            True if the posting is cleared.
        """
        if posting.meta is None:
            return False
        return SOURCE_REF_KEY in posting.meta

    def _get_account_for_iban(self, iban: str) -> str:
        """Get the Beancount account for a given IBAN.

        Args:
            iban: The IBAN from the statement.

        Returns:
            The mapped Beancount account name.
        """
        if iban and iban in self.account_map:
            return self.account_map[iban]
        return self.default_account

    def _get_all_accounts(self) -> set:
        """Get all accounts used by this source."""
        accounts = set(self.account_map.values())
        if self.default_account:
            accounts.add(self.default_account)
        return accounts

    def prepare(
        self,
        journal: JournalEditor,
        results: SourceResults,
    ) -> None:
        """Prepare import results from loaded statements.

        Args:
            journal: The journal editor.
            results: SourceResults to populate.
        """
        # Get all accounts used by this source
        all_accounts = self._get_all_accounts()

        # Build set of already-matched transaction IDs
        matched_ids: Dict[str, List[Tuple[Transaction, Posting]]] = {}

        for entry in journal.all_entries:
            if isinstance(entry, Transaction):
                for posting in entry.postings:
                    if posting.meta is None:
                        continue
                    # Check if posting belongs to any of our accounts
                    if posting.account not in all_accounts:
                        continue
                    stmt_ref = posting.meta.get(SOURCE_REF_KEY)
                    if stmt_ref is not None:
                        matched_ids.setdefault(stmt_ref, []).append((entry, posting))

        # Process all transactions from all statements
        valid_ids = set()
        for statement in self.statements:
            # Determine target account for this statement
            target_account = self._get_account_for_iban(statement.account_iban)
            
            for txn in statement.transactions:
                txn_id = _generate_transaction_id(txn)
                valid_ids.add(txn_id)

                existing = matched_ids.get(txn_id)
                if existing is not None:
                    if len(existing) > 1:
                        results.add_invalid_reference(
                            InvalidSourceReference(len(existing) - 1, existing))
                else:
                    # Create new transaction with proper account and account_iban
                    beancount_txn = self._make_transaction(txn, target_account, statement.account_iban)
                    results.add_pending_entry(
                        ImportResult(
                            date=txn.booking_date,
                            entries=[beancount_txn],
                            info=get_info(txn),
                        ))

            # Add balance assertion for statement end
            if statement.transactions:
                last_txn = statement.transactions[-1]
                balance_date = statement.period_end + datetime.timedelta(days=1)
                results.add_pending_entry(
                    ImportResult(
                        date=balance_date,
                        entries=[
                            Balance(
                                date=balance_date,
                                meta=collections.OrderedDict([
                                    ('filename', '<velobank>'),
                                    ('lineno', 0),
                                    ('source', 'velobank'),
                                    ('document', os.path.basename(statement.filename)),
                                    ('period_end', str(statement.period_end)),
                                ]),
                                account=target_account,
                                amount=Amount(last_txn.balance_after, DEFAULT_CURRENCY),
                                tolerance=None,
                                diff_amount=None,
                            )
                        ],
                        info=dict(
                            type='application/pdf',
                            filename=statement.filename,
                            line=0,
                        ),
                    ))

        # Check for invalid references (matched to non-existent transactions)
        for stmt_ref, postings in matched_ids.items():
            if stmt_ref not in valid_ids:
                results.add_invalid_reference(
                    InvalidSourceReference(len(postings), postings))

        # Generate Document directives for source files
        # Note: Duplicate detection is handled centrally in reconcile.py
        for statement in self.statements:
            if not statement.transactions:
                continue
            
            doc_basename = os.path.basename(statement.filename)
            target_account = self._get_account_for_iban(statement.account_iban)
            
            # Find max transaction date from this statement
            max_date = max(txn.booking_date for txn in statement.transactions)
            
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
                        type='application/pdf',
                        filename=doc_basename,
                    ),
                ))

        # Register all accounts
        for account in all_accounts:
            results.add_account(account)

    def _make_transaction(self, txn: RawTransaction, target_account: str, account_iban: str = '') -> Transaction:
        """Create a Beancount Transaction from a raw transaction.

        Args:
            txn: The raw transaction.
            target_account: The Beancount account to use for this transaction.
            account_iban: The IBAN of the account this transaction belongs to.

        Returns:
            A Beancount Transaction.
        """
        txn_id = _generate_transaction_id(txn)

        # Build metadata
        meta = collections.OrderedDict([
            (SOURCE_REF_KEY, txn_id),
            (SOURCE_BANK_KEY, 'VeloBank'),
        ])
        
        # Only add transaction_type if it's a known type (translated to English)
        translated_type = _get_transaction_type_if_known(txn.transaction_type or txn.description)
        if translated_type:
            meta[TRANSACTION_TYPE_KEY] = translated_type

        # Split counterparty name from address if needed
        counterparty_name = txn.counterparty
        counterparty_address = txn.counterparty_address
        
        if txn.counterparty:
            # Check if counterparty contains address (e.g., "SZWAJCA DAWIDul. KS. J. NYGI...")
            name, addr = _split_name_and_address(txn.counterparty)
            if addr:
                counterparty_name = name
                # Prefer extracted address, but don't overwrite if already set
                if not counterparty_address:
                    counterparty_address = addr

        if counterparty_name:
            meta[COUNTERPARTY_KEY] = counterparty_name
        if counterparty_address:
            meta[COUNTERPARTY_ADDRESS_KEY] = counterparty_address
        if txn.counterparty_iban:
            # VeloBank uses BBAN (without PL prefix)
            meta[COUNTERPARTY_BBAN_KEY] = txn.counterparty_iban
        if txn.title:
            meta[TITLE_KEY] = txn.title
        if txn.card_number:
            meta[CARD_NUMBER_KEY] = txn.card_number
        if account_iban:
            meta[ACCOUNT_IBAN_KEY] = account_iban
        
        # Tax payment metadata
        if txn.tax_nip:
            meta[TAX_NIP_KEY] = txn.tax_nip
        if txn.tax_symbol:
            meta[TAX_SYMBOL_KEY] = txn.tax_symbol
        if txn.tax_period:
            meta[TAX_PERIOD_KEY] = txn.tax_period
        if txn.tax_payer:
            meta[TAX_PAYER_KEY] = txn.tax_payer
        
        # Always add booking_date (when bank recorded the transaction)
        meta[BOOKING_DATE_KEY] = txn.booking_date
        
        # Always add transaction_date (when transaction occurred)
        meta[TRANSACTION_DATE_KEY] = txn.transaction_date
        
        # Add link to source document (only filename, not full path)
        meta[SOURCE_DOC_KEY] = os.path.basename(txn.filename)

        amount = Amount(txn.amount, DEFAULT_CURRENCY)

        # Determine payee and narration (with English translation for type)
        # Shorten payee by removing company type suffixes
        payee = counterparty_name or 'VeloBank'
        if counterparty_name:
            # Split at company type indicators and use first part
            payee_match = re.split(
                r'\s+(?:spółka|spolka|sp\s+|sp\.|s\.a\.)',
                counterparty_name,
                maxsplit=1,
                flags=re.IGNORECASE
            )
            if payee_match:
                payee = payee_match[0].strip()
        
        # For narration: use title if available, otherwise use translated type
        if txn.title:
            narration = txn.title
        else:
            narration = _translate_transaction_type(txn.transaction_type or txn.description)

        return Transaction(
            meta=None,
            date=txn.booking_date,
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
                    units=Amount(-txn.amount, DEFAULT_CURRENCY),
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
        return 'velobank'


def load(spec: dict, log_status) -> VelobankSource:
    """Load the VeloBank source.

    Args:
        spec: Configuration dictionary with 'directory' and 'assets_account'.
        log_status: Logging function.

    Returns:
        Configured VelobankSource instance.
    """
    return VelobankSource(log_status=log_status, **spec)
