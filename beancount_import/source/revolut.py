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

from beancount.core.data import Balance, Document, Posting, Transaction, EMPTY_SET
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
COUNTERPARTY_BBAN_KEY = 'counterparty_bban'
COUNTERPARTY_ADDRESS_KEY = 'counterparty_address'
REFERENCE_KEY = 'title'  # Transfer reference/title
ACCOUNT_IBAN_KEY = 'account_iban'
ACCOUNT_IBAN_PL_KEY = 'account_iban_pl'
CARD_NUMBER_KEY = 'card_number'
SOURCE_CARD_KEY = 'source_card'  # Card used for top-up (From:)
ORIGINAL_AMOUNT_KEY = 'original_amount'
ORIGINAL_CURRENCY_KEY = 'original_currency'
EXCHANGE_RATE_KEY = 'exchange_rate'
SOURCE_DOC_KEY = 'document'
BALANCE_KEY = 'balance'

# Transaction type normalization (raw CSV type -> nice display name)
TRANSACTION_TYPE_MAP = {
    # Credit card CSV types (uppercase)
    'CARD_PAYMENT': 'Card payment',
    'CARD_REFUND': 'Card refund',
    'TRANSFER': 'Transfer',
    'CASHBACK': 'Cashback',
    'FEE': 'Fee',
    'REFUND': 'Refund',
    'TOPUP': 'Top-up',
    'ATM': 'ATM withdrawal',
    # Regular account CSV types (Title Case)
    'Card Payment': 'Card payment',
    'Card Refund': 'Card refund',
    'Transfer': 'Transfer',
    'Exchange': 'Exchange',
    'Top-Up': 'Top-up',
    'Reward': 'Reward',
    'Fee': 'Fee',
}


def normalize_transaction_type(raw_type: str) -> str:
    """Normalize transaction type to nice display name."""
    return TRANSACTION_TYPE_MAP.get(raw_type, raw_type)


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
    # Raw datetime strings (with time)
    started_date_raw: Optional[str] = None  # Full datetime string with time
    completed_date_raw: Optional[str] = None  # Full datetime string with time
    # PDF enrichment fields
    iban: Optional[str] = None  # Account IBAN (LT)
    iban_pl: Optional[str] = None  # Account IBAN (PL)
    pdf_filename: Optional[str] = None  # Source PDF filename for document_2
    pdf_description: Optional[str] = None  # Description from PDF (e.g., "Open banking top-up")
    reference: Optional[str] = None  # Transfer reference/title
    counterparty_name: Optional[str] = None  # Counterparty name
    counterparty_iban: Optional[str] = None  # Counterparty IBAN
    counterparty_bban: Optional[str] = None  # Counterparty BBAN (Polish 26-digit)
    counterparty_address: Optional[str] = None  # Counterparty address
    card_number: Optional[str] = None  # Card used for payment
    source_card: Optional[str] = None  # Card for top-up
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
    iban: Optional[str] = None  # Account IBAN (LT)
    iban_pl: Optional[str] = None  # Account IBAN (PL)
    reference: Optional[str] = None  # Transfer reference/title
    counterparty_name: Optional[str] = None  # Counterparty name
    counterparty_iban: Optional[str] = None  # Counterparty IBAN
    counterparty_bban: Optional[str] = None  # Counterparty BBAN (Polish 26-digit)
    counterparty_address: Optional[str] = None  # Counterparty address (To:)
    card_number: Optional[str] = None  # Card used for payment (Card:)
    source_card: Optional[str] = None  # Card used for top-up (From: *xxxx)
    original_amount: Optional[str] = None
    original_currency: Optional[str] = None
    exchange_rate: Optional[str] = None


@dataclass
class PdfCurrencySection:
    """A currency section within a PDF (e.g., PLN Statement, EUR Statement)."""
    currency: str
    filename: Optional[str] = None  # Source PDF filename
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
    
    # Collect all IBANs globally first
    global_ibans: List[str] = []
    iban_pattern = re.compile(r'IBAN\s+([A-Z]{2}[A-Z0-9]{10,32})')
    for line in lines:
        iban_match = iban_pattern.search(line)
        if iban_match:
            iban = iban_match.group(1)
            if iban not in global_ibans:
                global_ibans.append(iban)
    
    # Patterns - English format
    currency_header_pattern = re.compile(r'^\s*([A-Z]{3})\s+Statement\s*$')
    date_pattern = re.compile(r'^([A-Z][a-z]{2}\s+\d{1,2},\s+\d{4})')  # "Jan 3, 2025"
    card_pattern = re.compile(r'Card:\s*(\d{6}\*+\d{4})')
    to_pattern = re.compile(r'To:\s*(.+)')
    from_pattern = re.compile(r'From:\s*(.+)')
    reference_pattern = re.compile(r'Reference:\s*(.+)')
    
    # Patterns - Polish format (credit card statements)
    # Polish months: sty, lut, mar, kwi, maj, cze, lip, sie, wrz, paź, lis, gru
    polish_date_pattern = re.compile(r'^(\d{1,2})\s+(sty|lut|mar|kwi|maj|cze|lip|sie|wrz|paź|lis|gru)\s+(\d{4})')
    polish_card_pattern = re.compile(r'Karta:\s*(\d{6}\*+\d{4})')
    polish_to_pattern = re.compile(r'Do:\s*(.+)')
    polish_from_pattern = re.compile(r'Od:\s*(.+)')
    
    # Pattern for original currency amount on its own line: €4.43, $151.05
    orig_currency_pattern = re.compile(r'^\s*([€$£])([0-9,.]+)\s*$')
    # Pattern to extract IBAN from "NAME, IBAN" format
    iban_in_address_pattern = re.compile(r'^(.+?),\s*([A-Z]{2}[A-Z0-9]{10,32})$')
    # Pattern to extract BBAN from "NAME, 26-digit-number" format (Polish account number)
    bban_in_address_pattern = re.compile(r'^(.+?),\s*(\d{26})$')
    
    # Polish month name to number mapping
    polish_months = {
        'sty': 1, 'lut': 2, 'mar': 3, 'kwi': 4, 'maj': 5, 'cze': 6,
        'lip': 7, 'sie': 8, 'wrz': 9, 'paź': 10, 'lis': 11, 'gru': 12
    }
    
    i = 0
    while i < len(lines):
        line = lines[i]
        
        # Check for currency section header (e.g., "PLN Statement")
        header_match = currency_header_pattern.search(line)
        if header_match:
            currency = header_match.group(1)
            if currency not in sections:
                current_section = PdfCurrencySection(
                    currency=currency,
                    filename=os.path.basename(pdf_path),
                )
                current_section.ibans = global_ibans.copy()
                sections[currency] = current_section
            else:
                current_section = sections[currency]
            i += 1
            continue
        
        # Look for transaction lines (start with date - English or Polish format)
        date_match = date_pattern.match(line.strip())
        polish_date_match = polish_date_pattern.match(line.strip())
        
        txn_date = None
        date_str = None
        
        if date_match:
            # English format: "Jan 3, 2025"
            try:
                date_str = date_match.group(1)
                txn_date = datetime.datetime.strptime(date_str, "%b %d, %Y").date()
            except ValueError:
                pass
        elif polish_date_match:
            # Polish format: "3 sty 2025"
            try:
                day = int(polish_date_match.group(1))
                month_name = polish_date_match.group(2)
                year = int(polish_date_match.group(3))
                month = polish_months.get(month_name, 1)
                txn_date = datetime.date(year, month, day)
                date_str = polish_date_match.group(0)
            except (ValueError, KeyError):
                pass
        
        if txn_date:
            
            # Extract description (between date and first amount)
            rest_of_line = line.strip()[len(date_str):].strip()
            
            # Find amount with currency to detect which section this belongs to
            # Pattern: "1,000.00 PLN" or "€23.36" or "$151.05"
            amount_pattern_re = re.compile(r'([\d,]+\.\d{2})\s+([A-Z]{3})')
            symbol_amount_pattern = re.compile(r'[€$£][\d,]+\.\d{2}')
            amount_match = amount_pattern_re.search(rest_of_line)
            symbol_match = symbol_amount_pattern.search(rest_of_line)
            
            # Detect currency from the line
            detected_currency = None
            if amount_match:
                detected_currency = amount_match.group(2)
                description = rest_of_line[:amount_match.start()].strip()
            elif symbol_match:
                # Handle €/$/£ format - strip from first currency symbol onward
                description = rest_of_line[:symbol_match.start()].strip()
                # Detect currency from symbol
                symbol = rest_of_line[symbol_match.start()]
                if symbol == '€':
                    detected_currency = 'EUR'
                elif symbol == '$':
                    detected_currency = 'USD'
                elif symbol == '£':
                    detected_currency = 'GBP'
            else:
                # Take words until we hit an amount
                words = rest_of_line.split()
                desc_words = []
                for word in words:
                    if re.match(r'^[\d,]+\.\d{2}$', word) or re.match(r'^[€$£][\d,]+', word):
                        break
                    desc_words.append(word)
                description = ' '.join(desc_words)
            
            # Create transaction info
            txn_info = PdfTransactionInfo(
                date=txn_date,
                description=description,
                amount=None,
            )
            
            # Set IBANs - prefer LT as main, PL as secondary
            for iban in global_ibans:
                if iban.startswith('LT'):
                    txn_info.iban = iban
                elif iban.startswith('PL'):
                    txn_info.iban_pl = iban
            if not txn_info.iban and global_ibans:
                txn_info.iban = global_ibans[0]
            
            # Look at following lines for details (up to 6 lines or next date)
            j = i + 1
            while j < min(i + 8, len(lines)):
                detail_line = lines[j]
                
                # Stop if we hit another date line (English or Polish format)
                if date_pattern.match(detail_line.strip()) or polish_date_pattern.match(detail_line.strip()):
                    break
                
                # Check for Reference: (transfer title)
                ref_match = reference_pattern.search(detail_line)
                if ref_match:
                    txn_info.reference = ref_match.group(1).strip()
                
                # Check for card number (Card: or Karta:)
                card_match = card_pattern.search(detail_line) or polish_card_pattern.search(detail_line)
                if card_match:
                    txn_info.card_number = card_match.group(1)
                
                # Check for To:/Do: address (recipient for payments)
                # Format: "To: Name, Address" or "Do: Name, City"
                to_match = to_pattern.search(detail_line) or polish_to_pattern.search(detail_line)
                if to_match:
                    to_value = to_match.group(1).strip()
                    # Try to extract IBAN from "NAME, IBAN" format
                    iban_match = iban_in_address_pattern.match(to_value)
                    if iban_match:
                        txn_info.counterparty_name = iban_match.group(1).strip()
                        txn_info.counterparty_iban = iban_match.group(2)
                    else:
                        # Try to extract BBAN from "NAME, 26-digit" format (Polish)
                        bban_match = bban_in_address_pattern.match(to_value)
                        if bban_match:
                            txn_info.counterparty_name = bban_match.group(1).strip()
                            txn_info.counterparty_bban = bban_match.group(2)
                        else:
                            # Just set as address (e.g., "Allegro, Poznan")
                            txn_info.counterparty_address = to_value
                
                # Check for From:/Od: (source card or sender info)
                # Format: "From: *6671" or "Od: NAME, IBAN"
                from_match = from_pattern.search(detail_line) or polish_from_pattern.search(detail_line)
                if from_match:
                    from_value = from_match.group(1).strip()
                    # If it's a card reference (like *6671), store as source_card
                    if from_value.startswith('*'):
                        txn_info.source_card = from_value
                    else:
                        # Try to extract IBAN from "NAME, IBAN" format
                        iban_match = iban_in_address_pattern.match(from_value)
                        if iban_match:
                            txn_info.counterparty_name = iban_match.group(1).strip()
                            txn_info.counterparty_iban = iban_match.group(2)
                        else:
                            # Try to extract BBAN from "NAME, 26-digit" format
                            bban_match = bban_in_address_pattern.match(from_value)
                            if bban_match:
                                txn_info.counterparty_name = bban_match.group(1).strip()
                                txn_info.counterparty_bban = bban_match.group(2)
                            else:
                                # Just address without IBAN
                                if not txn_info.counterparty_address:
                                    txn_info.counterparty_address = from_value
                
                # Check for Revolut Rate (English) or Kurs Revolut (Polish)
                rate_match = re.search(r'Revolut Rate\s+(.+?)\s*\(ECB', detail_line)
                if rate_match:
                    txn_info.exchange_rate = rate_match.group(1).strip()
                
                # Polish exchange rate: "Kurs Revolut: 1.00 PLN = 5.84 CZK (kurs ECB*: ...)"
                polish_rate_match = re.search(r'Kurs Revolut:\s*(.+?)\s*\(kurs ECB', detail_line, re.IGNORECASE)
                if polish_rate_match:
                    txn_info.exchange_rate = polish_rate_match.group(1).strip()
                    # Polish format has original amount at end of line: "566.26 CZK"
                    orig_at_end = re.search(r'([\d,.]+)\s+([A-Z]{3})\s*$', detail_line)
                    if orig_at_end:
                        amount = orig_at_end.group(1).replace(',', '')
                        currency = orig_at_end.group(2)
                        if currency != 'PLN':  # Only if it's not the local currency
                            txn_info.original_amount = amount
                            txn_info.original_currency = currency
                
                # Check for original currency amount (€4.43, $100.00) on its own line
                orig_match = orig_currency_pattern.match(detail_line)
                if orig_match:
                    symbol = orig_match.group(1)
                    amount = orig_match.group(2)
                    txn_info.original_amount = amount
                    if symbol == '€':
                        txn_info.original_currency = 'EUR'
                    elif symbol == '$':
                        txn_info.original_currency = 'USD'
                    elif symbol == '£':
                        txn_info.original_currency = 'GBP'
                
                # Also check for original amount on Revolut Rate line
                # Format 1: "Revolut Rate $1.00 = €0.97 (ECB rate...)   €17.00"
                # Format 2: "Revolut Rate $1.00 = 4.13 PLN (ECB rate...)   34.99 PLN"
                if not txn_info.original_amount and 'Revolut Rate' in detail_line:
                    # Try symbol format first (€17.00)
                    inline_orig = re.search(r'([€$£])([0-9,.]+)\s*$', detail_line)
                    if inline_orig:
                        symbol = inline_orig.group(1)
                        txn_info.original_amount = inline_orig.group(2)
                        if symbol == '€':
                            txn_info.original_currency = 'EUR'
                        elif symbol == '$':
                            txn_info.original_currency = 'USD'
                        elif symbol == '£':
                            txn_info.original_currency = 'GBP'
                    else:
                        # Try code format (34.99 PLN)
                        code_orig = re.search(r'([0-9,.]+)\s+([A-Z]{3})\s*$', detail_line)
                        if code_orig:
                            txn_info.original_amount = code_orig.group(1).replace(',', '')
                            txn_info.original_currency = code_orig.group(2)
                
                j += 1
            
            # Use detected_currency to select/create section
            if detected_currency:
                if detected_currency not in sections:
                    sections[detected_currency] = PdfCurrencySection(
                        currency=detected_currency,
                        filename=os.path.basename(pdf_path),
                        ibans=global_ibans.copy()
                    )
                target_section = sections[detected_currency]
                target_section.transactions.append(txn_info)
            elif current_section:
                # Fallback to current section if no currency detected
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
            
            started_date_raw = row['Started Date'].strip()
            try:
                started_date = parse_revolut_date(started_date_raw)
            except ValueError:
                continue
            
            completed_date = None
            completed_date_raw = None
            if row.get('Completed Date'):
                completed_date_raw = row['Completed Date'].strip()
                try:
                    completed_date = parse_revolut_date(completed_date_raw)
                except ValueError:
                    pass
            
            txn = RevolutTransaction(
                transaction_type=row.get('Type', '').strip(),
                started_date=started_date,
                completed_date=completed_date,
                started_date_raw=started_date_raw,
                completed_date_raw=completed_date_raw,
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
            
            started_date_raw = row['Started Date'].strip()
            try:
                started_date = parse_revolut_date(started_date_raw)
            except ValueError:
                continue
            
            completed_date = None
            completed_date_raw = None
            if row.get('Completed Date'):
                completed_date_raw = row['Completed Date'].strip()
                try:
                    completed_date = parse_revolut_date(completed_date_raw)
                except ValueError:
                    pass
            
            currency = row.get('Currency', 'PLN').strip()
            
            txn = RevolutTransaction(
                transaction_type=row.get('Type', '').strip(),
                started_date=started_date,
                completed_date=completed_date,
                started_date_raw=started_date_raw,
                completed_date_raw=completed_date_raw,
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
        
        # Get IBANs from PDF section (apply to all transactions in this currency)
        section_iban_lt = None
        section_iban_pl = None
        for iban in pdf_section.ibans:
            if iban.startswith('LT'):
                section_iban_lt = iban
            elif iban.startswith('PL'):
                section_iban_pl = iban
        
        # Build lookup of PDF transactions
        pdf_by_date: Dict[datetime.date, List[PdfTransactionInfo]] = {}
        for pdf_txn in pdf_section.transactions:
            pdf_by_date.setdefault(pdf_txn.date, []).append(pdf_txn)
        
        for csv_txn in csv_stmt.transactions:
            # Always assign IBANs from section
            if section_iban_lt:
                csv_txn.iban = section_iban_lt
            if section_iban_pl:
                csv_txn.iban_pl = section_iban_pl
            
            # Use completed_date for matching (that's what PDF shows)
            match_date = csv_txn.completed_date or csv_txn.started_date
            
            pdf_candidates = pdf_by_date.get(match_date, [])
            
            # Find best match by description and amount
            best_match = None
            best_score = 0
            best_match_idx = -1
            
            for idx, pdf_txn in enumerate(pdf_candidates):
                # Skip already matched transactions
                if getattr(pdf_txn, '_matched', False):
                    continue
                
                score = 0
                
                # Check if description starts with same words
                csv_desc_first = csv_txn.description.split()[0].lower() if csv_txn.description else ''
                pdf_desc_words = pdf_txn.description.split() if pdf_txn.description else []
                pdf_desc_first = pdf_desc_words[0].lower() if pdf_desc_words else ''
                
                if csv_desc_first and pdf_desc_first and csv_desc_first == pdf_desc_first:
                    score += 1
                    
                    # Check for second word match (e.g., "Payment from" both match)
                    if len(csv_txn.description.split()) > 1 and len(pdf_desc_words) > 1:
                        csv_second = csv_txn.description.split()[1].lower()
                        pdf_second = pdf_desc_words[1].lower()
                        if csv_second == pdf_second:
                            score += 1
                
                # Try to match by counterparty name in description
                # E.g., CSV "Payment from JOANNA MAZUR" should match PDF with counterparty_name "JOANNA MAZUR"
                if pdf_txn.counterparty_name and csv_txn.description:
                    if pdf_txn.counterparty_name.upper() in csv_txn.description.upper():
                        score += 5  # Strong match
                
                if score > best_score:
                    best_score = score
                    best_match = pdf_txn
                    best_match_idx = idx
            
            if best_match and best_score > 0:
                # Mark as matched to prevent duplicate matching
                best_match._matched = True
                
                # Enrich CSV transaction with additional PDF data
                # Copy PDF description (e.g., "Open banking top-up")
                csv_txn.pdf_description = best_match.description
                # Track source PDF for document_2
                csv_txn.pdf_filename = pdf_section.filename
                if best_match.reference:
                    csv_txn.reference = best_match.reference
                if best_match.counterparty_name:
                    csv_txn.counterparty_name = best_match.counterparty_name
                if best_match.counterparty_iban:
                    csv_txn.counterparty_iban = best_match.counterparty_iban
                if best_match.counterparty_bban:
                    csv_txn.counterparty_bban = best_match.counterparty_bban
                if best_match.counterparty_address:
                    csv_txn.counterparty_address = best_match.counterparty_address
                if best_match.card_number:
                    csv_txn.card_number = best_match.card_number
                if best_match.source_card:
                    csv_txn.source_card = best_match.source_card
                if best_match.original_amount:
                    csv_txn.original_amount = best_match.original_amount
                if best_match.original_currency:
                    csv_txn.original_currency = best_match.original_currency
                if best_match.exchange_rate:
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
                    filepath = ensure_file_has_suffix(filepath)
                    pdf_files.append((filepath, account_type))
        
        # Parse all PDFs first to build supplementary data - grouped by account_type
        pdf_sections_by_account: Dict[str, Dict[str, PdfCurrencySection]] = {}
        for pdf_path, account_type in pdf_files:
            try:
                sections = parse_pdf(pdf_path)
                if account_type not in pdf_sections_by_account:
                    pdf_sections_by_account[account_type] = {}
                
                for currency, section in sections.items():
                    if currency not in pdf_sections_by_account[account_type]:
                        pdf_sections_by_account[account_type][currency] = section
                    else:
                        # Merge transactions from same account_type
                        pdf_sections_by_account[account_type][currency].transactions.extend(section.transactions)
                        for iban in section.ibans:
                            if iban not in pdf_sections_by_account[account_type][currency].ibans:
                                pdf_sections_by_account[account_type][currency].ibans.append(iban)
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
        
        # Enrich CSV transactions with PDF data - matching by account_type
        for stmt in self.statements:
            pdf_sections = pdf_sections_by_account.get(stmt.account_type, {})
            if pdf_sections:
                match_csv_with_pdf([stmt], pdf_sections)
        
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

    @property
    def name(self) -> str:
        """Return the source name."""
        return 'revolut'

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
        # Normalize transaction type for display
        normalized_type = normalize_transaction_type(txn.transaction_type)
        
        # Determine if this is an internal Revolut operation
        desc = txn.description or ''
        desc_lower = desc.lower()
        has_external_account = txn.counterparty_iban or txn.counterparty_bban
        is_internal = (
            desc.startswith('To ') and not has_external_account or  # Internal transfers
            desc.startswith('Credit card') or
            desc.startswith('Apple Pay') or
            'portfolio' in desc_lower or
            desc.startswith('Exchanged') or
            desc.startswith('From ') and not has_external_account or  # Internal from
            'plan fee' in desc_lower or  # Ultra plan fee, Premium plan fee, etc.
            'plan termination' in desc_lower or  # Plan termination refund
            'refund' in desc_lower and not has_external_account or  # Other internal refunds
            'fee' in desc_lower and not has_external_account  # Other internal fees
        )
        
        # Determine payee and narration based on transaction type
        if is_internal:
            # Internal Revolut operations: payee = Revolut, narration = description
            payee = 'Revolut'
            narration = desc
            counterparty_for_meta = None  # No counterparty for internal ops
        elif txn.counterparty_name:
            # External transfer with PDF data
            payee = txn.counterparty_name
            # Use PDF description as narration (e.g., "Open banking top-up")
            # Fall back to reference, then transaction type
            narration = txn.pdf_description if txn.pdf_description else (txn.reference if txn.reference else normalized_type)
            counterparty_for_meta = txn.counterparty_name
        else:
            # External transaction without full PDF data
            # Try to extract clean payee from description patterns
            # "Transfer to PERSON NAME" -> "PERSON NAME"
            # "Transfer from PERSON NAME" -> "PERSON NAME" 
            # "Payment to MERCHANT" -> "MERCHANT"
            extracted_payee = None
            
            # Check for Transfer to/from pattern
            transfer_match = re.match(r'^Transfer\s+(?:to|from)\s+(.+)$', desc, re.IGNORECASE)
            if transfer_match:
                extracted_payee = transfer_match.group(1).strip()
            
            # Check for Payment to pattern
            if not extracted_payee:
                payment_match = re.match(r'^Payment\s+(?:to|from)\s+(.+)$', desc, re.IGNORECASE)
                if payment_match:
                    extracted_payee = payment_match.group(1).strip()
            
            # Use counterparty_address as payee if it looks like a name (not an address)
            if not extracted_payee and txn.counterparty_address:
                # If counterparty_address doesn't contain comma (indicating it's just a name)
                if ',' not in txn.counterparty_address:
                    extracted_payee = txn.counterparty_address
            
            payee = extracted_payee if extracted_payee else desc
            # For transfers: prefer reference (transfer title like "dzięki za TGE") over pdf_description
            # because pdf_description often just repeats "Transfer to NAME"
            if extracted_payee and txn.reference:
                narration = txn.reference
            else:
                narration = txn.pdf_description if txn.pdf_description else (txn.reference if txn.reference else normalized_type)
            counterparty_for_meta = payee
        
        # If payee == narration, use transaction_type as narration to avoid redundancy
        # e.g., "Google Play" "Google Play" -> "Google Play" "Card payment"
        if payee and narration and payee.lower() == narration.lower():
            narration = normalized_type
        
        # Build metadata
        account_id = f"{statement.account_type}_{txn.currency}"
        txn_id = _generate_transaction_id(statement.account_type, txn.currency, txn)
        
        meta = collections.OrderedDict()
        meta[SOURCE_REF_KEY] = txn_id
        meta[SOURCE_BANK_KEY] = 'Revolut'
        
        if txn.transaction_type:
            meta[TRANSACTION_TYPE_KEY] = normalized_type
        
        # Only add counterparty for non-internal transactions
        if counterparty_for_meta:
            meta[COUNTERPARTY_KEY] = counterparty_for_meta
        
        if txn.counterparty_iban:
            meta[COUNTERPARTY_IBAN_KEY] = txn.counterparty_iban
        
        if txn.counterparty_bban:
            meta[COUNTERPARTY_BBAN_KEY] = txn.counterparty_bban
        
        if txn.counterparty_address:
            meta[COUNTERPARTY_ADDRESS_KEY] = txn.counterparty_address
        
        if txn.reference:
            meta[REFERENCE_KEY] = txn.reference
        
        if txn.card_number:
            meta[CARD_NUMBER_KEY] = txn.card_number
        
        if txn.source_card:
            meta[SOURCE_CARD_KEY] = txn.source_card
        
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
        
        # Balance after transaction
        meta[BALANCE_KEY] = str(txn.balance_after)
        
        # Transaction dates from CSV (with full time)
        if txn.started_date_raw:
            meta['started_date'] = txn.started_date_raw
        if txn.completed_date_raw:
            meta['completed_date'] = txn.completed_date_raw
        
        # Link to source document(s)
        meta[SOURCE_DOC_KEY] = os.path.basename(statement.filename)
        
        # Add PDF as document_2 if transaction has PDF enrichment
        if txn.pdf_filename:
            meta['document_2'] = txn.pdf_filename
        
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
        # For FX transactions, use original currency with per-unit price
        if txn.original_amount and txn.original_currency and txn.original_currency != txn.currency:
            try:
                orig_amount = Decimal(txn.original_amount.replace(',', ''))
                # Sign should be opposite of main posting
                if txn.amount < 0:
                    orig_amount = orig_amount  # Positive (expense)
                else:
                    orig_amount = -orig_amount  # Negative (income)
                
                fixme_units = Amount(orig_amount, txn.original_currency)
                # Calculate per-unit price: PLN_amount / original_amount
                # E.g., 627.36 PLN / 151.05 USD = 4.1533 PLN per USD
                per_unit_price = abs(txn.amount) / abs(orig_amount)
                unit_price = Amount(per_unit_price.quantize(Decimal('0.0001')), txn.currency)
                fixme_posting = Posting(
                    account=FIXME_ACCOUNT,
                    units=fixme_units,
                    cost=None,
                    price=unit_price,
                    flag=None,
                    meta=None,
                )
            except (ValueError, InvalidOperation, ZeroDivisionError):
                # Fallback to simple posting
                fixme_units = Amount(-txn.amount, txn.currency)
                fixme_posting = Posting(
                    account=FIXME_ACCOUNT,
                    units=fixme_units,
                    cost=None,
                    price=None,
                    flag=None,
                    meta=None,
                )
        else:
            # Non-FX: simple opposite posting in same currency
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
        
        # Generate Document directives for source CSV files
        # (PDF files are used for enrichment only, CSV is the primary source)
        seen_files: Set[str] = set()
        for statement, txn in self.transactions:
            if statement.filename in seen_files:
                continue
            seen_files.add(statement.filename)
            
            account_id = f"{statement.account_type}_{txn.currency}"
            target_account = self._get_account_for_id(account_id)
            if target_account is None:
                continue
            
            # Find max date from this file
            max_date = max(
                (t.completed_date or t.started_date for s, t in self.transactions if s.filename == statement.filename),
                default=datetime.date.today()
            )
            
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


def load(spec: dict, log_status):
    """Load the Revolut source."""
    return RevolutSource(log_status=log_status, **spec)
