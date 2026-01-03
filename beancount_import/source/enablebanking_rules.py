"""Bank-specific parsing rules for EnableBanking source.

This module contains configurable rules for extracting payee, narration, and 
transaction_type from transaction data for different banks. Each bank can have 
multiple rules that are evaluated in order - the first matching rule is used.

Pattern for mBank transactions:
- remittance_information[0] = title (used as payee or narration depending on context)
- remittance_information[1] = transaction type (BLIK, PRZELEW, etc.)

When counterparty is available: counterparty -> payee, title -> narration
When counterparty is NOT available: title -> payee, type -> narration
"""

from dataclasses import dataclass
from typing import Callable, Dict, List, Optional, TYPE_CHECKING

if TYPE_CHECKING:
    from .enablebanking import EnableBankingTransaction


@dataclass
class ParsedTransaction:
    """Result of parsing transaction data.
    
    Attributes:
        payee: The payee name (merchant, counterparty, etc.)
        narration: Transaction description/title
        transaction_type: Optional override for transaction type (from second remittance line)
    """
    payee: str
    narration: str
    transaction_type: Optional[str] = None


@dataclass
class BankRule:
    """A single parsing rule for a bank.
    
    Attributes:
        name: Human-readable name for this rule (for debugging/logging)
        condition: Function that returns True if this rule should be applied
        extract: Function that extracts payee/narration/type from the transaction
    """
    name: str
    condition: Callable[['EnableBankingTransaction'], bool]
    extract: Callable[['EnableBankingTransaction'], ParsedTransaction]


# =============================================================================
# HELPER FUNCTIONS
# =============================================================================

def _has_two_remittance_lines(txn: 'EnableBankingTransaction') -> bool:
    """Check if transaction has exactly 2 remittance information lines."""
    return len(txn.remittance_information) == 2


def _get_counterparty(txn: 'EnableBankingTransaction') -> Optional[str]:
    """Get counterparty name based on credit/debit indicator.
    
    Also splits off address if embedded in name (comma or multiple spaces).
    """
    import re
    name = txn.debtor_name if txn.credit_debit_indicator == 'CRDT' else txn.creditor_name
    if not name:
        return None
    # Split off address (comma separator or 3+ spaces)
    if ',' in name:
        name = name.split(',', 1)[0].strip()
    else:
        parts = re.split(r'\s{3,}', name, maxsplit=1)
        if len(parts) >= 1:
            name = parts[0].strip()
    return name or None


def _get_title_and_type(txn: 'EnableBankingTransaction') -> tuple:
    """Extract title and type from remittance_information.
    
    Returns:
        (title, transaction_type) tuple
    """
    if len(txn.remittance_information) >= 2:
        return txn.remittance_information[0], txn.remittance_information[1]
    elif len(txn.remittance_information) == 1:
        return txn.remittance_information[0], None
    return None, None


# =============================================================================
# GENERIC RULES (apply to all banks)
# =============================================================================

GENERIC_RULES: List[BankRule] = [
    # Generic rule: 2 remittance lines with counterparty
    # counterparty -> payee, first line -> narration, second line -> type
    BankRule(
        name='generic_two_lines_with_counterparty',
        condition=lambda txn: (
            _has_two_remittance_lines(txn)
            and _get_counterparty(txn) is not None
        ),
        extract=lambda txn: ParsedTransaction(
            payee=_get_counterparty(txn),
            narration=txn.remittance_information[0],
            transaction_type=txn.remittance_information[1]
        )
    ),
    
    # Generic rule: 2 remittance lines WITHOUT counterparty
    # first line -> payee, second line -> type (used as narration)
    BankRule(
        name='generic_two_lines_no_counterparty',
        condition=lambda txn: (
            _has_two_remittance_lines(txn)
            and _get_counterparty(txn) is None
        ),
        extract=lambda txn: ParsedTransaction(
            payee=txn.remittance_information[0],
            narration=txn.remittance_information[1],  # type as narration when no title
            transaction_type=txn.remittance_information[1]
        )
    ),
    
    # Generic rule: 1 remittance line (fee, etc.)
    # For single-line transactions (like mBank fees), the line is both narration AND type
    BankRule(
        name='generic_single_line',
        condition=lambda txn: len(txn.remittance_information) == 1,
        extract=lambda txn: ParsedTransaction(
            payee=_get_counterparty(txn) or txn.bank,
            narration=txn.remittance_information[0],
            transaction_type=txn.remittance_information[0]  # Single line is the type
        )
    ),
    
    # Generic rule: counterparty exists but no remittance (Pekao card payments)
    BankRule(
        name='generic_counterparty_only',
        condition=lambda txn: (
            len(txn.remittance_information) == 0
            and _get_counterparty(txn) is not None
        ),
        extract=lambda txn: ParsedTransaction(
            payee=_get_counterparty(txn),
            narration=txn.bank_transaction_code or 'Transaction',
            transaction_type=txn.bank_transaction_code
        )
    ),
]


# =============================================================================
# BANK-SPECIFIC RULES
# =============================================================================
# These override generic rules for specific banks.
# Rules are evaluated in order - first match wins.

BANK_RULES: Dict[str, List[BankRule]] = {
    
    # -------------------------------------------------------------------------
    # mBank - has specific patterns in remittance_information
    # -------------------------------------------------------------------------
    'mbank': [
        # No special mbank-only rules needed - generic rules handle all cases
        # The pattern is:
        # - [title, type] with counterparty -> counterparty=payee, title=narration, type=transaction_type
        # - [title, type] without counterparty -> title=payee, type=narration (also transaction_type)
    ],
    
    # -------------------------------------------------------------------------
    # Revolut - uses bank_transaction_code
    # -------------------------------------------------------------------------
    'Revolut': [
        # Card payments with bank_transaction_code
        BankRule(
            name='card_payment',
            condition=lambda txn: (
                txn.bank_transaction_code in ('CARD_PAYMENT', 'OTP_PAYMENT')
                and txn.remittance_information
            ),
            extract=lambda txn: ParsedTransaction(
                payee=txn.remittance_information[0],
                narration=txn.bank_transaction_code or 'Card payment',
                transaction_type=txn.bank_transaction_code
            )
        ),
    ],
    
    # -------------------------------------------------------------------------
    # Pekao
    # -------------------------------------------------------------------------
    'pekao': [
        # Add pekao-specific rules if needed
    ],
}


def get_parsed_transaction(txn: 'EnableBankingTransaction') -> Optional[ParsedTransaction]:
    """Apply bank-specific and generic rules to extract payee, narration, and type.
    
    First tries bank-specific rules, then falls back to generic rules.
    
    Args:
        txn: The transaction to process
        
    Returns:
        ParsedTransaction if a rule matched, None otherwise
    """
    # Get bank-specific rules (case-insensitive lookup)
    bank_lower = txn.bank.lower()
    bank_rules = None
    
    for bank_name, rules in BANK_RULES.items():
        if bank_name.lower() == bank_lower:
            bank_rules = rules
            break
    
    # Try bank-specific rules first
    if bank_rules:
        for rule in bank_rules:
            try:
                if rule.condition(txn):
                    return rule.extract(txn)
            except (IndexError, AttributeError, TypeError):
                continue
    
    # Fall back to generic rules
    for rule in GENERIC_RULES:
        try:
            if rule.condition(txn):
                return rule.extract(txn)
        except (IndexError, AttributeError, TypeError):
            continue
    
    return None


# Keep backward compatibility
def get_payee_narration(txn: 'EnableBankingTransaction') -> Optional[ParsedTransaction]:
    """Backward-compatible alias for get_parsed_transaction."""
    return get_parsed_transaction(txn)
