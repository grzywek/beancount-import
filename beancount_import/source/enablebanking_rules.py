"""Bank-specific parsing rules for EnableBanking source.

This module contains configurable rules for extracting payee and narration
from transaction data for different banks. Each bank can have multiple rules
that are evaluated in order - the first matching rule is used.

Adding new rules:
1. Add a new BankRule to the BANK_RULES dict for your bank
2. Define a condition function that returns True when the rule should apply
3. Define an extract function that returns ParsedPayeeNarration

Example:
    BANK_RULES['mybank'] = [
        BankRule(
            name='my_rule',
            condition=lambda txn: 'KEYWORD' in txn.remittance_information[0],
            extract=lambda txn: ParsedPayeeNarration(
                payee=txn.remittance_information[0],
                narration='My narration'
            )
        ),
    ]
"""

from dataclasses import dataclass
from typing import Callable, Dict, List, Optional, TYPE_CHECKING

if TYPE_CHECKING:
    from .enablebanking import EnableBankingTransaction


@dataclass
class ParsedPayeeNarration:
    """Result of parsing payee and narration from a transaction."""
    payee: str
    narration: str


@dataclass
class BankRule:
    """A single parsing rule for a bank.
    
    Attributes:
        name: Human-readable name for this rule (for debugging/logging)
        condition: Function that returns True if this rule should be applied
        extract: Function that extracts payee/narration from the transaction
    """
    name: str
    condition: Callable[['EnableBankingTransaction'], bool]
    extract: Callable[['EnableBankingTransaction'], ParsedPayeeNarration]


# =============================================================================
# BANK RULES REGISTRY
# =============================================================================
# Add bank-specific rules here. Rules are evaluated in order - first match wins.

BANK_RULES: Dict[str, List[BankRule]] = {
    
    # -------------------------------------------------------------------------
    # mBank
    # -------------------------------------------------------------------------
    'mbank': [
        # BLIK payments: remittance has 2 lines, second contains "BLIK"
        # Example: ["WWW.ZEN.COM", "BLIK ZAKUP E-COMMERCE"]
        BankRule(
            name='blik_payment',
            condition=lambda txn: (
                len(txn.remittance_information) == 2
                and 'BLIK' in txn.remittance_information[1].upper()
            ),
            extract=lambda txn: ParsedPayeeNarration(
                payee=txn.remittance_information[0],
                narration=txn.remittance_information[1]
            )
        ),
        
        # Internal transfers: remittance has 2 lines, second contains "PRZELEW WEWNĘTRZNY"
        # Example: ["DAV/4231225 minus NN", "PRZELEW WEWNĘTRZNY PRZYCHODZĄCY"]
        BankRule(
            name='internal_transfer',
            condition=lambda txn: (
                len(txn.remittance_information) == 2
                and 'PRZELEW WEWN' in txn.remittance_information[1].upper()
            ),
            extract=lambda txn: ParsedPayeeNarration(
                payee=txn.debtor_name or txn.creditor_name or txn.bank,
                narration=txn.remittance_information[0]
            )
        ),
        
        # External transfers: remittance has 2 lines, second contains "PRZELEW ZEWNĘTRZNY"
        BankRule(
            name='external_transfer',
            condition=lambda txn: (
                len(txn.remittance_information) == 2
                and 'PRZELEW ZEWN' in txn.remittance_information[1].upper()
            ),
            extract=lambda txn: ParsedPayeeNarration(
                payee=txn.debtor_name or txn.creditor_name or txn.bank,
                narration=txn.remittance_information[0]
            )
        ),
        
        # Fees: single line remittance starting with "OPŁATA"
        BankRule(
            name='fee',
            condition=lambda txn: (
                len(txn.remittance_information) == 1
                and txn.remittance_information[0].upper().startswith('OPŁATA')
            ),
            extract=lambda txn: ParsedPayeeNarration(
                payee=txn.bank,
                narration=txn.remittance_information[0]
            )
        ),
    ],
    
    # -------------------------------------------------------------------------
    # Revolut (example placeholder - add real rules as needed)
    # -------------------------------------------------------------------------
    'Revolut': [
        # Card payments with bank_transaction_code
        BankRule(
            name='card_payment',
            condition=lambda txn: (
                txn.bank_transaction_code in ('CARD_PAYMENT', 'OTP_PAYMENT')
                and txn.remittance_information
            ),
            extract=lambda txn: ParsedPayeeNarration(
                payee=txn.remittance_information[0],
                narration=txn.bank_transaction_code or 'Card payment'
            )
        ),
    ],
    
    # -------------------------------------------------------------------------
    # Pekao (add rules as needed)
    # -------------------------------------------------------------------------
    'pekao': [
    ],
}


def get_payee_narration(txn: 'EnableBankingTransaction') -> Optional[ParsedPayeeNarration]:
    """Apply bank-specific rules to extract payee and narration.
    
    Evaluates rules for the transaction's bank in order, returning the result
    from the first matching rule.
    
    Args:
        txn: The transaction to process
        
    Returns:
        ParsedPayeeNarration if a rule matched, None otherwise
    """
    # Get rules for this bank (case-insensitive lookup)
    bank_lower = txn.bank.lower()
    rules = None
    
    for bank_name, bank_rules in BANK_RULES.items():
        if bank_name.lower() == bank_lower:
            rules = bank_rules
            break
    
    if not rules:
        return None
    
    # Try each rule in order
    for rule in rules:
        try:
            if rule.condition(txn):
                return rule.extract(txn)
        except (IndexError, AttributeError, TypeError):
            # Rule condition or extract failed - skip to next rule
            continue
    
    return None
