import datetime

POSTING_DATE_KEY = 'date'

POSTING_TRANSACTION_DATE_KEY = 'transaction_date'

def _ensure_date(value):
    """Convert a value to datetime.date if it's a string."""
    if isinstance(value, str):
        return datetime.date.fromisoformat(value)
    return value

def get_posting_date(entry, posting):
    """Returns the date associated with a posting."""
    result = ((posting.meta and (posting.meta.get(POSTING_DATE_KEY) or
                               posting.meta.get(POSTING_TRANSACTION_DATE_KEY))) or entry.date)
    return _ensure_date(result)
