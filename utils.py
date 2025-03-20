# utils.py

from .constants import KEYWORDS_TO_RENAME

def rename_column_if_keyword(column_name):
    """Rename the column if it matches any keyword in the list."""
    if column_name.upper() in KEYWORDS_TO_RENAME:
        return f"{column_name}_1"
    return column_name