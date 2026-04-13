import hashlib
import re


def sha1_text(text: str) -> str:
    return hashlib.sha1(text.encode('utf-8')).hexdigest()


def safe_name(text: str, limit: int = 80) -> str:
    text = re.sub(r'[^0-9A-Za-z一-鿿._-]+', '_', text.strip())
    text = re.sub(r'_+', '_', text).strip('._-')
    return text[:limit] or 'item'
