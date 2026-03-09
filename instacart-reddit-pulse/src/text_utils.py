from __future__ import annotations

import re
from typing import Iterable


def clean_text(text: str) -> str:
    """Normalize text for lightweight keyword and sentiment processing."""
    text = text or ""
    text = text.replace("\n", " ").replace("\r", " ")
    text = re.sub(r"https?://\S+", " ", text)
    text = re.sub(r"\s+", " ", text)
    return text.strip()


def assemble_doc_text(title: str, body: str, comments: Iterable[str], max_chars: int = 5000) -> str:
    """Build analysis text from post title/body and top comments."""
    chunks = [clean_text(title), clean_text(body)]
    chunks.extend(clean_text(c) for c in comments if c)
    doc_text = "\n".join(c for c in chunks if c)
    return doc_text[:max_chars]
