from __future__ import annotations

from typing import Dict, List

from src.text_utils import clean_text

THEME_KEYWORDS: Dict[str, List[str]] = {
    "fees_pricing": ["fee", "delivery fee", "service fee", "markup", "price", "instacart+"],
    "refunds_support": ["refund", "refund denied", "support", "customer service", "chargeback"],
    "substitutions_oos": ["substitution", "replacement", "out of stock", "oos"],
    "order_accuracy": ["missing", "wrong item", "incorrect", "damaged", "expired", "bad produce"],
    "delivery_reliability": ["late", "delayed", "canceled", "cancelled", "no show"],
    "tipping_shopper": ["tip", "tipping", "tip bait", "shopper", "driver", "batch"],
}


def detect_themes(text: str) -> List[str]:
    """Return list of matching theme keys using keyword search."""
    txt = clean_text(text).lower()
    found: List[str] = []
    for theme, keywords in THEME_KEYWORDS.items():
        if any(k.lower() in txt for k in keywords):
            found.append(theme)
    return found
