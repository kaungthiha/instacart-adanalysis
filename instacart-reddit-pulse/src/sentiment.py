from __future__ import annotations

from typing import Dict

from nltk.sentiment import SentimentIntensityAnalyzer


class VaderSentiment:
    """Thin wrapper around NLTK VADER for consistent labels."""

    def __init__(self) -> None:
        self.analyzer = SentimentIntensityAnalyzer()

    def score(self, text: str) -> Dict[str, float]:
        return self.analyzer.polarity_scores(text or "")

    @staticmethod
    def label(compound: float) -> str:
        if compound <= -0.05:
            return "negative"
        if compound >= 0.05:
            return "positive"
        return "neutral"
