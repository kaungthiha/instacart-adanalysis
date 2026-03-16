from __future__ import annotations

from datetime import datetime
from pathlib import Path

import nltk
import pandas as pd

from src.sentiment import VaderSentiment
from src.text_utils import assemble_doc_text
from src.themes import THEME_KEYWORDS, detect_themes
from src.viz import save_negative_themes, save_sentiment_trend


def ensure_vader() -> None:
    """Download VADER lexicon if not already available."""
    try:
        nltk.data.find("sentiment/vader_lexicon.zip")
    except LookupError:
        nltk.download("vader_lexicon")


def run_sample_pipeline() -> None:
    """Run analysis using bundled synthetic sample data and write outputs."""
    root = Path(__file__).resolve().parents[1]
    sample_path = root / "data" / "sample_input.csv"
    outputs_dir = root / "outputs"
    outputs_dir.mkdir(parents=True, exist_ok=True)

    if not sample_path.exists():
        raise FileNotFoundError("Missing data/sample_input.csv. Cannot run sample mode.")

    ensure_vader()

    posts_df = pd.read_csv(sample_path)
    posts_df["title"] = posts_df["title"].fillna("")
    posts_df["body"] = posts_df["body"].fillna("")

    sentiment = VaderSentiment()
    posts_df["doc_text"] = posts_df.apply(lambda r: assemble_doc_text(r["title"], r["body"], []), axis=1)
    posts_df["compound"] = posts_df["doc_text"].apply(lambda txt: sentiment.score(txt)["compound"])
    posts_df["sentiment"] = posts_df["compound"].apply(sentiment.label)

    if "created_dt" in posts_df.columns:
        posts_df["day"] = pd.to_datetime(posts_df["created_dt"], errors="coerce").dt.strftime("%Y-%m-%d")
    else:
        posts_df["day"] = pd.to_datetime(posts_df["created_utc"], unit="s", errors="coerce").dt.strftime("%Y-%m-%d")

    posts_df["themes"] = posts_df["doc_text"].apply(detect_themes)
    posts_df["is_negative"] = posts_df["sentiment"].eq("negative")

    daily = posts_df.groupby("day").agg(post_count=("post_id", "count"), negative_count=("is_negative", "sum")).reset_index()
    daily["negative_share"] = (daily["negative_count"] / daily["post_count"] * 100).round(1)

    negative_posts = posts_df[posts_df["is_negative"]].copy()
    negative_theme_rows = []
    for _, row in negative_posts.iterrows():
        for theme in row["themes"]:
            negative_theme_rows.append({"theme": theme, "post_id": row["post_id"], "subreddit": row["subreddit"]})
    negative_theme_df = pd.DataFrame(negative_theme_rows)

    if negative_theme_df.empty:
        theme_counts = pd.DataFrame({"theme": list(THEME_KEYWORDS.keys()), "count": [0] * len(THEME_KEYWORDS)})
        cross_subreddit_repeat = pd.DataFrame(columns=["theme", "subreddit_count"])
    else:
        theme_counts = negative_theme_df.groupby("theme").agg(count=("post_id", "nunique")).reset_index().sort_values("count", ascending=False)
        cross_subreddit_repeat = negative_theme_df.groupby("theme")["subreddit"].nunique().reset_index(name="subreddit_count")
        cross_subreddit_repeat = cross_subreddit_repeat[cross_subreddit_repeat["subreddit_count"] >= 2].sort_values("subreddit_count", ascending=False)

    save_sentiment_trend(daily.sort_values("day"), str(outputs_dir / "sentiment_trend.png"))
    save_negative_themes(theme_counts.head(6), str(outputs_dir / "negative_themes.png"))

    sentiment_dist = posts_df["sentiment"].value_counts(normalize=True).mul(100).round(1).rename_axis("sentiment").reset_index(name="pct")
    start_day = posts_df["day"].min() if not posts_df.empty else "N/A"
    end_day = posts_df["day"].max() if not posts_df.empty else "N/A"

    top_theme_rows = theme_counts.head(3).to_dict("records")
    observed_lines = []
    for r in top_theme_rows:
        share = 0.0 if len(negative_posts) == 0 else round((r["count"] / len(negative_posts)) * 100, 1)
        observed_lines.append(f"- **{r['theme']}** appeared in {r['count']} negative posts ({share}% of negative posts).")

    repeat_lines = "\n".join(
        [f"- {r.theme}: present in {r.subreddit_count} subreddits" for r in cross_subreddit_repeat.itertuples()]
    ) or "- No theme appeared in 2+ subreddits in this run."

    highest_negative = "No daily trend available."
    if not daily.empty:
        top_day = daily.sort_values("negative_share", ascending=False).iloc[0]
        highest_negative = f"Highest negative-share day: {top_day['day']} ({top_day['negative_share']}%)."

    memo = f"""# Instacart Reddit Pulse Memo

**Date range:** {start_day} to {end_day}  
**Data source:** Bundled synthetic sample data (directional draft mode).

## Executive summary
- Sampled {len(posts_df)} posts across {posts_df['subreddit'].nunique()} subreddits.
- Overall sentiment split: {', '.join([f"{r['sentiment']} {r['pct']}%" for _, r in sentiment_dist.iterrows()])}
- {highest_negative}

## What we observed
{chr(10).join(observed_lines) if observed_lines else '- No major negative themes found.'}

### Cross-subreddit repeat pain
{repeat_lines}

## Why it matters
- **Consumer side:** Persistent issues in fees, substitutions, and order accuracy can reduce repeat order intent.
- **Shopper side:** Delivery reliability and tipping/support friction can lower shopper supply quality and fill rate.
- **Marketplace risk:** When both sides report friction, service trust can decline faster than isolated incident metrics suggest.

## Recommendations
1. **Pricing clarity bet:** Add pre-checkout fee transparency and compare complaint-rate changes for fee-related sessions.
2. **Substitution confidence bet:** Improve substitution prompts with explicit item-quality preferences and track substitution acceptance + refund rates.
3. **Support recovery bet:** Add faster in-flow support for missing/wrong items with instant credit guardrails and monitor CSAT + repeat purchase.

## How to validate
1. **Experiment:** Fee transparency variant in checkout. **Success metric:** reduction in fee-related negative-theme share by 20% over 2 weeks.
2. **Experiment:** Substitution UX nudge for top categories. **Success metric:** +5% accepted substitutions, lower complaint volume.
3. **Experiment:** High-priority support lane for order-accuracy incidents. **Success metric:** faster resolution time and lower 7-day churn proxy.

## Limitations
- Synthetic sample data; directional demonstration only.
- VADER + keyword rules can miss sarcasm and context.
- Results are not causal.

## Ethics/compliance
- Aggregated outputs only; no raw post/comment text published in repo outputs.
- Local cached/raw data should be removed quickly using `python scripts/cleanup_local_data.py` (within 48 hours).
"""

    (outputs_dir / "product_memo.md").write_text(memo)

    print("Done. Generated outputs:")
    print("- outputs/sentiment_trend.png")
    print("- outputs/negative_themes.png")
    print("- outputs/product_memo.md")
    print(f"Generated at: {datetime.utcnow().isoformat()}Z")


if __name__ == "__main__":
    run_sample_pipeline()
