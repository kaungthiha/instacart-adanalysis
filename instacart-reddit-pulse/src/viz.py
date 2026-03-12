from __future__ import annotations

from pathlib import Path

import matplotlib.pyplot as plt
import pandas as pd


def save_sentiment_trend(daily_df: pd.DataFrame, output_path: str) -> None:
    """Save daily negative-share trend with post volume on secondary axis."""
    path = Path(output_path)
    path.parent.mkdir(parents=True, exist_ok=True)

    fig, ax1 = plt.subplots(figsize=(9, 5))
    ax2 = ax1.twinx()

    ax1.plot(daily_df["day"], daily_df["negative_share"], marker="o", color="#d62728", label="% negative")
    ax2.bar(daily_df["day"], daily_df["post_count"], alpha=0.2, color="#1f77b4", label="post volume")

    ax1.set_ylabel("Negative Share (%)")
    ax2.set_ylabel("Post Volume")
    ax1.set_xlabel("Day")
    ax1.set_title("Instacart Reddit Pulse: Daily Negative Share")
    ax1.tick_params(axis="x", rotation=45)
    ax1.grid(alpha=0.2)
    fig.tight_layout()
    fig.savefig(path, dpi=150)
    plt.close(fig)


def save_negative_themes(theme_df: pd.DataFrame, output_path: str) -> None:
    """Save bar chart of negative-theme mention counts."""
    path = Path(output_path)
    path.parent.mkdir(parents=True, exist_ok=True)

    fig, ax = plt.subplots(figsize=(8, 5))
    ax.bar(theme_df["theme"], theme_df["count"], color="#ff7f0e")
    ax.set_title("Top Themes in Negative Posts")
    ax.set_ylabel("Negative Post Count")
    ax.set_xlabel("Theme")
    ax.tick_params(axis="x", rotation=30)
    ax.grid(axis="y", alpha=0.2)
    fig.tight_layout()
    fig.savefig(path, dpi=150)
    plt.close(fig)
