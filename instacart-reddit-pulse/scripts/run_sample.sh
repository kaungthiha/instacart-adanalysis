#!/usr/bin/env bash
set -euo pipefail

# Run sample mode quickly. Prefer direct Python pipeline; fallback to notebook execution.
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"

cd "$ROOT_DIR"

if [[ ! -f "data/sample_input.csv" ]]; then
  echo "Missing data/sample_input.csv. Cannot run sample mode." >&2
  exit 1
fi

export USE_SAMPLE_DATA=1

if python -c "import pandas, matplotlib, nltk" >/dev/null 2>&1; then
  if python scripts/run_sample.py; then
    exit 0
  fi
fi

if python -m jupyter --version >/dev/null 2>&1; then
  python -m jupyter nbconvert \
    --to notebook \
    --execute analysis.ipynb \
    --output analysis.executed.ipynb \
    --ExecutePreprocessor.timeout=600

  echo "Done. Generated outputs should be in outputs/."
  echo "- outputs/sentiment_trend.png"
  echo "- outputs/negative_themes.png"
  echo "- outputs/product_memo.md"
  exit 0
fi

echo "Could not run sample pipeline because required packages are missing." >&2
echo "Install dependencies with: pip install -r requirements.txt" >&2
exit 1
