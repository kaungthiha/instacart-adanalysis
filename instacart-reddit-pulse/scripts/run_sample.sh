#!/usr/bin/env bash
set -euo pipefail

# Run the notebook in default sample mode and generate outputs for a quick draft.
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"

cd "$ROOT_DIR"

if [[ ! -f "analysis.ipynb" ]]; then
  echo "analysis.ipynb not found. Run this script from the project repo." >&2
  exit 1
fi

if [[ ! -f "data/sample_input.csv" ]]; then
  echo "Missing data/sample_input.csv. Cannot run sample mode." >&2
  exit 1
fi

export USE_SAMPLE_DATA=1

python -m jupyter nbconvert \
  --to notebook \
  --execute analysis.ipynb \
  --output analysis.executed.ipynb \
  --ExecutePreprocessor.timeout=600

echo "Done. Generated outputs should be in outputs/."
echo "- outputs/sentiment_trend.png"
echo "- outputs/negative_themes.png"
echo "- outputs/product_memo.md"
