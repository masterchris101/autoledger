# AutoLedger

AutoLedger is a config-driven Python automation tool that cleans raw bank/credit-card transaction CSV exports and generates repeatable budgeting and review outputs.

## Outputs
- clean_transactions.csv — standardized transactions with categories and flags
- monthly_summary.csv — spending totals by month and category
- top_merchants.csv — top merchants per month
- subscription_candidates.csv — recurring charges across months
- flagged_review.csv — duplicates / near-duplicates / suspicious charges

## Setup
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt

## Run
1) Put your CSV at data/transactions_raw.csv
2) Run: python src/cleaner.py

## Customize
Edit rules.json to tweak categories, keywords, thresholds, and near-duplicate window.
