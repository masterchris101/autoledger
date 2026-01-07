from pathlib import Path
import json
import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
INPUT = ROOT / "data" / "transactions_raw.csv"
OUTDIR = ROOT / "output"
RULES_PATH = ROOT / "rules.json"
OUTDIR.mkdir(parents=True, exist_ok=True)

def load_rules() -> dict:
    with open(RULES_PATH, "r", encoding="utf-8") as f:
        return json.load(f)

def normalize_description(s: str, rules: dict) -> str:
    s = str(s).strip().upper()
    for a, b in rules["merchant_normalization"].items():
        s = s.replace(a.upper(), b.upper())
    return s

def categorize(desc: str, rules: dict) -> str:
    d = desc.upper()
    for rule in rules["category_rules"]:
        cat = rule["category"]
        for kw in rule["keywords"]:
            if kw.upper() in d:
                return cat
    return "Other"

def main():
    rules = load_rules()

    df = pd.read_csv(INPUT)
    df = df.rename(columns={"Date": "date", "Description": "description", "Amount": "amount"})

    df["date"] = pd.to_datetime(df["date"], errors="coerce")
    df["amount"] = pd.to_numeric(df["amount"], errors="coerce")
    df["description_raw"] = df["description"]
    df["description"] = df["description"].apply(lambda x: normalize_description(x, rules))
    df = df.dropna(subset=["date", "description", "amount"]).copy()

    df["month"] = df["date"].dt.to_period("M").astype(str)
    df["category"] = df["description"].apply(lambda x: categorize(x, rules))
    df["spend_abs"] = df["amount"].abs()

    # Exact duplicates
    df = df.sort_values("date")
    df["is_duplicate"] = df.duplicated(subset=["date", "description", "amount"], keep="first")

    # Near-duplicates
    window_days = int(rules["duplicate_window_days"])
    df["is_near_duplicate"] = False
    for i in range(1, len(df)):
        prev = df.iloc[i - 1]
        curr = df.iloc[i]
        if (
            prev["description"] == curr["description"]
            and abs(prev["amount"] - curr["amount"]) < 0.01
            and (curr["date"] - prev["date"]).days <= window_days
        ):
            df.at[df.index[i], "is_near_duplicate"] = True

    # Weird scoring
    tiny_thr = float(rules["tiny_charge_threshold"])
    large_thr = float(rules["large_charge_threshold"])
    score_thr = int(rules["weird_score_threshold"])

    pts = rules["weird_score_rules"]
    df["weird_score"] = 0

    df.loc[df["description"].str.contains("UNKNOWN", na=False), "weird_score"] += int(pts["unknown_merchant_points"])
    df.loc[df["spend_abs"] <= tiny_thr, "weird_score"] += int(pts["tiny_charge_points"])
    df.loc[df.groupby("description")["description"].transform("count") == 1, "weird_score"] += int(pts["rare_merchant_points"])
    df.loc[df["spend_abs"] >= large_thr, "weird_score"] += int(pts["large_charge_points"])

    df["is_weird"] = df["weird_score"] >= score_thr

    # Outputs
    df.to_csv(OUTDIR / "clean_transactions.csv", index=False)

    monthly = (
        df[df["amount"] < 0]
        .groupby(["month", "category"], as_index=False)["spend_abs"]
        .sum()
        .sort_values(["month", "spend_abs"], ascending=[True, False])
    )
    monthly.to_csv(OUTDIR / "monthly_summary.csv", index=False)

    top_merchants = (
        df[df["amount"] < 0]
        .groupby(["month", "description"], as_index=False)["spend_abs"]
        .sum()
        .sort_values(["month", "spend_abs"], ascending=[True, False])
    )
    top_merchants.to_csv(OUTDIR / "top_merchants.csv", index=False)

    subs = (
        df[df["amount"] < 0]
        .assign(amount_round=lambda x: x["amount"].round(2))
        .groupby(["description", "amount_round"])["month"]
        .nunique()
        .reset_index(name="months_seen")
        .query("months_seen >= 2")
        .sort_values(["months_seen", "description"], ascending=[False, True])
    )
    subs.to_csv(OUTDIR / "subscription_candidates.csv", index=False)

    review = df[(df["is_duplicate"]) | (df["is_near_duplicate"]) | (df["is_weird"])]
    review.to_csv(OUTDIR / "flagged_review.csv", index=False)

    print("Config-driven build complete ✅")
    print("Outputs in /output:")
    print("- clean_transactions.csv")
    print("- monthly_summary.csv")
    print("- top_merchants.csv")
    print("- subscription_candidates.csv")
    print("- flagged_review.csv")

if __name__ == "__main__":
    main()
