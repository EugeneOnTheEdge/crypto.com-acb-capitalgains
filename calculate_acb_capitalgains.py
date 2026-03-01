import pandas as pd

# ---------------------------
# 1. Read CSV
# ---------------------------
folder = "transactions/"
filename = "2021_crypto_transactions_record_20260228_181126.csv"
file_path = folder + filename
df = pd.read_csv(file_path)

# Convert timestamp
df["Timestamp (UTC)"] = pd.to_datetime(df["Timestamp (UTC)"])

# Only CAD native currency
df = df[df["Native Currency"] == "CAD"].copy()

# Sort chronologically
df = df.sort_values("Timestamp (UTC)").reset_index(drop=True)

# ---------------------------
# 2. Storage for tracking ACB
# ---------------------------
holdings = {}   # coin -> {"quantity": float, "acb": float}
coins = set()

# Identify all coins dynamically
for _, row in df.iterrows():
    if pd.notna(row["To Currency"]):
        coins.add(row["To Currency"])
    if pd.notna(row["Currency"]):
        coins.add(row["Currency"])

# Create output columns
for coin in coins:
    df[f"adjusted cost base {coin}"] = 0.0
    df[f"capital gains {coin}"] = 0.0

# ---------------------------
# 3. Process transactions
# ---------------------------
for i, row in df.iterrows():

    tx_kind = str(row["Transaction Kind"]).lower()
    native_amount = float(row["Native Amount"])

    # ---------------- BUY ----------------
    if row["Currency"] == "CAD" and pd.notna(row["To Currency"]):
        coin = row["To Currency"]
        qty = float(row["To Amount"])
        cost = abs(native_amount)

        if coin not in holdings:
            holdings[coin] = {"quantity": 0.0, "acb": 0.0}

        holdings[coin]["quantity"] += qty
        holdings[coin]["acb"] += cost

        df.at[i, f"adjusted cost base {coin}"] = holdings[coin]["acb"]

    # ---------------- SELL / DISPOSAL ----------------
    elif row["Currency"] != "CAD" and native_amount > 0:
        coin = row["Currency"]
        qty_sold = abs(float(row["Amount"]))
        proceeds = native_amount

        if coin not in holdings or holdings[coin]["quantity"] == 0:
            continue

        avg_cost = holdings[coin]["acb"] / holdings[coin]["quantity"]
        acb_reduction = avg_cost * qty_sold
        gain = proceeds - acb_reduction

        # Update holdings
        holdings[coin]["quantity"] -= qty_sold
        holdings[coin]["acb"] -= acb_reduction

        df.at[i, f"capital gains {coin}"] = gain
        df.at[i, f"adjusted cost base {coin}"] = holdings[coin]["acb"]

# ---------------------------
# 4. Save result
# ---------------------------
df.to_csv("transformed/transformed_" + filename, index=False)

print("Done ✅ File saved as " + "transformed/transformed_" + filename)