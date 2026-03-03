import pandas as pd

# ---------------------------
# 1. Read CSV
# ---------------------------
folder = "transactions/"
file_name = "2021_crypto_transactions_record_20260228_181126.csv"
file_path = folder + file_name

df = pd.read_csv(file_path)

df["Timestamp (UTC)"] = pd.to_datetime(
    df["Timestamp (UTC)"],
    dayfirst=True,
    errors="coerce"  # invalid timestamps become NaT
)

df = df[df["Native Currency"] == "CAD"].copy()
df = df.sort_values("Timestamp (UTC)").reset_index(drop=True)

# Status column
df["processed"] = "NOT_PROCESSED"

# ---------------------------
# 2. Storage
# ---------------------------
holdings = {}
coins = set()

for _, row in df.iterrows():
    if pd.notna(row["To Currency"]) and row["To Currency"] != "":
        coins.add(row["To Currency"])
    if pd.notna(row["Currency"]) and row["Currency"] != "CAD":
        coins.add(row["Currency"])

for coin in coins:
    df[f"adjusted cost base {coin}"] = 0.0
    df[f"capital gains {coin}"] = 0.0
    df[f"average acb per unit {coin}"] = 0.0
    df[f"quantity remaining {coin}"] = 0.0
    df[f"spot price cad {coin}"] = 0.0

# ---------------------------
# 3. Process transactions safely
# ---------------------------
for i, row in df.iterrows():

    try:
        # Basic validation
        if pd.isna(row["Timestamp (UTC)"]):
            raise ValueError("Invalid timestamp")

        native_amount = float(row["Native Amount"])

        # ---------------- BUY ----------------
        if row["Currency"] == "CAD" and pd.notna(row["To Currency"]):

            coin = row["To Currency"]
            qty = float(row["To Amount"])
            cost = abs(native_amount)

            if qty <= 0:
                raise ValueError("Invalid quantity")

            if coin not in holdings:
                holdings[coin] = {
                    "quantity": 0.0,
                    "acb": 0.0,
                    "realized_gains": 0.0
                }

            spot_price = cost / qty

            holdings[coin]["quantity"] += qty
            holdings[coin]["acb"] += cost

            avg_acb = holdings[coin]["acb"] / holdings[coin]["quantity"]

            df.at[i, f"spot price cad {coin}"] = spot_price
            df.at[i, f"adjusted cost base {coin}"] = holdings[coin]["acb"]
            df.at[i, f"average acb per unit {coin}"] = avg_acb
            df.at[i, f"quantity remaining {coin}"] = holdings[coin]["quantity"]

            df.at[i, "processed"] = "OK"

        # ---------------- SELL ----------------
        elif row["Currency"] != "CAD" and native_amount > 0:

            coin = row["Currency"]
            qty_sold = abs(float(row["Amount"]))
            proceeds = native_amount

            if qty_sold <= 0:
                raise ValueError("Invalid sell quantity")

            if coin not in holdings or holdings[coin]["quantity"] <= 0:
                df.at[i, "processed"] = "WARNING"
                continue

            avg_cost = holdings[coin]["acb"] / holdings[coin]["quantity"]
            acb_reduction = avg_cost * qty_sold
            gain = proceeds - acb_reduction

            holdings[coin]["quantity"] -= qty_sold
            holdings[coin]["acb"] -= acb_reduction
            holdings[coin]["realized_gains"] += gain

            spot_price = proceeds / qty_sold

            if holdings[coin]["quantity"] < -1e-10:
                df.at[i, "processed"] = "WARNING"
            else:
                df.at[i, "processed"] = "OK"

            avg_acb = 0.0
            if holdings[coin]["quantity"] > 0:
                avg_acb = holdings[coin]["acb"] / holdings[coin]["quantity"]

            df.at[i, f"spot price cad {coin}"] = spot_price
            df.at[i, f"capital gains {coin}"] = gain
            df.at[i, f"adjusted cost base {coin}"] = holdings[coin]["acb"]
            df.at[i, f"average acb per unit {coin}"] = avg_acb
            df.at[i, f"quantity remaining {coin}"] = holdings[coin]["quantity"]

    except Exception:
        df.at[i, "processed"] = "ERROR"

# ---------------------------
# 4. Format Timestamp
# ---------------------------
df["Timestamp (UTC)"] = df["Timestamp (UTC)"].dt.strftime("%d-%b-%Y %H:%M")

# ---------------------------
# 5. Save
# ---------------------------
output_path = "transformed/" + file_name
df.to_csv(output_path, index=False)

# ---------------------------
# 6. Quick Status Summary
# ---------------------------
print("\nProcessing Summary:")
print(df["processed"].value_counts())

print(f"\nDone ✅ File saved to: {output_path}")