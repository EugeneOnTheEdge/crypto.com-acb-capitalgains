import pandas as pd

# ---------------------------
# 1. Read CSV
# ---------------------------
folder = "transactions/"
file_name = "2021_crypto_transactions_record_20260228_181126.csv"
file_path = folder + file_name

df = pd.read_csv(file_path)

# Parse original format safely (DD-MM-YY HH:MM)
df["Timestamp (UTC)"] = pd.to_datetime(
    df["Timestamp (UTC)"],
    format="%d-%m-%y %H:%M"
)

# Only CAD native currency
df = df[df["Native Currency"] == "CAD"].copy()

# Sort chronologically
df = df.sort_values("Timestamp (UTC)").reset_index(drop=True)

# ---------------------------
# 2. Storage for tracking ACB + gains
# ---------------------------
holdings = {}   # coin -> {"quantity": float, "acb": float, "realized_gains": float}
coins = set()

# Identify coins dynamically
for _, row in df.iterrows():
    if pd.notna(row["To Currency"]) and row["To Currency"] != "":
        coins.add(row["To Currency"])
    if pd.notna(row["Currency"]) and row["Currency"] != "CAD":
        coins.add(row["Currency"])

# Create dynamic columns
for coin in coins:
    df[f"adjusted cost base {coin}"] = 0.0
    df[f"capital gains {coin}"] = 0.0
    df[f"average acb per unit {coin}"] = 0.0
    df[f"quantity remaining {coin}"] = 0.0
    df[f"spot price cad {coin}"] = 0.0

# ---------------------------
# 3. Process transactions
# ---------------------------
for i, row in df.iterrows():

    native_amount = float(row["Native Amount"])

    # ---------------- BUY (CAD → CRYPTO) ----------------
    if row["Currency"] == "CAD" and pd.notna(row["To Currency"]):

        coin = row["To Currency"]
        qty = float(row["To Amount"])
        cost = abs(native_amount)

        if coin not in holdings:
            holdings[coin] = {
                "quantity": 0.0,
                "acb": 0.0,
                "realized_gains": 0.0
            }

        # Spot price
        spot_price = cost / qty if qty != 0 else 0.0

        holdings[coin]["quantity"] += qty
        holdings[coin]["acb"] += cost

        avg_acb = holdings[coin]["acb"] / holdings[coin]["quantity"]

        df.at[i, f"spot price cad {coin}"] = spot_price
        df.at[i, f"adjusted cost base {coin}"] = holdings[coin]["acb"]
        df.at[i, f"average acb per unit {coin}"] = avg_acb
        df.at[i, f"quantity remaining {coin}"] = holdings[coin]["quantity"]

    # ---------------- SELL / DISPOSAL (CRYPTO → CAD) ----------------
    elif row["Currency"] != "CAD" and native_amount > 0:

        coin = row["Currency"]
        qty_sold = abs(float(row["Amount"]))
        proceeds = native_amount

        if coin not in holdings or holdings[coin]["quantity"] <= 0:
            continue

        # Spot price
        spot_price = proceeds / qty_sold if qty_sold != 0 else 0.0

        avg_cost = holdings[coin]["acb"] / holdings[coin]["quantity"]
        acb_reduction = avg_cost * qty_sold
        gain = proceeds - acb_reduction

        holdings[coin]["quantity"] -= qty_sold
        holdings[coin]["acb"] -= acb_reduction
        holdings[coin]["realized_gains"] += gain

        # Clean floating-point dust
        if holdings[coin]["quantity"] <= 1e-10:
            holdings[coin]["quantity"] = 0.0
            holdings[coin]["acb"] = 0.0

        avg_acb = 0.0
        if holdings[coin]["quantity"] > 0:
            avg_acb = holdings[coin]["acb"] / holdings[coin]["quantity"]

        df.at[i, f"spot price cad {coin}"] = spot_price
        df.at[i, f"capital gains {coin}"] = gain
        df.at[i, f"adjusted cost base {coin}"] = holdings[coin]["acb"]
        df.at[i, f"average acb per unit {coin}"] = avg_acb
        df.at[i, f"quantity remaining {coin}"] = holdings[coin]["quantity"]

# ---------------------------
# 4. Convert Timestamp Format
# ---------------------------
df["Timestamp (UTC)"] = df["Timestamp (UTC)"].dt.strftime("%d-%b-%Y %H:%M")

# ---------------------------
# 5. Save updated CSV
# ---------------------------
output_path = "transformed/" + file_name
df.to_csv(output_path, index=False)

# ---------------------------
# 6. Print Summary
# ---------------------------
print("\n=== REALIZED CAPITAL GAINS SUMMARY (CAD) ===\n")

total_realized = 0.0

for coin, data in holdings.items():
    print(f"{coin}:")
    print(f"  Remaining Quantity: {round(data['quantity'], 8)}")
    print(f"  Remaining ACB: {round(data['acb'], 2)} CAD")
    print(f"  Total Realized Gains: {round(data['realized_gains'], 2)} CAD\n")

    total_realized += data["realized_gains"]

print("--------------------------------------------------")
print(f"TOTAL REALIZED CAPITAL GAINS (CAD): {round(total_realized, 2)}")
print(f"TAXABLE PORTION (50% 🇨🇦 inclusion rate): {round(total_realized * 0.5, 2)}")
print("--------------------------------------------------")

print(f"\nDone ✅ File saved to: {output_path}")