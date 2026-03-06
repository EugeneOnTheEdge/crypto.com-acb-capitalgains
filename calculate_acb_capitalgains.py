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
    errors="coerce"
)

df = df[df["Native Currency"] == "CAD"].copy()
df = df.sort_values("Timestamp (UTC)").reset_index(drop=True)

# New columns
df["processed"] = "NOT_PROCESSED"
df["transaction type"] = "OTHER"
df["disposition status"] = "NON-DISPOSITION"
df["exempt from ACB/capital gains calculation?"] = "UNKNOWN"

# ---------------------------
# 2. Storage
# ---------------------------
holdings = {}
coins = set(df["Currency"].dropna().unique())

for coin in coins:
    if coin != "CAD":
        df[f"adjusted cost base {coin}"] = 0.0
        df[f"capital gains {coin}"] = 0.0
        df[f"average acb per unit {coin}"] = 0.0
        df[f"quantity remaining {coin}"] = 0.0
        df[f"spot price cad {coin}"] = 0.0

# ---------------------------
# 3. Process Transactions
# ---------------------------
for i, row in df.iterrows():

    try:

        if pd.isna(row["Timestamp (UTC)"]):
            raise ValueError("Invalid timestamp")

        kind = str(row["Transaction Kind"]).lower()
        currency = row["Currency"]
        native_amount = float(row["Native Amount"])
        amount = float(row["Amount"]) if pd.notna(row["Amount"]) else 0.0

        # ----------------------------------
        # BUY
        # ----------------------------------
        if kind in ["crypto_purchase", "viban_purchase", "recurring_buy"]:

            coin = row["To Currency"]
            qty = float(row["To Amount"])
            cost = abs(native_amount)

            df.at[i, "transaction type"] = "BUY"
            df.at[i, "exempt from ACB/capital gains calculation?"] = "NOT_EXEMPT"

            if qty <= 0:
                raise ValueError("Invalid quantity")

            if coin not in holdings:
                holdings[coin] = {"quantity": 0.0, "acb": 0.0}

            spot_price = cost / qty

            holdings[coin]["quantity"] += qty
            holdings[coin]["acb"] += cost

            avg_acb = holdings[coin]["acb"] / holdings[coin]["quantity"]

            df.at[i, f"spot price cad {coin}"] = spot_price
            df.at[i, f"adjusted cost base {coin}"] = holdings[coin]["acb"]
            df.at[i, f"average acb per unit {coin}"] = avg_acb
            df.at[i, f"quantity remaining {coin}"] = holdings[coin]["quantity"]

            df.at[i, "processed"] = "OK"

        # ----------------------------------
        # SELL
        # ----------------------------------
        elif kind in ["crypto_viban_exchange", "crypto_exchange"]:

            coin = currency
            qty_sold = abs(amount)
            proceeds = native_amount

            df.at[i, "transaction type"] = "SELL"
            df.at[i, "disposition status"] = "DISPOSITION"
            df.at[i, "exempt from ACB/capital gains calculation?"] = "NOT_EXEMPT"

            if coin not in holdings or holdings[coin]["quantity"] <= 0:
                df.at[i, "processed"] = "WARNING"
                continue

            if qty_sold > holdings[coin]["quantity"]:
                df.at[i, "processed"] = "WARNING"
                continue

            avg_cost = holdings[coin]["acb"] / holdings[coin]["quantity"]
            acb_reduction = avg_cost * qty_sold
            gain = proceeds - acb_reduction

            holdings[coin]["quantity"] -= qty_sold
            holdings[coin]["acb"] -= acb_reduction

            spot_price = proceeds / qty_sold

            avg_acb = (
                holdings[coin]["acb"] / holdings[coin]["quantity"]
                if holdings[coin]["quantity"] > 0 else 0
            )

            df.at[i, f"spot price cad {coin}"] = spot_price
            df.at[i, f"capital gains {coin}"] = gain
            df.at[i, f"adjusted cost base {coin}"] = holdings[coin]["acb"]
            df.at[i, f"average acb per unit {coin}"] = avg_acb
            df.at[i, f"quantity remaining {coin}"] = holdings[coin]["quantity"]

            df.at[i, "processed"] = "OK"

        # ----------------------------------
        # INCOME
        # ----------------------------------
        elif kind in ["referral_card_cashback", "staking_reward"]:

            coin = currency
            qty = amount
            income_value = native_amount

            df.at[i, "transaction type"] = "INCOME"
            df.at[i, "exempt from ACB/capital gains calculation?"] = "NOT_EXEMPT"

            if coin not in holdings:
                holdings[coin] = {"quantity": 0.0, "acb": 0.0}

            holdings[coin]["quantity"] += qty
            holdings[coin]["acb"] += income_value

            avg_acb = holdings[coin]["acb"] / holdings[coin]["quantity"]

            df.at[i, f"adjusted cost base {coin}"] = holdings[coin]["acb"]
            df.at[i, f"average acb per unit {coin}"] = avg_acb
            df.at[i, f"quantity remaining {coin}"] = holdings[coin]["quantity"]

            df.at[i, "processed"] = "OK"

        # ----------------------------------
        # DEPOSIT (likely own wallet)
        # ----------------------------------
        elif kind in ["crypto_deposit"]:

            df.at[i, "transaction type"] = "DEPOSIT"
            df.at[i, "exempt from ACB/capital gains calculation?"] = "EXEMPT"
            df.at[i, "processed"] = "NOT_PROCESSED"

        # ----------------------------------
        # WITHDRAWAL (unknown ownership)
        # ----------------------------------
        elif kind in ["exchange_to_crypto_transfer"]:

            df.at[i, "transaction type"] = "WITHDRAWAL"
            df.at[i, "exempt from ACB/capital gains calculation?"] = "UNKNOWN"
            df.at[i, "processed"] = "WARNING"

        # ----------------------------------
        # SUPERCHARGER
        # ----------------------------------
        elif kind in [
            "supercharger_deposit",
            "supercharger_withdrawal"
        ]:

            df.at[i, "transaction type"] = "LOCK"
            df.at[i, "exempt from ACB/capital gains calculation?"] = "EXEMPT"
            df.at[i, "processed"] = "NOT_PROCESSED"

        else:

            df.at[i, "transaction type"] = "OTHER"
            df.at[i, "exempt from ACB/capital gains calculation?"] = "UNKNOWN"
            df.at[i, "processed"] = "NOT_PROCESSED"

    except Exception:

        df.at[i, "processed"] = "ERROR"
        df.at[i, "transaction type"] = "ERROR"
        df.at[i, "disposition status"] = "ERROR"
        df.at[i, "exempt from ACB/capital gains calculation?"] = "ERROR"

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
# 6. Summary
# ---------------------------
print("\nProcessing Summary:")
print(df["processed"].value_counts())

print("\nTransaction Type Summary:")
print(df["transaction type"].value_counts())

print("\nACB Exemption Summary:")
print(df["exempt from ACB/capital gains calculation?"].value_counts())

print("\nDisposition Status Summary:")
print(df["disposition status"].value_counts())

print(f"\nDone ✅ File saved to: {output_path}")