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

# ---------------------------
# 2. Columns
# ---------------------------
df["processed"] = "NOT_PROCESSED"
df["transaction type"] = "OTHER"
df["disposition status"] = "NON_DISPOSITION"
df["exempt from ACB/capital gains calculation?"] = "NON_EXEMPT"

# ---------------------------
# 3. Storage
# ---------------------------
holdings = {}
coins = set()

for _, row in df.iterrows():
    if pd.notna(row["Currency"]) and row["Currency"] != "CAD":
        coins.add(row["Currency"])
    if pd.notna(row["To Currency"]) and row["To Currency"] != "CAD":
        coins.add(row["To Currency"])

for coin in coins:
    df[f"adjusted cost base {coin}"] = 0.0
    df[f"capital gains {coin}"] = 0.0
    df[f"average acb per unit {coin}"] = 0.0
    df[f"quantity remaining {coin}"] = 0.0
    df[f"spot price cad {coin}"] = 0.0

# ---------------------------
# 4. Processing
# ---------------------------
for i, row in df.iterrows():
    try:
        if pd.isna(row["Timestamp (UTC)"]):
            raise ValueError("Invalid timestamp")

        kind = str(row["Transaction Kind"]).lower()
        currency = row["Currency"]
        native_amount = float(row["Native Amount"]) if pd.notna(row["Native Amount"]) else 0.0
        amount = float(row["Amount"]) if pd.notna(row["Amount"]) else 0.0

        # ---------------- BUY ----------------
        if kind in ["crypto_purchase", "viban_purchase", "recurring_buy"]:
            coin = row["To Currency"]
            qty = float(row["To Amount"])
            cost = abs(native_amount)

            df.at[i, "transaction type"] = "BUY"

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

        # ---------------- SELL ----------------
        elif kind in ["crypto_viban_exchange", "crypto_withdrawal", "card_top_up"]:
            coin = currency
            qty_sold = abs(amount)
            proceeds = native_amount

            df.at[i, "transaction type"] = "SELL"
            df.at[i, "disposition status"] = "DISPOSITION"

            if coin not in holdings or holdings[coin]["quantity"] <= 0 or qty_sold > holdings[coin]["quantity"]:
                df.at[i, "processed"] = "WARNING"
                continue

            avg_cost = holdings[coin]["acb"] / holdings[coin]["quantity"]
            acb_reduction = avg_cost * qty_sold
            gain = proceeds - acb_reduction

            holdings[coin]["quantity"] -= qty_sold
            holdings[coin]["acb"] -= acb_reduction
            spot_price = proceeds / qty_sold
            avg_acb = holdings[coin]["acb"] / holdings[coin]["quantity"] if holdings[coin]["quantity"] > 0 else 0.0

            df.at[i, f"spot price cad {coin}"] = spot_price
            df.at[i, f"capital gains {coin}"] = gain
            df.at[i, f"adjusted cost base {coin}"] = holdings[coin]["acb"]
            df.at[i, f"average acb per unit {coin}"] = avg_acb
            df.at[i, f"quantity remaining {coin}"] = holdings[coin]["quantity"]
            df.at[i, "processed"] = "OK"


        # ---------------- INCOME (Rewards / Cashback / Crypto Earn Interest) ----------------
        elif kind in [
            "referral_card_cashback",
            "staking_reward",
            "crypto_earn_interest_paid",
            "supercharger_reward_to_app_credited",
            "reimbursement",
            "admin_wallet_credited",
            "mco_stake_reward",
            "rewards_platform_deposit_credited"
        ]:
            coin = currency
            qty = amount
            income_value = native_amount

            df.at[i, "transaction type"] = "INCOME"
            df.at[i, "processed"] = "OK"
            df.at[i, "exempt from ACB/capital gains calculation?"] = "NON_EXEMPT"

            if coin not in holdings:
                holdings[coin] = {"quantity": 0.0, "acb": 0.0}

            holdings[coin]["quantity"] += qty
            holdings[coin]["acb"] += income_value
            avg_acb = holdings[coin]["acb"] / holdings[coin]["quantity"]

            df.at[i, f"adjusted cost base {coin}"] = holdings[coin]["acb"]
            df.at[i, f"average acb per unit {coin}"] = avg_acb
            df.at[i, f"quantity remaining {coin}"] = holdings[coin]["quantity"]


        # ---------------- INTERNAL TRANSFERS ----------------
        elif kind in ["supercharger_deposit", "supercharger_withdrawal", "crypto_earn_program_created", "crypto_earn_program_withdrawn", "lockup_upgrade"]:
            df.at[i, "transaction type"] = "INTERNAL_TRANSFER"
            df.at[i, "exempt from ACB/capital gains calculation?"] = "EXEMPT"
            df.at[i, "processed"] = "OK"


        # ---------------- WALLET DEPOSITS ----------------
        elif kind in ["exchange_to_crypto_transfer", "crypto_deposit"]:
            coin = currency
            qty = amount
            cost = native_amount

            df.at[i, "transaction type"] = "TRANSFER"
            df.at[i, "exempt from ACB/capital gains calculation?"] = "NON_EXEMPT"

            if coin not in holdings:
                holdings[coin] = {"quantity": 0.0, "acb": 0.0}

            if qty > 0:

                spot_price = cost / qty

                holdings[coin]["quantity"] += qty
                holdings[coin]["acb"] += cost

                avg_acb = holdings[coin]["acb"] / holdings[coin]["quantity"]

                df.at[i, f"spot price cad {coin}"] = spot_price
                df.at[i, f"adjusted cost base {coin}"] = holdings[coin]["acb"]
                df.at[i, f"average acb per unit {coin}"] = avg_acb
                df.at[i, f"quantity remaining {coin}"] = holdings[coin]["quantity"]

            df.at[i, "processed"] = "OK"


        elif kind == "reimbursement_reverted":
            coin = currency
            qty = amount
            value = native_amount

            df.at[i, "transaction type"] = "INCOME_REVERSAL"

            if coin not in holdings:
                holdings[coin] = {"quantity": 0.0, "acb": 0.0}

            holdings[coin]["quantity"] += qty
            holdings[coin]["acb"] += value

            avg_acb = (
                holdings[coin]["acb"] / holdings[coin]["quantity"]
                if holdings[coin]["quantity"] != 0 else 0
            )

            df.at[i, f"adjusted cost base {coin}"] = holdings[coin]["acb"]
            df.at[i, f"average acb per unit {coin}"] = avg_acb
            df.at[i, f"quantity remaining {coin}"] = holdings[coin]["quantity"]

            df.at[i, "processed"] = "OK"


        # ---------------- SWAP (CRYPTO -> CRYPTO) ----------------
        elif kind == "crypto_exchange":

            sell_coin = currency
            buy_coin = row["To Currency"]

            qty_sold = abs(amount)
            qty_bought = float(row["To Amount"])
            value_cad = abs(native_amount)

            df.at[i, "transaction type"] = "SWAP"
            df.at[i, "disposition status"] = "DISPOSITION"
            df.at[i, "exempt from ACB/capital gains calculation?"] = "NON_EXEMPT"

            # ---------- SELL SIDE ----------
            if sell_coin not in holdings or holdings[sell_coin]["quantity"] <= 0:
                df.at[i, "processed"] = "WARNING"
                continue

            if qty_sold > holdings[sell_coin]["quantity"]:
                df.at[i, "processed"] = "WARNING"
                continue

            avg_cost_sell = holdings[sell_coin]["acb"] / holdings[sell_coin]["quantity"]

            acb_reduction = avg_cost_sell * qty_sold
            gain = value_cad - acb_reduction

            holdings[sell_coin]["quantity"] -= qty_sold
            holdings[sell_coin]["acb"] -= acb_reduction

            sell_spot = value_cad / qty_sold

            avg_acb_sell = (
                holdings[sell_coin]["acb"] / holdings[sell_coin]["quantity"]
                if holdings[sell_coin]["quantity"] > 0 else 0
            )

            df.at[i, f"spot price cad {sell_coin}"] = sell_spot
            df.at[i, f"capital gains {sell_coin}"] = gain
            df.at[i, f"adjusted cost base {sell_coin}"] = holdings[sell_coin]["acb"]
            df.at[i, f"average acb per unit {sell_coin}"] = avg_acb_sell
            df.at[i, f"quantity remaining {sell_coin}"] = holdings[sell_coin]["quantity"]

            # ---------- BUY SIDE ----------
            if buy_coin not in holdings:
                holdings[buy_coin] = {"quantity": 0.0, "acb": 0.0}

            holdings[buy_coin]["quantity"] += qty_bought
            holdings[buy_coin]["acb"] += value_cad

            buy_spot = value_cad / qty_bought

            avg_acb_buy = holdings[buy_coin]["acb"] / holdings[buy_coin]["quantity"]

            df.at[i, f"spot price cad {buy_coin}"] = buy_spot
            df.at[i, f"adjusted cost base {buy_coin}"] = holdings[buy_coin]["acb"]
            df.at[i, f"average acb per unit {buy_coin}"] = avg_acb_buy
            df.at[i, f"quantity remaining {buy_coin}"] = holdings[buy_coin]["quantity"]

            df.at[i, "processed"] = "OK"


        # ---------------- OTHER / NOT PROCESSED ----------------
        else:
            df.at[i, "transaction type"] = "OTHER"
            df.at[i, "processed"] = "NOT_PROCESSED"


    except Exception:
        df.at[i, "processed"] = "ERROR"
        df.at[i, "transaction type"] = "ERROR"
        df.at[i, "disposition status"] = "ERROR"
        df.at[i, "exempt from ACB/capital gains calculation?"] = "ERROR"

# ---------------------------
# 5. Format Timestamp
# ---------------------------
df["Timestamp (UTC)"] = df["Timestamp (UTC)"].dt.strftime("%d-%b-%Y %H:%M")

# ---------------------------
# 6. Save
# ---------------------------
output_path = "transformed/" + file_name
df.to_csv(output_path, index=False)

# ---------------------------
# 7. Summary
# ---------------------------
print("\nProcessing Summary:")
print(df["processed"].value_counts())

print("\nTransaction Type Summary:")
print(df["transaction type"].value_counts())

print("\nDisposition Status Summary:")
print(df["disposition status"].value_counts())

print("\nExemption Summary:")
print(df["exempt from ACB/capital gains calculation?"].value_counts())

print(f"\nDone ✅ File saved to: {output_path}")