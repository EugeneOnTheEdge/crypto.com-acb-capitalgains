# crypto.com-acb-capitalgains
ACB (Adjusted Cost Base) and capital gains calculator for Crypto.com's CSV transactions
Enriches the transaction history with ACB and capital gains for each crypto coin.
Also generates a separate summary report detailing per-coin final aggregated ACB and capital gains. 

## SUPPORTED TRANSACTIONS
🇨🇦 Canada Revenue Agency logic for the following:
- Crypto buy: usual ACB calculation (ACB + total purchase price)
- Crypto sell: treated as "Disposal", reduce ACB proportionally and calculate Capital Gains for the transaction
- crypto deposits: treated as coming from another person, ACB increased by FMV (Fair Market Value)
- crypto withdrawal: treated similar to crypto sell, giving to another person. Reduce ACB and calculate CG per transaction
- cashbacks, staking/trading/Supercharger rewards: treated as income, ACB increased by FMV
- swaps: source crypto treated as sell; destination crypto treated as buy
- reimbursements and reversals: as deposits/withdrawals
- adjustments (admin): as deposits 
- CRO lock-ups, Supercharger transfers: treated as internal transfer - exempted from capital gains calculation (non-disposal)

Anytime an ACB goes below 0 for a disposal transaction, it will be marked as "WARNING" and will skip processing.


## Upcoming
Per-coin T1 Schedule 3 report generati
