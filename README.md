# 💳 Credit Card Fraud Detection — SQL Project

A multi-phase SQL analysis project focused on identifying fraudulent and high-risk financial behaviour using a real-world style dataset of users, credit cards, and transactions.

---

## 📁 Dataset

The analysis uses a combined `master_data` table built from three source tables:

| Table | Key Columns |
|---|---|
| **User Data** | `client_id`, `age`, `gender`, `yearly_income`, `total_debt`, `credit_score`, `latitude`, `longitude` |
| **Card Data** | `card_id`, `card_brand`, `card_type`, `credit_limit`, `has_chip`, `year_pin_last_changed`, `card_on_dark_web` |
| **Transaction Data** | `transaction_id`, `date`, `amount`, `use_chip`, `merchant_id`, `merchant_city`, `merchant_state` |

---

## 🗂️ Project Structure

```
credit-card-fraud-detection/
│
├── phase1_risk_flagging/
│   ├── 01_over_limit_transactions.sql
│   ├── 02_stale_pin_over_limit.sql
│   └── 03_yoy_spending_spike.sql
│
├── phase2_behavioral_anomalies/
│   ├── 01_merchant_risk_analysis.sql
│   ├── 02_high_frequency_customers.sql
│   └── 03_triple_risk_flag.sql
│
├── phase3_risk_scoring/
│   └── 01_customer_risk_score.sql
│
└── README.md
```

---

## 🔍 Methodology

### Phase 1 — Risk Flagging
Identifying direct risk signals at the transaction and customer level.

| Query | Signal | Technique |
|---|---|---|
| Over-limit transactions | Amount exceeds card credit limit | `CASE WHEN`, `GROUP BY` |
| Stale PIN + over-limit combo | PIN unchanged 10+ years AND over-limit transactions | `DATEDIFF`, `HAVING`, `CASE WHEN` |
| Year-on-year spending spike | Customer spending increased >50% vs prior year | CTE, `LAG()` window function |

### Phase 2 — Behavioral Anomalies
Identifying suspicious patterns across merchants and customers.

| Query | Signal | Technique |
|---|---|---|
| Merchant risk analysis | Merchants with high % of over-limit transactions | CTE, `SUM(CASE WHEN)`, percentage calculation |
| High frequency customers | Customers transacting above average frequency | CTE, `AVG() OVER()` window function |
| Triple risk flag | High debt + low credit score + high transaction frequency | CTE, compound `CASE WHEN`, `AVG() OVER()` |

### Phase 3 — Risk Scoring
Combining all 5 flags into a single composite risk score per customer.

| Score | Category |
|---|---|
| 4 – 5 | 🔴 Critical |
| 3 | 🟠 High |
| 2 | 🟡 Medium |
| 0 – 1 | 🟢 Low |

Techniques used: 9-CTE chain, `LEFT JOIN`, `COALESCE` for null safety, derived `risk_percent` score.

---

## 📊 Key Findings

### Risk Score Distribution
Out of all customers analysed:

| Risk Category | Customer Count |
|---|---|
| 🔴 Critical | 60 |
| 🟠 High | 435 |
| 🟡 Medium | 575 |
| 🟢 Low | 12 |

> **~78% of customers had a stale PIN** (not updated in 10+ years) — the single most widespread risk signal in the dataset.

### High-Risk Merchants
Merchants with the highest proportion of over-limit transactions:

| Merchant ID | City | State | Transactions | Over-Limit % |
|---|---|---|---|---|
| 59935 | Dallas | TX | 531 | 22.64% |
| 22204 | Aurora | CO | 525 | 9.62% |
| 49789 | Dallas | TX | 594 | 6.78% |
| 59935 | Tucson | AZ | 521 | 1.92% |
| 61195 | Tucson | AZ | 531 | 1.89% |
| 59935 | Miami | FL | 784 | 5.13% |
| 39991 | Lincoln Park | MI | 114 | 0.88% |

> Merchant `59935` appears across **3 different cities** — a notable pattern worth further investigation.

---

## 🛠️ SQL Techniques Used

- **Window Functions** — `LAG()`, `AVG() OVER()`, `SUM() OVER()` with `PARTITION BY`
- **CTEs** — multi-step chained CTEs including a 9-CTE final scoring query
- **Conditional Aggregation** — `SUM(CASE WHEN ... THEN 1 ELSE 0 END)`
- **NULL Safety** — `COALESCE` across all flag columns
- **LEFT JOINs** — ensuring no customers are dropped from scoring due to missing flag data
- **Derived Metrics** — `debt_to_income_ratio`, `risk_percent`, `over_limit_pct`, `yoy_change_pct`

---

## ▶️ How to Run

1. Clone the repository
2. Import your source tables (`users`, `cards`, `transactions`) into MySQL
3. Create `master_data` by joining all three tables
4. Run queries in order — Phase 1 → Phase 2 → Phase 3
5. Phase 3 scoring query is self-contained and references no external tables beyond `master_data`

---

## 🔗 Related Projects

This dataset also powers:
- **Customer Financial Health Analysis** *(coming soon)*
- **Spending Behaviour & Merchant Insights** *(coming soon)*

---

*Built with MySQL | Dataset: Synthetic financial transaction data*
