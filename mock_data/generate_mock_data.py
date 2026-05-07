from __future__ import annotations

import random
import string
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

import pandas as pd

SEED = 42
random.seed(SEED)

OUT_DIR = Path(__file__).parent
N_CUSTOMERS = 500
N_ACCOUNTS = 800
N_TRANSACTIONS = 5_000
N_FLAGS = 120


def _rand_date(start: date, end: date) -> date:
    delta = (end - start).days
    return start + timedelta(days=random.randint(0, delta))


def _rand_ts(start: date, end: date) -> datetime:
    d = _rand_date(start, end)
    return datetime(d.year, d.month, d.day, random.randint(0, 23), random.randint(0, 59), tzinfo=timezone.utc)


def _rand_str(length: int = 8) -> str:
    return "".join(random.choices(string.ascii_uppercase + string.digits, k=length))


customer_ids = [f"CUST{i:06d}" for i in range(1, N_CUSTOMERS + 1)]
rm_ids = [f"RM{i:04d}" for i in range(1, 21)]
today = date.today()

customers = pd.DataFrame(
    {
        "customer_id": customer_ids,
        "full_name": [f"Customer {i}" for i in range(1, N_CUSTOMERS + 1)],
        "date_of_birth": [str(_rand_date(date(1950, 1, 1), date(2000, 12, 31))) for _ in range(N_CUSTOMERS)],
        "email": [f"customer{i}@example.com" for i in range(1, N_CUSTOMERS + 1)],
        "phone": [f"+44{random.randint(7000000000, 7999999999)}" for _ in range(N_CUSTOMERS)],
        "country_code": random.choices(["GB", "US", "DE", "FR", "SG", "AE"], k=N_CUSTOMERS),
        "customer_segment": random.choices(["RETAIL", "CORPORATE", "PRIVATE_BANKING"], weights=[7, 2, 1], k=N_CUSTOMERS),
        "kyc_status": random.choices(["VERIFIED", "PENDING", "FAILED"], weights=[85, 12, 3], k=N_CUSTOMERS),
        "onboarded_date": [str(_rand_date(date(2015, 1, 1), today)) for _ in range(N_CUSTOMERS)],
        "relationship_manager_id": [random.choice(rm_ids) if random.random() > 0.3 else None for _ in range(N_CUSTOMERS)],
        "last_updated_ts": [str(_rand_ts(date(2024, 1, 1), today)) for _ in range(N_CUSTOMERS)],
    }
)

account_ids = [f"ACC{i:08d}" for i in range(1, N_ACCOUNTS + 1)]

accounts = pd.DataFrame(
    {
        "account_id": account_ids,
        "customer_id": random.choices(customer_ids, k=N_ACCOUNTS),
        "account_type": random.choices(["CURRENT", "SAVINGS", "LOAN", "INVESTMENT"], weights=[5, 3, 1, 1], k=N_ACCOUNTS),
        "currency": random.choices(["GBP", "USD", "EUR"], weights=[6, 3, 1], k=N_ACCOUNTS),
        "balance": [round(random.uniform(-5_000, 500_000), 2) for _ in range(N_ACCOUNTS)],
        "credit_limit": [round(random.choice([0, 5_000, 10_000, 25_000, 50_000]), 2) for _ in range(N_ACCOUNTS)],
        "opened_date": [str(_rand_date(date(2015, 1, 1), today)) for _ in range(N_ACCOUNTS)],
        "closed_date": [str(_rand_date(date(2020, 1, 1), today)) if random.random() < 0.05 else None for _ in range(N_ACCOUNTS)],
        "account_status": random.choices(["ACTIVE", "DORMANT", "CLOSED", "FROZEN"], weights=[80, 10, 8, 2], k=N_ACCOUNTS),
        "branch_code": [f"BR{random.randint(100, 999)}" for _ in range(N_ACCOUNTS)],
        "last_updated_ts": [str(_rand_ts(date(2024, 1, 1), today)) for _ in range(N_ACCOUNTS)],
    }
)

transactions = pd.DataFrame(
    {
        "transaction_id": [f"TXN{i:010d}" for i in range(1, N_TRANSACTIONS + 1)],
        "account_id": random.choices(account_ids, k=N_TRANSACTIONS),
        "transaction_date": [str(_rand_ts(date(2024, 1, 1), today)) for _ in range(N_TRANSACTIONS)],
        "value_date": [str(_rand_ts(date(2024, 1, 1), today)) for _ in range(N_TRANSACTIONS)],
        "transaction_type": random.choices(["CREDIT", "DEBIT"], weights=[4, 6], k=N_TRANSACTIONS),
        "amount": [round(abs(random.gauss(500, 800)), 2) for _ in range(N_TRANSACTIONS)],
        "currency": random.choices(["GBP", "USD", "EUR"], weights=[6, 3, 1], k=N_TRANSACTIONS),
        "merchant_category_code": [str(random.randint(1000, 9999)) for _ in range(N_TRANSACTIONS)],
        "counterparty_id": [f"CP{random.randint(1000, 9999)}" if random.random() > 0.1 else None for _ in range(N_TRANSACTIONS)],
        "channel": random.choices(["BRANCH", "ATM", "ONLINE", "POS"], weights=[1, 2, 5, 4], k=N_TRANSACTIONS),
        "reference": [f"REF{_rand_str(10)}" for _ in range(N_TRANSACTIONS)],
        "last_updated_ts": [str(_rand_ts(date(2024, 1, 1), today)) for _ in range(N_TRANSACTIONS)],
    }
)

risk_flags = pd.DataFrame(
    {
        "flag_id": [f"FLAG{i:06d}" for i in range(1, N_FLAGS + 1)],
        "customer_id": random.choices(customer_ids, k=N_FLAGS),
        "account_id": [random.choice(account_ids) if random.random() > 0.2 else None for _ in range(N_FLAGS)],
        "flag_type": random.choices(["AML", "FRAUD", "PEP", "SANCTION", "ADVERSE_MEDIA"], weights=[3, 4, 1, 1, 1], k=N_FLAGS),
        "severity": random.choices(["LOW", "MEDIUM", "HIGH", "CRITICAL"], weights=[4, 3, 2, 1], k=N_FLAGS),
        "flag_status": random.choices(["OPEN", "UNDER_REVIEW", "CLOSED", "ESCALATED"], weights=[4, 3, 2, 1], k=N_FLAGS),
        "raised_date": [str(_rand_ts(date(2023, 1, 1), today)) for _ in range(N_FLAGS)],
        "closed_date": [str(_rand_ts(date(2024, 1, 1), today)) if random.random() < 0.2 else None for _ in range(N_FLAGS)],
        "raised_by": [f"USER{random.randint(100, 999)}" for _ in range(N_FLAGS)],
        "description": [f"Flag description {i}" for i in range(1, N_FLAGS + 1)],
        "last_updated_ts": [str(_rand_ts(date(2024, 1, 1), today)) for _ in range(N_FLAGS)],
    }
)

for name, df in [("customers", customers), ("accounts", accounts), ("transactions", transactions), ("risk_flags", risk_flags)]:
    path = OUT_DIR / f"{name}.csv"
    df.to_csv(path, index=False)
    print(f"Wrote {len(df):,} rows → {path}")
