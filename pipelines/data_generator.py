import os
import random
import uuid
from datetime import datetime, timedelta
from typing import List, Dict, Generator
import pandas as pd
from faker import Faker

# Configure locale for realistic data
FAKE = Faker()

class BankingDataGenerator:
    """
    Generates synthetic banking data for testing Lakehouse architectures.
    Produces Customers, Accounts, and Transactions.
    """

    def __init__(self, output_dir: str = "data_source"):
        self.output_dir = output_dir
        self._ensure_directory()

    def _ensure_directory(self) -> None:
        """Creates the output directory if it does not exist."""
        if not os.path.exists(self.output_dir):
            os.makedirs(self.output_dir)

    def generate_customers(self, num_records: int) -> pd.DataFrame:
        """Generates a DataFrame of customer KYC data."""
        data = []
        for _ in range(num_records):
            data.append({
                "customer_id": str(uuid.uuid4()),
                "first_name": FAKE.first_name(),
                "last_name": FAKE.last_name(),
                "email": FAKE.email(),
                "address": FAKE.address().replace("\n", ", "),
                "created_at": FAKE.date_between(start_date="-2y", end_date="today")
            })
        return pd.DataFrame(data)

    def generate_accounts(self, customer_ids: List[str]) -> pd.DataFrame:
        """Generates financial accounts linked to existing customers."""
        data = []
        account_types = ["Savings", "Checking", "Investment"]
        
        for cid in customer_ids:
            # 1 to 3 accounts per customer
            num_accounts = random.randint(1, 3)
            for _ in range(num_accounts):
                data.append({
                    "account_id": str(uuid.uuid4()),
                    "customer_id": cid,
                    "account_type": random.choice(account_types),
                    "current_balance": round(random.uniform(100.00, 50000.00), 2),
                    "opened_at": FAKE.date_between(start_date="-2y", end_date="today")
                })
        return pd.DataFrame(data)

    def generate_transactions(self, account_ids: List[str], num_tx_per_account: int = 10) -> pd.DataFrame:
        """Generates ledger transactions for given accounts."""
        data = []
        tx_types = ["Debit", "Credit"]
        
        for aid in account_ids:
            for _ in range(num_tx_per_account):
                data.append({
                    "transaction_id": str(uuid.uuid4()),
                    "account_id": aid,
                    "amount": round(random.uniform(5.00, 3000.00), 2),
                    "transaction_type": random.choice(tx_types),
                    "transaction_date": FAKE.date_time_between(start_date="-1y", end_date="now")
                })
        return pd.DataFrame(data)

    def save_to_csv(self, df: pd.DataFrame, filename: str) -> None:
        """Persists data to CSV format mimicking a raw source system."""
        path = os.path.join(self.output_dir, filename)
        df.to_csv(path, index=False)
        print(f"File generated: {path} | Records: {len(df)}")

if __name__ == "__main__":
    print("--- Starting Banking Data Generation ---")
    
    # 1. Initialize Generator
    generator = BankingDataGenerator(output_dir="./landing_zone")
    
    # 2. Generate Customers
    df_customers = generator.generate_customers(100)
    generator.save_to_csv(df_customers, "customers.csv")
    
    # 3. Generate Accounts (Linked to Customers)
    customer_ids = df_customers["customer_id"].tolist()
    df_accounts = generator.generate_accounts(customer_ids)
    generator.save_to_csv(df_accounts, "accounts.csv")
    
    # 4. Generate Transactions (Linked to Accounts)
    account_ids = df_accounts["account_id"].tolist()
    df_transactions = generator.generate_transactions(account_ids, num_tx_per_account=20)
    generator.save_to_csv(df_transactions, "transactions.csv")
    
    print("--- Data Generation Complete ---")
