import pandas as pd
import numpy as np
from faker import Faker
import random

fake = Faker()

# Parameters
n_jobs = 50
sites = ["Alpha HQ", "Beta Plant", "Gamma Office", "Delta Site", "Epsilon Plant", "Zeta Plant"]
services = ["Cleaning", "Maintenance", "Security"]
start_date = pd.to_datetime("2025-10-01")

# ----------------------------
# Generate client dataset
# ----------------------------
client_records = []
for i in range(n_jobs):
    record = {
        "order_id": f"C{i+1:03d}",
        "job_date": (start_date + pd.Timedelta(days=random.randint(0, 30))).strftime("%Y-%m-%d"),
        "site": random.choice(sites),
        "service_type": random.choice(services),
        "amount": round(random.uniform(1000, 2000), 2),
    }
    client_records.append(record)

client_df = pd.DataFrame(client_records)
client_df.to_csv("data/client_data.csv", index=False)

# ----------------------------
# Generate internal dataset with small variations
# ----------------------------
internal_records = []
for i, row in client_df.iterrows():
    # Sometimes introduce a minor site name difference
    site_name = row["site"]
    if random.random() < 0.2:
        site_name = site_name.replace("HQ", "Headquarters").replace("Office", "Off.")

    # Slight revenue variance
    revenue = row["amount"] * (1 + random.uniform(-0.02, 0.02))  # ±2%
    internal_records.append({
        "job_id": f"I{i+1000}",
        "job_date": row["job_date"],
        "site": site_name,
        "service_type": row["service_type"],
        "revenue": round(revenue, 2)
    })

# Add some internal-only jobs
for i in range(5):
    internal_records.append({
        "job_id": f"I{2000+i}",
        "job_date": (start_date + pd.Timedelta(days=random.randint(0,30))).strftime("%Y-%m-%d"),
        "site": random.choice(sites),
        "service_type": random.choice(services),
        "revenue": round(random.uniform(1000,2000),2)
    })

internal_df = pd.DataFrame(internal_records)
internal_df.to_csv("data/internal_data.csv", index=False)