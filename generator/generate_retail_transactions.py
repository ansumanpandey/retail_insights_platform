import duckdb
import pandas as pd
import datetime
import random
from faker import Faker

# -----------------------
# Setup
# -----------------------
random.seed(42)
fake = Faker("en_IN")

con = duckdb.connect("retail_banking.duckdb")

today = datetime.date.today()
load_date = today.isoformat()

# -----------------------
# Card BINs
# -----------------------
card_bins = {
    "VISA": ["4111", "4213", "4319", "4514"],
    "MASTERCARD": ["5123", "5214", "5326", "5537"],
    "RUPAY": ["606156", "608014", "607626", "606333"]
}

# -----------------------
# Merchant Data (MCC-based)
# -----------------------
merchant_data = [
    ("5411", "Groceries", ["Big Bazaar", "DMart", "Reliance Fresh", "Spencer's"]),
    ("5812", "Restaurants", ["KFC", "McDonalds", "Bikanervala", "Dominos"]),
    ("5541", "Fuel", ["Indian Oil", "HP Petrol Pump", "Bharat Petroleum"]),
    ("4814", "Telecom", ["Airtel", "Jio", "Vodafone Idea"]),
    ("5732", "Electronics", ["Croma", "Reliance Digital", "Vijay Sales"]),
    ("5651", "Clothing", ["Zara", "H&M", "Pantaloons"]),
    ("4111", "Transport", ["Ola", "Uber", "Rapido"]),
    ("5999", "Online Shopping", ["Amazon", "Flipkart", "Myntra"]),
    ("5912", "Pharmacy", ["Apollo Pharmacy", "MedPlus", "1mg"]),
    ("7832", "Cinema", ["PVR Cinemas", "INOX", "Carnival"])
]

# -----------------------
# City Weighting
# -----------------------
cities = [
    ("Mumbai", 0.20),
    ("Delhi", 0.18),
    ("Bangalore", 0.16),
    ("Hyderabad", 0.12),
    ("Chennai", 0.10),
    ("Pune", 0.08),
    ("Ahmedabad", 0.06),
    ("Jaipur", 0.05),
    ("Indore", 0.03),
    ("Kochi", 0.02)
]

def weighted_choice(options):
    return random.choices(
        population=[x[0] for x in options],
        weights=[x[1] for x in options],
        k=1
    )[0]

# -----------------------
# Generate Transactions
# -----------------------
transactions = []
NUM_TRANSACTIONS = 20000

for i in range(NUM_TRANSACTIONS):

    card_network = random.choice(["VISA", "MASTERCARD", "RUPAY"])
    bin_prefix = random.choice(card_bins[card_network])

    pan = f"{bin_prefix}{random.randint(1000000000, 9999999999)}"
    last4 = pan[-4:]
    masked_pan = f"{pan[:6]}******{last4}"

    mcc, category, merchants = random.choice(merchant_data)
    merchant_name = random.choice(merchants)

    if category == "Groceries":
        amount = random.uniform(300, 3000)
    elif category == "Fuel":
        amount = random.uniform(800, 4000)
    elif category == "Restaurants":
        amount = random.uniform(200, 2500)
    elif category == "Online Shopping":
        amount = random.uniform(500, 20000)
    elif category == "Electronics":
        amount = random.uniform(2000, 60000)
    else:
        amount = random.uniform(100, 5000)

    amount = round(amount, 2)

    # Refund logic
    if random.random() < 0.02:
        txn_type = "REFUND"
        amount = round(amount * random.uniform(0.3, 1.0), 2)
    else:
        txn_type = "DEBIT"

    # Fraud logic
    rnd = random.random()
    if rnd < 0.0004:
        fraud = "CONFIRMED"
    elif rnd < 0.005:
        fraud = "SUSPECTED"
    else:
        fraud = "NO_FRAUD"

    # Time patterns
    hour = random.choices(
        population=[9, 10, 11, 13, 18, 19, 20, 21],
        weights=[5, 10, 12, 15, 10, 12, 20, 16],
        k=1
    )[0]

    txn_time = datetime.datetime.now().replace(
        hour=hour,
        minute=random.randint(0, 59),
        second=random.randint(0, 59),
        microsecond=0
    )

    transactions.append({
        "transaction_id": f"TXN{today.strftime('%Y%m%d')}{i:06d}",
        "customer_id": random.randint(100000, 999999),
        "card_network": card_network,
        "masked_pan": masked_pan,
        "card_last4": last4,
        "transaction_type": txn_type,
        "posting_date": today,
        "transaction_timestamp": txn_time,
        "mcc": mcc,
        "merchant_category": category,
        "merchant_name": merchant_name,
        "city": weighted_choice(cities),
        "country": "India",
        "amount": amount,
        "fraud_flag": fraud
    })

# -----------------------
# Load into DuckDB
# -----------------------
df = pd.DataFrame(transactions)
df["load_date"] = load_date

con.execute("""
CREATE TABLE IF NOT EXISTS retail_card_transactions (
    transaction_id VARCHAR,
    customer_id INTEGER,
    card_network VARCHAR,
    masked_pan VARCHAR,
    card_last4 VARCHAR,
    transaction_type VARCHAR,
    posting_date DATE,
    transaction_timestamp TIMESTAMP,
    mcc VARCHAR,
    merchant_category VARCHAR,
    merchant_name VARCHAR,
    city VARCHAR,
    country VARCHAR,
    amount DOUBLE,
    fraud_flag VARCHAR,
    load_date DATE
)
""")

# Idempotent load
con.execute(
    "DELETE FROM retail_card_transactions WHERE load_date = ?",
    [load_date]
)

con.execute("INSERT INTO retail_card_transactions SELECT * FROM df")

print(f"Inserted {len(df)} transactions for load_date={load_date}")
