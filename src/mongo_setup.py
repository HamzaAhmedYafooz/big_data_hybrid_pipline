from pymongo import MongoClient
from config.settings import MONGO_URI, MONGO_DB_NAME, COLLECTION_VALIDATED, COLLECTION_RAW, COLLECTION_QUARANTINE, COLLECTION_CHECKPOINTS

def setup_mongodb():
    print("⚙️  Setting up MongoDB...")
    client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=5000) #****************************
    db = client[MONGO_DB_NAME]

    print("---> Clearing old data from the database to ensure a clean run...")
    db[COLLECTION_RAW].drop()
    db[COLLECTION_VALIDATED].drop()
    db[COLLECTION_QUARANTINE].drop()
    db[COLLECTION_CHECKPOINTS].drop()

    # Creating Unique Index on order_id for idempotency
    print("---> Creating Unique Index on order_id field in orders_validated collection...")
    db[COLLECTION_VALIDATED].create_index("order_id", unique=True)
    print("✅ --------------------------------------")
    print("✅ Successfully set up MongoDB.")
    print("✅ --------------------------------------")
    client.close()

if __name__ == "__main__":
    setup_mongodb()