from pymongo import MongoClient

def test_mongodb_connection():
    try:
        # Connect to MongoDB (default: localhost, port 27017)
        client = MongoClient("mongodb://localhost:27017/")
        
        # Access the test database
        db = client["test_database"]
        
        # Access a test collection
        collection = db["test_collection"]
        
        # Insert a sample document
        sample_document = {"name": "MongoDB Test", "status": "Connection Successful"}
        collection.insert_one(sample_document)
        print("Inserted document:", sample_document)
        
        # Retrieve the inserted document
        retrieved_document = collection.find_one({"name": "MongoDB Test"})
        print("Retrieved document:", retrieved_document)
        
        # Clean up (optional): Drop the test database
        client.drop_database("test_database")
        print("Test database dropped.")
        
    except Exception as e:
        print("Error connecting to MongoDB:", e)

# Run the test
test_mongodb_connection()
