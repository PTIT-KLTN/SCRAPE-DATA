from motor.motor_asyncio import AsyncIOMotorClient
from dotenv import load_dotenv
import os
from pymongo import MongoClient
load_dotenv(override=True)

uri = os.getenv("MONGO_URI")

_client = None
_sync_client = None

def get_db():
    global _client
    if _client is None:
        _client = AsyncIOMotorClient(uri)
        print(f"Connected to MongoDB successfully. URI: {uri}")
    return _client.markendation

def get_sync_db():
    """Get synchronous PyMongo database connection"""
    global _sync_client
    if _sync_client is None:
        _sync_client = MongoClient(uri)
        print(f"Connected to MongoDB (Sync) successfully. URI: {uri}")
    return _sync_client.markendation