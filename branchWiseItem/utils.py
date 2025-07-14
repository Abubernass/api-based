
from pymongo import MongoClient
from bson import ObjectId

# MongoDB connection and collection getter for item groups
# def get_branchWiseItem_collection():
#     client = MongoClient("mongodb://localhost:27017/")
#     db = client["reactfluttertest"]  # Adjust database name as per your MongoDB setup
#     return db['branchWiseItem']  