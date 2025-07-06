from pymongo import MongoClient
import os
from dotenv import load_dotenv

# connection_string = "mongodb://chat-history-with-cosmos:aWQkNybTHAZ4ZHgYXGNb4E2VDQ2BGP8k0WYyGPuziM4D5TayG2Pf5fnxFSD8Y3nI6wmXJvph3In1ACDbKj2jRQ==@chat-history-with-cosmos.mongo.cosmos.azure.com:10255/?ssl=true&replicaSet=globaldb&retrywrites=false&maxIdleTimeMS=120000&appName=@chat-history-with-cosmos@"
# mongo_client = MongoClient(connection_string)
# db = mongo_client["ChatHistoryDatabase"]
# collection = db["chat-history-with-cosmos"]

# #conversation_id = "e2713eee-be3c-4896-8db6-c5e65d342c55"
# test_conv_id = "ae54d78a-e9ef-4189-b986-efabf1599b3f"
# conversation_id = "10deef48-464e-4987-9f1e-448383e3cbfb" #R

chat_history_retrieval_limit = 10
# chat_history_retrieved = list(collection.find({"id": test_conv_id}))

from datetime import datetime

load_dotenv()

connection_string = os.getenv("MONGO_CONNECTION_STRING")
mongo_client = MongoClient(connection_string)
db = mongo_client["ChatHistoryDatabase"]
collection = db["chat-history-with-cosmos"]

# Create datetime objects for start and end of the day
target_date = datetime(2025, 3, 21)
next_date = datetime(2025, 3, 22)  # To get full day until midnight

# Query for documents within the date range
chat_history_retrieved = list(collection.find({
    "timestamp": {
        "$gte": target_date,
        "$lt": next_date
    }
}))

recent_chat_history = chat_history_retrieved[-chat_history_retrieval_limit:] if chat_history_retrieved else []
provided_conversation_history = []

for doc in recent_chat_history:
    user_message = doc.get("manager_agent_prompt", "")
    ai_message = doc.get("worker_response", "")
    score = doc.get("score", "")
    provided_conversation_history.append({"role": "manager_agent_prompt", "content": user_message})
    provided_conversation_history.append({"role": "worker_response", "content": ai_message})
    provided_conversation_history.append({"role": "score","content": score})

def get_best_worker_response(conversation_history):
    c=0
    max_score = 0
    max_at = 0
    
    for i in range(len(conversation_history)):
        if conversation_history[i]['role'] == "score" and conversation_history[i]["content"] > max_score:
            max_score = conversation_history[i]["content"]
            max_at = i

    return conversation_history[max_at-2]['content']

print("here it is : ")
print("INTERNAL MONOLOG : ")
print("*****************************************************************************")
c=0
k = get_best_worker_response(provided_conversation_history)
print(k)
print("*****************************************************************************")
