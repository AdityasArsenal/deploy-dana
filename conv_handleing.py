import os
from datetime import datetime
from pymongo import MongoClient
    
def inserting_chat_buffer(conversation_id, collection, user_prompt, model_response, reference_points):
    # Insert a chat document into the collection
    chat_history_doc = {
        "id": conversation_id,
        "user_prompt": user_prompt,
        "model_response": model_response,
        "timestamp": datetime.utcnow().isoformat(),
        "references": reference_points
    }
    collection.insert_one(chat_history_doc)

def conv_history(conversation_id, collection, chat_history_retrieval_limit):
    chat_history_retrieved = list(collection.find({"id": conversation_id}))
    
    recent_chat_history = chat_history_retrieved[-chat_history_retrieval_limit:] if chat_history_retrieved else []
    provided_conversation_history = []
    
    for doc in recent_chat_history:
        user_message = doc.get("user_prompt", "")
        ai_message = doc.get("model_response", "")
        provided_conversation_history.append({"role": "user", "content": user_message})
        provided_conversation_history.append({"role": "assistant", "content": ai_message})
    
    return provided_conversation_history

def inserting_agent_chat_buffer(agents_conversation_id, conversation_id, collection, sub_question, worker_response, context_chunks):
    chat_history_doc = {
        "id": agents_conversation_id,
        "tid": conversation_id,
        "sub_question": sub_question,
        "worker_response": worker_response,
        "timestamp": datetime.utcnow().isoformat(),
        "references": context_chunks
    }
    collection.insert_one(chat_history_doc)

def agents_conv_history(agents_conversation_id, collection, chat_history_retrieval_limit):
    chat_history_retrieved = list(collection.find({"id": agents_conversation_id}))

    recent_chat_history = chat_history_retrieved[-chat_history_retrieval_limit:] if chat_history_retrieved else []
    provided_conversation_history = []
    
    for doc in recent_chat_history:
        manager_agent_message = doc.get("sub_question", "")
        worker_agent_message = doc.get("worker_response", "")
        provided_conversation_history.append({"role": "manager_agent", "content": f"subquestion = {manager_agent_message}"})
        provided_conversation_history.append({"role": "worker_agent", "content": f"answer ={worker_agent_message}"})

    return provided_conversation_history

def agents_total_conv_history(conversation_id, collection, chat_history_retrieval_limit):
    chat_history_retrieved = list(collection.find({"tid": conversation_id}))

    recent_chat_history = chat_history_retrieved[-chat_history_retrieval_limit:] if chat_history_retrieved else []
    provided_conversation_history = []
    
    for doc in recent_chat_history:
        manager_agent_message = doc.get("sub_question", "")
        worker_agent_message = doc.get("worker_response", "")
        provided_conversation_history.append({"role": "manager_agent", "content": f"subquestion = {manager_agent_message}"})
        provided_conversation_history.append({"role": "worker_agent", "content": f"answer ={worker_agent_message}"})

    return provided_conversation_history

def monolog(provided_conversation_history):
    print("INTERNAL MONOLOG : ")
    print("*****************************************************************************")
    c=0
    for i in provided_conversation_history:
        if i['role'] == 'manager_agent':
            prefix = "⚪" 
        elif i['role'] == 'worker_agent':
            prefix = "⚫"
        print(f"{prefix} : {i['content']}") 
        
        c+=1
    print("*****************************************************************************")

def get_best_worker_response(conversation_history):
    c=0
    max_score = 0
    max_at = 0
    
    for i in range(len(conversation_history)):
        if conversation_history[i]['role'] == "score" and conversation_history[i]["content"] > max_score:
            max_score = conversation_history[i]["content"]
            max_at = i

    return conversation_history[max_at-2]['content']

# connection_string = "mongodb://chat-history-with-cosmos:aWQkNybTHAZ4ZHgYXGNb4E2VDQ2BGP8k0WYyGPuziM4D5TayG2Pf5fnxFSD8Y3nI6wmXJvph3In1ACDbKj2jRQ==@chat-history-with-cosmos.mongo.cosmos.azure.com:10255/?ssl=true&replicaSet=globaldb&retrywrites=false&maxIdleTimeMS=120000&appName=@chat-history-with-cosmos@"
# mongo_client = MongoClient(connection_string)
# db = mongo_client["ChatHistoryDatabase"]
# collection = db["chat-history-with-cosmos"]

# chat_history_retrieval_limit = 10
# conversation_id = "10deef48-464e-4987-9f1e-448383e3cbfb" 

# conv_history(conversation_id, collection, chat_history_retrieval_limit)
