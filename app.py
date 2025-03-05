from fastapi import FastAPI
from pydantic import BaseModel
import os
from openai import AzureOpenAI
from azure.identity import DefaultAzureCredential, get_bearer_token_provider
from dotenv import load_dotenv
from azure.cosmos import CosmosClient, PartitionKey
import uuid
from datetime import datetime
from pymongo import MongoClient
from typing import Optional
import platform

# Check if the operating system is Windows
if platform.system() == "Windows":
    import win32api  # Example import from pywin32
    # Initialize or configure Windows-specific functionality here
else:
    # Initialize or configure alternative functionality for non-Windows systems
    pass


# Load environment variables
load_dotenv()
app = FastAPI()

# Azure configuration
endpoint = os.getenv("AZURE_OPENAI_ENDPOINT")
deployment = os.getenv("AZURE_OPENAI_DEPLOYED_NAME")
azure_search_endpoint = os.getenv("AZURE_AI_SEARCH_ENDPOINT")
azure_search_index = os.getenv("AZURE_AI_SEARCH_INDEX")
azure_search_api_key = os.getenv("AZURE_SEARCH_API_KEY")
system_prompt = "You are a factual AI assistant that answers solely from provided documents and conversation context. Use only verified source data.Stay precise, concise, and context-driven."

# Cosmos DB configuration using MongoDB API
connection_string = "mongodb://chat-history-with-cosmos:aWQkNybTHAZ4ZHgYXGNb4E2VDQ2BGP8k0WYyGPuziM4D5TayG2Pf5fnxFSD8Y3nI6wmXJvph3In1ACDbKj2jRQ==@chat-history-with-cosmos.mongo.cosmos.azure.com:10255/?ssl=true&replicaSet=globaldb&retrywrites=false&maxIdleTimeMS=120000&appName=@chat-history-with-cosmos@"
mongo_client = MongoClient(connection_string)
db = mongo_client["ChatHistoryDtabse"]
collection = db["chat-history-with-cosmos"]
chat_history_retrival_limit = 10

# Token authentication for Azure OpenAI
token_provider = get_bearer_token_provider(
    DefaultAzureCredential(), "https://cognitiveservices.azure.com/.default"
)
ai_client = AzureOpenAI(
    azure_endpoint=endpoint,
    azure_ad_token_provider=token_provider,
    api_version="2024-05-01-preview",
)

# Extend the ChatRequest to optionally include a conversation_id
class ChatRequest(BaseModel):
    user_prompt: str
    conversation_id: Optional[str] = None

def play_ground(
    client,
    deployment,
    user_prompt,
    azure_search_endpoint,
    azure_search_index,
    azure_search_api_key,
    conversation_id
):   
    # Retrieve the chat history documents using the conversation id
    chat_history_retrieved = list(collection.find({"id": conversation_id}))

    recent_chat_history = chat_history_retrieved[-chat_history_retrival_limit:] if chat_history_retrieved else []
    
    provided_conversation_history = []
    for doc in recent_chat_history:
        user_message = doc.get("user_prompt", "")
        ai_message = doc.get("model_response", "")
        provided_conversation_history.append({"role": "user", "content": user_message})
        provided_conversation_history.append({"role": "assistant", "content": ai_message})
    
    completion = client.chat.completions.create(
        model=deployment,
        messages=[
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": f"Previous conversation: {provided_conversation_history}, my question: {user_prompt}"}
        ],
        max_tokens=800,
        temperature=0.7,
        top_p=0.95,
        frequency_penalty=0,
        presence_penalty=0,
        stop=None,
        extra_body={
            "data_sources": [
                {
                    "type": "azure_search",
                    "parameters": {
                        "endpoint": azure_search_endpoint,
                        "index_name": azure_search_index,
                        "authentication": {
                            "type": "api_key",
                            "key": azure_search_api_key
                        }
                    }
                }
            ]
        }
    )
    
    # Extract the response and context citations
    response_message = completion.choices[0].message.content
    context_chunks = [citation['content'] for citation in completion.choices[0].message.context.get('citations', [])]
    
    print("Response:")
    print(response_message)
    print("\nContext Chunks:")
    print(context_chunks)
    print(f"Chat history retrieved: {provided_conversation_history}")
    
    return response_message, context_chunks

def inserting_chat_buffer(conversation_id, user_prompt, model_response, references):
    # Insert a chat document into the collection
    chat_history_doc = {
        "id": conversation_id,
        "user_prompt": user_prompt,
        "model_response": model_response,
        "timestamp": datetime.utcnow().isoformat(),
        "references": references
    }
    collection.insert_one(chat_history_doc)
    print("Document inserted:", chat_history_doc)

# API Endpoint to receive user input and return AI response
@app.post("/chat")
def chat(request: ChatRequest):
    # Use provided conversation_id or generate a new one if missing
    conversation_id = request.conversation_id or str(uuid.uuid4())
    
    model_response, reference_points = play_ground(
        ai_client,
        deployment,
        request.user_prompt,
        azure_search_endpoint,
        azure_search_index,
        azure_search_api_key,
        conversation_id
    )
    
    inserting_chat_buffer(conversation_id, request.user_prompt, model_response, reference_points)
    
    return {
        "response": model_response,
        "references": reference_points,
        "conversation_id": conversation_id
    }
