from fastapi import FastAPI
from pydantic import BaseModel
from dotenv import load_dotenv
import uuid
from pymongo import MongoClient
from typing import Optional
from openai import AzureOpenAI
import os
import uvicorn

from conv_handleing import conv_history, inserting_chat_buffer, agents_conv_history
from agentic import manager
from conv_to_pdf import conversation_to_pdf

load_dotenv(override=False)
app = FastAPI()

# Extend the ChatRequest to optionally include a conversation_id
class ChatRequest(BaseModel):
    user_prompt: str
    conversation_id: Optional[str] = None

connection_string = "mongodb://chat-history-with-cosmos:aWQkNybTHAZ4ZHgYXGNb4E2VDQ2BGP8k0WYyGPuziM4D5TayG2Pf5fnxFSD8Y3nI6wmXJvph3In1ACDbKj2jRQ==@chat-history-with-cosmos.mongo.cosmos.azure.com:10255/?ssl=true&replicaSet=globaldb&retrywrites=false&maxIdleTimeMS=120000&appName=@chat-history-with-cosmos@"
mongo_client = MongoClient(connection_string)
db = mongo_client["ChatHistoryDatabase"]
connection = db["chat-history-with-cosmos"]
chat_history_retrieval_limit = 10

endpoint = os.getenv("AZURE_OPENAI_ENDPOINT")
deployment = os.getenv("AZURE_OPENAI_DEPLOYED_NAME")
api_key = os.getenv("AZURE_OPENAI_KEY")

client =  AzureOpenAI(
    api_key=api_key,
    azure_endpoint=endpoint,
    api_version="2024-05-01-preview",
)

def agentic_flow(user_prompt,conversation_id):
    max_iterations = 3
    
    provided_conversation_history = conv_history(conversation_id, connection, chat_history_retrieval_limit)
    agents_conversation_id = None
    no_iterations = 0
    context_chunks = ""

    print(f"🟢  USER : {user_prompt}")
    final_response, iteratations, context_chunks =  manager(client, deployment, user_prompt, provided_conversation_history, max_iterations, connection, chat_history_retrieval_limit, no_iterations, context_chunks, agents_conversation_id)

    #print(f"🟢{iteratations} times the worker was asked to improve the response")
    #print(f"🔵chunks used:  {context_chunks}")
    print(f"🔴  MODEL : {final_response}")
    
    return final_response, context_chunks

@app.post("/chat")
def chat(request: ChatRequest):
    # Use provided conversation_id or generate a new one if missing
    conversation_id = request.conversation_id or str(uuid.uuid4())
    model_response, reference_points = agentic_flow(request.user_prompt, conversation_id)
    inserting_chat_buffer(conversation_id, connection, request.user_prompt, model_response, reference_points)
    
    return {
        "response": model_response,
        "references": reference_points,
        "conversation_id": conversation_id
    }

@app.get("/")
def home():
    return {"message": "Hello, World!"}

# pp = agents_conv_history("f4285eea-5126-473d-a9b7-e3d528a5d42d", connection, 10)
# conversation_to_pdf(pp,"hh")
