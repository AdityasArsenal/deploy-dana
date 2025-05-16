from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from dotenv import load_dotenv
import uuid
from motor.motor_asyncio import AsyncIOMotorClient
from typing import Optional
from openai import AsyncAzureOpenAI
import os
import uvicorn

from conv_handleing import conv_history, inserting_chat_buffer, agents_conv_history
from agentic import manager
from conv_to_pdf import conversation_to_pdf

load_dotenv(override=False)
app = FastAPI()


chat_history_retrieval_limit = 10 # number of previous conversation to be used by manager agent


# Add CORS middleware
origins = [
    "http://localhost", # Allow localhost
    "http://localhost:5173", # Allow Vite default port
    "http://localhost:3000", # Allow common React dev port
    "https://www.esgai.space/", # Add your Vercel URL here
    "https://www.esgai.space",
    "https://deploy-dana-frontend-woj6-git-main-adityasarsenals-projects.vercel.app/"
    # Add the deployed frontend URL here if applicable
    # "https://your-frontend-domain.com",
]

app.add_middleware(
    CORSMiddleware,
    allow_origins=origins, # List of allowed origins
    allow_credentials=True,
    allow_methods=["*"], # Allow all methods (GET, POST, etc.)
    allow_headers=["*"], # Allow all headers
)

# Extend the ChatRequest to optionally include a conversation_id
class ChatRequest(BaseModel):
    user_prompt: str
    conversation_id: Optional[str] = None

connection_string = "mongodb://chat-history-with-cosmos:aWQkNybTHAZ4ZHgYXGNb4E2VDQ2BGP8k0WYyGPuziM4D5TayG2Pf5fnxFSD8Y3nI6wmXJvph3In1ACDbKj2jRQ==@chat-history-with-cosmos.mongo.cosmos.azure.com:10255/?ssl=true&replicaSet=globaldb&retrywrites=false&maxIdleTimeMS=120000&appName=@chat-history-with-cosmos@"
mongo_client = AsyncIOMotorClient(connection_string)
db = mongo_client["ChatHistoryDatabase"]
connection = db["chat-history-with-cosmos"]

endpoint = os.getenv("AZURE_OPENAI_ENDPOINT")
deployment = os.getenv("AZURE_OPENAI_DEPLOYED_NAME")
api_key = os.getenv("AZURE_OPENAI_KEY")

client = AsyncAzureOpenAI(
    api_key=api_key,
    azure_endpoint=endpoint,
    api_version="2024-05-01-preview",
)

async def agentic_flow(user_prompt,conversation_id):
    
    provided_conversation_history = await conv_history(conversation_id, connection, chat_history_retrieval_limit)

    print(f"🟢  USER : {user_prompt}")
    final_response, all_context_chunks, agents_conv_pdf_url = await manager(client, deployment, user_prompt, provided_conversation_history, connection, chat_history_retrieval_limit, conversation_id)

    #print(f"🟢{iteratations} times the worker was asked to improve the response")
    #print(f"🔵chunks used:  {context_chunks}")
    print(f"🔴  MODEL : {final_response}")
    
    
    return final_response, all_context_chunks, agents_conv_pdf_url

@app.post("/chat")
async def chat(request: ChatRequest):
    # Use provided conversation_id or generate a new one if missing
    conversation_id = request.conversation_id or str(uuid.uuid4())
    model_response, all_context_chunks, agents_conv_pdf_url = await agentic_flow(request.user_prompt, conversation_id)
    await inserting_chat_buffer(conversation_id, connection, request.user_prompt, model_response, all_context_chunks)
    
    return {
        "response": model_response,
        "references": all_context_chunks,
        "conversation_id": conversation_id,
        "agents_conv_pdf_url" : agents_conv_pdf_url
    }

@app.get("/")
async def home():
    return {"message": "Hello, World!"}
