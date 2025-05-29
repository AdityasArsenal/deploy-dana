"""
ESGAI FastAPI Application

This module provides the HTTP API interface for the ESGAI agentic system.
It handles user requests, manages conversation state, and orchestrates the
multi-agent workflow for processing ESG-related queries.

Key Features:
- RESTful API endpoints for chat functionality
- Conversation history management with MongoDB
- CORS middleware for frontend integration
- Integration with the agentic workflow

Dependencies:
- agentic.py: Core agent orchestration logic
- tools/conv_handler.py: Conversation history management
- tools/conv_to_pdf_handler.py: PDF generation (via agentic flow)
- Azure OpenAI: LLM services
- MongoDB: Conversation persistence

Related Files:
- agentic.py: Main agentic workflow orchestration
- agents/*.py: Individual agent implementations
- tools/conv_handler.py: Database operations for conversations
"""

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
import os
from dotenv import load_dotenv
import uuid
from motor.motor_asyncio import AsyncIOMotorClient
from typing import Optional, Dict, Any, List, Tuple
from openai import AsyncAzureOpenAI  
import uvicorn
from tools.conv_handler import conv_history, inserting_chat_buffer
from agentic import manager
from tools.conv_to_pdf_handler import conversation_to_pdf

# Load environment variables from .env file
load_dotenv()
app = FastAPI()

# Configuration for conversation context
chat_history_retrieval_limit = 10 # number of previous conversation to be used by director agent to respond.

# CORS configuration for frontend integration
# Add CORS middleware
origins = [
    "http://localhost", # Allow localhost
    "http://localhost:5173", # Allow Vite default port
    "http://localhost:3000", # Allow common React dev port
    "https://www.esgai.space/", # Add your Vercel URL here
    "https://www.esgai.space",
    "https://deploy-dana-frontend-woj6-git-main-adityasarsenals-projects.vercel.app/",
    "https://esgai-frontend-fngrdkfke5h0aphb.eastus2-01.azurewebsites.net",
    "https://esgai-frontend-fngrdkfke5h0aphb.eastus2-01.azurewebsites.net/",
    "https://esgai-frontend-fngrdkfke5h0aphb.eastus2-01.azurewebsites.net/"
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

# Request model for chat endpoint
class ChatRequest(BaseModel):
    """
    Pydantic model for chat API requests
    
    Attributes:
        user_prompt (str): The user's question or message
        conversation_id (Optional[str]): Existing conversation ID for context
        new_session (Optional[bool]): Flag to start a new conversation session
        
    Related Files:
        - tools/conv_handler.py: Handles conversation persistence using this data
        - agentic.py: Processes the user_prompt through the agent workflow
    """
    user_prompt: str
    conversation_id: Optional[str] = None
    new_session: Optional[bool] = False

# MongoDB connection configuration
# Used by tools/conv_handler.py for conversation persistence
connection_string = os.getenv("MONGO_CONNECTION_STRING")
mongo_client = AsyncIOMotorClient(connection_string)
db = mongo_client["ChatHistoryDatabase"]
connection = db["chat-history-with-cosmos"]

# Azure OpenAI configuration
# Used by agentic.py and all agent modules
endpoint = os.getenv("AZURE_OPENAI_ENDPOINT")
deployment = os.getenv("AZURE_OPENAI_DEPLOYED_NAME")
api_key = os.getenv("AZURE_OPENAI_KEY")

llm_client = AsyncAzureOpenAI(
    api_key=api_key,
    azure_endpoint=endpoint,
    api_version="2024-05-01-preview"
)

async def agentic_flow(user_prompt: str, conversation_id: str) -> Tuple[str, List[str], str]:
    """
    Orchestrates the complete agentic workflow for processing user queries.
    
    This function serves as the main entry point for the multi-agent system,
    coordinating between conversation history retrieval and the manager agent.
    
    Args:
        user_prompt (str): The user's question to be processed
        conversation_id (str): Unique identifier for conversation context
        
    Returns:
        Tuple[str, List[str], str]: (final_response, all_context_chunks, agents_conv_pdf_url)
        
    Workflow:
        1. Retrieve conversation history via tools/conv_handler.py
        2. Invoke manager agent from agentic.py
        3. Return comprehensive response with context and PDF URL
        
    Related Files:
        - agentic.py: Contains the manager() function that orchestrates agents
        - tools/conv_handler.py: Provides conv_history() for context retrieval
        - agents/director_agent.py: Generates final response (called via agentic.py)
        - tools/conv_to_pdf_handler.py: Creates PDF summary (via director agent)
    """
    
    # Retrieve conversation history using tools/conv_handler.py
    provided_conversation_history = await conv_history(conversation_id, connection, chat_history_retrieval_limit)

    print(f"🟢  USER : {user_prompt}")
    
    # Execute the main agentic workflow from agentic.py
    # This coordinates: Manager -> Workers -> Director agents
    final_response, all_context_chunks, agents_conv_pdf_url = await manager(llm_client, deployment, user_prompt, provided_conversation_history, connection, chat_history_retrieval_limit, conversation_id)

    # print(f"🔴  MODEL : {final_response}")

    return final_response, all_context_chunks, agents_conv_pdf_url

@app.post("/chat")
async def chat(request: ChatRequest) -> Dict[str, Any]:
    """
    Main chat endpoint that processes user queries through the agentic system.
    
    This endpoint handles:
    - Conversation session management (new/existing)
    - Agentic workflow orchestration
    - Response persistence to database
    - Structured response formatting with references and PDF
    
    Args:
        request (ChatRequest): Contains user_prompt, conversation_id, and new_session flag
        
    Returns:
        Dict[str, Any]: JSON response containing:
            - response: Final synthesized answer from director agent
            - references: Context chunks used for response generation
            - conversation_id: Session identifier for frontend state management
            - agents_conv_pdf_url: Download link for conversation summary PDF
            
    Related Files:
        - agentic.py: Processes the request through manager agent
        - tools/conv_handler.py: Persists conversation via inserting_chat_buffer()
        - agents/director_agent.py: Generates final response (via agentic flow)
        - tools/conv_to_pdf_handler.py: Creates downloadable PDF summary
    """
    # Generate new conversation_id in these cases:
    # 1. new_session is True
    # 2. conversation_id is missing
    # 3. conversation_id is "string" (default value in Swagger UI)
    if request.new_session or not request.conversation_id or request.conversation_id == "string":
        conversation_id = str(uuid.uuid4())
    else:
        conversation_id = request.conversation_id
    
    # Execute the complete agentic workflow
    model_response, all_context_chunks, agents_conv_pdf_url = await agentic_flow(request.user_prompt, conversation_id)
    
    # Persist the conversation to database using tools/conv_handler.py
    await inserting_chat_buffer(conversation_id, connection, request.user_prompt, model_response, all_context_chunks)
    
    # Return structured response with all relevant data
    return {
        "response": model_response,
        "references": all_context_chunks,
        "conversation_id": conversation_id,
        "agents_conv_pdf_url" : agents_conv_pdf_url
    }

@app.get("/")
async def home() -> Dict[str, str]:
    """
    Health check endpoint for API status verification.
    
    Returns:
        Dict[str, str]: Simple status message
    """
    return {"message": "Hello, World!"}
