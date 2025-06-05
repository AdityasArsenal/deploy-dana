"""
ESGAI Agentic Orchestration Module

This module implements the core agentic workflow for processing ESG-related queries.
It coordinates between multiple specialized agents to break down complex questions,
retrieve relevant information, and synthesize comprehensive responses.

Dependencies:
- agents/director_agent.py: Final response synthesis
- agents/sub_question_handler.py: Sub-question processing middleware  
- agents/worker_agent.py: Individual question processing (via sub_question_handler)
- tools/conv_handler.py: Conversation history management
- tools/conv_to_pdf_handler.py: PDF generation and upload
- tools/json_parseing.py: LLM response parsing
- prompts/*.txt: System prompts for each agent type

External Dependencies:
- Azure OpenAI: LLM processing
- Azure AI Search: Vector/semantic search via tools/v_search.py
- MongoDB: Conversation persistence via tools/conv_handler.py
"""

import json
from agents.worker_agent import worker
import os
import uuid
import asyncio
from typing import List, Dict, Any, Tuple
from openai import AsyncAzureOpenAI
from motor.motor_asyncio import AsyncIOMotorCollection
from azure.search.documents import SearchClient
from tools.conv_handler import get_agents_conv_history, inserting_agent_chat_buffer, monolog, get_best_worker_response, get_agents_total_conv_history
from tools.conv_to_pdf_handler import conversation_to_pdf, upload_pdf_to_blob
from tools.json_parseing import parse_json_from_model_response
from agents.director_agent import director
from agents.sub_question_handler import process_sub_question
from dotenv import load_dotenv
from azure.search.documents import SearchClient
from azure.core.credentials import AzureKeyCredential

# Load environment variables for API keys and configuration
load_dotenv()

# Configuration parameters


limit_subquestions = 10
top_k = 10

# Load system prompts from text files
def load_prompt_from_file(file_path: str) -> str:
    """
    Load prompt content from a text file with UTF-8 encoding
    
    Args:
        file_path (str): Path to the prompt file in prompts/ directory
        
    Returns:
        str: Content of the prompt file
        
    Related Files:
        - prompts/manager_system_prompt.txt: Manager agent instructions
        - prompts/worker_system_prompt.txt: Worker agent instructions  
        - prompts/director_system_prompt.txt: Director agent instructions
    """
    with open(file_path, 'r', encoding='utf-8') as file:
        return file.read()

# Load system prompts for each agent type
# These prompts define the behavior and capabilities of each agent
director_system_prompt = load_prompt_from_file('./prompts/director_system_prompt.txt')
manager_system_prompt = load_prompt_from_file('./prompts/1manager_system_prompt.txt').replace('{limit_subquestions}', str(limit_subquestions))
worker_system_prompt = load_prompt_from_file('./prompts/worker_system_prompt.txt')

# Azure AI Search configuration
# Used by tools/v_search.py for semantic hybrid search
azure_search_endpoint = os.getenv("AI_SEARCH_ENDPOINT")
azure_search_index = os.getenv("AI_SEARCH_INDEX")
azure_search_api_key = os.getenv("AI_SEARCH_API_KEY")
#Azure AI search client
search_client = SearchClient(endpoint = azure_search_endpoint, index_name = azure_search_index, credential = AzureKeyCredential(azure_search_api_key))

async def manager(
    llm_client: AsyncAzureOpenAI,
    deployment: str,
    user_prompt: str,
    user_conversation_history: List[Dict[str, str]],
    connection: AsyncIOMotorCollection,
    chat_history_retrieval_limit: int,
    conversation_id: str
) -> Tuple[str, List[str], str]:   
    """
    Manager agent that orchestrates the entire agentic workflow.
    
    This function implements the core logic for:
    1. Breaking down user questions into targeted sub-questions
    2. Identifying relevant companies for filtering search results
    3. Coordinating parallel processing by worker agents
    4. Collecting and aggregating information from all workers
    5. Invoking the director agent for final response synthesis
    
    Args:
        llm_client (AsyncAzureOpenAI): Azure OpenAI client for LLM interactions
        deployment (str): Azure OpenAI deployment name
        user_prompt (str): The user's original question
        user_conversation_history (List[Dict[str, str]]): Previous conversation context
        connection (AsyncIOMotorCollection): MongoDB connection for data persistence
        chat_history_retrieval_limit (int): Number of previous messages to include
        conversation_id (str): Unique identifier for this conversation
        
    Returns:
        Tuple[str, List[str], str]: (director_response, all_context_chunks, conv_pdf_url)
        
    Workflow:
        1. Generate sub-questions using manager_system_prompt
        2. Parse JSON response using tools/json_parseing.py
        3. Process sub-questions via agents/sub_question_handler.py
        4. Collect context chunks from all worker responses
        5. Synthesize final response via agents/director_agent.py
        
    Related Files:
        - agents/sub_question_handler.py: Processes individual sub-questions
        - agents/director_agent.py: Synthesizes final response
        - agents/worker_agent.py: Handles individual question processing
        - tools/json_parseing.py: Parses LLM JSON responses
        - tools/v_search.py: Performs semantic search (via worker agents)
    """

    # Generate unique conversation ID for tracking agent interactions
    agents_conversation_id = str(uuid.uuid4())
    print(f"- Vector search index = {azure_search_index} AND agents_convertation_id = {agents_conversation_id}")
    
    # Generate sub-questions based on user query
    # This uses the manager_system_prompt loaded from prompts/manager_system_prompt.txt
    completion = await llm_client.chat.completions.create(
        model=deployment,
        messages=[
            {"role": "system", "content": manager_system_prompt},
            {"role": "user", "content": f"Previous conversation between user and you: {user_conversation_history},\n user's question: {user_prompt}"},
        ],
        max_tokens=800,
        temperature=0.7,
        top_p=0.95,
        frequency_penalty=0,
        presence_penalty=0,
        stop=None
    )

    manager_json_output = completion.choices[0].message.content
    
    # Parse the model response using tools/json_parseing.py
    required_keys = ["list_of_sub_questions", "company_names"]
    normalized_manager_response, error = parse_json_from_model_response(manager_json_output, required_keys)
    
    #if the json parsing fails, then fallback to the default questions
    if error:
        print(f"Using fallback questions due to parsing error: {error}")
        # Fallback to default questions if parsing fails
        list_of_sub_questions = [
            f"What are the carbon emissions of Hindustan Petroleum Corporation Limited?",
            f"What are the carbon emissions of Indian Oil Corporation Limited?",
            f"What is the greenhouse gas emission data for Hindustan Petroleum Corporation Limited?"
        ]
        company_names = ["Hindustan Petroleum Corporation Limited"]
    
    #or else use the questions from the model response
    else:
        list_of_sub_questions = normalized_manager_response["list_of_sub_questions"]
        company_names = normalized_manager_response["company_names"]

    tasks = [
        process_sub_question(
            llm_client, 
            deployment, 
            sub_question, 
            company_names, 
            search_client, 
            worker_system_prompt, 
            top_k,
            agents_conversation_id,
            conversation_id,
            connection
        ) 
        for sub_question in list_of_sub_questions
    ]
    
    # Run all tasks concurrently for efficient processing
    print(f"- Processing {len(tasks)} sub-questions in parallel...")
    results = await asyncio.gather(*tasks)
    
    # Collect all context chunks from worker responses
    # These chunks contain the relevant information retrieved from the knowledge base
    all_context_chunks = []
    for _, context_chunks in results:
        all_context_chunks.extend(context_chunks)
    
    print(f"- Collected {len(all_context_chunks)} context chunks from all workers")

    # Generate final response from director agent using all collected information
    # The director agent (agents/director_agent.py) synthesizes all worker responses
    direcotr_response, conv_pdf_url = await director(
        llm_client,
        director_system_prompt,
        deployment,
        user_prompt,
        user_conversation_history,
        connection,
        all_context_chunks,
        agents_conversation_id,
        conversation_id
    )
    return direcotr_response, all_context_chunks, conv_pdf_url