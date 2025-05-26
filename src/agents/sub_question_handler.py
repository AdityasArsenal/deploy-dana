"""
Sub-Question Handler Module

This module serves as middleware between the Manager agent and Worker agents,
orchestrating the processing of individual sub-questions and managing data flow
and persistence throughout the agent interaction workflow.

Key Responsibilities:
- Coordinate between manager and worker agents
- Process individual sub-questions through worker agents
- Persist agent conversations to database
- Handle data flow and error management

Dependencies:
- agents/worker_agent.py: Core sub-question processing
- tools/conv_handler.py: Database persistence for agent conversations
- MongoDB: Conversation data storage

Related Files:
- agentic.py: Calls process_sub_question() for each generated sub-question
- agents/worker_agent.py: Executes the actual sub-question processing
- agents/director_agent.py: Uses the results for final synthesis
- tools/conv_handler.py: Provides inserting_agent_chat_buffer() for persistence
"""

from typing import List, Tuple
from openai import AsyncAzureOpenAI
from azure.search.documents import SearchClient
from motor.motor_asyncio import AsyncIOMotorCollection
from agents.worker_agent import worker
from tools.conv_handler import inserting_agent_chat_buffer

async def process_sub_question(
    llm_client: AsyncAzureOpenAI, 
    deployment: str, 
    sub_question: str, 
    company_names: List[str], 
    search_client: SearchClient, 
    worker_system_prompt: str, 
    top_k: int,
    agents_conversation_id: str,
    conversation_id: str,
    connection: AsyncIOMotorCollection
) -> Tuple[str, List[str]]:
    """
    Processes a single sub-question using worker agents and stores results in the database.
    
    This function acts as middleware in the agentic workflow, coordinating between
    the manager agent (which generates sub-questions) and worker agents (which process them).
    It also handles data persistence to ensure conversation continuity and traceability.
    
    Args:
        llm_client (AsyncAzureOpenAI): Azure OpenAI client for LLM interactions
        deployment (str): Azure OpenAI deployment name
        sub_question (str): Individual sub-question to be processed
        company_names (List[str]): List of companies for search filtering
        search_client (SearchClient): Azure AI Search client for semantic search
        worker_system_prompt (str): System prompt for worker agent behavior
        top_k (int): Number of context chunks to retrieve
        agents_conversation_id (str): Unique ID for this agent conversation session
        conversation_id (str): Overall conversation identifier
        connection (AsyncIOMotorCollection): MongoDB connection for data persistence
        
    Returns:
        Tuple[str, List[str]]: (worker_response, context_chunks)
            - worker_response (str): Worker agent's answer to the sub-question
            - context_chunks (List[str]): Source information used by the worker
            
    Workflow:
        1. Invoke worker agent via agents/worker_agent.py
        2. Process sub-question with semantic search and LLM response
        3. Persist conversation data via tools/conv_handler.py
        4. Return worker response and context for aggregation
        
    Related Files:
        - agentic.py: Creates tasks for each sub-question using this function
        - agents/worker_agent.py: Provides worker() function for sub-question processing
        - tools/conv_handler.py: Provides inserting_agent_chat_buffer() for persistence
        - agents/director_agent.py: Uses aggregated results for final synthesis
        - tools/v_search.py: Used by worker for semantic search (indirect dependency)
    """
    
    # Process the sub-question using the worker agent
    # This performs semantic search and generates a focused response
    worker_response, context_chunks = await worker(
        llm_client, 
        deployment, 
        sub_question, 
        company_names, 
        "", # agents_conversation_history - currently unused
        search_client, 
        worker_system_prompt, 
        top_k,
        conversation_id
    )
    
    # Persist the agent conversation to database using tools/conv_handler.py
    # This ensures conversation traceability and enables director agent context retrieval
    await inserting_agent_chat_buffer(
        agents_conversation_id, 
        conversation_id,
        connection,
        sub_question, 
        worker_response, 
        context_chunks
    )

    # print(conversation_id)
    # print(agents_conversation_id)
    # print("================================") 
    
    # Return worker response and context chunks for aggregation
    # These will be collected by the manager agent and passed to the director
    return worker_response, context_chunks