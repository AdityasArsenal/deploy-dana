"""
Conversation Handler Module

This module manages conversation persistence and retrieval for the ESGAI system,
handling both user conversations and internal agent interactions. It provides
the database layer for maintaining conversation context and history.

Key Responsibilities:
- Persist user conversations to MongoDB
- Retrieve conversation history for context
- Manage agent conversation data for internal tracking
- Format conversation data for different use cases

Dependencies:
- MongoDB (Azure Cosmos DB): Primary data storage
- PyMongo: Database driver for async operations

Related Files:
- app.py: Uses conv_history() and inserting_chat_buffer() for user conversations
- agents/sub_question_handler.py: Uses inserting_agent_chat_buffer() for agent data
- agents/director_agent.py: Uses get_agents_conv_history() and get_agents_total_conv_history()
- tools/conv_to_pdf_handler.py: Uses conversation data for PDF generation

External Dependencies:
- Azure Cosmos DB with MongoDB API
- Collection structure: user conversations and agent interactions
"""

import os
from datetime import datetime
from typing import List, Dict, Any, Optional
from motor.motor_asyncio import AsyncIOMotorCollection

async def inserting_chat_buffer(
    conversation_id: str, 
    collection: AsyncIOMotorCollection, 
    user_prompt: str, 
    model_response: str, 
    reference_points: List[str]
) -> None:
    """
    Insert user conversation data into the MongoDB collection.
    
    This function persists user interactions with the system, including the
    user's question, the final model response, and reference materials used.
    Essential for maintaining conversation context across sessions.
    
    Args:
        conversation_id (str): Unique identifier for the conversation session
        collection (AsyncIOMotorCollection): MongoDB collection for data storage
        user_prompt (str): The user's original question or message
        model_response (str): Final synthesized response from director agent
        reference_points (List[str]): Context chunks used for response generation
        
    Returns:
        None
        
    Related Files:
        - app.py: Calls this function after agentic workflow completion
        - agents/director_agent.py: Provides model_response (via app.py)
        - tools/v_search.py: Provides reference_points (via agent workflow)
        
    Database Schema:
        - id: conversation_id for grouping related messages
        - user_prompt: User's input message
        - model_response: System's final response
        - timestamp: UTC timestamp for chronological ordering
        - references: Source materials used for response generation
    """
    # Insert a chat document into the collection
    chat_history_doc = {
        "id": conversation_id,
        "user_prompt": user_prompt,
        "model_response": model_response,
        "timestamp": datetime.utcnow().isoformat(),
        "references": reference_points
    }
    await collection.insert_one(chat_history_doc)

async def conv_history(
    conversation_id: str, 
    collection: AsyncIOMotorCollection, 
    chat_history_retrieval_limit: int
) -> List[Dict[str, str]]:
    """
    Retrieve conversation history for providing context to agents.
    
    This function fetches previous conversation messages to provide context
    for the current user query, enabling coherent multi-turn conversations.
    
    Args:
        conversation_id (str): Unique identifier for the conversation session
        collection (AsyncIOMotorCollection): MongoDB collection containing conversations
        chat_history_retrieval_limit (int): Maximum number of previous messages to retrieve
        
    Returns:
        List[Dict[str, str]]: Formatted conversation history with role/content structure
            Format: [{"role": "user", "content": "..."}, {"role": "assistant", "content": "..."}]
            
    Related Files:
        - app.py: Calls this function in agentic_flow() for context retrieval
        - agentic.py: Receives conversation history for manager agent context
        - agents/director_agent.py: Uses conversation context for response synthesis
        
    Data Flow:
        1. Retrieve all messages for the conversation_id
        2. Limit to most recent messages based on chat_history_retrieval_limit
        3. Format as alternating user/assistant messages for LLM context
    """
    chat_history_retrieved = await collection.find({"id": conversation_id}).to_list(length=None)
    
    recent_chat_history = chat_history_retrieved[-chat_history_retrieval_limit:] if chat_history_retrieved else []
    provided_conversation_history = []
    
    for doc in recent_chat_history:
        user_message = doc.get("user_prompt", "")
        ai_message = doc.get("model_response", "")
        provided_conversation_history.append({"role": "user", "content": user_message})
        provided_conversation_history.append({"role": "assistant", "content": ai_message})
    
    return provided_conversation_history

async def inserting_agent_chat_buffer(
    agents_conversation_id: str, 
    conversation_id: str, 
    collection: AsyncIOMotorCollection, 
    sub_question: str, 
    worker_response: str, 
    context_chunks: List[str]
) -> None:
    """
    Persist agent conversation data for internal tracking and PDF generation.
    
    This function stores the interactions between manager and worker agents,
    providing transparency into the agentic workflow and enabling detailed
    reporting of the information gathering process.
    
    Args:
        agents_conversation_id (str): Unique ID for this specific agent conversation session
        conversation_id (str): Parent conversation ID linking to user session
        collection (AsyncIOMotorCollection): MongoDB collection for agent data storage
        sub_question (str): Sub-question generated by manager agent
        worker_response (str): Response generated by worker agent
        context_chunks (List[str]): Source information used by worker agent
        
    Returns:
        None
        
    Related Files:
        - agents/sub_question_handler.py: Calls this function after worker processing
        - agents/worker_agent.py: Provides worker_response and context_chunks
        - agents/director_agent.py: Retrieves this data via get_agents_conv_history()
        - tools/conv_to_pdf_handler.py: Uses this data for PDF report generation
        
    Database Schema:
        - id: agents_conversation_id for grouping agent interactions
        - tid: parent conversation_id for linking to user session
        - sub_question: Manager agent's generated sub-question
        - worker_response: Worker agent's detailed response
        - timestamp: UTC timestamp for chronological ordering
        - references: Context chunks used for response generation
    """
    chat_history_doc = {
        "id": agents_conversation_id,
        "tid": conversation_id,
        "sub_question": sub_question,
        "worker_response": worker_response,
        "timestamp": datetime.utcnow().isoformat(),
        "references": context_chunks
    }
    await collection.insert_one(chat_history_doc)

async def get_agents_conv_history(
    agents_conversation_id: str, 
    collection: AsyncIOMotorCollection
) -> List[Dict[str, str]]:
    """
    Retrieve agent conversation history for director agent context.
    
    This function fetches the interactions between manager and worker agents
    to provide context for the director agent's response synthesis process.
    
    Args:
        agents_conversation_id (str): Unique ID for the agent conversation session
        collection (AsyncIOMotorCollection): MongoDB collection containing agent data
        
    Returns:
        List[Dict[str, str]]: Formatted agent conversation history
            Format: [{"role": "manager_agent", "content": "subquestion = ..."}, 
                    {"role": "worker_agent", "content": "answer = ..."}]
            
    Related Files:
        - agents/director_agent.py: Primary consumer for response synthesis context
        - agents/sub_question_handler.py: Populates the data this function retrieves
        - agentic.py: Coordinates the workflow that generates this conversation data
        
    Data Flow:
        1. Retrieve all agent interactions for the agents_conversation_id
        2. Format as manager_agent/worker_agent role pairs
        3. Provide structured context for director agent synthesis
    """
    chat_history_retrieved = await collection.find({"id": agents_conversation_id}).to_list(length=None)

    recent_chat_history = chat_history_retrieved if chat_history_retrieved else []
    provided_conversation_history = []
    
    for doc in recent_chat_history:
        manager_agent_message = doc.get("sub_question", "")
        worker_agent_message = doc.get("worker_response", "")
        provided_conversation_history.append({"role": "manager_agent", "content": f"subquestion = {manager_agent_message}"})
        provided_conversation_history.append({"role": "worker_agent", "content": f"answer ={worker_agent_message}"})

    return provided_conversation_history

async def get_agents_total_conv_history(
    conversation_id: str, 
    collection: AsyncIOMotorCollection
) -> List[Dict[str, str]]:
    """
    Retrieve complete agent conversation history for PDF generation.
    
    This function fetches all agent interactions across all sub-questions
    for a given user conversation, used primarily for comprehensive PDF reports.
    
    Args:
        conversation_id (str): Parent conversation ID for the user session
        collection (AsyncIOMotorCollection): MongoDB collection containing agent data
        
    Returns:
        List[Dict[str, str]]: Complete formatted agent conversation history
            Format: [{"role": "manager_agent", "content": "subquestion = ..."}, 
                    {"role": "worker_agent", "content": "answer = ..."}]
            
    Related Files:
        - agents/director_agent.py: Uses this for PDF generation coordination
        - tools/conv_to_pdf_handler.py: Primary consumer for comprehensive PDF reports
        - agents/sub_question_handler.py: Populates the underlying data
        
    Data Flow:
        1. Retrieve all agent interactions with matching conversation_id (via tid field)
        2. Include all sub-question/answer pairs across the entire session
        3. Format for PDF report generation with complete workflow transparency
    """
    chat_history_retrieved = await collection.find({"tid": conversation_id}).to_list(length=None)

    recent_chat_history = chat_history_retrieved if chat_history_retrieved else []
    provided_conversation_history = []
    
    for doc in recent_chat_history:
        manager_agent_message = doc.get("sub_question", "")
        worker_agent_message = doc.get("worker_response", "")
        provided_conversation_history.append({"role": "manager_agent", "content": f"subquestion = {manager_agent_message}"})
        provided_conversation_history.append({"role": "worker_agent", "content": f"answer ={worker_agent_message}"})

    return provided_conversation_history

def monolog(provided_conversation_history: List[Dict[str, str]]) -> None:
    """
    Debug utility to display agent conversation history in console.
    
    This function provides a formatted console output of agent interactions
    for debugging and development purposes, helping visualize the agent workflow.
    
    Args:
        provided_conversation_history (List[Dict[str, str]]): Agent conversation data
            Expected format: [{"role": "manager_agent|worker_agent", "content": "..."}]
            
    Returns:
        None
        
    Usage:
        Primarily for development and debugging to visualize agent interactions.
        Can be enabled/disabled by uncommenting the call in director_agent.py.
        
    Related Files:
        - agents/director_agent.py: Contains commented call to this function
        - Used with data from get_agents_conv_history() or get_agents_total_conv_history()
    """
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

def get_best_worker_response(conversation_history: List[Dict[str, str]]) -> str:
    """
    Utility function to extract the highest-scored worker response.
    
    This function appears to be for selecting the best response when multiple
    worker agents provide scored responses, though it's currently unused in the main workflow.
    
    Args:
        conversation_history (List[Dict[str, str]]): Conversation data with scoring information
            Expected to contain entries with role "score" and numeric content
            
    Returns:
        str: Content of the highest-scored response
        
    Note:
        This function appears to be legacy code or for future scoring mechanisms.
        Currently not integrated into the main agentic workflow.
        
    Related Files:
        - Currently unused in the main workflow
        - May be relevant for future agent scoring/selection features
    """
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
