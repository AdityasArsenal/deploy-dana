"""
Director Agent Module

The Director Agent is the final stage in the ESGAI agentic workflow, responsible for
synthesizing information gathered by worker agents into a comprehensive, coherent response.
It also handles conversation persistence and PDF generation for user reference.

Key Responsibilities:
- Synthesize worker agent responses into a final answer
- Generate conversation summaries as downloadable PDFs
- Manage conversation history and context integration
- Upload PDF summaries to Azure Blob storage

Dependencies:
- tools/conv_to_pdf_handler.py: PDF generation and upload functionality
- tools/conv_handler.py: Conversation history retrieval and management
- Azure OpenAI: LLM services for response synthesis
- Azure Blob Storage: PDF hosting (via conv_to_pdf_handler)

Related Files:
- agentic.py: Invokes director() after collecting worker responses
- agents/worker_agent.py: Provides context chunks that director synthesizes
- agents/sub_question_handler.py: Coordinates worker responses (via agentic.py)
- tools/conv_handler.py: Manages conversation data persistence
"""

import os
from typing import List, Dict, Any, Tuple
from openai import AsyncAzureOpenAI
from motor.motor_asyncio import AsyncIOMotorCollection
from tools.conv_to_pdf_handler import conversation_to_pdf, upload_pdf_to_blob, conversation_with_context_to_pdf
from tools.conv_handler import get_agents_conv_history, get_agents_total_conv_history

async def director(
    llm_client: AsyncAzureOpenAI,
    director_system_prompt: str,
    deployment: str,
    user_prompt: str,
    user_conversation_history: List[Dict[str, str]],
    connection: AsyncIOMotorCollection,
    all_context_chunks: List[str],                     #list of lists if chunks
    agents_conversation_id: str,
    conversation_id: str
) -> Tuple[str, str]:
    """
    Synthesizes information from worker agents to generate a final comprehensive response.
    
    This function represents the culmination of the agentic workflow, taking all the
    information gathered by worker agents and creating a cohesive, comprehensive answer.
    It also handles conversation documentation and PDF generation.
    
    Args:
        llm_client (AsyncAzureOpenAI): Azure OpenAI client for LLM interactions
        director_system_prompt (str): System prompt defining director behavior
        deployment (str): Azure OpenAI deployment name
        user_prompt (str): Original user question
        user_conversation_history (List[Dict[str, str]]): Previous conversation context
        connection (AsyncIOMotorCollection): MongoDB connection for conversation data
        all_context_chunks (List[str]): Information gathered by all worker agents
        agents_conversation_id (str): Unique ID for this agent conversation session
        conversation_id (str): Overall conversation identifier
        
    Returns:
        Tuple[str, str]: (director_response, conv_pdf_url)
            - director_response (str): Final synthesized answer
            - conv_pdf_url (str): URL to downloadable conversation PDF
            
    Workflow:
        1. Retrieve agent conversation history via tools/conv_handler.py
        2. Generate synthesis using director_system_prompt and context chunks
        3. Create PDF summary via tools/conv_to_pdf_handler.py
        4. Upload PDF to Azure Blob and return access URL
        
    Related Files:
        - agentic.py: Calls this function with collected worker responses
        - tools/conv_handler.py: Provides get_agents_conv_history() and get_agents_total_conv_history()
        - tools/conv_to_pdf_handler.py: Handles conversation_to_pdf() and upload_pdf_to_blob()
        - prompts/director_system_prompt.txt: Defines synthesis behavior
        - agents/worker_agent.py: Provides the context chunks being synthesized
    """
    print("DDDD")
    
    # Retrieve conversation history between agents using tools/conv_handler.py
    # This provides context about what the worker agents discussed and found
    agents_conversation_history = await get_agents_conv_history(agents_conversation_id, connection)
    
    # Generate final synthesis using the director system prompt
    # The director_system_prompt is loaded from prompts/director_system_prompt.txt in agentic.py
    completion = await llm_client.chat.completions.create(
        model=deployment,
        messages=[
            {"role": "system", "content": director_system_prompt},
            {"role": "user", "content": f"Previous conversation between user and you: {user_conversation_history},\nMy question: {user_prompt}"},
            {"role": "assistant", "content": f"Previous conversation between you and worker agent: {agents_conversation_history}"}
        ],
        max_tokens=800,
        temperature=0.7,
        top_p=0.95,
        frequency_penalty=0,
        presence_penalty=0,
        stop=None
    )

    direcotr_response = completion.choices[0].message.content
    
    # monolog(agents_conversation_history)
    # Save the conversation to PDF
    output_dir="conversation_pdfs"

    # Get complete conversation history for PDF generation
    # This includes all user interactions and agent responses
    agents_total_conversation_history = await get_agents_total_conv_history(conversation_id, connection)

    # Generate PDF summary using tools/conv_to_pdf_handler.py
    # Creates a formatted document with conversation history and final response
    pdf_path = await conversation_to_pdf(agents_total_conversation_history, direcotr_response, output_dir)
    
    #pdf with all the context chunks
    # pdf_path_with_context = await conversation_with_context_to_pdf(user_prompt, agents_total_conversation_history, all_context_chunks, direcotr_response, output_dir)

    # Upload PDF to Azure Blob storage and get public URL
    # Allows users to download the conversation summary
    conv_pdf_url = await upload_pdf_to_blob(pdf_path)
    
    # Clean up local PDF file after upload
    os.remove(pdf_path)

    # print(f"Conversation saved to PDF: {pdf_path}")
    
    return direcotr_response, conv_pdf_url
