"""
Worker Agent Module

Worker agents are the core information retrieval and processing units in the ESGAI system.
Each worker agent handles a specific sub-question by performing semantic search, retrieving
relevant context, and generating focused responses based on the retrieved information.

Key Responsibilities:
- Process individual sub-questions from the manager agent
- Perform semantic hybrid search using company name filters
- Generate contextual responses based on retrieved information
- Return both responses and source context for transparency

Dependencies:
- tools/v_search.py: Semantic hybrid search functionality
- Azure AI Search: Vector and text search capabilities
- Azure OpenAI: LLM services for response generation

Related Files:
- agents/sub_question_handler.py: Invokes worker() for each sub-question
- agentic.py: Coordinates worker responses via sub_question_handler
- agents/director_agent.py: Synthesizes worker responses into final answer
- tools/v_search.py: Provides semantic_hybrid_search() functionality
"""

from typing import List, Tuple
from openai import AsyncAzureOpenAI
from azure.search.documents import SearchClient
from tools.v_search import semantic_hybrid_search

async def worker(
    llm_client: AsyncAzureOpenAI,
    deployment: str,
    sub_question: str,
    company_names: List[str],
    agents_conversation_history: str,
    search_client: SearchClient,
    system_prompt: str,
    top_k: int,
    conversation_id: str
) -> Tuple[str, List[str]]:
    """
    Processes individual sub-questions by retrieving relevant information from a knowledge base.
    
    This function represents the core information processing unit of the agentic system.
    It takes a specific sub-question, searches for relevant information using semantic
    search, and generates a focused response based on the retrieved context.
    
    Args:
        llm_client (AsyncAzureOpenAI): Azure OpenAI client for LLM interactions
        deployment (str): Azure OpenAI deployment name
        sub_question (str): Specific question to be processed
        company_names (List[str]): List of company names for filtering search results
        agents_conversation_history (str): Previous agent interactions (currently unused)
        search_client (SearchClient): Azure AI Search client for semantic search
        system_prompt (str): Worker agent behavior instructions
        top_k (int): Number of context chunks to retrieve from search
        conversation_id (str): Conversation identifier for tracking
        
    Returns:
        Tuple[str, List[str]]: (response_message, context_chunks)
            - response_message (str): Worker's answer to the sub-question
            - context_chunks (List[str]): Source information used for the response
            
    Workflow:
        1. Perform semantic hybrid search via tools/v_search.py
        2. Filter results using company names for relevance
        3. Generate contextual response using retrieved information
        4. Return both response and source context for transparency
        
    Related Files:
        - tools/v_search.py: Provides semantic_hybrid_search() for information retrieval
        - agents/sub_question_handler.py: Calls worker() for each sub-question
        - agents/director_agent.py: Uses context_chunks for final synthesis
        - agentic.py: Coordinates workers via sub_question_handler
        - prompts/worker_system_prompt.txt: Defines worker behavior (loaded in agentic.py)
    """
    # return "worker responded", ["","",""]

    # Perform semantic hybrid search using tools/v_search.py
    # This combines vector similarity and text search with company name filtering
    context_chunks, titles = await semantic_hybrid_search(sub_question, search_client, top_k, company_names)
    
    user_message_with_context = f"#My question: {sub_question}\n\n#Relevant information: {context_chunks}"

    # Generate response using worker system prompt and retrieved context
    # The system_prompt is loaded from prompts/worker_system_prompt.txt in agentic.py
    completion = await llm_client.chat.completions.create(
        model=deployment,
        messages=[
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": user_message_with_context}
        ],
        max_tokens=800,
        temperature=0.7,
        top_p=0.95,
        frequency_penalty=0,
        presence_penalty=0,
        stop=None
    )

    response_message = completion.choices[0].message.content
    
    # Return both the generated response and the source context
    # Context chunks are used by director agent for synthesis and transparency
    return response_message, context_chunks