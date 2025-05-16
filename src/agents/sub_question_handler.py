from agents.worker_agent import worker
from tools.conv_handler import inserting_agent_chat_buffer

async def process_sub_question(
    llm_client, 
    deployment, 
    sub_question, 
    company_names, 
    agents_conversation_history, 
    search_client, 
    worker_system_prompt, 
    top_k,
    agents_conversation_id,
    conversation_id,
    connection
):
    """Process a single sub-question and store results in the database"""
    worker_response, context_chunks = await worker(
        llm_client, 
        deployment, 
        sub_question, 
        company_names, 
        agents_conversation_history, 
        search_client, 
        worker_system_prompt, 
        top_k,
        conversation_id
    )
    
    await inserting_agent_chat_buffer(
        agents_conversation_id, 
        conversation_id,
        connection,
        sub_question, 
        worker_response, 
        context_chunks
    )

    print(conversation_id)
    print(agents_conversation_id)
    print("================================")
    
    return worker_response, context_chunks