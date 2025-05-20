import json
from agents.worker_agent import worker
import os
import uuid
import asyncio
from tools.conv_handler import get_agents_conv_history, inserting_agent_chat_buffer, monolog, get_best_worker_response, get_agents_total_conv_history
from tools.conv_to_pdf_handler import conversation_to_pdf, upload_pdf_to_blob
from tools.json_parseing import parse_json_from_model_response
from agents.director_agent import director
from agents.sub_question_handler import process_sub_question
from dotenv import load_dotenv
from azure.search.documents import SearchClient
from azure.core.credentials import AzureKeyCredential

load_dotenv()

limit_subquestions = 10# maximum number of sub-questions to be created by manager agent
top_k = 10 # number of chunks to be used by worker agent

# Load system prompts from text files
def load_prompt_from_file(file_path):
    with open(file_path, 'r', encoding='utf-8') as file:
        return file.read()

director_system_prompt = load_prompt_from_file('./prompts/director_system_prompt.txt')
manager_system_prompt = load_prompt_from_file('./prompts/manager_system_prompt.txt').replace('{limit_subquestions}', str(limit_subquestions))
worker_system_prompt = load_prompt_from_file('./prompts/worker_system_prompt.txt')

print(f"director_system_prompt: {director_system_prompt}")
print(f"manager_system_prompt: {manager_system_prompt}")
print(f"worker_system_prompt: {worker_system_prompt}")
    


# Azure AI Search configuration
azure_search_endpoint = os.getenv("AI_SEARCH_ENDPOINT")
azure_search_index = os.getenv("AI_SEARCH_INDEX")
azure_search_api_key = os.getenv("AI_SEARCH_API_KEY")
#Azure AI search client
search_client = SearchClient(endpoint = azure_search_endpoint, index_name = azure_search_index, credential = AzureKeyCredential(azure_search_api_key))

async def manager(
    llm_client,
    deployment,
    user_prompt,
    user_conversation_history,
    connection,
    chat_history_retrieval_limit,
    conversation_id
):   
    
    print("MMMMMMM")
    agents_conversation_id = str(uuid.uuid4())
    print(f"Vector search index = {azure_search_index} AND agents_convertation_id = {agents_conversation_id}")
    
    completion = await llm_client.chat.completions.create(
        model=deployment,
        messages=[
            {"role": "system", "content": manager_system_prompt},
            {"role": "user", "content": f"Previous conversation between user and you: {user_conversation_history},\nMy question: {user_prompt}"},
        ],
        max_tokens=800,
        temperature=0.7,
        top_p=0.95,
        frequency_penalty=0,
        presence_penalty=0,
        stop=None
    )

    manager_json_output = completion.choices[0].message.content
    
    # Parse the model response with the new function
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
     
    print(f"number of sub-questions {len(list_of_sub_questions)}")
    print(f"company_names within the sub-questions: {company_names}")

    # Create a list of tasks for each sub-question
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
    
    # Run all tasks concurrently
    print(f"Processing {len(tasks)} sub-questions in parallel...")
    results = await asyncio.gather(*tasks)
    
    # Collect all context chunks from the results
    all_context_chunks = []
    for _, context_chunks in results:
        all_context_chunks.extend(context_chunks)
    
    print(f"Collected {len(all_context_chunks)} context chunks from all workers")

    direcotr_response, conv_pdf_url = await director(
        llm_client,
        director_system_prompt,
        deployment,
        user_prompt,
        user_conversation_history,
        connection,
        chat_history_retrieval_limit,
        all_context_chunks,
        agents_conversation_id,
        conversation_id
    )
    return direcotr_response, all_context_chunks, conv_pdf_url