from fastapi import FastAPI
from pydantic import BaseModel
import os
from openai import AzureOpenAI
from dotenv import load_dotenv
import uuid
from datetime import datetime
from pymongo import MongoClient
from typing import Optional

# Load environment variables
load_dotenv()

app = FastAPI()

# Azure OpenAI configuration
endpoint = os.getenv("AZURE_OPENAI_ENDPOINT")
deployment = os.getenv("AZURE_OPENAI_DEPLOYED_NAME")
api_key = os.getenv("AZURE_OPENAI_KEY")

# Azure AI Search configuration
azure_search_endpoint = os.getenv("AZURE_AI_SEARCH_ENDPOINT")
azure_search_index = os.getenv("AZURE_AI_SEARCH_INDEX")
azure_search_api_key = os.getenv("AZURE_SEARCH_API_KEY")

system_prompt = """
You are an ESG consultant with 10-years of experience in Environment, Social and Governance topics in context of India's BRSR standards. Your task is to first break down the user's prompt and understand what is being asked, and then extract the exact information from your reports database to respond with information requested by the user. With great efforts, prioritise on accuracy of information available only from the reports that you have. Always respond with citations on which section of the report where you got the information from, so it's easier for the user to check that and verify in the reports themselves.

When responding, give a summary of the requested information in natural language first. So that the user understands the information in a better manner from the first paragraph of your response. And follow this paragraph with well-structured bullet points or a table/matrix of the most important quantitative information you found. At the end of your response, always give a short summary of your response. And finally, always ask users if they need more help in analytically-relevant topics around what they asked; by suggesting them some questions if they would like to ask as follow ups.

If the information requested by a user is not found, try to give information that seems relevant/close to what the user requested, but stick to the information from your reports only. And in such cases, suggest users to ask questions around information you have available in the report but be sure that it is relevant to what the user asked originally. 

The Business Responsibility and Sustainability Reporting (BRSR) framework, mandated by SEBI, requires the top 1,000 listed companies in India to disclose detailed information on their Environmental, Social, and Governance (ESG) performance. But remember that, as of now, you have details of the following companies only:
BF UTILITIES LIMITED
Hindustan Construction Company Limited
HMT LIMITED
ITI LIMITED
LAURUS LABS LIMITED
NETWEB TECHNOLOGIES INDIA LIMITED
Network18 Media & Investments Limited
Persistent Systems Limited
Procter & Gamble Health Limited
Siemens Limited
Zee Entertainment Enterprises Limited

5. A user will generally ask questions around the information that they believe to be inside the reports that you have. Below is a comprehensive list of the key information that must be inside in each report that you have:
General Disclosures:
1. Corporate Identity Number (CIN) of the entity.
2. Name, year of incorporation, and registered office address of the entity.
3. Corporate address, contact details (email, phone), and website.
4. Financial year for which reporting is done.
5. Stock exchanges where shares are listed.
6. Paid-up capital and turnover details.
7. Reporting boundary (standalone or consolidated basis).
8. Name and contact details of the person responsible for BRSR queries[5][6].
ESG Disclosures:
The BRSR framework is structured around nine principles from the National Guidelines for Responsible Business Conduct (NGRBC), covering:
1. Environmental Responsibility
- Electricity consumption, water usage, air emissions.
- Investments in sustainable goods/services and environmental initiatives[1][7].
2. Social Responsibility
- Employee well-being: Gender ratio, parental benefits, unionized workforce percentage.
- Representation of women in top management.
- Policies for inclusion of vulnerable/marginalized groups[1][6].
3. Governance Responsibility
- Anti-corruption and anti-bribery measures.
- Policies addressing conflicts of interest[1][7].
4. Human Rights
- Mechanisms to prevent human rights violations.
- Adherence to minimum wage and fair wage practices[1][2].
5. Stakeholder Engagement
- Engagement with marginalized groups and handling consumer complaints.
- Cybersecurity and data privacy policies[1][3].
6. Responsible Public Policy Engagement
- Trade/industry affiliations and anti-competitive conduct disclosures[1].
7. Economic Responsibility
- Disclosure on economic development initiatives that benefit society[7].
8. Product Responsibility
- Product recall procedures and consumer feedback mechanisms[1].
9. Inclusive Growth
- Policies promoting equitable development for marginalized communities[1][7].
Specific Metrics
1. Quantitative metrics across ESG pillars for benchmarking performance.
2. Value chain disclosures: Suppliers and buyers contributing to 75% of procurement/sales value must submit BRSR core reports[3].
3. Complaints/grievances under each principle from NGRBC guidelines[6].
Integration with Annual Reports:
The BRSR report must be included as part of a company's annual report to ensure transparency in non-financial disclosures alongside financial performance[1][2].

"""

# Replace line 91 with environment variable
connection_string = os.getenv("MONGO_CONNECTION_STRING")
mongo_client = MongoClient(connection_string)
db = mongo_client["ChatHistoryDatabase"]
collection = db["chat-history-with-cosmos"]
chat_history_retrieval_limit = 10

# OpenAI Client Initialization
ai_client = AzureOpenAI(
    api_key=api_key,
    azure_endpoint=endpoint,
    api_version="2024-05-01-preview",
)

# Extend the ChatRequest to optionally include a conversation_id
class ChatRequest(BaseModel):
    user_prompt: str
    conversation_id: Optional[str] = None

def play_ground(client, deployment, user_prompt, azure_search_endpoint, azure_search_index, azure_search_api_key, conversation_id):   
    # Retrieve the chat history documents using the conversation id
    chat_history_retrieved = list(collection.find({"id": conversation_id}))
    
    recent_chat_history = chat_history_retrieved[-chat_history_retrieval_limit:] if chat_history_retrieved else []
    provided_conversation_history = []
    
    for doc in recent_chat_history:
        user_message = doc.get("user_prompt", "")
        ai_message = doc.get("model_response", "")
        provided_conversation_history.append({"role": "user", "content": user_message})
        provided_conversation_history.append({"role": "assistant", "content": ai_message})
    
    #print(f"system prompt{system_prompt}"),
    print(f"Previous conversation: {provided_conversation_history},\n")
    print("*****************************************************************************")
    print(f"My question🟢: {user_prompt}")

    completion = client.chat.completions.create(
        model=deployment,
        messages=[
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": f"Previous conversation: {provided_conversation_history},\nMy question: {user_prompt}"}
        ],
        max_tokens=800,
        temperature=0.7,
        top_p=0.95,
        frequency_penalty=0,
        presence_penalty=0,
        stop=None,
        extra_body={
            "data_sources": [
                {
                    "type": "azure_search",
                    "parameters": {
                        "endpoint": azure_search_endpoint,
                        "index_name": azure_search_index,
                        "authentication": {
                            "type": "api_key",
                            "key": azure_search_api_key
                        }
                    }
                }
            ]
        }
    )
    
    # Extract the response and context citations
    response_message = completion.choices[0].message.content
    context_chunks = [citation['content'] for citation in completion.choices[0].message.context.get('citations', [])]
    
    return response_message, context_chunks

def inserting_chat_buffer(conversation_id, user_prompt, model_response, references):
    # Insert a chat document into the collection
    chat_history_doc = {
        "id": conversation_id,
        "user_prompt": user_prompt,
        "model_response": model_response,
        "timestamp": datetime.utcnow().isoformat(),
        "references": references
    }
    collection.insert_one(chat_history_doc)

# API Endpoint to receive user input and return AI response
@app.post("/chat")
def chat(request: ChatRequest):
    # Use provided conversation_id or generate a new one if missing
    conversation_id = request.conversation_id or str(uuid.uuid4())
    
    model_response, reference_points = play_ground(
        ai_client,
        deployment,
        request.user_prompt,
        azure_search_endpoint,
        azure_search_index,
        azure_search_api_key,
        conversation_id
    )
    
    print(f"response:🔵 {model_response}")
    inserting_chat_buffer(conversation_id, request.user_prompt, model_response, reference_points)
    print(f"number of chunks retrived {len(reference_points)}")
    # for i in reference_points:
    #     print("==================")
    #     print(f"chunks used {i}")
            
    return {
        "response": model_response,
        "references": reference_points,
        "conversation_id": conversation_id
    }


@app.get("/")
def home():
    return {"message": "Hello, World!"}
