from azure.search.documents.models import QueryType, VectorizableTextQuery, VectorFilterMode
from azure.search.documents import SearchClient
import asyncio

async def semantic_hybrid_search(
    query: str,
    search_client: SearchClient,
    top: int = 10,
    company_names: list[str] = []
) -> list[str]:
    """
    Perform a hybrid search (text + vector) and return the top chunks.

    Args:
        query (str): The user's search string.
        search_client (SearchClient): Your initialized Azure SearchClient.
        top (int): How many final results to return.
        company_names (list[str]): List of company names to filter results.

    Returns:
        List[str]: The returned "chunk" field from each top hit.
    """
    # 1) Build the vector query for semantic similarity (vector search)
    
    # print("vvvvvvvvvvvvvvvvvvv")
    
    vec_q = VectorizableTextQuery(
        text=query,
        k_nearest_neighbors=50,
        fields="text_vector",
        exhaustive=True
    )

    filter_elements = []
    #filtering the results based on the company name and then vector search
    if company_names and len(company_names) > 0:
        for company_name in company_names:
            company_name = company_name.replace(" ", "")
            
            # print(company_name)

            titlename1 = f"{company_name}.xml"
            titlename2 = f"{company_name}.pdf"

            company_name = company_name.upper()

            titlename11 = f"{company_name}.xml"
            titlename22 = f"{company_name}.pdf"

            filter_elements.append(titlename1)
            filter_elements.append(titlename2)
            filter_elements.append(titlename11)
            filter_elements.append(titlename22)


        filter_string = f"search.in(title, '{','.join(filter_elements)}')"
        # print("filter_string: ",filter_string)
        
        parent_filter = filter_string

    else:
        parent_filter = None

    # 2) Execute a hybrid search: full‐text + vector in one call
    # Use run_in_executor to run the synchronous search method in a thread pool
    def search_sync():
        return search_client.search(
            search_text=query,            
            vector_queries=[vec_q],       
            query_type=QueryType.SIMPLE,  
            select=["title", "chunk"], 
            filter=parent_filter,                    
            vector_filter_mode=VectorFilterMode.PRE_FILTER,
            top=top
        )
    
    # Run the synchronous search operation in a thread pool
    results = await asyncio.to_thread(search_sync)
    
    # Materialize into a list so we can iterate twice
    docs = list(results)

    # 3) Materialize and return the chunk strings
    chunks = [doc["chunk"] for doc in docs]
    titles = [doc["title"] for doc in docs]

    # which filter_element is appearing in the titles how many times
    # x=0
    # p=0
    # for i in titles:
    #     for filter_element in filter_elements:
    #         if i == filter_element:
    #             x+=1
    #             print(f"{filter_element}: {x}")

    return chunks, titles

    # j = 0
    # for i in chunks:
    #     j += 1
    #     print("===============chunk:=================")
    #     print(f"{j}. {i}\n")
    #     print("===============chunk:=================")


#Test code for single company
# import os
# from dotenv import load_dotenv
# from azure.core.credentials import AzureKeyCredential

# load_dotenv()

# azure_search_endpoint = os.getenv("AI_SEARCH_ENDPOINT")
# azure_search_index = os.getenv("AI_SEARCH_INDEX")
# # azure_search_index = "test-vector-index"
# azure_search_api_key = os.getenv("AI_SEARCH_API_KEY")

# #Azure AI search client
# search_client = SearchClient(endpoint = azure_search_endpoint, index_name = azure_search_index, credential = AzureKeyCredential(azure_search_api_key))

# company_names=["ITILIMITED"]

# query = f"what is {company_names[0]} CARBON EMISSIONS"
# top_k = 10

# chunks, titles = semantic_hybrid_search(query, search_client, top_k, company_names)

# for i in range(len(chunks)):
#     print(f"\n{i}. {titles[i]} //chunk title\n")
#     print("===============chunk:=================")
#     print(f"## chunk: {chunks[i]}")


##To check if  a list of companies are missing from the vector dataset
# def verify_missing_companies(json_file_path):
#     """
#     Verify which companies from the JSON file are missing from the vector dataset.
    
#     Args:
#         json_file_path (str): Path to the JSON file containing company names
        
#     Returns:
#         dict: Dictionary with confirmed missing and found companies
#     """
#     # Load company names from JSON file
#     with open(json_file_path, 'r') as file:
#         data = json.load(file)
    
#     company_names = data.get('company_names', [])
    
#     # Set up Azure search client
#     load_dotenv()
#     azure_search_endpoint = os.getenv("AI_SEARCH_ENDPOINT")
#     azure_search_index = os.getenv("AI_SEARCH_INDEX")
#     azure_search_api_key = os.getenv("AI_SEARCH_API_KEY")
#     search_client = SearchClient(
#         endpoint=azure_search_endpoint, 
#         index_name=azure_search_index, 
#         credential=AzureKeyCredential(azure_search_api_key)
#     )
    
#     # Results storage
#     missing_companies = []
#     found_companies = []
    
#     # Check each company
#     for company in company_names:
#         query = f"information about {company}"
#         company_list = [company]
        
#         # Use existing search function
#         chunks, titles = semantic_hybrid_search(query, search_client, top=10, company_names=company_list)

        
#         # If no results returned, company is confirmed missing
#         if not chunks:
#             missing_companies.append(company)
#         else:
#             found_companies.append(company)
    
#     return {
#         "missing_companies": missing_companies,
#         "found_companies": found_companies
#     }
