from azure.search.documents.models import QueryType, VectorizableTextQuery, VectorFilterMode
from azure.search.documents import SearchClient
import asyncio
from typing import Tuple, List

#search.in this is a strict filter only gives reselts with the exact match
async def semantic_hybrid_search(
    query: str,
    search_client: SearchClient,
    top: int = 10,
    company_names: list[str] = []
) -> Tuple[List[str], List[str]]:
    
    # 1) Build the vector query for semantic similarity (vector search)
    
    vec_q = VectorizableTextQuery(
        text=query,
        k_nearest_neighbors=50,
        fields="text_vector",
        exhaustive=True
    )

    filter_elements = []
    #filtering the results based on the company name and then vector search
    if company_names and len(company_names) > 0:
        # Build filters for company-specific documents
        # Supports both original and uppercase company name variants
        for company_name in company_names:
            company_name = company_name.replace(" ", "")
            # if "LIMITED" or "limited" or "Limited" in company_name:
            #     company_name = company_name
            # else:
            #     company_name = company_name + " Limited"

            print(company_name)
            # print(company_name)

            # Generate document title patterns for filtering
            # Supports both PDF and XML document formats
            titlename1 = f"{company_name}.xml"
            titlename2 = f"{company_name}.pdf"

            company_name = company_name.upper()

            titlename11 = f"{company_name}.xml"
            titlename22 = f"{company_name}.pdf"

            filter_elements.append(titlename1)
            filter_elements.append(titlename2)
            filter_elements.append(titlename11)
            filter_elements.append(titlename22)

        # Create Azure AI Search filter string for company documents
        filter_string = f"search.in(title, '{','.join(filter_elements)}')"
        # print("filter_string: ",filter_string)
        
        parent_filter = filter_string

    else:
        parent_filter = None

    # 2) Execute a hybrid search: full‐text + vector in one call
    # Use run_in_executor to run the synchronous search method in a thread pool
    def search_sync():
        """
        Synchronous search operation wrapped for async execution.
        
        Combines vector similarity search with text search for optimal results.
        Uses PRE_FILTER mode to apply company filters before vector search.
        """
        return search_client.search(
            search_text=query,            # Text-based search component
            vector_queries=[vec_q],       # Vector similarity component
            query_type=QueryType.SIMPLE,
            select=["title", "chunk"],    # Return document title and content chunk
            filter=parent_filter,         # Company name filtering                   
            vector_filter_mode=VectorFilterMode.PRE_FILTER,
            top=top
        )
    
    # Run the synchronous search operation in a thread pool
    # This allows async operation while using the synchronous Azure SDK
    results = await asyncio.to_thread(search_sync)
    
    # Materialize into a list so we can iterate twice
    docs = list(results)

    # 3) Extract and return the relevant information
    # Separate chunks (content) from titles (sources) for transparency
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


# Test code - Comment out or wrap in if __name__ == "__main__" to prevent auto-execution
# This code is for testing the semantic_hybrid_search function directly
if __name__ == "__main__":
    """
    Standalone testing functionality for semantic_hybrid_search.
    
    This section allows direct testing of the search functionality without
    going through the full agentic workflow. Useful for development and debugging.
    
    Related Files:
        - agentic.py: Production usage of this search functionality
        - agents/worker_agent.py: How search results are used in the workflow
    """
    import os
    from dotenv import load_dotenv
    from azure.core.credentials import AzureKeyCredential

    load_dotenv()

    azure_search_endpoint = os.getenv("AI_SEARCH_ENDPOINT")
    azure_search_index = os.getenv("AI_SEARCH_INDEX")
    # azure_search_index = "test-vector-index"
    azure_search_api_key = os.getenv("AI_SEARCH_API_KEY")

    # Placeholder values - You need to fill these in
    test_query = "Your search query here" 
    test_top_k = 5 
    test_company_names = ["CompanyA", "CompanyB Limited"] 

    if not all([azure_search_endpoint, azure_search_index, azure_search_api_key]):
        print("Azure search environment variables (AI_SEARCH_ENDPOINT, AI_SEARCH_INDEX, AI_SEARCH_API_KEY) are not set.")
        print("Please set them in your .env file or environment.")
    else:
        search_client_instance = SearchClient(
            endpoint=azure_search_endpoint,
            index_name=azure_search_index,
            credential=AzureKeyCredential(azure_search_api_key)
        )
        print(f"Testing with Index: {azure_search_index}")
        print(f"Query: {test_query}")
        print(f"Top K: {test_top_k}")
        print(f"Company Names: {test_company_names}")

        async def run_test_search():
            try:
                chunks, titles = await semantic_hybrid_search(
                    query=test_query,
                    search_client=search_client_instance,
                    top=test_top_k,
                    company_names=test_company_names
                )
                
                print(f"\nNumber of chunks retrieved: {len(chunks)}")
                if chunks:
                    for i in range(len(chunks)):
                        print(f"  {i+1}. Title: {titles[i]}")
                        # print(f"     Chunk: {chunks[i][:100]}...") # Print first 100 chars of chunk
                else:
                    print("No results found.")
            except Exception as e:
                print(f"An error occurred during the test search: {e}")
        
        asyncio.run(run_test_search())


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
