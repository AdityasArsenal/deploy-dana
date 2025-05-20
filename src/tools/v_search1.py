from azure.search.documents.models import QueryType, VectorizableTextQuery, VectorFilterMode
from azure.search.documents import SearchClient
import asyncio

#search.ismatch this is a fuzzy filter can gives reselts with the fuzzy match
async def semantic_hybrid_search(
    query: str,
    search_client: SearchClient,
    top: int = 10,
    company_names: list[str] = []
) -> list[str]:
    """
    Perform a hybrid search (text + vector) with fuzzy filtering for company names.

    Args:
        query (str): The user's search string.
        search_client (SearchClient): Initialized Azure SearchClient.
        top (int): Number of final results to return.
        company_names (list[str]): List of company names to filter results.

    Returns:
        Tuple[List[str], List[str]]: The returned "chunk" and "title" fields from top hits.
    """
    # Build the vector query for semantic similarity
    vec_q = VectorizableTextQuery(
        text=query,
        k_nearest_neighbors=50,
        fields="text_vector",
        exhaustive=True
    )

    # Construct fuzzy filter for company names
    if company_names and len(company_names) > 0:
        filter_elements = []
        for company_name in company_names:
            # Normalize company name: remove spaces, lowercase
            normalized_name = company_name.replace(" ", "").lower()
            # Use search.ismatch with wildcard and fuzzy search
            # Match titles containing the normalized name (e.g., "relianceindustries" matches "RelianceIndustriesLimited.xml")



            filter_elements.append(
                f"search.ismatch('{normalized_name}*~', 'title', 'full', 'any')"
            )
        # Combine filters with OR for multiple companies
        filter_string = " or ".join(filter_elements)
        parent_filter = filter_string

        print(f"The filter string: {filter_string}")
    else:
        parent_filter = None

    # Execute hybrid search: full-text + vector
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

    # Run synchronous search in a thread pool
    results = await asyncio.to_thread(search_sync)
    docs = list(results)

    # Return chunks and titles
    chunks = [doc["chunk"] for doc in docs]
    titles = [doc["title"] for doc in docs]
    return chunks, titles

# This code is for testing the semantic_hybrid_search function directly
if __name__ == "__main__":
    import os
    from dotenv import load_dotenv
    from azure.core.credentials import AzureKeyCredential

    load_dotenv()

    azure_search_endpoint = os.getenv("AI_SEARCH_ENDPOINT")
    azure_search_index = os.getenv("AI_SEARCH_INDEX")
    azure_search_api_key = os.getenv("AI_SEARCH_API_KEY")

    #Azure AI search client
    search_client = SearchClient(endpoint = azure_search_endpoint, index_name = azure_search_index, credential = AzureKeyCredential(azure_search_api_key))

    company_names=["R Systems International Limited", "reliance industries", "ITI Limited"]

    query = f"what is {company_names[0]} and {company_names[1]} and {company_names[2]} CARBON EMISSIONS"
    top_k = 20
    print(f"Index: {azure_search_index}")

    # Create an async function to run our search
    async def run_test_search():
        chunks, titles = await semantic_hybrid_search(query, search_client, top_k, company_names)
        
        print(f"number of chunks: {len(chunks)}")
        for i in range(len(chunks)):
            print(f"\n{i}. {titles[i]} //chunk title")
            # print(f"## chunk: {chunks[i]}")
    
    # Run the async function
    asyncio.run(run_test_search())
