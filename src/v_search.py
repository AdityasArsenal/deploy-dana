from azure.search.documents.models import QueryType, VectorizableTextQuery, VectorFilterMode
from azure.search.documents import SearchClient

def semantic_hybrid_search(
    query: str,
    search_client: SearchClient,
    top: int = 3,
    company_name: str = "HPCL"
) -> list[str]:
    """
    Perform a hybrid search (text + vector) and return the top chunks.

    Args:
        query (str): The user’s search string.
        search_client (SearchClient): Your initialized Azure SearchClient.
        top (int): How many final results to return.
        k_nearest (int): How many nearest neighbors to fetch in the vector space.

    Returns:
        List[str]: The returned “chunk” field from each top hit.
    """
    # 1) Build the vector query for semantic similarity (vector search) :contentReference[oaicite:0]{index=0}
    vec_q = VectorizableTextQuery(
        text=query,
        k_nearest_neighbors=50,
        fields="text_vector",
        exhaustive=True
    )

    print(search_client._index_name)
    print(query)

    parent_filter = f"companyName eq '{company_name}'"

    # 2) Execute a hybrid search: full‐text + vector in one call :contentReference[oaicite:1]{index=1}
    results = search_client.search(
        search_text=query,             # your keyword text query
        vector_queries=[vec_q],        # plus this vector subquery
        query_type=QueryType.SIMPLE,   # SIMPLE lets you mix text & vector ◆
        select=["title", "chunk"],              # only retrieve the ‘chunk’ field
        filter=parent_filter,                     # apply before k‑NN
        vector_filter_mode=VectorFilterMode.PRE_FILTER,
        top=top
    )

      # Materialize into a list so we can iterate twice
    docs = list(results)

    # 3) Materialize and return the chunk strings
    chunks = [doc["chunk"] for doc in docs]
    titles = [doc["title"] for doc in docs]

    print("===============titles:=================")
    print(titles)
    print("===============titles:=================")

    return chunks, titles

    # j = 0
    # for i in chunks:
    #     j += 1
    #     print("===============chunk:=================")
    #     print(f"{j}. {i}\n")
    #     print("===============chunk:=================")


import os
from dotenv import load_dotenv
from azure.core.credentials import AzureKeyCredential

load_dotenv()

azure_search_endpoint = os.getenv("AZURE_AI_SEARCH_ENDPOINT")
azure_search_index = os.getenv("AZURE_AI_SEARCH_INDEX")
azure_search_api_key = os.getenv("AZURE_SEARCH_API_KEY")

#Azure AI search client
search_client = SearchClient(endpoint = azure_search_endpoint, index_name = "vector-db-free-hpcl-iocl", credential = AzureKeyCredential(azure_search_api_key))

query = "what carbon emission is HPCL"
top_k = 5

semantic_hybrid_search(query, search_client, top_k)
