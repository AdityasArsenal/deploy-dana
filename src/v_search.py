from azure.search.documents.models import QueryType, VectorizableTextQuery, VectorFilterMode
from azure.search.documents import SearchClient

def semantic_hybrid_search(
    query: str,
    search_client: SearchClient,
    top: int = 5,
    company_name: str = ""
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

    #filtering the results based on the company name and then vector search
    print(company_name)
    titlename1 = f"{company_name}.xml"
    titlename2 = f"{company_name}.pdf"

    parent_filter = f"search.in(title, '{titlename1},{titlename2}')"

    # 2) Execute a hybrid search: full‐text + vector in one call :contentReference[oaicite:1]{index=1}
    results = search_client.search(
        search_text=query,            
        vector_queries=[vec_q],       
        query_type=QueryType.SIMPLE,  
        select=["title", "chunk","companyName"], 
        filter=parent_filter,                    
        vector_filter_mode=VectorFilterMode.PRE_FILTER,
        top=top
    )

      # Materialize into a list so we can iterate twice
    docs = list(results)

    # 3) Materialize and return the chunk strings
    chunks = [doc["chunk"] for doc in docs]
    titles = [doc["title"] for doc in docs]

    x=0
    p=0
    for i in titles:
        if i == f"{titlename1}":
            x+=1
        elif i == f"{titlename2}":
            p+=1
    print("xmls: ",x)
    print("pdfs: ",p)
    return chunks, titles

    # j = 0
    # for i in chunks:
    #     j += 1
    #     print("===============chunk:=================")
    #     print(f"{j}. {i}\n")
    #     print("===============chunk:=================")


# import os
# from dotenv import load_dotenv
# from azure.core.credentials import AzureKeyCredential

# load_dotenv()

# azure_search_endpoint = "https://vectors-of-all-companies.search.windows.net"
# azure_search_index = "new-vector-index"
# azure_search_api_key = "ADD API KEY HERE"

# #Azure AI search client
# search_client = SearchClient(endpoint = azure_search_endpoint, index_name = azure_search_index, credential = AzureKeyCredential(azure_search_api_key))

# query = "what is  ReligareEnterprisesLimited's gas production"
# top_k = 10
# company_name="ReligareEnterprisesLimited"

# chunks, titles = semantic_hybrid_search(query, search_client, top_k, company_name)

# print(chunks)
# print(titles)

