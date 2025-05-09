from azure.search.documents.models import QueryType, VectorizableTextQuery, VectorFilterMode
from azure.search.documents import SearchClient

def semantic_hybrid_search(
    query: str,
    search_client: SearchClient,
    top: int = 10,
    company_names: list[str] = []
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
    
    print("vvvvvvvvvvvvvvvvvvv")
    
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
            
            print(company_name)

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
        print("filter_string: ",filter_string)
        
        parent_filter = filter_string

    else:
        parent_filter = None

    # 2) Execute a hybrid search: full‐text + vector in one call :contentReference[oaicite:1]{index=1}
    results = search_client.search(
        search_text=query,            
        vector_queries=[vec_q],       
        query_type=QueryType.SIMPLE,  
        select=["title", "chunk"], 
        filter=parent_filter,                    
        vector_filter_mode=VectorFilterMode.PRE_FILTER,
        top=top
    )

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


# import os
# from dotenv import load_dotenv
# from azure.core.credentials import AzureKeyCredential

# load_dotenv()

# azure_search_endpoint = os.getenv("AZURE_SEARCH_ENDPOINT")
# azure_search_index = os.getenv("AZURE_SEARCH_INDEX")
# azure_search_api_key = os.getenv("AZURE_SEARCH_API_KEY")

# #Azure AI search client
# search_client = SearchClient(endpoint = azure_search_endpoint, index_name = azure_search_index, credential = AzureKeyCredential(azure_search_api_key))

# query = "what is BF UTILITIES LIMITED CARBON EMISSIONS"
# top_k = 10
# company_names=[]

# chunks, titles = semantic_hybrid_search(query, search_client, top_k, company_names)

# print(chunks)
# print(titles)

