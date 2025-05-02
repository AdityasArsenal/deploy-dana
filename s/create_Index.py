import os
from azure.core.credentials import AzureKeyCredential
from azure.search.documents.indexes import SearchIndexClient
from azure.search.documents.indexes.models import (
    SearchIndex,
    SearchField,
    SearchFieldDataType,
    SimpleField,
    SearchableField,
    VectorSearch,
    HnswAlgorithmConfiguration,
    VectorSearchProfile,
    VectorSearchVectorizer,
    AzureOpenAIVectorizer,
    AzureOpenAIVectorizerParameters
)

from dotenv import load_dotenv

load_dotenv()

def create_search_index(endpoint, api_key, index_name):
    # Create a client
    credential = AzureKeyCredential(api_key)
    client = SearchIndexClient(endpoint=endpoint, credential=credential)

    # Define the vector search configuration
    vector_search = VectorSearch(
        algorithms=[
            HnswAlgorithmConfiguration(
                name="myHnsw",
                parameters={
                    "m": 4,
                    "efConstruction": 400,
                    "efSearch": 500,
                    "metric": "cosine"
                }
            )
        ],

        profiles=[
            VectorSearchProfile(
                name="myProfile",
                algorithm_configuration_name="myHnsw",
                vectorizer_name="openai-vectorizer"
            )
        ],

        vectorizers=[
            AzureOpenAIVectorizer(
                vectorizer_name="openai-vectorizer",
                parameters=AzureOpenAIVectorizerParameters(
                    resource_url=os.getenv("ADA_ENDPOINT"),      # your OpenAI endpoint
                    api_key=os.getenv("ADA_KEY"),                          # your OpenAI API key
                    deployment_name="text-embedding-ada-002",                   # your embedding deployment
                    model_name="text-embedding-ada-002"                         # required in newer API versions
                )
            )
        ]

    )

    # Define the fields with the correct parameter for vector search profile
    fields = [
        SimpleField(name="chunk_id", type=SearchFieldDataType.String, key=True),
        SimpleField(name="parent_id", type=SearchFieldDataType.String, filterable=False),
        SearchableField(name="chunk", type=SearchFieldDataType.String),
        SearchableField(name="title", type=SearchFieldDataType.String),
        SearchField(
            name="text_vector",
            type=SearchFieldDataType.Collection(SearchFieldDataType.Single),
            searchable=True,
            vector_search_dimensions=1536,
            vector_search_profile_name="myProfile"  # Corrected parameter name
        ),
        SimpleField(name="companyName", type=SearchFieldDataType.String, filterable=True)
    ]

    # Create the index
    index = SearchIndex(name=index_name, fields=fields, vector_search=vector_search)
    result = client.create_index(index)

    print(f"res : {result}")
    print(f"Index {index_name} created successfully.")

if __name__ == "__main__":

    endpoint = os.getenv("AI_SEARCH_ENDPOINT")
    api_key = os.getenv("AI_SEARCH_API_KEY")

    print(f"endpoint : {endpoint}")

    index_name = "kliuuuuuuuuuuuuuuutllllllllllllllllllliutyilyuliyuh"
    create_search_index(endpoint, api_key, index_name)

