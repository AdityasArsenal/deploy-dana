import os
from dotenv import load_dotenv
from azure.core.credentials import AzureKeyCredential
from azure.search.documents.indexes import SearchIndexerClient

# Load environment variables
load_dotenv()

# Configuration constants
SERVICE_ENDPOINT = os.getenv("AI_SEARCH_ENDPOINT")
API_KEY = os.getenv("AI_SEARCH_API_KEY")
DATA_SOURCE_NAME = "companydata-blob-datasource"
TARGET_INDEX_NAME = "new-vector-index"
SKILLSET_NAME = "chunking-skillset"
INDEXER_NAME = "new-company-indexer"

def get_search_indexer_client():
    """Initializes and returns a SearchIndexerClient."""
    if not SERVICE_ENDPOINT or not API_KEY:
        raise ValueError("AI_SEARCH_ENDPOINT and AI_SEARCH_API_KEY must be set in .env file or environment variables.")
    credential = AzureKeyCredential(API_KEY)
    return SearchIndexerClient(endpoint=SERVICE_ENDPOINT, credential=credential)

if __name__ == '__main__':
    # Example of how to use this config to get a client
    try:
        client = get_search_indexer_client()
        print(f"Successfully initialized SearchIndexerClient for endpoint: {client.endpoint}")
        # You can add more checks here, like listing data sources if needed for a quick test
        # datasources = list(client.get_data_source_connections())
        # print(f"Found {len(datasources)} data sources.")
    except ValueError as ve:
        print(f"Configuration Error: {ve}")
    except Exception as e:
        print(f"Error initializing client: {e}") 