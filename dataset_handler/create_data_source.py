from azure.search.documents.indexes import SearchIndexerClient
from azure.search.documents.indexes.models import SearchIndexerDataSourceConnection, SearchIndexerDataContainer
from azure.core.credentials import AzureKeyCredential
import os
from dotenv import load_dotenv

load_dotenv()

ai_search_endpoint = os.getenv("AI_SEARCH_ENDPOINT")
ai_search_api_key = os.getenv("AI_SEARCH_API_KEY")
blob_conn_string = os.getenv("STORAGE_ACCOUNT_CONNECTION_STRING")

idxr_client = SearchIndexerClient(ai_search_endpoint, AzureKeyCredential(ai_search_api_key))

ds_container = SearchIndexerDataContainer(name="test-company-data")
ds_connection = SearchIndexerDataSourceConnection(
    name="test-companydata-blob-datasourcee",
    description="Azure Blob Storage connection to indexer",
    type="azureblob",
    connection_string=blob_conn_string,
    container=ds_container
)

idxr_client.create_or_update_data_source_connection(ds_connection)

