from azure.search.documents.indexes import SearchIndexerClient
from azure.search.documents.indexes.models import (
    SearchIndexer,
    FieldMapping,
    FieldMappingFunction,
    SearchIndexerDataSourceConnection,
    SearchIndexerDataContainer
)
from azure.core.credentials import AzureKeyCredential
import os

# 1) Initialize the indexer client
ai_search_endpoint = os.getenv("AI_SEARCH_ENDPOINT")
ai_search_api_key      = os.getenv("AI_SEARCH_API_KEY")
idxr_client  = SearchIndexerClient(ai_search_endpoint, AzureKeyCredential(ai_search_api_key))

# 2) (Re)create your data source if needed
#create data source here 

ds_container = SearchIndexerDataContainer(
    name="companydata",
    query="*"       # all blobs under /companydata
)
ds_connection = SearchIndexerDataSourceConnection(
    name="companydata-blob",
    type="azureblob",
    connection_string=os.getenv("AZURE_STORAGE_CONN"),
    container=ds_container
)
idxr_client.create_or_update_data_source_connection(ds_connection)


# 3) Build the field mapping for companyName
company_mapping = FieldMapping(
    source_field_name="metadata_storage_path",
    target_field_name="companyName",
    mapping_function=FieldMappingFunction.extract_token_at_position(
        delimiter="/",
        position=4
    )
)

# 4) Create or update the indexer with this mapping
indexer = SearchIndexer(
    name="companydata-indexer",
    data_source_name="companydata-blob",
    target_index_name="kliuuuuuuuuuuuuuuutllllllllllllllllllliutyilyuliyuh",
    field_mappings=[company_mapping],
    # schedule={"interval": "PT5M"}  # optional scheduling
)
idxr_client.create_or_update_indexer(indexer)

# 5) Run the indexer on‑demand and check status
idxr_client.run_indexer("companydata-indexer")
status = idxr_client.get_indexer_status("companydata-indexer")
print("Last run status:", status.last_result.status)
print("Processed blobs:", status.last_result.items_processed)
print("Errors:", status.last_result.errors)
