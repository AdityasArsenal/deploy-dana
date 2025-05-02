from azure.core.credentials import AzureKeyCredential
from azure.search.documents.indexes import SearchIndexClient
import os
from dotenv import load_dotenv

load_dotenv()

# 1) Create the client
endpoint = os.getenv("AI_SEARCH_ENDPOINT")
api_key = os.getenv("AI_SEARCH_API_KEY")
client   = SearchIndexClient(endpoint, AzureKeyCredential(api_key))

# 2) Delete the index by name
index_names = ["kliuutllllllllllllllllllliutyilyuliyuh","kliutliutyilyuliyuh","kliuutliutyilyuliyuh","your-index-name"]

for index_name in index_names:
    
    client.delete_index(index_name)  # Deletes the index and its documents unconditionally
    print(f"Index '{index_name}' deleted.")