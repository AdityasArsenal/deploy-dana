from azure.storage.blob import BlobServiceClient
from download_docs import download_xml_files
from file_to_blob import upload_folder_to_blob
# from mapping import create_indexer
from create_index import create_search_index
import os
from dotenv import load_dotenv
load_dotenv()



# download the docs
parent_folder_name = "company_files" #to save all the docs
destination_folder_name = "company_files" #to upload the docs to blob storage
limit = 2000 #limit the number of docs to download

#upload the docs to blob storage
connection_string = os.getenv("STORAGE_ACCOUNT_CONNECTION_STRING")
blob_container_name = "companiesdataaa"
blob_service_client = BlobServiceClient.from_connection_string(connection_string)
container_client = blob_service_client.get_container_client(blob_container_name)

company_names, xml_file_names, brsr_file_names, urls_of_files_uploaded, all_uploaded_blobs = download_xml_files("docs/All_xml_links.xlsx", destination_folder_name, parent_folder_name, limit, blob_container_name, container_client)

def count_blobs_in_container(container_client):
    blob_list = container_client.list_blobs()
    return len(list(blob_list))



number_of_blobs = count_blobs_in_container(container_client)
print(f"Number of blobs in the container: {number_of_blobs}")



