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
limit = 10 #limit the number of docs to download

company_names = download_xml_files("docs/All_xml_links.xlsx", parent_folder_name, limit)



#upload the docs to blob storage
connection_string = os.getenv("STORAGE_ACCOUNT_CONNECTION_STRING")
blob_container_name = "companiesdataaa"
blob_service_client = BlobServiceClient.from_connection_string(connection_string)
container_client = blob_service_client.get_container_client(blob_container_name)
urls_of_files_uploaded = [] # all the urls of the files uploaded

for company in os.listdir(parent_folder_name):
    company_folder = os.path.join(parent_folder_name, company)

    urls = upload_folder_to_blob(blob_container_name, company_folder, container_client)

    urls_of_files_uploaded.extend(urls)
    
    print(f"Uploaded file from {company} to blob storage /n URLs is : {urls}/n")



#create the index
# index_name = "new-vector-index"
# endpoint = os.getenv("AI_SEARCH_ENDPOINT")
# api_key = os.getenv("AI_SEARCH_API_KEY")

# create_search_index(endpoint, api_key, index_name)


