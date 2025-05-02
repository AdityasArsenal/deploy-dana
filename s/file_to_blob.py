from azure.storage.blob import BlobServiceClient
import os

def upload_folder_to_blob(folder_path):
    connection_string = "DefaultEndpointsProtocol=https;AccountName=blobbstore;AccountKey=VMaB7g29Bzjz08nhva9ENFG0stLfycm4Y7Q0jTsts/i+z0AEupZkQo6GONStpNpV+VcNdm0LGWL3+AStyW8pYg==;EndpointSuffix=core.windows.net"
    container_name = "companiesdataaa"
    
    # Get folder name for virtual folder and remove spaces
    folder_name = os.path.basename(folder_path).replace(" ", "")
    virtual_folder = f"{folder_name}/"  # Ends with a slash

    blob_service_client = BlobServiceClient.from_connection_string(connection_string)
    container_client = blob_service_client.get_container_client(container_name)

    try:
        container_client.create_container()
    except Exception:
        pass  # Container may already exist

    # Create a folder marker
    folder_blob = container_client.get_blob_client(virtual_folder)
    folder_blob.upload_blob(b'', overwrite=True)

    uploaded_urls = []
    # Upload all files from the folder
    for filename in os.listdir(folder_path):
        file_path = os.path.join(folder_path, filename)
        if os.path.isfile(file_path):
            # Remove spaces from filename
            blob_name = filename.replace(" ", "")
            blob_path = virtual_folder + blob_name
            blob_client = container_client.get_blob_client(blob_path)
            
            with open(file_path, "rb") as data:
                blob_client.upload_blob(data, overwrite=True)
            
            # Get the proper URL for the uploaded blob
            uploaded_urls.append(blob_client.url)
            print(f"Uploaded {filename} as {blob_name} to {container_name}/{virtual_folder}")
            print(f"URL: {blob_client.url}")

    return uploaded_urls


# Example usage:

parent_folder_path = "company_files"
file_paths = []

for filename in os.listdir(parent_folder_path):
    file_path = os.path.join(parent_folder_path, filename)
    urls = []

    file_paths.append(file_path)
    urls.append(upload_folder_to_blob(file_path))

    print(f"Uploaded files from {filename} /n and URLs: {urls}/n")