import os
from dotenv import load_dotenv

load_dotenv()

def upload_folder_to_blob(connection_string, blob_container_name, company_folder, container_client):
    
    # Get folder name for virtual folder and remove spaces
    company_folder = os.path.basename(company_folder).replace(" ", "")
    virtual_folder = f"{company_folder}/"  # Ends with a slash

    try:
        container_client.create_container()
    except Exception:
        pass  # Container may already exist

    # Create a folder marker
    folder_blob = container_client.get_blob_client(virtual_folder)
    folder_blob.upload_blob(b'', overwrite=True)

    uploaded_urls = []
    # Upload all files from the folder
    for filename in os.listdir(company_folder):
        file_path = os.path.join(company_folder, filename)
        if os.path.isfile(file_path):
            # Remove spaces from filename
            blob_name = filename.replace(" ", "")
            blob_path = virtual_folder + blob_name
            blob_client = container_client.get_blob_client(blob_path)
            
            with open(file_path, "rb") as data:
                blob_client.upload_blob(data, overwrite=True)
            
            # Get the proper URL for the uploaded blob
            uploaded_urls.append(blob_client.url)
            print(f"Uploaded {filename} as {blob_name} to {blob_container_name}/{virtual_folder}")
            print(f"URL: {blob_client.url}")

    return uploaded_urls


# Example usage:
