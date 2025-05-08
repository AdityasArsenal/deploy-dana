import os
from dotenv import load_dotenv
from azure.core.exceptions import ResourceExistsError

load_dotenv()

def upload_folder_to_blob(blob_container_name, local_company_folder_path, container_client):
    
    # Derive the virtual folder name for blob storage from the base name of the local_company_folder_path
    virtual_folder_base_name = os.path.basename(local_company_folder_path).replace(" ", "")
    virtual_folder_for_blob = f"{virtual_folder_base_name}/"  # Ends with a slash

    try:
        print(f"Attempting to ensure container '{blob_container_name}' exists...")
        container_client.create_container()
        print(f"Container '{blob_container_name}' created successfully or was already ready.")
    except ResourceExistsError:
        print(f"Container '{blob_container_name}' already exists. Proceeding to use it.")
    # Note: Other exceptions during create_container() will now propagate, 
    # rather than being caught by a general 'except Exception: pass'.

    # Create a folder marker in blob storage using the derived virtual folder name
    folder_blob_client = container_client.get_blob_client(virtual_folder_for_blob)
    folder_blob_client.upload_blob(b'', overwrite=True)

    uploaded_urls = []
    
    # This print shows the actual path being used by os.listdir
    print(f"Listing files in local directory: {local_company_folder_path}")
    print(f"Uploading to virtual folder in blob: {virtual_folder_for_blob}")
    
    # Iterate over files in the original local directory
    for filename in os.listdir(local_company_folder_path): # Use the original full path
        file_path_on_disk = os.path.join(local_company_folder_path, filename) # Use original full path to build disk path
        
        if os.path.isfile(file_path_on_disk):
            # Prepare blob name (remove spaces from filename)
            blob_name_in_virtual_folder = filename.replace(" ", "")
            # Construct full blob path using the virtual folder for blob
            full_blob_path = virtual_folder_for_blob + blob_name_in_virtual_folder
            
            blob_client = container_client.get_blob_client(full_blob_path)
            
            with open(file_path_on_disk, "rb") as data:
                blob_client.upload_blob(data, overwrite=True)
            
            uploaded_urls.append(blob_client.url)
            print(f"Uploaded local file '{filename}' as blob '{blob_name_in_virtual_folder}' to container '{blob_container_name}', path '{virtual_folder_for_blob}'")
            print(f"URL: {blob_client.url}")

    return uploaded_urls


# Example usage:
