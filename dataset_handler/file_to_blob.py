import os
from dotenv import load_dotenv
from azure.core.exceptions import ResourceExistsError

load_dotenv()

def upload_folder_to_blob(blob_container_name, local_company_folder_path, container_client):
    
    # Derive the virtual folder name for blob storage from the base name of the local_company_folder_path
    virtual_folder_base_name = os.path.basename(local_company_folder_path).replace(" ", "")
    virtual_folder_for_blob = f"{virtual_folder_base_name}/"  # Ends with a slash

    # Commented out because we just created the container in the main_DB.py file no need to check if it exists
    try:
        # print(f"Attempting to ensure container '{blob_container_name}' exists.../n")
        container_client.create_container()
        # print(f"Container '{blob_container_name}' created successfully or was already ready./n")
    except ResourceExistsError:
        print(f"Container '{blob_container_name}' already exists. Proceeding to use it. \n")
    # Note: Other exceptions during create_container() will now propagate, 
    # rather than being caught by a general 'except Exception: pass'.

    # Create a folder marker in blob storage using the derived virtual folder name
    folder_blob_client = container_client.get_blob_client(virtual_folder_for_blob)
    folder_blob_client.upload_blob(b'', overwrite=True)

    uploaded_urls = []
    blob_names = []
    
    # Iterate over files in the original local directory
    for filename in os.listdir(local_company_folder_path): # Use the original full path
        file_path_on_disk = os.path.join(local_company_folder_path, filename) # Use original full path to build disk path
        
        if os.path.isfile(file_path_on_disk):
            # Prepare blob name (remove spaces from filename)
            blob_name_in_virtual_folder = filename.replace(" ", "")
            blob_names.append(blob_name_in_virtual_folder)
            # Construct full blob path using the virtual folder for blob
            full_blob_path = virtual_folder_for_blob + blob_name_in_virtual_folder
            
            blob_client = container_client.get_blob_client(full_blob_path)
            
            with open(file_path_on_disk, "rb") as data:
                # print(f"Uploading {filename} to blob storage... \n")
                blob_client.upload_blob(data, overwrite=True)
            
            uploaded_urls.append(blob_client.url)

    return uploaded_urls, blob_names


# Example usage:
