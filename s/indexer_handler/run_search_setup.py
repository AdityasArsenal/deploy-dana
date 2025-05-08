from search_config import (
    get_search_indexer_client, 
    SKILLSET_NAME, 
    INDEXER_NAME, 
    DATA_SOURCE_NAME, 
    TARGET_INDEX_NAME
)
from search_skillset_manager import create_or_update_chunking_skillset
from search_indexer_manager import create_or_update_new_company_indexer, run_and_monitor_indexer

def main():
    print("Starting Azure Search setup...")
    
    client = None
    try:
        client = get_search_indexer_client()
        print("Search client initialized.")
    except ValueError as ve:
        print(f"Configuration Error: {ve}")
        return
    except Exception as e:
        print(f"Failed to initialize search client: {e}")
        return

    # 1. Create or Update Skillset
    if not create_or_update_chunking_skillset(client, SKILLSET_NAME):
        print("Skillset creation/update failed. Exiting.")
        return
    print("Skillset setup completed.")

    # 2. Create or Update Indexer
    if not create_or_update_new_company_indexer(client, INDEXER_NAME, DATA_SOURCE_NAME, TARGET_INDEX_NAME, SKILLSET_NAME):
        print("Indexer creation/update failed. Exiting.")
        return
    print("Indexer setup completed.")

    # 3. Optionally, run the indexer and monitor
    # Set to True to run the indexer immediately after setup
    RUN_INDEXER_AFTER_SETUP = True

    if RUN_INDEXER_AFTER_SETUP:
        print("Proceeding to run and monitor the indexer...")
        run_and_monitor_indexer(client, INDEXER_NAME)
    else:
        print(f"Indexer '{INDEXER_NAME}' is set up. You can run it from the Azure portal or by modifying RUN_INDEXER_AFTER_SETUP.")

    print("\nAzure Search setup process finished.")
    print("Please check the Azure portal for detailed status and logs, especially if issues persist.")

if __name__ == "__main__":
    main() 