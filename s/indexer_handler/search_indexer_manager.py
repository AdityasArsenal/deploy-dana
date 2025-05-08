from azure.search.documents.indexes.models import (
    SearchIndexer,
    FieldMapping,
    IndexingParameters,
    BlobIndexerDataToExtract
)

def create_or_update_new_company_indexer(indexer_client, indexer_name, data_source_name, target_index_name, skillset_name):
    """Defines and creates or updates the 'new-company-indexer'."""
    field_mappings = [
        FieldMapping(source_field_name="metadata_storage_path", target_field_name="parent_id"),
        FieldMapping(source_field_name="metadata_storage_name", target_field_name="title"),
        FieldMapping(source_field_name="/document/myTextChunks/*", target_field_name="chunk")
    ]

    indexer_configuration_dict = {
        "indexedFileNameExtensions": ".pdf,.xml",
        "dataToExtract": BlobIndexerDataToExtract.CONTENT_AND_METADATA
    }

    indexer_parameters_obj = IndexingParameters(
        configuration=indexer_configuration_dict
    )

    indexer = SearchIndexer(
        name=indexer_name,
        description="Indexer to process company PDF and XML files, chunk them, and vectorize.",
        data_source_name=data_source_name,
        target_index_name=target_index_name,
        skillset_name=skillset_name,
        field_mappings=field_mappings,
        parameters=indexer_parameters_obj
    )

    try:
        print(f"Creating or updating indexer '{indexer_name}'...")
        indexer_client.create_or_update_indexer(indexer)
        print(f"Indexer '{indexer_name}' created or updated successfully.")
        return True
    except Exception as e:
        print(f"Error creating/updating indexer '{indexer_name}': {e}")
        if hasattr(e, 'message'):
            print(f"SDK Error Message: {e.message}")
        return False

def run_and_monitor_indexer(indexer_client, indexer_name):
    """Runs the specified indexer and prints its status."""
    try:
        print(f"Running indexer '{indexer_name}'...")
        indexer_client.run_indexer(indexer_name)
        print(f"Indexer run initiated for '{indexer_name}'.")

        import time
        print("Waiting for 30 seconds before checking indexer status...")
        time.sleep(30)
        
        status = indexer_client.get_indexer_status(indexer_name)
        print(f"Indexer '{indexer_name}' current status overview:")
        print(f"  Overall status: {status.status}")

        if status.last_result:
            print(f"  Last Result (ongoing or last completed operation details):")
            print(f"    Status: {status.last_result.status}")
            items_processed = getattr(status.last_result, 'items_processed', 'N/A')
            items_failed = getattr(status.last_result, 'items_failed', 'N/A')
            print(f"    Items processed: {items_processed}")
            print(f"    Items failed: {items_failed}")
            if status.last_result.errors:
                print("    Errors in last operation:")
                for error_item in status.last_result.errors:
                    error_details = getattr(error_item, 'details', 'N/A')
                    doc_key = getattr(error_item, 'key', 'N/A')
                    print(f"      - Message: {error_item.message}, Document: {doc_key}, Details: {error_details}")
            else:
                print("    No errors reported in the last operation.")
        else:
            print("  No 'last_result' available.")

        if status.execution_history:
            print("  Recent Execution History (latest first, up to 3):")
            for exec_item in status.execution_history[:3]:
                run_id = getattr(exec_item, 'id', 'N/A')
                print(f"    -----------------------------------------------------")
                print(f"    - Run Status: {exec_item.status}")
                print(f"      Run ID: {run_id}")
                print(f"      Start: {exec_item.start_time}, End: {exec_item.end_time}")
                print(f"      Items Processed: {exec_item.items_processed}, Items Failed: {exec_item.items_failed}")
                if exec_item.errors:
                    print("      Errors in this run:")
                    for error_item in exec_item.errors[:2]:
                        error_details_hist = getattr(error_item, 'details', 'N/A')
                        doc_key_hist = getattr(error_item, 'key', 'N/A')
                        print(f"        - {error_item.message} (Doc: {doc_key_hist}, Details: {error_details_hist})")
                else:
                    print("      No errors in this run.")
                if exec_item.warnings:
                    print("      Warnings in this run:")
                    for warning_item in exec_item.warnings[:2]:
                        warning_details_hist = getattr(warning_item, 'details', 'N/A')
                        doc_key_warning = getattr(warning_item, 'key', 'N/A')
                        print(f"        - {warning_item.message} (Doc: {doc_key_warning}, Details: {warning_details_hist})")
                else:
                    print("      No warnings in this run.")
            print(f"    -----------------------------------------------------")
        else:
            print("  No execution history available.")
        return True
    except Exception as e:
        print(f"Error running or getting status for indexer '{indexer_name}': {e}")
        return False

if __name__ == '__main__':
    # This is an example of how to use this manager directly
    from search_config import get_search_indexer_client, INDEXER_NAME, DATA_SOURCE_NAME, TARGET_INDEX_NAME, SKILLSET_NAME
    
    print("Attempting to create/update indexer directly from indexer_manager.py (for testing purposes)...")
    client = None
    try:
        client = get_search_indexer_client()
    except ValueError as ve:
        print(f"Configuration error: {ve}")
        exit()
    except Exception as e:
        print(f"Failed to initialize client: {e}")
        exit()

    if client:
        success = create_or_update_new_company_indexer(client, INDEXER_NAME, DATA_SOURCE_NAME, TARGET_INDEX_NAME, SKILLSET_NAME)
        if success:
            print("Indexer creation/update operation successful.")
            # run_and_monitor_indexer(client, INDEXER_NAME) # Uncomment to also run and monitor
        else:
            print("Indexer creation/update operation failed.") 