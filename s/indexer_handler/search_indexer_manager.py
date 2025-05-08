from azure.search.documents.indexes.models import (
    SearchIndexer,
    FieldMapping,
    FieldMappingFunction,
    IndexingParameters,
    BlobIndexerDataToExtract
)

def create_or_update_new_company_indexer(indexer_client, indexer_name, data_source_name, target_index_name, skillset_name):
    """Defines and creates or updates the 'new-company-indexer' based on ixr1.json."""
    field_mappings = [
        FieldMapping(
            source_field_name="metadata_storage_name",
            target_field_name="title",
            mapping_function=None
        ),
        FieldMapping(
            source_field_name="metadata_storage_path",
            target_field_name="companyName",
            mapping_function=FieldMappingFunction(
                name="extractTokenAtPosition",
                parameters={"delimiter": "/", "position": 0}
            )
        )
    ]

    indexer_configuration_dict = {
        "dataToExtract": BlobIndexerDataToExtract.CONTENT_AND_METADATA,
        "parsingMode": "default"
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
    