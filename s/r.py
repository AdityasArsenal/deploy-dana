from azure.search.documents.indexes.models import FieldMapping, FieldMappingFunction

company_mapping = FieldMapping(
    source_field_name="metadata_storage_path",
    target_field_name="companyName",
    mapping_function=FieldMappingFunction.extract_token_at_position(
        delimiter="/",
        position=4
    )
)

print(company_mapping)