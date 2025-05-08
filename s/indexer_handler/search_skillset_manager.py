from azure.search.documents.indexes.models import (
    SearchIndexerSkillset,
    SplitSkill,
    InputFieldMappingEntry,
    OutputFieldMappingEntry
)

def create_or_update_chunking_skillset(indexer_client, skillset_name):
    """Defines and creates or updates a skillset for chunking document content."""
    split_skill = SplitSkill(
        name="chunker-skill",
        description="Split content into pages/chunks",
        context="/document",
        text_split_mode="pages",
        maximum_page_length=2000,  # Suitable for ada-002
        page_overlap_length=200,   # Some overlap can be beneficial
        inputs=[
            InputFieldMappingEntry(name="text", source="/document/content"),
        ],
        outputs=[
            OutputFieldMappingEntry(name="textItems", target_name="myTextChunks")
        ]
    )

    skillset = SearchIndexerSkillset(
        name=skillset_name,
        skills=[split_skill],
        description="Skillset to chunk documents"
    )

    try:
        print(f"Creating or updating skillset '{skillset_name}'...")
        indexer_client.create_or_update_skillset(skillset)
        print(f"Skillset '{skillset_name}' created or updated successfully.")
        return True
    except Exception as e:
        print(f"Error creating/updating skillset '{skillset_name}': {e}")
        return False

if __name__ == '__main__':
    # This is an example of how to use this manager directly
    # You would typically call this from your main orchestration script
    from search_config import get_search_indexer_client, SKILLSET_NAME
    
    print("Attempting to create/update skillset directly from skillset_manager.py (for testing purposes)...")
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
        success = create_or_update_chunking_skillset(client, SKILLSET_NAME)
        if success:
            print("Skillset operation successful.")
        else:
            print("Skillset operation failed.") 