# ESGAI Project Documentation

## Overview
ESGAI is an agent-based system that processes user queries related to ESG (Environmental, Social, and Governance) information, specifically focusing on company carbon emissions and related environmental data. The system uses a multi-agent approach to break down complex queries, search for relevant information, and synthesize comprehensive answers.

## Architecture
The system follows a hierarchical agent architecture:

1. **Manager Agent**: Coordinates the overall process, breaks user queries into sub-questions
2. **Worker Agents**: Process individual sub-questions by retrieving and analyzing relevant information
3. **Director Agent**: Synthesizes information from worker agents to create a final comprehensive response

## Core Files

### src/agentic.py
**Main orchestration module** that implements the agent workflow:
- **Dependencies**: All agent modules, tools, prompts, Azure services
- **Key Functions**: 
  - `manager()`: Orchestrates the complete agentic workflow
  - `load_prompt_from_file()`: Loads system prompts for agent behavior
- **Integrations**: 
  - Calls `agents/sub_question_handler.py` for parallel processing
  - Uses `agents/director_agent.py` for final synthesis
  - Leverages `tools/json_parseing.py` for LLM response parsing
- **Configuration**: Loads Azure OpenAI and AI Search configurations

### src/app.py
**FastAPI web application** that provides the HTTP interface:
- **Dependencies**: agentic.py, tools/conv_handler.py, MongoDB, Azure OpenAI
- **Key Functions**:
  - `agentic_flow()`: Entry point for multi-agent processing
  - `chat()`: Main API endpoint handling user requests
- **Features**: CORS middleware, conversation management, session handling
- **Integrations**: Calls `agentic.py` manager function, persists data via `tools/conv_handler.py`

### src/agents/director_agent.py
**Final synthesis agent** that creates comprehensive responses:
- **Dependencies**: tools/conv_to_pdf_handler.py, tools/conv_handler.py, Azure services
- **Key Functions**:
  - `director()`: Synthesizes worker responses into final answer
- **Responsibilities**: PDF generation, conversation persistence, response synthesis
- **Integrations**: 
  - Called by `agentic.py` with aggregated worker responses
  - Uses context chunks from `agents/worker_agent.py` (via aggregation)
  - Generates PDFs via `tools/conv_to_pdf_handler.py`

### src/agents/worker_agent.py
**Core information retrieval agents** for processing sub-questions:
- **Dependencies**: tools/v_search.py, Azure AI Search, Azure OpenAI
- **Key Functions**:
  - `worker()`: Processes individual sub-questions with semantic search
- **Responsibilities**: Semantic search, context retrieval, focused response generation
- **Integrations**:
  - Called by `agents/sub_question_handler.py` for each sub-question
  - Uses `tools/v_search.py` for information retrieval
  - Results aggregated by `agentic.py` manager function

### src/agents/sub_question_handler.py
**Middleware component** between Manager and Worker agents:
- **Dependencies**: agents/worker_agent.py, tools/conv_handler.py, MongoDB
- **Key Functions**:
  - `process_sub_question()`: Coordinates worker processing and data persistence
- **Responsibilities**: Worker coordination, conversation persistence, data flow management
- **Integrations**:
  - Called by `agentic.py` for each generated sub-question
  - Invokes `agents/worker_agent.py` for processing
  - Persists results via `tools/conv_handler.py`

## Tools

### src/tools/v_search.py
**Semantic hybrid search engine** for information retrieval:
- **Dependencies**: Azure AI Search, Azure OpenAI (embeddings)
- **Key Functions**:
  - `semantic_hybrid_search()`: Combines vector and text search with company filtering
- **Features**: Company-specific filtering, hybrid search, async operations
- **Integrations**:
  - Primary consumer: `agents/worker_agent.py`
  - Configuration provided by `agentic.py`
  - Supports PDF and XML document formats

### src/tools/conv_handler.py
**Conversation management system** for database operations:
- **Dependencies**: MongoDB (Azure Cosmos DB)
- **Key Functions**:
  - `conv_history()`: Retrieves conversation context
  - `inserting_chat_buffer()`: Persists user conversations
  - `inserting_agent_chat_buffer()`: Persists agent interactions
  - `get_agents_conv_history()`: Retrieves agent conversation history
- **Integrations**:
  - Used by `app.py` for conversation retrieval and persistence
  - Used by `agents/sub_question_handler.py` for agent conversation storage
  - Used by `agents/director_agent.py` for conversation history access

### src/tools/conv_to_pdf_handler.py
**PDF generation and storage system**:
- **Dependencies**: Azure Blob Storage
- **Key Functions**:
  - `conversation_to_pdf()`: Creates formatted PDF from conversation data
  - `upload_pdf_to_blob()`: Uploads PDF to Azure Blob Storage
- **Integrations**:
  - Called by `agents/director_agent.py` for conversation summaries
  - Provides downloadable URLs returned to users via `app.py`

### src/tools/json_parseing.py
**LLM response parsing utility**:
- **Key Functions**:
  - `parse_json_from_model_response()`: Extracts structured data from LLM outputs
- **Features**: Error handling, validation, fallback mechanisms
- **Integrations**:
  - Used by `agentic.py` manager function for parsing sub-questions and company names
  - Provides robust JSON extraction from potentially malformed LLM responses

## Prompts
The system uses three main prompt templates that define agent behavior:

### prompts/manager_system_prompt.txt
**Manager agent instructions** for query decomposition:
- Defines how to break complex questions into sub-questions
- Specifies company name identification requirements
- Sets JSON output format requirements
- **Used by**: `agentic.py` manager function

### prompts/worker_system_prompt.txt
**Worker agent instructions** for information processing:
- Guides sub-question analysis approach
- Defines response format and context utilization
- **Used by**: `agents/worker_agent.py` for response generation

### prompts/director_system_prompt.txt
**Director agent instructions** for response synthesis:
- Defines synthesis methodology for multiple information sources
- Specifies final response format and structure
- **Used by**: `agents/director_agent.py` for final answer generation

## Data Flow Architecture

```
User Query (app.py)
    ↓
Manager Agent (agentic.py)
    ↓
Sub-questions + Company Names
    ↓
Parallel Processing:
├── Sub-question Handler (sub_question_handler.py)
│   ├── Worker Agent (worker_agent.py)
│   │   └── Semantic Search (v_search.py)
│   └── Database Persistence (conv_handler.py)
├── Sub-question Handler...
└── Sub-question Handler...
    ↓
Aggregated Results (agentic.py)
    ↓
Director Agent (director_agent.py)
    ├── Response Synthesis
    ├── PDF Generation (conv_to_pdf_handler.py)
    └── Blob Storage Upload
    ↓
Final Response + PDF URL (app.py)
```

## Workflow
1. **User Input**: Query submitted through `/chat` endpoint in `app.py`
2. **History Retrieval**: `tools/conv_handler.py` fetches conversation context
3. **Query Decomposition**: `agentic.py` manager breaks query into sub-questions using `tools/json_parseing.py`
4. **Parallel Processing**: Multiple `agents/sub_question_handler.py` instances coordinate worker agents
5. **Information Retrieval**: `agents/worker_agent.py` uses `tools/v_search.py` for semantic search
6. **Data Persistence**: Agent conversations stored via `tools/conv_handler.py`
7. **Response Synthesis**: `agents/director_agent.py` creates comprehensive answer
8. **Documentation**: PDF summary generated via `tools/conv_to_pdf_handler.py`
9. **Response Delivery**: Final answer, references, and PDF URL returned to user

## Key Integration Points

- **Azure OpenAI**: Used by all agent modules for LLM processing
- **Azure AI Search**: Vector and text search via `tools/v_search.py`
- **MongoDB**: Conversation persistence via `tools/conv_handler.py`
- **Azure Blob Storage**: PDF hosting via `tools/conv_to_pdf_handler.py`

## Development Notes

All modules now contain comprehensive inline documentation with:
- File-level module descriptions
- Function-level documentation with args, returns, and workflow
- Cross-references to related files and dependencies
- Integration points and data flow descriptions
- Error handling and fallback mechanisms 