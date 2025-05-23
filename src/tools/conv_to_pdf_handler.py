"""
Conversation to PDF Handler Module

This module handles the generation and storage of PDF reports from conversation data.
It creates comprehensive, formatted reports of agent interactions and uploads them
to Azure Blob Storage for user access and documentation purposes.

Key Responsibilities:
- Convert conversation history to formatted PDF documents
- Handle markdown formatting for readable reports
- Upload PDF reports to Azure Blob Storage
- Provide downloadable URLs for generated reports

Dependencies:
- ReportLab: PDF generation library
- Azure Blob Storage: Cloud storage for PDF files
- Markdown processing: Text formatting for reports

Related Files:
- agents/director_agent.py: Primary consumer for PDF generation and upload
- tools/conv_handler.py: Provides conversation data for PDF content
- app.py: Returns PDF URLs to users (via director agent)

External Dependencies:
- Azure Blob Storage account with configured container
- ReportLab library for PDF generation
- Proper Azure storage credentials and container permissions
"""

import os
import asyncio
from typing import List, Dict, Tuple, Any
from reportlab.lib.pagesizes import letter
from reportlab.pdfgen import canvas
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
from reportlab.platypus import SimpleDocTemplate, Paragraph, Spacer
from reportlab.lib.units import inch
from reportlab.lib import colors
from datetime import datetime
import re
from azure.storage.blob import BlobServiceClient
from dotenv import load_dotenv

load_dotenv()

# Blob storage configuration
# Used for uploading generated PDF reports
container_name = os.getenv("BLOB_CONTAINER_FOR_REPORT")
connection_string=os.getenv("STORAGE_ACCOUNT_CONNECTION_STRING")
print("*****************")
print(connection_string)
print("*****************")

def markdown_to_reportlab(text: str) -> str:
    """
    Convert markdown formatting to ReportLab HTML-like markup.
    
    This function transforms common markdown elements into ReportLab-compatible
    markup for proper formatting in PDF documents, ensuring readable reports.
    
    Args:
        text (str): Raw text content with markdown formatting
        
    Returns:
        str: Text converted to ReportLab-compatible markup
        
    Supported Markdown Elements:
        - Headers (# ## ###)
        - Bold text (**)
        - Italic text (*)
        - Bullet points (-)
        - Numbered lists
        - Document references [doc1]
        
    Related Files:
        - Used by _conversation_to_pdf_sync() for formatting agent responses
        - Processes text from tools/conv_handler.py conversation data
    """
    # Convert headers
    text = re.sub(r'### (.*?)\n', r'<font face="Helvetica-Bold" size="12">\1</font><br/>', text)
    text = re.sub(r'## (.*?)\n', r'<font face="Helvetica-Bold" size="14">\1</font><br/>', text)
    text = re.sub(r'# (.*?)\n', r'<font face="Helvetica-Bold" size="16">\1</font><br/>', text)
    
    # Convert bold text
    text = re.sub(r'\*\*(.*?)\*\*', r'<b>\1</b>', text)
    
    # Convert italic text
    text = re.sub(r'\*(.*?)\*', r'<i>\1</i>', text)
    
    # Convert bullet points
    text = re.sub(r'- (.*?)\n', r'• \1<br/>', text)
    
    # Convert numbered lists
    text = re.sub(r'(\d+)\. (.*?)\n', r'\1. \2<br/>', text)
    
    # Handle line breaks
    text = text.replace('\n', '<br/>')
    
    # Handle document references like [doc1], [doc2]
    text = re.sub(r'\[(doc\d+)\]', r'<i>[\1]</i>', text)
    
    return text

async def conversation_to_pdf(
    conversation_history: List[Dict[str, str]], 
    direcotr_response: str,
    output_dir: str = "./conversation_pdfs"
) -> str:
    """
    Generate a comprehensive PDF report from conversation history.
    
    This function creates a formatted PDF document containing all agent interactions,
    sub-questions, responses, and the final director synthesis. Essential for
    providing users with detailed documentation of the agentic workflow.
    
    Args:
        conversation_history (List[Dict[str, str]]): Agent conversation data
            Format: [{"role": "manager_agent|worker_agent", "content": "..."}]
        direcotr_response (str): Final synthesized response from director agent
        output_dir (str): Directory path for saving PDF files (default: "./conversation_pdfs")
        
    Returns:
        str: File path to the generated PDF document
        
    Workflow:
        1. Create output directory if needed
        2. Generate timestamped filename
        3. Format conversation as Q&A pairs
        4. Apply markdown formatting via markdown_to_reportlab()
        5. Build structured PDF with ReportLab
        6. Return file path for upload process
        
    Related Files:
        - agents/director_agent.py: Primary caller for PDF generation
        - tools/conv_handler.py: Provides conversation_history data
        - upload_pdf_to_blob(): Handles subsequent upload to Azure Blob Storage
        
    PDF Structure:
        - Title and generation timestamp
        - Description of the agentic process
        - Numbered Q&A pairs from agent interactions
        - Final summary (director response)
    """
    # This function uses the reportlab library which is not async-compatible
    # Run the CPU-intensive PDF generation in a thread pool to not block the event loop
    return await asyncio.to_thread(_conversation_to_pdf_sync, conversation_history, direcotr_response, output_dir)

def _conversation_to_pdf_sync(
    conversation_history: List[Dict[str, str]], 
    direcotr_response: str, 
    output_dir: str
) -> str:
    """
    Synchronous PDF generation implementation.
    
    This function contains the actual PDF generation logic using ReportLab,
    wrapped in asyncio.to_thread() to avoid blocking the async event loop.
    
    Args:
        conversation_history (List[Dict[str, str]]): Agent conversation data
        direcotr_response (str): Final director agent response
        output_dir (str): Directory for PDF output
        
    Returns:
        str: Generated PDF file path
        
    Implementation Details:
        - Uses ReportLab for PDF generation (synchronous library)
        - Creates custom styles for different content types
        - Processes markdown formatting for readable output
        - Generates timestamped filenames for uniqueness
        
    Related Files:
        - Called by conversation_to_pdf() for async compatibility
        - Uses markdown_to_reportlab() for text formatting
    """
    # Create output directory if it doesn't exist
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)
    
    # Generate filename with timestamp
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    filename = f"Researched_info_{timestamp}.pdf"
    filepath = os.path.join(output_dir, filename)
    
    # Create PDF document
    doc = SimpleDocTemplate(filepath, pagesize=letter)
    styles = getSampleStyleSheet()
    
    # Create custom styles
    answer_style = ParagraphStyle(
        'AnswerStyle',
        parent=styles['BodyText'],
        spaceBefore=6,
        spaceAfter=12,
        leftIndent=20,
        fontSize=10
    )
    
    question_style = ParagraphStyle(
        'QuestionStyle',
        parent=styles['Heading2'],
        spaceBefore=10,
        fontSize=12,
        textColor=colors.black
    )

    discription_style = ParagraphStyle(
    'DiscriptionStyle',
    parent=styles['BodyText'],
    spaceBefore=6,
    fontSize=8,
    textColor=colors.gray
    )

    story = []
    
    # Add title
    title_style = styles["Title"]
    story.append(Paragraph("Generated Report for your Analysis", title_style))
    story.append(Paragraph(f"Generated on: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}", styles["Normal"]))
    
    story.append(Paragraph(
        """(Generated Report for your Analysis:
            This report gives a glimpse of what happened in the backend after you asked your query to the demo tool. The questions below are the broken-down queries that Manager Agent created from your original query. And the answers are that the generated response by the Worker Agent after he retrieved the data from the vector database. The Director Agent collects this report and then responds to your original query, which is added as summary at the end of this report.)""", discription_style))
    
    story.append(Spacer(1, 0.25*inch))  # Add space
    
    # Format conversation as numbered Q&A pairs
    qa_pairs = []
    current_question = None
    
    # Group into Q&A pairs
    for item in conversation_history:
        if item['role'] == 'manager_agent':
            current_question = item['content'].replace("subquestion = ", "")

        elif item['role'] == 'worker_agent':
            answer = item['content'].replace("answer =", "")
            qa_pairs.append({'question': current_question, 'answer': answer})
    
    # Add numbered Q&A pairs to PDF
    for i, pair in enumerate(qa_pairs, 1):
        # Add question
        question_text = f"{i}. {pair['question']}"
        story.append(Paragraph(question_text, question_style))
        
        # Add answer with indentation, converting markdown to reportlab format
        formatted_answer = markdown_to_reportlab(pair['answer'])
        answer_text = f"{formatted_answer}"
        story.append(Paragraph(answer_text, answer_style))
        
        # Add spacing between Q&A pairs
        story.append(Spacer(1, 0.2*inch))
    
    # Symmary (director response)
    story.append(Paragraph("Summary", question_style))
    final_formatted_answer = markdown_to_reportlab(direcotr_response)
    story.append(Paragraph(final_formatted_answer, answer_style))
    story.append(Spacer(1, 0.2*inch))
    
    # Build PDF
    doc.build(story)
    
    return filepath

async def upload_pdf_to_blob(pdf_path: str) -> str:
    """
    Upload generated PDF to Azure Blob Storage and return public URL.
    
    This function handles the upload of PDF reports to Azure Blob Storage,
    providing users with downloadable links to their conversation summaries.
    
    Args:
        pdf_path (str): Local file path to the generated PDF document
        
    Returns:
        str: Public URL for accessing the uploaded PDF
        
    Workflow:
        1. Create Azure Blob Storage client using connection string
        2. Upload PDF file with overwrite enabled
        3. Return public blob URL for user access
        4. Enable users to download comprehensive workflow reports
        
    Related Files:
        - agents/director_agent.py: Primary caller after PDF generation
        - conversation_to_pdf(): Provides pdf_path for upload
        - app.py: Returns blob URL to users (via director agent)
        
    Configuration:
        - Uses STORAGE_ACCOUNT_CONNECTION_STRING environment variable
        - Uses BLOB_CONTAINER_FOR_REPORT environment variable
        - Requires proper Azure storage account setup and permissions
    """
    # Use a thread pool to run the synchronous blob upload
    return await asyncio.to_thread(_upload_pdf_to_blob_sync, pdf_path)

def _upload_pdf_to_blob_sync(pdf_path: str) -> str:
    """
    Synchronous Azure Blob Storage upload implementation.
    
    This function contains the actual blob upload logic, wrapped in
    asyncio.to_thread() to avoid blocking the async event loop.
    
    Args:
        pdf_path (str): Local file path to the PDF to upload
        
    Returns:
        str: Public URL of the uploaded blob
        
    Implementation Details:
        - Uses Azure Blob Storage SDK (synchronous operations)
        - Overwrites existing blobs with same name
        - Returns direct blob URL for user access
        - Handles file reading and upload in chunks
        
    Related Files:
        - Called by upload_pdf_to_blob() for async compatibility
        - Used by agents/director_agent.py for file cleanup coordination
    """
    # Create the BlobServiceClient using the connection string
    blob_service_client = BlobServiceClient.from_connection_string(connection_string)
    
    # Get a blob client
    blob_client = blob_service_client.get_blob_client(container=container_name, blob=os.path.basename(pdf_path))

    # Upload the file
    with open(pdf_path, "rb") as pdf_file:
        blob_client.upload_blob(pdf_file, overwrite=True)

    return blob_client.url  # Return the URL of the uploaded PDF

# Example usage:
# from consertations_handling import agents_conv_history
# conversation_history = agents_conv_history(conversation_id, collection, chat_history_retrieval_limit)
# conversation_to_pdf(conversation_history)