from reportlab.lib.pagesizes import letter
from reportlab.pdfgen import canvas
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
from reportlab.platypus import SimpleDocTemplate, Paragraph, Spacer
from reportlab.lib.units import inch
from reportlab.lib import colors
import os
from datetime import datetime
import re

def markdown_to_reportlab(text):
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

def conversation_to_pdf(conversation_history, output_dir="conversation_pdfs"):

    # Create output directory if it doesn't exist
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)
    
    # Generate filename with timestamp
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    filename = f"agent_conversation_{timestamp}.pdf"
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
    
    story = []
    
    # Add title
    title_style = styles["Title"]
    story.append(Paragraph("Agent Conversation History", title_style))
    story.append(Paragraph(f"Generated on: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}", styles["Normal"]))
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
    
    # Build PDF
    doc.build(story)
    
    return filepath


# Example usage:
# from consertations_handling import agents_conv_history
# conversation_history = agents_conv_history(conversation_id, collection, chat_history_retrieval_limit)
# conversation_to_pdf(conversation_history)