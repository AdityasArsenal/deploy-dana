import os
from tools.conv_to_pdf_handler import conversation_to_pdf, upload_pdf_to_blob
from tools.conv_handler import get_agents_conv_history, get_agents_total_conv_history

async def director(
    llm_client,
    director_system_prompt,
    deployment,
    user_prompt,
    user_conversation_history,
    connection,
    all_context_chunks,                     #list of lists if chunks
    agents_conversation_id,
    conversation_id
):
    print("DDDD")
    agents_conversation_history = await get_agents_conv_history(agents_conversation_id, connection)
    completion = await llm_client.chat.completions.create(
        model=deployment,
        messages=[
            {"role": "system", "content": director_system_prompt},
            {"role": "user", "content": f"Previous conversation between user and you: {user_conversation_history},\nMy question: {user_prompt}"},
            {"role": "assistant", "content": f"Previous conversation between you and worker agent: {agents_conversation_history}"}
        ],
        max_tokens=800,
        temperature=0.7,
        top_p=0.95,
        frequency_penalty=0,
        presence_penalty=0,
        stop=None
    )

    direcotr_response = completion.choices[0].message.content
    
    # monolog(agents_conversation_history)
    # Save the conversation to PDF
    output_dir="conversation_pdfs"

    agents_total_conversation_history = await get_agents_total_conv_history(conversation_id, connection)

    pdf_path = await conversation_to_pdf(agents_total_conversation_history, direcotr_response, output_dir)
    conv_pdf_url = await upload_pdf_to_blob(pdf_path)
    os.remove(pdf_path)

    # print(f"Conversation saved to PDF: {pdf_path}")
    
    return direcotr_response, conv_pdf_url
