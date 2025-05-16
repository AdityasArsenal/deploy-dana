from tools.v_search import semantic_hybrid_search

async def worker(
    llm_client,
    deployment,
    sub_question,
    company_names,
    agents_conversation_history,
    search_client,
    system_prompt,
    top_k,
    conversation_id
):
    # return "worker responded", ["","",""]

    context_chunks, titles = await semantic_hybrid_search(sub_question, search_client, top_k, company_names)

    user_message_with_context = f"#My question: {sub_question}\n\n#Relevant information: {context_chunks}"

    completion = await llm_client.chat.completions.create(
        model=deployment,
        messages=[
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": user_message_with_context}
        ],
        max_tokens=800,
        temperature=0.7,
        top_p=0.95,
        frequency_penalty=0,
        presence_penalty=0,
        stop=None
    )

    response_message = completion.choices[0].message.content
    
    return response_message, context_chunks