from v_search import semantic_hybrid_search

def worker(
    client,
    deployment,
    sub_question,
    company_names,
    agents_conversation_history,
    search_client,
    system_prompt,
    top_k
):
    # return "worker responded", ["","",""]

    context_chunks, titles = semantic_hybrid_search(sub_question, search_client, top_k, company_names)

    user_message_with_context = f"My question: {sub_question}\n\nRelevant information:: {context_chunks}"

    completion = client.chat.completions.create(
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

    print("===============context chunks:=================")
    print(context_chunks)
    print("===============context chunks:=================")
    print(f"\Message: {completion.choices[0].message}")

    print("===============sub :=================")
    print(f"Sub: {sub_question} AND company names: {company_names}\n")
    print(f"user role: Previous conversation: {agents_conversation_history},\nMy question: {sub_question}")
    print("===============sub :=================")

    response_message = completion.choices[0].message.content
    #print("worker exicuted")