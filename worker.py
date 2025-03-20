import json

system_prompt = """
#Role :You are an ESG consultant with 10-years of experience in India’s BRSR standards and International GRI standards. You have a deep understanding of sustainability consulting, BRSR reporting, XBRL reporting, sustainability reporting, GRI guidelines etc. As an expert in ESG consulting, you know what information is generally available inside the XBRL Datasheets; Indian BRSR and Sustainability Reports; and also in global GRI-standard sustainability reports.
Your task is to understand the question first and then create a well-structured answer using the information chunks provided from the data source to you. Your answer must always contain all the relevant qualitative and quantitative information that you find inside these chunks. If the chunks do not contain the exact information required to answer the question directly then try to provide the closest information that you can from the provided chunks. Always answer with well structured and clear bullet points having both qualitative and quantitative data.


and brfore responding say worker speaking
"""

def worker(
    client,
    deployment,
    manager_edited_prompt,
    provided_conversation_history,
    azure_search_endpoint,
    azure_search_index, 
    azure_search_api_key,
):

    completion = client.chat.completions.create(
        model=deployment,
        messages=[
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": f"Previous conversation: {provided_conversation_history},\nMy question: {manager_edited_prompt}"}
        ],
        max_tokens=800,
        temperature=0.7,
        top_p=0.95,
        frequency_penalty=0,
        presence_penalty=0,
        stop=None,
        extra_body={
            "data_sources": [
                {
                    "type": "azure_search",
                    "parameters": {
                        "endpoint": azure_search_endpoint,
                        "index_name": azure_search_index,
                        "authentication": {
                            "type": "api_key",
                            "key": azure_search_api_key
                        }
                    }
                }
            ]
        }
    )
    
    response_message = completion.choices[0].message.content
    context_chunks = [citation['content'] for citation in completion.choices[0].message.context.get('citations', [])]

    response_message = completion.choices[0].message.content
    #print("worker exicuted")
    return response_message, context_chunks
