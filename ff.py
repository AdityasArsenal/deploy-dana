import streamlit as st
import requests

def ping_server():
    try:
        # Replace with your actual Render service URL
        response = requests.get("https://dana-test-v.onrender.com/")
        response2 = requests.get("https://dana-test-v-qif0.onrender.com/")
        print("--------------------------------")
        print(f"Ping status of first server: {response.status_code}")
        print(f"Ping status of second server: {response2.status_code}")
        print("--------------------------------")
    except Exception as e:
        print(f"Ping error: {e}")

# Backend URL
WEBHOOK_URL = "https://deploy-dana-production.up.railway.app/chat"

# Initialize session state if not already present
if "messages" not in st.session_state:
    st.session_state.messages = []
if "conversation_id" not in st.session_state:
    st.session_state.conversation_id = None
if "pdf_urls" not in st.session_state:
    st.session_state.pdf_urls = {}

def display_chat():
    for i, message in enumerate(st.session_state.messages):
        with st.chat_message(message["role"]):
            st.markdown(message["content"])

            # Show additional agent conversation PDF link if available
            if message["role"] == "user" and i in st.session_state.pdf_urls:
                pdf_url = st.session_state.pdf_urls[i]
                st.markdown(f"[researched info⬇️]({pdf_url})", unsafe_allow_html=True)

# Display chat history
display_chat()
ping_server()

# Get user input
user_input = st.chat_input("Enter your message here")
if user_input:
    index = len(st.session_state.messages)  # Track index for PDF linking
    st.session_state.messages.append({"role": "user", "content": user_input})
    
    # Build payload including conversation_id if available
    payload = {"user_prompt": user_input}
    if st.session_state.conversation_id:
        payload["conversation_id"] = st.session_state.conversation_id
    
    with st.spinner("Looking for relevant stuff..."):
        response = requests.post(WEBHOOK_URL, json=payload)
    
    if response.status_code == 200:
        json_response = response.json()
        ai_message = json_response.get("response", "Sorry, no response was generated.")
        st.session_state.conversation_id = json_response.get("conversation_id", st.session_state.conversation_id)
        st.session_state.messages.append({"role": "assistant", "content": ai_message})
        
        # Check if agents_conv_pdf_url is available
        agents_conv_pdf_url = json_response.get("agents_conv_pdf_url")
        if agents_conv_pdf_url:
            st.session_state.pdf_urls[index] = agents_conv_pdf_url
    else:
        st.error(f"Error: {response.status_code} - {response.text}")
    
    st.rerun()
