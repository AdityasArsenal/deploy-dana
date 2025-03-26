import streamlit as st
import requests
import time

WEBHOOK_URL = "https://dana-test-v.onrender.com/chat"
MAX_RETRIES = 5
RETRY_DELAY = 20  # seconds

def send_chat_request(payload):
    """
    Attempt to send chat request with multiple retries
    
    Args:
        payload (dict): Request payload
    
    Returns:
        tuple: (response, error)
    """
    for attempt in range(MAX_RETRIES):
        try:
            response = requests.post(WEBHOOK_URL, json=payload, timeout=30)
            
            # Check for successful response
            if response.status_code == 200:
                return response, None
            
            # Log non-200 status codes
            st.warning(f"Attempt {attempt + 1}: Non-200 status code: {response.status_code}")
        
        except requests.exceptions.RequestException as e:
            st.warning(f"Attempt {attempt + 1}: Connection error - {e}")
        
        # Exponential backoff
        time.sleep(RETRY_DELAY * (attempt + 1))
    
    return None, "Failed to connect after multiple attempts"

# Initialize session state
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

display_chat()

user_input = st.chat_input("Enter your message here")
if user_input:
    index = len(st.session_state.messages)  # Track index for PDF linking
    st.session_state.messages.append({"role": "user", "content": user_input})
    
    # Build payload including conversation_id if available
    payload = {"user_prompt": user_input}
    if st.session_state.conversation_id:
        payload["conversation_id"] = st.session_state.conversation_id
    
    with st.spinner("Connecting and processing your request..."):
        response, error = send_chat_request(payload)
        
        if response:
            json_response = response.json()
            ai_message = json_response.get("response", "Sorry, no response was generated.")
            st.session_state.conversation_id = json_response.get("conversation_id", st.session_state.conversation_id)
            st.session_state.messages.append({"role": "assistant", "content": ai_message})
            
            agents_conv_pdf_url = json_response.get("agents_conv_pdf_url")
            if agents_conv_pdf_url:
                st.session_state.pdf_urls[index] = agents_conv_pdf_url
        else:
            st.error(error)
    
    st.rerun()