import streamlit as st
from rag.agent_iteration2 import generate_response

#Streamlit helper function
def write_message(role, content, save = True):
    """
    This is a helper function that saves a message to the
     session state and then writes a message to the UI
    """
    # Append to session state
    if save:
        st.session_state.messages.append({"role": role, "content": content})

    # Write to UI
    with st.chat_message(role):
        st.markdown(content)

#Configure streamlit page
st.set_page_config("SwissParlGraph", page_icon="🇨🇭")

# Set up Session State
if "messages" not in st.session_state:
    st.session_state.messages = [
        {"role": "assistant", "content": "Grüezi, ich bin der SwissParlGraph Chatbot. Wie kann ich dir helfen?"},
    ]

# Submit handler
def handle_submit(message):
    """
    Submit handler:
    """

    # Handle the response
    with st.spinner('Suche nach der Information...'):

        response = generate_response(message)    
        write_message('assistant', response)


# Display messages in Session State
for message in st.session_state.messages:
    write_message(message['role'], message['content'], save=False)

# Handle any user input
if prompt := st.chat_input("Frag mich!"):
    # Display user message in chat message container
    write_message('user', prompt)

    # Generate a response
    handle_submit(prompt)
