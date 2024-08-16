from langchain.agents import AgentExecutor, create_react_agent
from langchain import hub
from langchain.tools import Tool
from langchain.chains.conversation.memory import ConversationBufferWindowMemory
from rag.tools.vector import kg_qa
from rag.tools.vector_local import kg_qa_local
from rag.tools.cypher_finetuned import cypher_qa
import pandas as pd
from rag.llm import llm
from langchain.prompts import PromptTemplate


#Funtion for generating tool results
def run_retriever(query):
    results = kg_qa.invoke({"query":query})
    return results

def run_cypher(query):
    results = cypher_qa.invoke(query)
    return results

def run_retriever_local(query):
    results = kg_qa_local.invoke({"query":query})
    return results

#Define tools for langchain agent
tools = [
    Tool.from_function(
        name="General Chat",
        description="Für generellen Chat, welcher nicht von anderen Tools abgedeckt ist",
        func=llm.invoke,
        return_direct=True
    ),
    Tool.from_function(
        name="Global Vector Search", 
        description="Stellt grundlegende und allgemeine Informationen zur Schweizer Politik insbesondere Politiker und Institutionen mit Vektorsuche zur Verfügung",
        func = run_retriever,
        return_direct=False,
        return_intermediate_steps=True
    ),
    Tool.from_function(
        name="Graph Cypher QA Chain",  
        description="Stellt Informationen zur Schweizer Politik insbesondere Politiker und Institutionen mit Hilfe von Graphsuche zur Verfügung", 
        func = run_cypher, 
        return_direct=False,
        return_intermediate_steps=True
    ),
    Tool.from_function(
        name="Local Vector Search",  
        description="Stellt lokale und detaillierte Informationen zur Schweizer Politik insbesondere Politiker und Institutionen mit Hilfe von Vektorsuche zur Verfügung", 
        func = run_retriever_local, 
        return_direct=False,
        return_intermediate_steps=True
    ),
]

#Initiate conversation memory for langchain agent
memory = ConversationBufferWindowMemory(
    memory_key='chat_history',
    k=3,
    return_messages=False,
    output_key='output'
)

#Revised RAG-prompt for langchain agent
agent_prompt = PromptTemplate.from_template("""
Du bist ein Experte in Schweizer Politik.
Sei als hilfreich wie möglich und gib so viel Informationen wie möglich zurück.
Beantworte keine Fragen, welche nicht mit Schweizer Politik, Politiker oder dem Schweizer Parlament zu tun haben.
                                            
Beantworte KEINE Fragen mit deinem vortrainierten Wissen, sondern nutze nur die Informationen aus dem Kontext.

TOOLS:
------

You have access to the following tools:

{tools}

To use a tool, please use the following format:

```
Thought: Do I need to use a tool? Yes
Action: the action to take, should be one of [{tool_names}]
Action Input: the input to the action
Observation: the result of the action
```

When you have a response to say to the Human, or if you do not need to use a tool, you MUST use the format:

```
Thought: Do I need to use a tool? No
Final Answer: [your response here]
```

Use ALL tools before you tell the human that you do not know the answer.
Only use the output of the tools for your response and ignore your pretrained knowledge.

Begin!

Previous conversation history:
{chat_history}

New input: {input}
{agent_scratchpad}
""")

#Initiate langchain agent
agent = create_react_agent(llm, tools, agent_prompt)
agent_executor = AgentExecutor(
    agent=agent,
    tools=tools,
    memory=memory,
    handle_parsing_errors=True,
    verbose=True,
    return_intermediate_steps=True
    )

#Define function for generating a response from langchain agent
def generate_response(prompt):
    """
    Create a handler that calls the Conversational agent
    and returns a response to be rendered in the UI
    """

    response = agent_executor.invoke({"input": prompt})

    return response['output']


#Function for passing multiple queries to langchain agent
def generate_shuffled_response_list(prompts):
    
    questions = prompts["Question"].to_list()
    lst = []

    for question in questions:
        try:
            response = agent_executor.invoke({"input": question})
            lst.append(response)
        except Exception as e:
            response = {'input': question,
                        'chat_history': None,
                        'output': e, 
                        'intermediate_steps': None}
            lst.append(response)

    answers = pd.json_normalize(lst)

    df = pd.concat([prompts.reset_index(drop=True), answers], axis=1)

    return df

