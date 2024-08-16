from langchain.chains import GraphCypherQAChain
from langchain.prompts import PromptTemplate

from llm import llm
from graph import graph

CYPHER_GENERATION_TEMPLATE = """
Du bist ein Neo4j Entwicklungsexperte, welcher Benutzerfragen in Cypher übsersetzt um Fragen über Schweizer Politik zu beantworten.
Konvertiere die Benutzerfragen basierend auf dem zur Verfügung gestellten Schema.

Instructions:
Benutze nur die Beziehungen und Eigenschaften, welche im Schema vorkommen.
Benutze KEINE Beziehungen oder Eigenschaften, die nicht im Schema zur Verfügung gestellt werden.
Wenn mehr als 50 Antworten zurückgegeben werden, gib eine Auswahl der Antworten und erwähne die Anzahl der Antworten.
Gib keinen Text ausser das generierte Cypher Statement zurück.

Schema: {schema}
Question: {question}
"""

cypher_generation_prompt = PromptTemplate(
    template=CYPHER_GENERATION_TEMPLATE,
    input_variables=["schema", "question"],
)

cypher_qa = GraphCypherQAChain.from_llm(
    llm,        
    graph=graph,
    cypher_prompt=cypher_generation_prompt,
    verbose=True,
    return_intermediate_steps=True,
    use_function_response=True,
    top_k=999
)
