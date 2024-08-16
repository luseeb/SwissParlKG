import streamlit as st
from langchain_community.vectorstores.neo4j_vector import Neo4jVector
from rag.llm import llm, embeddings
from langchain.chains import RetrievalQA

#Initiate Neo4j-Vectorindex from existing database index
neo4jvector = Neo4jVector.from_existing_index(
    embeddings,                              
    url=st.secrets["NEO4J_URI"],             
    username=st.secrets["NEO4J_USERNAME"],   
    password=st.secrets["NEO4J_PASSWORD"],   
    index_name="geschäfte",                
    node_label="Text",                     
    text_node_property="info",              
    embedding_node_property="vector",
    database="swissparlgraph2",
    retrieval_query="""
RETURN
    node.info AS text,
    score,
    {
        title: node.title,
        source: node.source
    } AS metadata
"""
)

#Initiate Neo4j-Vector retriever
retriever = neo4jvector.as_retriever()

#Initiate QA-Chain
kg_qa = RetrievalQA.from_chain_type(
    llm,                 
    chain_type="stuff",  
    retriever=retriever 
)
