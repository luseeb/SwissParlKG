import streamlit as st
from langchain_community.graphs import Neo4jGraph

#Initiate Neo4j-Graph instance
graph = Neo4jGraph(
    url=st.secrets["NEO4J_URI"],
    username=st.secrets["NEO4J_USERNAME"],
    password=st.secrets["NEO4J_PASSWORD"],
    database="swissparlgraph"
)
