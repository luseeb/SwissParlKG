from utils.neo4j_python_connection import Neo4jConnection
import os
import pandas as pd
import os
from utils import clean_text
from langchain.text_splitter import CharacterTextSplitter
from langchain_community.vectorstores import Neo4jVector
from langchain_openai import OpenAIEmbeddings

#Initiate Neo4j-connection
conn = Neo4jConnection(uri=os.getenv('NEO4J_url'), 
                       user=os.getenv('NEO4J_user'),              
                       pwd=os.getenv('NEO4J_pwd'))

#Define Neo4j-database
db = "lobbywatch-v1"

#Function for loading lobbywatch data from GraphML file
def load_lobbywatch(db):
    query = '''
            CALL apoc.import.graphml('lobbywatch_export.graphml/lobbywatch.graphml',
            {batchSize: 10000, readLabels: true, storeNodeIds: true, defaultRelationshipType:"VERWANDT"}) 
            '''       
    return conn.query(query, db=db)

#Function for embedding and storing text documents in Neo4j-database
def store_docs_in_neo4j(documents, db):
    """
    Store and index text with Neo4j.
    """
    # Neo4j credentials
    url=os.getenv('NEO4J_url')
    username = os.getenv('NEO4J_user')
    password = os.getenv('NEO4J_pwd')

    # OpenAI credentials
    openai_api_secret_key = os.getenv('OPENAI_API_KEY')

    # Instantiate Neo4j vector from documents
    Neo4jVector.from_documents(
        documents,
        OpenAIEmbeddings(openai_api_key=openai_api_secret_key),
        database=db,
        url=url,
        username=username,
        password=password,
        index_name="organisation",  
        node_label="Text", 
        text_node_property="info", 
        embedding_node_property="vector",
        create_id_index=True 
    )

#Function for loading texts about organisations from stored Lobbywatch data
def load_process_organisation_texts(db):
    query = '''
            MATCH (o:Organisation)
            RETURN o.id, o.beschreibung
            '''
    result = conn.query_values(query, db=db)
    df = pd.DataFrame(result, 
                      columns=["ID", "Beschreibung"])

    df["Beschreibung"] = [clean_text(item, keep_punctuation=True) for item in df["Beschreibung"]]

    text_splitter = CharacterTextSplitter.from_tiktoken_encoder(
        chunk_size=512, chunk_overlap=20)
    
    g_dict = pd.DataFrame(df['ID']).to_dict(orient='records')

    docs = text_splitter.create_documents(
        texts=df["Beschreibung"],
        metadatas=[dict(item, Name="Beschreibung") for item in g_dict]
        )
   
    return docs
    
#Load, embed and store texts from lobbywatch data
def load_embed_store_docs(db, **kwargs):
    try:
        print(
            f"\nLoad data from Lobbywatch and store OpenAI embeddings in a Neo4j Vector\n\t"
        )

        processed_docs = load_process_organisation_texts(db, **kwargs)
        store_docs_in_neo4j(processed_docs, db)

    except Exception as e:
        print(f"\n\tAn unexpected error occurred: {e}")

#Link text entities to their respective parent entity
def link_organisation_text(db):
    query = '''
                MATCH (o:Organisation)
                MERGE (t:Text {ID: o.id})
                MERGE (o)-[l:HAT_TEXT]->(t)
                RETURN count(l)
            '''       
    return conn.query(query, db=db)
