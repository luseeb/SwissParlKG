import os
from utils.neo4j_python_connection import Neo4jConnection
from langchain_community.document_loaders import WikipediaLoader
from langchain.text_splitter import CharacterTextSplitter
from langchain_community.vectorstores import Neo4jVector
from langchain_openai import OpenAIEmbeddings

#Initiate Neo4j-database connection
conn = Neo4jConnection(uri=os.getenv('NEO4J_url'), 
                       user=os.getenv('NEO4J_user'),              
                       pwd=os.getenv('NEO4J_pwd'))

#Get names of members of parliament
def get_person_names(db):
    query = '''
            MATCH (p:Parlamentarier)
            RETURN p.Personennummer, p.Name
            '''       
    result = conn.query_values(query, db=db)
    [item.append("Parlamentarier") for item in result]

    return result

#Get names of departements
def get_department_names(db):
    query = '''
            MATCH (d:Departement)
            RETURN d.Departementsnummer, d.Name
            '''       
    result = conn.query_values(query, db=db)
    result = [[item[0], item[1].replace('Parlament', 'Parlament (Schweiz)')] for item in result]
    [item.append("Departement") for item in result]

    return result

#Get names of federal councils
def get_rat_names(db):
    query = '''
            MATCH (r:Rat)
            RETURN r.Ratnummer, r.Name
            '''       
    result = conn.query_values(query, db=db)
    result = [[item[0], item[1].replace(item[1], item[1] + ' (Schweiz)')] for item in result]
    [item.append("Rat") for item in result]

    return result


#Get names of parties
def get_party_names(db):
    query = '''
            MATCH (p:Partei)
            RETURN p.Parteinummer, p.Name
            '''       
    result = conn.query_values(query, db=db)
    result = [[item[0], item[1].replace('Liberal-Demokratische Partei', 'Liberal-Demokratische Partei (Basel-Stadt)')] for item in result]
    [item.append("Partei") for item in result]

    return result

#Get names of cantons
def get_canton_names(db):
    query = '''
            MATCH (k:Kanton)
            RETURN k.Kantonsnummer, k.Name
            '''       
    result = conn.query_values(query, db=db)
    result = [[item[0], item[1].replace(item[1], 'Kanton '+ item[1])] for item in result]
    [item.append("Kanton") for item in result]

    return result


#Function for loading data from wikipedia
def load_wikipedia_data(query: list, lang: str = "de", load_max_docs: int = 1):
    """
    Load data from Wikipedia based on the given query.
    """
    # Read the wikipedia article
    raw_documents = WikipediaLoader(query=query[1], lang=lang, load_max_docs=load_max_docs).load()
    raw_documents[0].metadata.update(parent_id = query[0], label = query[2])

    return raw_documents

#Function for processing wikipedia data
def process_wikipedia_data(raw_documents):
    """
    Process (chunk and clean) the loaded Wikipedia data.
    """
    # Define chunking strategy
    text_splitter = CharacterTextSplitter.from_tiktoken_encoder(
        chunk_size=512, chunk_overlap=20
    )
    # Chunk the document
    documents = text_splitter.split_documents(raw_documents)

    # Remove summary from metadata
    for d in documents:
        del d.metadata["summary"]

    return documents

#Function for storing documents in Neo4j-database
def store_data_in_neo4j(documents, database):
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
        database=database,
        url=url,
        username=username,
        password=password,
        index_name="wikipedia",  
        node_label="Text", 
        text_node_property="info", 
        embedding_node_property="vector",
        create_id_index=True 
    )

#Load, embed and store wikipedia data in Neo4j-database
def load_embed_store_wiki(query, database):
    try:
        print(
            f"\nLoad data from Wikipedia and store OpenAI embeddings in a Neo4j Vector\n\tQuery: {query}\n"
        )

        raw_docs = load_wikipedia_data(query)
        processed_docs = process_wikipedia_data(raw_docs)
        store_data_in_neo4j(processed_docs, database)

    except Exception as e:
        print(f"\n\tAn unexpected error occurred: {e}")


