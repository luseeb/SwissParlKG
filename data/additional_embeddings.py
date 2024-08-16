import os
from utils.neo4j_python_connection import Neo4jConnection
import pandas as pd
from openai import OpenAI 
from tqdm import tqdm

#Initiate Neo4j-database connection
conn = Neo4jConnection(uri=os.getenv('NEO4J_url'), 
                       user=os.getenv('NEO4J_user'),              
                       pwd=os.getenv('NEO4J_pwd'))

#Define database for reading text nodes
read_db = "swissparlgraph2"

#Define database for writing new vector embeddings
write_db = "vector"

#Function for reading all text nodes
def read_write_text_nodes(db_read, db_write):
    query = '''
            MATCH (t:Text)
            RETURN t.Text_ID, t.ID, t.info, t.Name, t.Parent_Label, t.Parent_Name, t.Quelle, t.Titel
            '''       
    result = conn.query(query, db=db_read)
    df = pd.DataFrame(result, columns=["Text_ID", "KG_ID", "info", "Name", "Parent_Label", "Parent_Name", "Quelle", "Titel"])

    query = '''
            UNWIND $rows AS row
            MERGE (t:Text {Text_ID: row.Text_ID})
            SET t.KG_ID = row.KG_ID,
            t.info = row.info,
            t.Name = row.Name,
            t.Parent_Label = row.Parent_Label,
            t.Parent_Name = row.Parent_Name,
            t.Quelle = row.Quelle,
            t.Titel = row.Titel
            RETURN count(t) as total
            '''
    return conn.query(query, parameters = {'rows':df.to_dict('records')}, db=db_write)

#Function for returning OpenAI embeddings
def get_embedding(text_to_embbed):
    
    client = OpenAI()
    text = text_to_embbed
    return client.embeddings.create(input = [text],
                                	model='text-embedding-ada-002').data[0].embedding

#Function for loading and processing text nodes
def load_process_texts(db):
    query = '''
            MATCH (t:Text) 
            WHERE t.info IS NOT NULL
            RETURN t.Text_ID, t.info
            '''
    result = conn.query_values(query, db=db)
    df = pd.DataFrame(result, 
                      columns=["Text_ID", "info"])

    tqdm.pandas()
    df['info'] = df['info'].str.split(r'\.\s(?=[A-Z])', regex=True)
    df = df.explode("info")
    df = df[df['info'] != ""]
    processed_texts = df.reset_index(drop=True)
    processed_texts["index"] = processed_texts.index

    processed_texts["embedding"] = processed_texts["info"].astype(str).progress_apply(get_embedding)

    return processed_texts.to_csv("embeddings.csv", index=False)

#Function for storing vector embeddings in Neo4j-Database
def store_nodes_in_neo4j(db):
    query = '''
            LOAD CSV WITH HEADERS FROM 'file:///embeddings.csv' as row
            MATCH (t:Text {Text_ID: row.Text_ID})
            MERGE (t)-[l:HAT_CHUNK]->(c:Chunk {Chunk_ID: row.index})
            SET c.info = row.info,
            c.Parent_Name = t.Parent_Name,
            c.Parent_ID = toInteger(row.ID),
            c.vector = apoc.convert.fromJsonList(row.embedding)
            RETURN count(l) as total
            '''
    
    return conn.query(query, db=db)    


#Reading all text nodes, store in new database and add vector embeddings
read_write_text_nodes(read_db, write_db)

load_process_texts(write_db)

store_nodes_in_neo4j(write_db)
