from utils.neo4j_python_connection import Neo4jConnection
import os
import lobbywatch_lass as lw

#Initiate Neo4j-database connection
conn = Neo4jConnection(uri=os.getenv('NEO4J_url'), 
                       user=os.getenv('NEO4J_user'),              
                       pwd=os.getenv('NEO4J_pwd'))

#Define Neo4j-database
db = "lobbywatch-v1"

#Load and process lobbywatch data as defined in lobbywatch_class.py
lw.load_lobbywatch(db=db)

lw.load_process_organisation_texts(db)

lw.load_embed_store_docs(db)

lw.link_organisation_text(db)
