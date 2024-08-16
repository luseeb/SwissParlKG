import os
from utils.neo4j_python_connection import Neo4jConnection
import wikipedia_class as wk

#Initiate Neo4j-database connection
conn = Neo4jConnection(uri=os.getenv('NEO4J_url'), 
                       user=os.getenv('NEO4J_user'),              
                       pwd=os.getenv('NEO4J_pwd'))

#Define Neo4j-database for loading query terms
db_load = "swissparlgraph"

#Define Neo4j-databsae for storing wikipedia data
db_write = "wiki-v1"

#Process wikipedia data as definded in wikipedia_class.py
party = wk.get_party_names(db=db_load)

[wk.load_embed_store_wiki(item, database=db_write) for item in party]

pers = wk.get_person_names(db=db_load)

[wk.load_embed_store_wiki(item, database=db_write) for item in pers]

dep = wk.get_department_names(db=db_load)

[wk.load_embed_store_wiki(item, database=db_write) for item in dep]

rat = wk.get_rat_names(db=db_load)

[wk.load_embed_store_wiki(item, database=db_write) for item in rat]

can = wk.get_canton_names(db=db_load)

[wk.load_embed_store_wiki(item, database=db_write) for item in can]
