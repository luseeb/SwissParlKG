import os
from utils.neo4j_python_connection import Neo4jConnection
import pandas as pd
import integration_class as di

#Initiate Neo4j-database connection
conn = Neo4jConnection(uri=os.getenv('NEO4J_url'), 
                       user=os.getenv('NEO4J_user'),              
                       pwd=os.getenv('NEO4J_pwd'))

#Define database for loading lobbywatch data
read_db = "lobbywatch-v1"

#Define database for loading wikipedia data
read_db_wiki = "wiki-v1"

#Define database for final knowledge graph
write_db = "swissparlgraph"


#Set database constraints to ensure uniqueness of entities
conn.query('CREATE CONSTRAINT Person IF NOT EXISTS FOR (p:Person) REQUIRE p.ID IS UNIQUE',
           db=write_db)

conn.query('CREATE CONSTRAINT Organisation IF NOT EXISTS FOR (o:Organisation) REQUIRE o.ID IS UNIQUE',
           db=write_db)

conn.query('CREATE CONSTRAINT Interessenraum IF NOT EXISTS FOR (i:Interessenraum) REQUIRE i.ID IS UNIQUE',
           db=write_db)

conn.query('CREATE CONSTRAINT Interessengruppe IF NOT EXISTS FOR (g:Interessengruppe) REQUIRE g.ID IS UNIQUE',
           db=write_db)

conn.query('CREATE CONSTRAINT Branche IF NOT EXISTS FOR (b:Branche) REQUIRE b.ID IS UNIQUE',
           db=write_db)


#Integrate data from lobbywatch with data from parlamentsdienste
di.integrate_person_canton_link(read_db, write_db)

di.integrate_person_person_link(read_db, write_db)

di.integrate_organisation(read_db, write_db)

di.integrate_organisation_organisation_link(read_db, write_db)

di.integrate_organisation_text_link(read_db, write_db)

di.integrate_parlamentarier_organisation_link(read_db, write_db)

di.integrate_person_organisation_link(read_db, write_db)

di.integrate_organisation_interessenraum_link(read_db, write_db)

di.integrate_organisation_interessengruppe_link(read_db, write_db)

di.integrate_interessengruppe_branche_link(read_db, write_db)

di.integrate_branche_kommission_link(read_db, write_db)

#Integrate data from wikipedia with data from parlamentsdienste
di.integrate_wikipedia_link(read_db_wiki, write_db)
