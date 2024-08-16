import os
from utils.neo4j_python_connection import Neo4jConnection
import parlament_class as pc
from datetime import datetime

#Initiate Neo4j connection
conn = Neo4jConnection(uri=os.getenv('NEO4J_url'), 
                       user=os.getenv('NEO4J_user'),              
                       pwd=os.getenv('NEO4J_pwd'))

#Define database
db = "parlament-v2"

#Set database constraints to ensure uniqueness of entities
conn.query('CREATE CONSTRAINT person IF NOT EXISTS FOR (p:Person) REQUIRE p.Personennummer IS UNIQUE',
           db=db)
conn.query('CREATE CONSTRAINT party IF NOT EXISTS FOR (g:Partei) REQUIRE g.Parteinummer IS UNIQUE',
           db=db)
conn.query('CREATE CONSTRAINT Rat IF NOT EXISTS FOR (r:Rat) REQUIRE r.Ratnummer IS UNIQUE',
           db=db)
conn.query('CREATE CONSTRAINT Kanton IF NOT EXISTS FOR (k:Kanton) REQUIRE k.Kantonsnummer IS UNIQUE',
           db=db)
conn.query('CREATE CONSTRAINT Fraktion IF NOT EXISTS FOR (f:Fraktion) REQUIRE f.Fraktionsnummer IS UNIQUE',
           db=db)
conn.query('CREATE CONSTRAINT Kommission IF NOT EXISTS FOR (k:Kommission) REQUIRE k.Kommissionsnummer IS UNIQUE',
           db=db)
conn.query('CREATE CONSTRAINT Abstimmung IF NOT EXISTS FOR (a:Abstimmung) REQUIRE a.Abstimmungsnummer IS UNIQUE',
           db=db)
conn.query('CREATE CONSTRAINT Geschäft IF NOT EXISTS FOR (g:Geschäft) REQUIRE g.Geschäftsnummer IS UNIQUE',
           db=db)
conn.query('CREATE CONSTRAINT Session IF NOT EXISTS FOR (s:Session) REQUIRE s.Sessionsnummer IS UNIQUE',
           db=db)
conn.query('CREATE CONSTRAINT Gesetz IF NOT EXISTS FOR (g:Gesetz) REQUIRE g.Gesetzesnummer IS UNIQUE',
           db=db)


#Run imports as defined in parlament_class.py 
tx = pc.membercouncil('MemberCouncil', db=db, Active = True)
print(tx)

tx = pc.person_occupation('PersonOccupation', db=db)
print(tx)

tx = pc.person_address('PersonAddress', db=db)
print(tx)

tx = pc.citizenship('Citizenship', db=db)
print(tx)

tx = pc.member_committee('MemberCommittee', db=db)
print(tx)
 
tx = pc.committee('Committee', db=db)
print(tx)

#Filter businesses that had a status change in the current legislation
tx = pc.load_embed_store_docs('Business', db=db, BusinessStatusDate__gt=datetime.fromisoformat('2023-12-03 23:00:00 Z'))
print(tx)

#Filter businesses that had a status change in the current legislation
tx = pc.business('Business', db=db, BusinessStatusDate__gt=datetime.fromisoformat('2023-12-03 23:00:00 Z'))
print(tx)

tx = pc.vote('Vote', db=db, IdLegislativePeriod=52)
print(tx)

tx = pc.voting('Voting', db=db, IdLegislativePeriod=52)
print(tx)

tx = pc.BusinessRole('BusinessRole', db=db)
print(tx)

tx = pc.related_business('RelatedBusiness', db=db)
print(tx)

tx = pc.business_responsibility('BusinessResponsibility', db=db)
print(tx)

tx = pc.session('Session', db=db)
print(tx)

tx = pc.bill('Bill', db=db)
print(tx)

tx = pc.resolution('Resolution', db=db)
print(tx)
