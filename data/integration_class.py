import os
from utils.neo4j_python_connection import Neo4jConnection
import pandas as pd

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

#Integrate data from lobbywatch with data from parlamentsdienste
def integrate_person_canton_link(db_read, db_write):
    query = '''
            MATCH (p:Parlamentarier)-[l:WOHNT_IM_KANTON]->(k:Kanton)
            RETURN p.parlament_biografie_id, k.id
            '''       
    result = conn.query(query, db=db_read)
    df = pd.DataFrame(result, columns=["Personennummer", "Kantonsnummer"])

    query = '''
            UNWIND $rows AS row
            MATCH (p:Person {Personennummer: toFloat(row.Personennummer)})
            WITH row, p
            MATCH (k:Kanton {Kantonsnummer: toFloat(row.Kantonsnummer)})
            MERGE (p)-[l:WOHNT_IM_KANTON]->(k)
            RETURN count(l) as total
            '''
    return conn.query(query, parameters = {'rows':df.to_dict('records')}, db=db_write)


def integrate_person_person_link(db_read, db_write):
    query = '''
            MATCH (p:Parlamentarier)-[l:HAT_ZUTRITTSBERECHTIGTER]->(z:Person)
            RETURN p.parlament_biografie_id, z.id, l.funktion, z.anzeige_name, 
                z.nachname, z.vorname, z.beruf, z.geschlecht, z.beschreibung_de
            '''       
    result = conn.query(query, db=db_read)
    df = pd.DataFrame(result, 
                      columns=["Personennummer", "id", "Funktion", "Name", "Nachname",
                               "Vorname", "Beruf", "Geschlecht", "Beschreibung"])

    query = '''
            UNWIND $rows AS row
            MATCH (p:Person {Personennummer: toFloat(row.Personennummer)})
            WITH row, p
            MERGE (z:Person {ID: toFloat(row.id)})
            SET z.Name = row.Vorname + " " + row.Nachname,
            z.Nachname = row.Nachname,
            z.Vorname = row.Vorname,
            z.Beruf = row.Beruf,
            z.Geschlecht = row.Geschlecht,
            z.Beschreibung = row.Beschreibung
            MERGE (p)-[l:HAT_ZUTRITTSBERECHTIGTER]->(z)
            SET l.Funktion = row.Funktion
            RETURN count(l) as total
            '''
    return conn.query(query, parameters = {'rows':df.to_dict('records')}, db=db_write)


def integrate_organisation(db_read, db_write):
    query = '''
                MATCH (o:Organisation)
                RETURN o.anzeige_name_de, o.id, o.beschreibung, o.uid, o.adresse_plz, o.ort, o.rechtsform

            '''       
    result = conn.query(query, db=db_read)
    df = pd.DataFrame(result, 
                      columns=["Name", "ID", "Beschreibung", "uid", "PLZ", "Ort", "Rechtsform"])

    query = '''
            UNWIND $rows AS row
            MERGE (o:Organisation {ID: toFloat(row.ID)})            
            SET o.Name = row.Name,
            o.Beschreibung = row.Beschreibung,
            o.uid = row.uid,
            o.Postleitzahl = row.PLZ,
            o.Ort = row.Ort,
            o.Rechtsform = row.Rechtsform
            RETURN count(o) as total
            '''
    return conn.query(query, parameters = {'rows':df.to_dict('records')}, db=db_write)


def integrate_organisation_organisation_link(db_read, db_write):
    query = '''
                MATCH (o:Organisation)-[l]->(z:Organisation)
                RETURN o.id, l.id, l.art, z.id
            '''       
    result = conn.query(query, db=db_read)
    df = pd.DataFrame(result, 
                      columns=["id_a", "id_link", "art", "id_b"])

    queries = [
        '''
            UNWIND $rows AS row
            WITH row
            WHERE row.art = "arbeitet fuer"
            MATCH (o:Organisation {ID: toFloat(row.id_a)})
            WITH row, o
            MATCH (z:Organisation {ID: toFloat(row.id_b)})
            MERGE (o)-[l:ARBEITET_FUER]->(z)
            RETURN count(l)
        ''',
        '''
            UNWIND $rows AS row
            WITH row
            WHERE row.art = "beteiligt an"
            MATCH (o:Organisation {ID: toFloat(row.id_a)})
            WITH row, o
            MATCH (z:Organisation {ID: toFloat(row.id_b)})
            MERGE (o)-[l:BETEILIGT_AN]->(z)
            RETURN count(l)
        ''',
        '''
            UNWIND $rows AS row
            WITH row
            WHERE row.art = "mitglied von"
            MATCH (o:Organisation {ID: toFloat(row.id_a)})
            WITH row, o
            MATCH (z:Organisation {ID: toFloat(row.id_b)})
            MERGE (o)-[l:MITGLIED_VON]->(z)
            RETURN count(l)
        ''',
        '''
            UNWIND $rows AS row
            WITH row
            WHERE row.art = "partner von"
            MATCH (o:Organisation {ID: toFloat(row.id_a)})
            WITH row, o
            MATCH (z:Organisation {ID: toFloat(row.id_b)})
            MERGE (o)-[l:PARTNER_VON]->(z)
            RETURN count(l)
        ''',
        '''
            UNWIND $rows AS row
            WITH row
            WHERE row.art = "tochtergesellschaft von"
            MATCH (o:Organisation {ID: toFloat(row.id_a)})
            WITH row, o
            MATCH (z:Organisation {ID: toFloat(row.id_b)})
            MERGE (o)-[l:TOCHTERGESELLSCHAFT_VON]->(z)
            RETURN count(l)
        '''
    ]
    for query in queries:
        conn.query(query, parameters = {'rows':df.to_dict('records')}, db=db_write)
    return print("Organisation links import finished")

def integrate_organisation_text_link(db_read, db_write):
    query = '''
                MATCH (o:Organisation)-[l]-(t:Text)
                RETURN o.id, l.id, t.ID, t.info, t.Name, t.vector
            '''       
    result = conn.query(query, db=db_read)
    df = pd.DataFrame(result, 
                      columns=["id_a", "id_link", "id_b", "info", "Name", "vector"])

    query = '''
            UNWIND $rows AS row
            MERGE (o:Organisation {ID: toFloat(row.id_a)})            
            WITH row, o
            MERGE (t:Text {ID: toFloat(row.id_b)})
            SET t.Name = row.Name,
            t.info = row.info,
            t.vector = row.vector
            MERGE (o)-[l:HAT_TEXT]->(t)
            RETURN count(l) as total
            '''
    return conn.query(query, parameters = {'rows':df.to_dict('records')}, db=db_write)


def integrate_parlamentarier_organisation_link(db_read, db_write):
    query = '''
                MATCH (p:Parlamentarier)-[l]-(o:Organisation)
                RETURN p.parlament_biografie_id, o.id, l.id, type(l), l.status 
            '''       
    result = conn.query(query, db=db_read)
    df = pd.DataFrame(result, 
                      columns=["id_a", "id_b", "id_link", "type", "status"])

    queries = [
        '''
            UNWIND $rows AS row
            WITH row
            WHERE row.type = "HAT_INTERESSENBINDUNG_MIT"
            MATCH (p:Person {Personennummer: toFloat(row.id_a)})
            WITH row, p
            MATCH (o:Organisation {ID: toFloat(row.id_b)})
            MERGE (p)-[l:HAT_INTERESSENBINDUNG_MIT]->(o)
            RETURN count(l)
        ''',
        '''
            UNWIND $rows AS row
            WITH row
            WHERE row.type = "VERGUETED"
            MATCH (p:Person {Personennummer: toFloat(row.id_a)})
            WITH row, p
            MATCH (o:Organisation {ID: toFloat(row.id_b)})
            MERGE (p)<-[l:VERGUETED]-(o)
            RETURN count(l)
        '''
    ]
    for query in queries:
        conn.query(query, parameters = {'rows':df.to_dict('records')}, db=db_write)
    return print("Organisation-Parlamentarier links import finished")


def integrate_person_organisation_link(db_read, db_write):
    

    query = '''
                MATCH (p:Person)-[l]-(o:Organisation)
                RETURN p.id, o.id, l.id, type(l), l.art, l.beschreibung
            '''       
    result = conn.query(query, db=db_read)
    df = pd.DataFrame(result, 
                      columns=["id_a", "id_b", "id_link", "type", "art", "beschreibung"])

    queries = [
        '''
            UNWIND $rows AS row
            WITH row
            WHERE row.type = "HAT_MANDAT"
            MATCH (p:Person {ID: toFloat(row.id_a)})
            WITH row, p
            MATCH (o:Organisation {ID: toFloat(row.id_b)})
            MERGE (p)-[l:HAT_MANDAT]->(o)
            SET l.Art = row.art,
            l.Beschreibung = row.beschreibung
            RETURN count(l)
        ''',
        '''
            UNWIND $rows AS row
            WITH row
            WHERE row.type = "VERGUETED"
            MATCH (p:Person {ID: toFloat(row.id_a)})
            WITH row, p
            MATCH (o:Organisation {ID: toFloat(row.id_b)})
            MERGE (p)<-[l:VERGUETED]-(o)
            RETURN count(l)
        '''
    ]
    for query in queries:
        conn.query(query, parameters = {'rows':df.to_dict('records')}, db=db_write)
    return print("Organisation-Person links import finished")


def integrate_organisation_interessenraum_link(db_read, db_write):
    query = '''
                MATCH (o:Organisation)-[l]-(i:Interessenraum)
                RETURN o.id, l.id, i.id, i.anzeige_name, i.beschreibung 

            '''       
    result = conn.query(query, db=db_read)
    df = pd.DataFrame(result, 
                      columns=["id_a", "id_link", "id_b", "Name", "Beschreibung"])

    query = '''
            UNWIND $rows AS row
            MERGE (o:Organisation {ID: toFloat(row.id_a)})            
            WITH row, o
            MERGE (i:Interessenraum {ID: toFloat(row.id_b)})
            SET i.Name = row.Name,
            i.Beschreibung = row.Beschreibung
            MERGE (o)-[l:HAT_INTERESSENRAUM]->(i)
            RETURN count(l) as total
            '''
    return conn.query(query, parameters = {'rows':df.to_dict('records')}, db=db_write)


def integrate_organisation_interessengruppe_link(db_read, db_write):
    query = '''
                MATCH (o:Organisation)-[l]-(g:Interessengruppe)
                RETURN o.id, l.id, g.id, g.anzeige_name, g.beschreibung, g.alias_namen
            '''       
    result = conn.query(query, db=db_read)
    df = pd.DataFrame(result, 
                      columns=["id_a", "id_link", "id_b", "name", "beschreibung", "beispiele"])

    query = '''
            UNWIND $rows AS row
            MERGE (o:Organisation {ID: toFloat(row.id_a)})            
            WITH row, o
            MERGE (g:Interessengruppe {ID: toFloat(row.id_b)})
            SET g.Name = row.name,
            g.Beschreibung = row.beschreibung,
            g.Beispiele = row.beispiele
            MERGE (o)-[l:GEHOERT_ZU]->(g)
            RETURN count(l) as total
            '''
    return conn.query(query, parameters = {'rows':df.to_dict('records')}, db=db_write)


def integrate_interessengruppe_branche_link(db_read, db_write):
    query = '''
                MATCH (g:Interessengruppe)-[l]-(b:Branche)
                RETURN g.id, l.id, b.id, b.anzeige_name, b.beschreibung
            '''       
    result = conn.query(query, db=db_read)
    df = pd.DataFrame(result, 
                      columns=["id_a", "id_link", "id_b", "name", "beschreibung"])

    query = '''
            UNWIND $rows AS row
            MERGE (g:Interessengruppe {ID: toFloat(row.id_a)})            
            WITH row, g
            MERGE (b:Branche {ID: toFloat(row.id_b)})
            SET b.Name = row.name,
            b.Beschreibung = row.beschreibung
            MERGE (g)-[l:IST_IN_BRANCHE]->(b)
            RETURN count(l) as total
            '''
    return conn.query(query, parameters = {'rows':df.to_dict('records')}, db=db_write)


def integrate_branche_kommission_link(db_read, db_write):
    query = '''
                MATCH (k:Kommission)-[l]-(b:Branche)
                RETURN k.parlament_id, l.id, b.id
            '''       
    result = conn.query(query, db=db_read)
    df = pd.DataFrame(result, 
                      columns=["id_a", "id_link", "id_b"])

    query = '''
            UNWIND $rows AS row
            MATCH (k:Kommission {Kommissionsnummer: toFloat(row.id_a)})            
            WITH row, k
            MERGE (b:Branche {ID: toFloat(row.id_b)})
            MERGE (b)-[l:HAT_ZUSTAENDIGE_KOMMISSION]->(k)
            RETURN count(l) as total
            '''
    return conn.query(query, parameters = {'rows':df.to_dict('records')}, db=db_write)

#Integrate data from wikipedia with data from parlamentsdienste
def integrate_wikipedia_link(db_read, db_write):
    query = '''
                MATCH (w)
                RETURN w.id, w.parent_id, w.label, w.title, w.source, w.info, w.vector
            '''       
    result = conn.query(query, db=db_read)
    df = pd.DataFrame(result, 
                      columns=["id", "parent_id", "label", "title", "source", "info", "vector"])

    queries = [
        '''
            UNWIND $rows AS row
            WITH row
            WHERE row.label = "Parlamentarier"
            MATCH (p:Parlamentarier {Personennummer: row.parent_id})
            WITH row, p
            MERGE (t:Text {ID: row.id})
            MERGE (p)-[l:HAT_TEXT]->(t)
            SET t.Parent_Label = row.label,
            t.Titel = row.title, 
            t.Quelle = row.source,
            t.info = row.info,
            t.vector = row.vector
            RETURN count(l)
        ''',
        '''
        UNWIND $rows AS row
        WITH row
        WHERE row.label = "Departement"
        MATCH (d:Departement {Departementsnummer: row.parent_id})
        WITH row, d
        MERGE (t:Text {ID: row.id})
        MERGE (d)-[l:HAT_TEXT]->(t)
        SET t.Parent_Label = row.label,
        t.Titel = row.title, 
        t.Quelle = row.source,
        t.info = row.info,
        t.vector = row.vector
        RETURN count(l)
        ''',
        '''
        UNWIND $rows AS row
        WITH row
        WHERE row.label = "Rat"
        MATCH (d:Rat {Ratnummer: row.parent_id})
        WITH row, d
        MERGE (t:Text {ID: row.id})
        MERGE (d)-[l:HAT_TEXT]->(t)
        SET t.Parent_Label = row.label,
        t.Titel = row.title,
        t.Quelle = row.source,
        t.info = row.info,
        t.vector = row.vector
        RETURN count(l)
        ''',
        '''
        UNWIND $rows AS row
        WITH row
        WHERE row.label = "Partei"
        MATCH (d:Partei {Parteinummer: row.parent_id})
        WITH row, d
        MERGE (t:Text {ID: row.id})
        MERGE (d)-[l:HAT_TEXT]->(t)
        SET t.Parent_Label = row.label,
        t.Titel = row.title,
        t.Quelle = row.source,
        t.info = row.info,
        t.vector = row.vector
        RETURN count(l)
        ''',
        '''
        UNWIND $rows AS row
        WITH row
        WHERE row.label = "Kanton"
        MATCH (d:Kanton {Kantonsnummer: row.parent_id})
        WITH row, d
        MERGE (t:Text {ID: row.id})
        MERGE (d)-[l:HAT_TEXT]->(t)
        SET t.Parent_Label = row.label,
        t.Titel = row.title, 
        t.Quelle = row.source,
        t.info = row.info,
        t.vector = row.vector
        RETURN count(l)
        '''
    ]
    for query in queries:
        conn.query(query, parameters = {'rows':df.to_dict('records')}, db=db_write)
    return print("Wikipedia integration import finished")


