import swissparlpy as spp
import pandas as pd
from utils.neo4j_python_connection import Neo4jConnection
import os
from utils.utils import clean_text
from langchain.text_splitter import CharacterTextSplitter
from langchain_community.vectorstores import Neo4jVector
from langchain_openai import OpenAIEmbeddings

#Initiate DBMS connection
conn = Neo4jConnection(uri=os.getenv('NEO4J_url'), 
                       user=os.getenv('NEO4J_user'),              
                       pwd=os.getenv('NEO4J_pwd'))

#Generic function for loading tables from Swiss Parliament Webservices
def load_table(table, language = 'DE', **kwargs):
    """Loads table and returns it as a Pandas dataframe
    
    Parameters
    ----------
    table : str
        Name of the table
    language : str
        Language of the loaded table, defauls to 'DE'
    kwargs : 
        Optional arguments for filtering the returned data
        
    Returns
    -------
    Pandas Dataframe
        Returned table
    """
    table = spp.get_data(table, Language = language, **kwargs)
    table_df = pd.DataFrame(table)
    return table_df

#Loading data about the member of parliament and storing it to a Neo4j database
def membercouncil(table, db, **kwargs):
    """Loads table and stores selected entities, relationships and properties to
    the specified Neo4j database
    
    Parameters
    ----------
    table : str
        Name of the table
    db : str
        Name of the database
    kwargs : 
        Optional arguments for filtering the returned data
        
    Returns
    -------
    str
        Completion message
    """
    df = load_table(table, **kwargs)
    queries = ['''
            UNWIND $rows AS row
            MERGE (p:Person {Personennummer: row.ID})
            SET p.Nachname = row.LastName,
            p.Vorname = row.FirstName,
            p.Name = row.FirstName + " " + row.LastName,
            p.Geburtsdatum = Date(row.DateOfBirth),
            p.Geschlecht = row.GenderAsString,
            p.Zivilstand = row.MartialStatusText,   
            p.Aktiv = row.Active  
            RETURN count(*) as total
            ''',
            '''
            UNWIND $rows AS row
            MERGE (g:Partei {Parteinummer: row.Party})
            SET g.Name = row.PartyName,
            g.Abkürzung = row.PartyAbbreviation
            WITH row, g
            MATCH (p:Person {Personennummer: row.ID})
            MERGE (p)-[:MITGLIED_VON]->(g)
            RETURN count(*) as total
            ''',
            '''
            UNWIND $rows AS row
            WITH row
            WHERE row.Council <> 98
            MERGE (r:Rat {Ratnummer: row.Council})
            SET r.Name = row.CouncilName,
            r.Abkürzung = row.CouncilAbbreviation
            WITH row, r
            MATCH (p:Person {Personennummer: row.ID})
            MERGE (p)-[l:MITGLIED_VON]->(r)
            SET l.Eintrittsdatum = Date(row.DateJoining)
            RETURN count(*) as total
            ''',
            '''
            MATCH (p:Person)-[]->(r:Rat)
            WHERE r.Ratnummer < 3
            SET p :Parlamentarier
            RETURN count(p)
            ''',
            '''
            UNWIND $rows AS row
            MERGE (k:Kanton {Kantonsnummer: row.Canton})
            SET k.Name = row.CantonName,
            k.Abkürzung = row.CantonAbbreviation
            WITH row, k
            MATCH (p:Person {Personennummer: row.ID})
            MERGE (p)-[:REPRÄSENTIERT]->(k)
            RETURN count(*) as total
            ''',
            '''
            UNWIND $rows AS row
            WITH row
            WHERE row.ParlGroupNumber <> 0
            MERGE (f:Fraktion {Fraktionsnummer: row.ParlGroupNumber})
            SET f.Name = row.ParlGroupName,
            f.Abkürzung = row.ParlGroupAbbreviation
            WITH row, f
            MATCH (p:Person {Personennummer: row.ID})
            MERGE (p)-[l:MITGLIED_VON]->(f)
            SET l.Funktion = row.ParlGroupFunctionText
            RETURN count(*) as total
            ''',
            '''
            UNWIND $rows AS row
            MATCH (p:Partei {Parteinummer: row.Party})
            WITH row, p
            MATCH (f:Fraktion {Fraktionsnummer: row.ParlGroupNumber})
            MERGE (p)-[l:TEIL_VON]->(f)
            RETURN count(l) as count
            '''
    ]
    for query in queries:
        conn.query(query, parameters = {'rows':df.to_dict('records')}, db=db)
    return print("MemberCouncil import finished")

#Loading data about member of parliaments occupation and storing it to a Neo4j database
def person_occupation(table, db, **kwargs):
    """Loads table and stores selected entities, relationships and properties to
    the specified Neo4j database
    
    Parameters
    ----------
    table : str
        Name of the table
    db : str
        Name of the database
    kwargs : 
        Optional arguments for filtering the returned data
        
    Returns
    -------
    str
        Number of entities/relationships processed
    """
    df = load_table(table, **kwargs)
    query = '''
            UNWIND $rows AS row
            MATCH (p:Person {Personennummer: row.PersonNumber})
            SET p.Berufsbezeichnung = row.OccupationName,
            p.Arbeitgeber = row.Employer,
            p.Berufstitel = row.JobTitle
            RETURN count(p.Berufsbezeichnung) as count 
            '''       
    return conn.query(query, parameters = {'rows':df.to_dict('records')}, db=db)

#Loading data about the member of parliaments addresses and storing it to a Neo4j database
def person_address(table, db, **kwargs):
    """Loads table and stores selected entities, relationships and properties to
    the specified Neo4j database
    
    Parameters
    ----------
    table : str
        Name of the table
    db : str
        Name of the database
    kwargs : 
        Optional arguments for filtering the returned data
        
    Returns
    -------
    str
        Number of entities/relationships processed
    """
    df = load_table(table, **kwargs)
    query = '''
            UNWIND $rows AS row
            MATCH (p:Person {Personennummer: row.PersonNumber})
            SET p.Adresse = row.AddressLine1,
            p.Gemeinde = row.City,
            p.Postleitzahl_Adresse = row.Postcode,
            p.Adressentyp = row.AddressTypeName
            RETURN count(p.Adresse) as count
            '''       
    return conn.query(query, parameters = {'rows':df.to_dict('records')}, db=db)

#Loading data about the member of parliaments citizenship and storing it to a Neo4j database
def citizenship(table, db, **kwargs):
    """Loads table and stores selected entities, relationships and properties to
    the specified Neo4j database
    
    Parameters
    ----------
    table : str
        Name of the table
    db : str
        Name of the database
    kwargs : 
        Optional arguments for filtering the returned data
        
    Returns
    -------
    str
        Number of entities/relationships processed
    """
    df = load_table(table, **kwargs)
    query = '''
            UNWIND $rows AS row
            MATCH (p:Person {Personennummer: row.PersonNumber})
            SET p.Heimatort = row.City,
            p.Postleitzahl_Heimatort = row.PostCode
            RETURN count(p.Heimatort) as count
            '''       
    return conn.query(query, parameters = {'rows':df.to_dict('records')}, db=db)

#Loading data about the members of parliaments committee membership and storing it to a Neo4j database
def member_committee(table, db, **kwargs):
    """Loads table and stores selected entities, relationships and properties to
    the specified Neo4j database
    
    Parameters
    ----------
    table : str
        Name of the table
    db : str
        Name of the database
    kwargs : 
        Optional arguments for filtering the returned data
        
    Returns
    -------
    str
        Number of entities/relationships processed
    """
    df = load_table(table, **kwargs)
    query = '''
            UNWIND $rows AS row
            MERGE (c:Kommission {Kommissionsnummer: row.CommitteeNumber})
            SET c.Name = row.CommitteeName,
            c.Typ = row.CommitteeTypeName,
            c.Abkürzung = row.Abbreviation
            WITH row, c
            MATCH (p:Person {Personennummer: row.PersonNumber})
            MERGE (p)-[l:MITGLIED_VON]->(c)
            SET l.Funktion = row.CommitteeFunctionName
            RETURN count(c.Name) as total
            '''       
    return conn.query(query, parameters = {'rows':df.to_dict('records')}, db=db)

#Loading data about the committees of the Swiss parliament and storing it to a Neo4j database
def committee(table, db, **kwargs):
    """Loads table and stores selected entities, relationships and properties to
    the specified Neo4j database
    
    Parameters
    ----------
    table : str
        Name of the table
    db : str
        Name of the database
    kwargs : 
        Optional arguments for filtering the returned data
        
    Returns
    -------
    str
        Completion message
    """
    df = load_table(table, **kwargs)
    queries = [
        '''
            UNWIND $rows AS row
            MATCH (c:Kommission {Kommissionsnummer: row.CommitteeNumber})
            WITH row, c
            MATCH (r:Rat {Ratnummer: row.Council})
            MERGE (c)-[:TEIL_VON]->(r)
            RETURN count(r) as total
            ''',
            '''
            UNWIND $rows AS row
            WITH row
            WHERE row.Council = 3
            MATCH (c:Kommission {Kommissionsnummer: row.CommitteeNumber})
            WITH row, c
            MATCH (r:Rat)
            WHERE r.Ratnummer <> 99
            MERGE (c)-[l:TEIL_VON]->(r)
            RETURN count(l) as total
            '''
    ]
    for query in queries:
        conn.query(query, parameters = {'rows':df.to_dict('records')}, db=db)
    return print("Committee import finished") 

#Loading data about the votes in Swiss Parliament and storing it to a Neo4j database
def vote(table, db, **kwargs):
    """Loads table and stores selected entities, relationships and properties to
    the specified Neo4j database
    
    Parameters
    ----------
    table : str
        Name of the table
    db : str
        Name of the database
    kwargs : 
        Optional arguments for filtering the returned data
        
    Returns
    -------
    str
        Completion message
    """
    df = load_table(table, **kwargs)
    queries = [
        '''
            UNWIND $rows AS row
            MERGE (a:Abstimmung {Abstimmungsnummer: row.RegistrationNumber})
            SET a.Geschäftsnummer = row.BusinessNumber,
            a.Geschäftskurznummer = row.BusinessShortNumber,
            a.Geschäftstitel = row.BusinessTitle,
            a.Geltungsbereich = row.Subject,
            a.Bedeutung_Ja = row.MeaningYes,
            a.Bedeutung_Nein = row.MeaningNo,
            a.Zeitpunkt = datetime(row.VoteEndWithTimezone)
            WITH row, a
            MERGE (s:Session {Sessionsnummer: row.IdSession})
            SET s.Name = row.SessionName
            MERGE (a)-[:DURCHGEFÜHRT_WÄHREND]->(s)
            RETURN count(a) as total
            ''',
            '''
            UNWIND $rows AS row
            MATCH (a:Abstimmung {Abstimmungsnummer: row.RegistrationNumber})
            WITH row, a
            MERGE (g:Geschäft {Geschäftsnummer: row.BusinessNumber})
            SET g.Geschäftskurznummer = row.BusinessShortNumber
            MERGE (a)-[l:BEHANDELT]->(g)
            RETURN count(l) as total
            '''
    ]
    for query in queries:
        conn.query(query, parameters = {'rows':df.to_dict('records')}, db=db)
    return print("Vote import finished")

#Loading data about the voting of members of the Swiss parliament and storing it to a Neo4j database
def voting(table, db, **kwargs):
    """Loads table and stores selected entities, relationships and properties to
    the specified Neo4j database
    
    Parameters
    ----------
    table : str
        Name of the table
    db : str
        Name of the database
    kwargs : 
        Optional arguments for filtering the returned data
        
    Returns
    -------
    str
        Completion message
    """
    df = load_table(table, **kwargs)
    queries = [
        '''
            UNWIND $rows AS row
            WITH row
            WHERE row.Decision = 1
            MATCH (p:Person {Personennummer: row.PersonNumber})
            WITH row, p
            MATCH (a:Abstimmung {Abstimmungsnummer: row.RegistrationNumber})
            MERGE (p)-[l:JA_GESTIMMT]->(a)
            SET l.Entscheidung = row.DecisionText
            RETURN count(l) as total
            ''',
            '''
            UNWIND $rows AS row
            WITH row
            WHERE row.Decision = 2
            MATCH (p:Person {Personennummer: row.PersonNumber})
            WITH row, p
            MATCH (a:Abstimmung {Abstimmungsnummer: row.RegistrationNumber})
            MERGE (p)-[l:NEIN_GESTIMMT]->(a)
            SET l.Entscheidung = row.DecisionText
            RETURN count(l) as total
            ''',
            '''
            UNWIND $rows AS row
            WITH row
            WHERE row.Decision > 2
            MATCH (p:Person {Personennummer: row.PersonNumber})
            WITH row, p
            MATCH (a:Abstimmung {Abstimmungsnummer: row.RegistrationNumber})
            MERGE (p)-[l:NICHT_ABGESTIMMT]->(a)
            SET l.Entscheidung = row.DecisionText
            RETURN count(l) as total            
            '''
    ]
    for query in queries:
        conn.query(query, parameters = {'rows':df.to_dict('records')}, db=db)
    return print("Voting import finished")
  
#Loading data about the businesses of the Swiss parliament and storing it to a Neo4j database
def business(table, db, **kwargs):
    """Loads table and stores selected entities, relationships and properties to
    the specified Neo4j database
    
    Parameters
    ----------
    table : str
        Name of the table
    db : str
        Name of the database
    kwargs : 
        Optional arguments for filtering the returned data
        
    Returns
    -------
    str
        Completion message
    """
    df = load_table(table, **kwargs)

    text_columns = ['Description', 'InitialSituation', 'Proceedings', 'SubmittedText', 
                    'ReasonText', 'DocumentationText', 'MotionText', 'FederalCouncilResponseText',
                    'FederalCouncilProposalText']
    for column in text_columns:
        df[column] = [clean_text(item, keep_punctuation=True) for item in df[column]]

    df_sub = df[['ID', 'TagNames']]
    df_sub.loc[:,'TagNames'] = df_sub.loc[:,'TagNames'].str.split('|')
    df_sub = df_sub.explode('TagNames')

    queries = [
        '''
            UNWIND $rows AS row
            MERGE (g:Geschäft {Geschäftsnummer: row.ID})
            SET g.Geschäftskurznummer = row.BusinessShortNumber,
            g.Geschäftstyp = row.BusinessTypeName,
            g.Titel = row.Title,
            g.Beschreibung = row.Description,
            g.Empfehlung_Bundesrat = row.FederalCouncilProposalText,            
            g.Status = row.BusinessStatusText,
            g.Zeitpunkt_Statusupdate = datetime(row.BusinessStatusDate),
            g.Einreichungsdatum = date(row.SubmissionDate),
            g.Legislationsnummer_Einreichung = row.SubmissionLegislativePeriod
            RETURN count(*) as total
            ''',            
            '''            
            MATCH (t:Text)             
            MATCH (g:Geschäft {Geschäftsnummer: t.ID})
            MERGE (g)-[l:HAT_TEXT]->(t)
            RETURN count(l) as total 
            ''',            
            '''
            UNWIND $rows AS row
            MERGE (s:Session {Sessionsnummer: row.SubmissionSession})
            WITH row, s
            MATCH (g:Geschäft {Geschäftsnummer: row.ID})
            MERGE (g)-[:EINGEREICHT_WÄHREND]->(s)
            RETURN count(*) as total  
            '''
    ]
    for query in queries:
        conn.query(query, parameters = {'rows':df.to_dict('records')}, db=db)

    query = '''
            UNWIND $rows AS row
            MERGE (t:Thema {Name: row.TagNames})            
            WITH row, t
            MATCH (g:Geschäft {Geschäftsnummer: row.ID})
            MERGE (g)-[l:IST_TEIL_VON]->(t)
            RETURN count(t.Name) as total
            '''       
    conn.query(query, parameters = {'rows':df_sub.to_dict('records')}, db=db)
    
    return print("Business import finished")

#Loading data about the businesses of the Swiss parliament, which are already stored in the Neo4j database 
def load_table_business_in_db(table, db, language = 'DE', **kwargs):
    """Loads table and stores selected entities, relationships and properties to
    the specified Neo4j database
    
    Parameters
    ----------
    table : str
        Name of the table
    db : str
        Name of the database
    language : str
        Language of the returned table, defaults to 'DE'
    kwargs : 
        Optional arguments for filtering the returned data
        
    Returns
    -------
    Pandas dataframe
        The returned data
    """
    query = '''
            MATCH (g:Geschäft)
            RETURN g.Geschäftsnummer
            '''       
    result = conn.query_values(query, db=db)
    lst = [item for row in result for item in row]
    
    table_df = pd.DataFrame()
    for nummer in lst:
        tbl = spp.get_data(table, Language = language, BusinessNumber=nummer, **kwargs)
        table_df = pd.concat([table_df, pd.DataFrame(tbl)], ignore_index=True)   
 
    return table_df

#Load, clean, chunk various texts describing businesses and return them as Langchain documents
def load_process_business_texts(table, **kwargs):
    """Loads and processes text data from business table and returns them as Langchain documents 
    
    Parameters
    ----------
    table : Pandas dataframe
        Table with data
    kwargs : 
        Optional arguments for filtering the returned data
        
    Returns
    -------
    list
        List of processed documents
    """
    df = load_table_business(table, **kwargs)
    df.rename(columns={'Description': 'Beschreibung', 
                       'InitialSituation': 'Ausgangssituation',
                       'Proceedings': 'Verhandlungen',
                       'SubmittedText': 'Einreichungstext',
                       'ReasonText': 'Begründungstext',
                       'DocumentationText': 'Dokumentationstext',
                       'MotionText': 'Motionstext',
                       'FederalCouncilResponseText': 'Antwort_Bundesrat'}, 
                       inplace=True)

    text_columns = ['Beschreibung', 'Ausgangssituation', 'Verhandlungen', 'Einreichungstext', 
                    'Begründungstext', 'Dokumentationstext', 'Motionstext', 'Antwort_Bundesrat',
                    ]
    for column in text_columns:
        df[column] = [clean_text(item, keep_punctuation=True) for item in df[column]]

    text_splitter = CharacterTextSplitter.from_tiktoken_encoder(
        chunk_size=512, chunk_overlap=20)

    g_dict = pd.DataFrame(df['ID']).to_dict(orient='records')

    docs = []
    for column in text_columns:
        temp = text_splitter.create_documents(
            texts=df[column],
            metadatas=[dict(item, Name=column) for item in g_dict]
        )
        docs.append(temp)
    
    return [x for xx in docs for x in xx]

#Storing Langchain documents in Neo4j database and Index them as Vector
def store_docs_in_neo4j(documents, db):
    """Stores list of documents in specified Neo4j database
    
    Parameters
    ----------
    documents : list
        List of processed documents
    db : str
        Name of the database
    """
    # Neo4j DBMS credentials
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
        index_name="geschäfte",  
        node_label="Text", 
        text_node_property="info", 
        embedding_node_property="vector",
        create_id_index=True 
    )

#Load, embed and store business texts in Neo4j database
def load_embed_store_docs(table, db, **kwargs):
    try:
        print(
            f"\nLoad data from Parlament and store OpenAI embeddings in a Neo4j Vector\n\t"
        )

        processed_docs = load_process_business_texts(table, **kwargs)
        store_docs_in_neo4j(processed_docs, db)

    except Exception as e:
        print(f"\n\tAn unexpected error occurred: {e}")

#Loading data about the relationship between businesses and other entities and storing it to a Neo4j database
def BusinessRole(table, db, **kwargs):
    """Loads table and stores selected entities, relationships and properties to
    the specified Neo4j database
    
    Parameters
    ----------
    table : str
        Name of the table
    db : str
        Name of the database
    kwargs : 
        Optional arguments for filtering the returned data
        
    Returns
    -------
    str
        Completion message
    """
    df = load_table_business_in_db(table, db=db, **kwargs)
    queries = [
        '''
            UNWIND $rows AS row
            WITH row
            WHERE row.ParlGroupNumnber <> 0
            MERGE (g:Geschäft {Geschäftsnummer: row.BusinessNumber})
            WITH row, g
            MATCH (f:Fraktion {Fraktionsnummer: row.ParlGroupNumber})
            MERGE (f)-[l:HAT_EINGEREICHT]->(g)
            SET l.Rolle = row.RoleName
            RETURN count(l) as total 
            ''',
            '''
            UNWIND $rows AS row
            WITH row
            WHERE row.CantonNumber <> 0
            MERGE (g:Geschäft {Geschäftsnummer: row.BusinessNumber})
            WITH row, g
            MATCH (k:Kanton {Kantonsnummer: row.CantonNumber})
            MERGE (k)-[l:HAT_EINGEREICHT]->(g)
            SET l.Rolle = row.RoleName
            RETURN count(l) as total 
            ''',
            '''
            UNWIND $rows AS row
            WITH row
            WHERE row.CommitteeNumber <> 0 
            MERGE (g:Geschäft {Geschäftsnummer: row.BusinessNumber})
            WITH row, g
            MATCH (k:Kommission {Kommissionsnummer: row.CommitteeNumber})
            MERGE (k)-[l:HAT_EINGEREICHT]->(g)
            SET l.Rolle = row.RoleName
            RETURN count(l) as total 
            ''',
            '''
            UNWIND $rows AS row
            WITH row
            WHERE row.Role = 7 AND row.MemberCouncilNumber <> 0
            MATCH (p:Person {Personennummer: row.MemberCouncilNumber})
            WITH row, p
            MATCH (g:Geschäft {Geschäftsnummer: row.BusinessNumber})
            MERGE (p)-[l:HAT_EINGEREICHT]->(g)
            SET l.Rolle = row.RoleName
            RETURN count(l) as total
            ''',
            '''
            UNWIND $rows AS row
            WITH row
            WHERE row.Role = 2 AND row.MemberCouncilNumber <> 0
            MATCH (p:Person {Personennummer: row.MemberCouncilNumber})
            WITH row, p
            MATCH (g:Geschäft {Geschäftsnummer: row.BusinessNumber})
            MERGE (p)-[l:IST_SPRECHER_FÜR]->(g)
            SET l.Rolle = row.RoleName
            RETURN count(l) as total
            ''',
            '''
            UNWIND $rows AS row
            WITH row
            WHERE row.Role = 3 AND row.MemberCouncilNumber <> 0
            MATCH (p:Person {Personennummer: row.MemberCouncilNumber})
            WITH row, p
            MATCH (g:Geschäft {Geschäftsnummer: row.BusinessNumber})
            MERGE (p)-[l:HAT_MITUNTERZEICHNET]->(g)
            SET l.Rolle = row.RoleName
            RETURN count(l) as total
            ''',
            '''
            UNWIND $rows AS row
            WITH row
            WHERE row.Role = 1 AND row.MemberCouncilNumber <> 0
            MATCH (p:Person {Personennummer: row.MemberCouncilNumber})
            WITH row, p
            MATCH (g:Geschäft {Geschäftsnummer: row.BusinessNumber})
            MERGE (p)-[l:HAT_BEKÄMPFT]->(g)
            SET l.Rolle = row.RoleName
            RETURN count(l) as total
            ''',
            '''
            UNWIND $rows AS row
            WITH row
            WHERE row.Role = 4 AND row.MemberCouncilNumber <> 0
            MATCH (p:Person {Personennummer: row.MemberCouncilNumber})
            WITH row, p
            MATCH (g:Geschäft {Geschäftsnummer: row.BusinessNumber})
            MERGE (p)-[l:HAT_ÜBERNOMMEN]->(g)
            SET l.Rolle = row.RoleName
            RETURN count(l) as total
            '''
    ]
    for query in queries:
        conn.query(query, parameters = {'rows':df.to_dict('records')}, db=db)
    return print("BusinessRole import finished")

#Loading data about the relationship between businesses and storing it to a Neo4j database
def related_business(table, db, **kwargs):
    """Loads table and stores selected entities, relationships and properties to
    the specified Neo4j database
    
    Parameters
    ----------
    table : str
        Name of the table
    db : str
        Name of the database
    kwargs : 
        Optional arguments for filtering the returned data
        
    Returns
    -------
    str
        Completion message
    """  
    df = load_table_business_in_db(table, db=db, **kwargs)
    queries = [
        '''
            UNWIND $rows AS row
            MATCH (g:Geschäft {Geschäftsnummer: row.BusinessNumber})
            WITH row, g
            MATCH (f:Geschäft {Geschäftsnummer: row.RelatedBusinessNumber})
            MERGE (g)-[l:VERWANDT_MIT]->(f)
            RETURN count(l) as total 
            '''     
    ]
    for query in queries:
        conn.query(query, parameters = {'rows':df.to_dict('records')}, db=db)
    return print("RelatedBusiness import finished")

#Loading data about the departements and their responsibility for businesses and storing it to a Neo4j database
def business_responsibility(table, db, **kwargs):
    """Loads table and stores selected entities, relationships and properties to
    the specified Neo4j database
    
    Parameters
    ----------
    table : str
        Name of the table
    db : str
        Name of the database
    kwargs : 
        Optional arguments for filtering the returned data
        
    Returns
    -------
    str
        Completion message
    """  
    df = load_table_business_in_db(table, db=db, **kwargs)
    queries = [
        '''
            UNWIND $rows AS row
            MATCH (g:Geschäft {Geschäftsnummer: row.BusinessNumber})
            WITH row, g
            MERGE (d:Departement {Departementsnummer: row.DepartmentNumber})
            SET d.Name = row.DepartmentName,
            d.Abkürzung = row.DepartmentAbbreviation
            MERGE (d)-[l:VERANTWORTLICH]->(g)
            SET l.Federführung = row.IsLeading
            RETURN count(l) as total 
            '''     
    ]
    for query in queries:
        conn.query(query, parameters = {'rows':df.to_dict('records')}, db=db)
    return print("BusinessResponsibility import finished")

#Loading data about the bills that are being adressed in Parliament and storing it to a Neo4j database
def bill(table, db, **kwargs):
    """Loads table and stores selected entities, relationships and properties to
    the specified Neo4j database
    
    Parameters
    ----------
    table : str
        Name of the table
    db : str
        Name of the database
    kwargs : 
        Optional arguments for filtering the returned data
        
    Returns
    -------
    str
        Completion message
    """  
    df = load_table_business_in_db(table, db=db, **kwargs)
    queries = [
        '''
            UNWIND $rows AS row
            WITH row
            WHERE row.BillType <> 0
            MATCH (g:Geschäft {Geschäftsnummer: row.BusinessNumber})
            WITH row, g
            MERGE (b:Gesetz {Gesetzesnummer: row.ID})
            SET b.Name = row.Title,
            b.Typ = row.BillTypeName
            MERGE (g)-[l:BEHANDELT]->(b)
            RETURN count(l) as total 
            '''     
    ]
    for query in queries:
        conn.query(query, parameters = {'rows':df.to_dict('records')}, db=db)
    return print("Bill import finished")

#Loading data about the bills, which are already in the Neo4j database
def load_table_bill_in_db(table, db, language = 'DE', **kwargs):
    query = '''
            MATCH (g:Gesetz)
            RETURN g.Gesetzesnummer
            '''       
    result = conn.query_values(query, db=db)
    lst = [item for row in result for item in row]
    
    table_df = pd.DataFrame()
    for nummer in lst:
        tbl = spp.get_data(table, Language = language, IdBill=nummer, **kwargs)
        table_df = pd.concat([table_df, pd.DataFrame(tbl)], ignore_index=True)   
 
    return table_df

#Loading data about the decision regardings the bills and storing it to a Neo4j database
def resolution(table, db, **kwargs):
    """Loads table and stores selected entities, relationships and properties to
    the specified Neo4j database
    
    Parameters
    ----------
    table : str
        Name of the table
    db : str
        Name of the database
    kwargs : 
        Optional arguments for filtering the returned data
        
    Returns
    -------
    str
        Completion message
    """  
    df = load_table_bill_in_db(table, db=db, **kwargs)
    queries = [
        '''
            UNWIND $rows AS row
            WITH row
            WHERE row.Council <> 0
            MERGE (g:Gesetz {Gesetzesnummer: row.IdBill})
            WITH row, g
            MATCH (r:Rat {Ratnummer: row.Council})
            MERGE (r)-[l:HAT_ENTSCHIEDEN]->(g)
            SET l.Entscheidung = row.ResolutionText,
            l.Entscheidungsdatum = row.ResolutionDate
            RETURN count(l) as total 
            ''',
            '''
            UNWIND $rows AS row
            WITH row
            WHERE row.CommitteeType <> 0
            MERGE (g:Gesetz {Gesetzesnummer: row.IdBill})
            WITH row, g
            MATCH (k:Kommission {Kommissionsnummer: row.Committee})
            MERGE (k)-[l:HAT_ENTSCHIEDEN]->(g)
            SET l.Entscheidung = row.ResolutionText,
            l.Entscheidungsdatum = row.ResolutionDate
            RETURN count(l) as total 
            '''
    ]
    for query in queries:
        conn.query(query, parameters = {'rows':df.to_dict('records')}, db=db)
    return print("Resolution import finished")

#Loading data about sessions of the Parliament, which are already in the Neo4j database
def load_table_session_in_db(table, db, language = 'DE', **kwargs):
    query = '''
            MATCH (s:Session)
            RETURN s.Sessionsnummer
            '''       
    result = conn.query_values(query, db=db)
    lst = [item for row in result for item in row]
    
    table_df = pd.DataFrame()
    for nummer in lst:
        tbl = spp.get_data(table, Language = language, ID=nummer, **kwargs)
        table_df = pd.concat([table_df, pd.DataFrame(tbl)], ignore_index=True)   
 
    return table_df

#Loading data about the sessions of the parliament and storing it to a Neo4j database
def session(table, db, **kwargs):
    """Loads table and stores selected entities, relationships and properties to
    the specified Neo4j database
    
    Parameters
    ----------
    table : str
        Name of the table
    db : str
        Name of the database
    kwargs : 
        Optional arguments for filtering the returned data
        
    Returns
    -------
    str
        Completion message
    """  
    df = load_table_session_in_db(table, db=db, **kwargs)
    queries = [
        '''
            UNWIND $rows AS row
            MERGE (s:Session {Sessionsnummer: row.ID})
            SET s.Name = row.SessionName,
            s.Startdatum = Date(row.StartDate),
            s.Enddatum = Date(row.EndDate),
            s.Typ = row.TypeName,
            s.Legislationsnummer = row.LegislativePeriodNumber
            RETURN count(s) as total 
            '''     
    ]
    for query in queries:
        conn.query(query, parameters = {'rows':df.to_dict('records')}, db=db)
    return print("Session import finished")

