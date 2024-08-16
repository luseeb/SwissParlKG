from langchain.chains import GraphCypherQAChain
from langchain.prompts import PromptTemplate
from rag.llm import llm
from rag.graph import graph

#Prompt for cypher generation tool including few-shot examples
CYPHER_GENERATION_TEMPLATE = """
Du bist ein Neo4j Entwicklungsexperte, welcher Benutzerfragen in Cypher übsersetzt um Fragen über Schweizer Politik zu beantworten.
Konvertiere die Benutzerfragen basierend auf dem zur Verfügung gestellten Schema.

Instructions:
Benutze nur die Beziehungen und Eigenschaften, welche im Schema vorkommen.
Benutze KEINE Beziehungen oder Eigenschaften, die nicht im Schema zur Verfügung gestellt werden.
Inkludiere keine Erklärungen oder Entschuldigungen in deiner Antwort.
Wenn mehr als 50 Antworten zurückgegeben werden, gib eine Auswahl der Antworten und erwähne die Anzahl der Antworten.
Gib keinen Text ausser das generierte Cypher Statement zurück.
Nutze NIEMALS LIMIT 1, sondern gib immer mindestens 20 Resultate zurück.
Vergewissere dich, dass bei UNION Befehlen immer diesselben Attribute für die zurückgegebenen Kolonnen verwendet werden.

Beispiele:

Vergleiche das Abstimmungsverhalten von Parlamentariern:
MATCH (p1:Parlamentarier)-[v1:JA_GESTIMMT|NEIN_GESTIMMT]->(a:Abstimmung),
      (p2:Parlamentarier)-[v2:JA_GESTIMMT|NEIN_GESTIMMT]->(a:Abstimmung)
WITH a.Geschäftstitel AS Abstimmung, type(v1)=type(v2) AS gleich_gestimmt
RETURN DISTINCT gleich_gestimmt, count(*) AS Anzahl

Finde Parlamentarier die im direkten oder indirekten Zusammenhang mit Organisationen in einem spezifischen Themengebiet stehen:
MATCH (p:Parlamentarier)-[r:HAT_INTERESSENBINDUNG_MIT]->(o:Organisation)-[:HAT_TEXT]->(t:Text)
WHERE (t.info CONTAINS "Erdöl")
RETURN p.Name AS Parlamentarier, NULL AS Lobbyist, o.Name AS Organisation, t.info AS Hintergrundinfo
UNION
MATCH (p:Parlamentarier)-[:HAT_ZUTRITTSBERECHTIGTER]->(z:Person)-[l:HAT_MANDAT]->(o:Organisation)-[:HAT_TEXT]->(t:Text)
WHERE (t.info CONTAINS "Erdöl")
RETURN p.Name AS Parlamentarier, z.Name AS Lobbyist, o.Name AS Organisation, t.info AS Hintergrundinfo

Finde ähnliche Entiäten basierend auf ihren Text Attributen:
MATCH (p:Geschäft)-[:HAT_TEXT]->(t:Text)
CALL db.index.vector.queryNodes('geschäfte', 5, t.vector)
YIELD node AS text, score
MATCH (text)<-[:HAT_TEXT]-(z)
WHERE elementId(p) <> elementId(z)
RETURN z.Titel, score, text.Name, text.info

Finde die Gesetze die verabschiedet wurden inkl. Zeitpunkt:
MATCH (h:Geschäft)-[:BEHANDELT]->(g:Gesetz)<-[l:HAT_ENTSCHIEDEN]-(r:Rat)
WHERE l.Entscheidung IN ["Annahme in der Schlussabstimmung", "Beschluss gemäss Entwurf", "Beschluss abweichend vom Entwurf", "Zustimmung", "Beschluss gemäss Antrag der Einigungskonferenz"] AND h.Status = "Erledigt"
RETURN g.Name, l.Entscheidung, l.Entscheidungsdatum

Finde das Abstimmungsresultat eines Geschäfts:
MATCH (g:Geschäft)<-[:BEHANDELT]-(a:Abstimmung)
RETURN a.Resultat, a.Anzahl_Ja_Stimmen, a.Anzahl_Nein_Stimmen, a.Anzahl_Nicht_Abgestimmt, a.Bedeutung_Ja, a.Bedeutung_Nein

Finde Informationen zu der Verbindung von Parlamentariern mit verschiedenen Branchen/Interessengruppen:
MATCH (p:Parlamentarier)-[:HAT_INTERESSENBINDUNG_MIT]->(o:Organisation)-[:GEHOERT_ZU]->(i:Interessengruppe)-[:IST_IN_BRANCHE]->(b:Branche) 
RETURN b.Name AS Branche, p.Name AS Parlamentarier

Finde die Organisationen mit am meisten Einfluss im Parlament:
MATCH (p:Parlamentarier)-[r:HAT_INTERESSENBINDUNG_MIT]->(o:Organisation)<-[l:HAT_MANDAT]-(z:Person)<-[HAT_ZUTRITTSBERECHTIGTER]-(t:Parlamentarier)
RETURN  o.Name AS Organisation, count(r)+count(l) AS Einfluss
ORDER BY Einfluss DESC

Schema: {schema}
Question: {question}
"""

#Initiate prompt template for cypher generation tool
cypher_generation_prompt = PromptTemplate(
    template=CYPHER_GENERATION_TEMPLATE,
    input_variables=["schema", "question"],
)

#Initiate cygher generation tool
cypher_qa = GraphCypherQAChain.from_llm(
    llm,        
    graph=graph,
    cypher_prompt=cypher_generation_prompt,
    verbose=True,
    return_intermediate_steps=True,
    use_function_response=True,
    top_k=50
)
