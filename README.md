# SwissParlKG

Dieses Repository dient als Code-Dokumentation für die MAS-Thesis "SwissParlGraph: Ein Knowledge Graph über die aktuelle Legislatur der Schweizer Politik". Der Code dient der Datenextraktion aus drei verschiedenen Datenquellen, sowie deren Integration in eine Neo4j-Datenbank. Ausserdem wurde basierend auf dieser Neo4j-Datenbank eine RAG-Applikation entwickelt und mit einem Streamlit-Frontend zugänglich gemacht. Schliesslich wurde die RAG-Applikation mit einem Fragekatalog evaluiert. In den folgenden Abschnitten werden die einzelnen Code-Dateien zur besseren Übersicht kurz erläutert.

## Datenextraktion
Im Unterordner SwissParlKG/data/ sind die Python-Dateien für die Datenextraktion gespeichert:

* *lobbywatch_class.py, parlament_class.py, wikipedia_class.py*: Diese Dateien beinhalten die Funktionen für die Extraktion der Daten aus der respektiven Quelle sowie für die Speicherung der Daten in einer Neo4j-Datenbank.
* *lobbywatch_dataload.py, parlament_dataload.py, wikipedia_dataload.py*: Diese Dateien dienen zur Ausführung der Datenextraktion und basieren auf den in den _class-Dateien definierten Funktionen.
* *additional_embeddings.py*: Mit dieser Datei werden die in der Neo4j-Datenbank gespeicherten Texte in einzelne Sätze aufgesplittet, Vektor-Einbettungen berechnet und schliesslich in einer separaten Neo4j-Datenbank abgespeichert.
* *utils/neo4j_python_connection.py*: Diese Hilfe-Klasse dient der Initiierung einer Verbindung zu einer Neo4j-Instanz.
* *utils/utils.py*: Diese Datei beinhaltet Hilfe-Funktionen.

## Datenintegration
Die Dateien für die Datenintegration sind ebenfalls unter SwissParlKG/data/ gespeichert:
* *integration_class.py*: In dieser Datei werden Funktionen für die Integration der Informationen aus den verschiedenen Datenquellen definiert.
* *integration_dataload.py*: Diese Datei dient zur Zusammenführung der verschiedenen Datenquellen in einer einzigen Neo4j-Datenbank.

## Retrieval Augmented Generation
Der Unterordner SwissParlKG/rag/ beinhaltet die Dateien für die RAG-Implementierung:
* *llm.py, graph.py*: Diese Dateien dienen der Initiierung des LLM und des Knowledge Graphen.
* *agent_iteration1.py, agent_iteration2.py*: Diese Dateien beinhalten den Code und die Anweisungen für die Erstellung des LangChain-Agenten für die jewweilige Iteration.
* *tools/cypher.py, tools/cypher_finetuned.py*: In diesen Dateien werden die Werkzeuge für die Cypher-Generierung erstellt.
* *tools/vector.py, tools/vector_local.py*: Diese beiden Dateien dienen der Etablierung der Werkzeuge für die Vektorsuche.

## Evaluation
* Im Hauptordner ist die Datei *chatbot.py* abgespeichert, welche für den Aufbau des Streamlit Chatbot-Frontend verantwortlich ist. Mit dieser Web-basierten Applikation kann die RAG-Implementierung live mit einzelnen Queries getestet werden.
* Die Datei *rag/running_evaluation.py* dient schliesslich der automatisierten Abfrage von Informationen aus dem Knowledge Graphen.
* Der Unterordner *evaluation/questions/* enthält den Fragekatalog sowie die Konversationen, welche für die Evaluierung der RAG-Implementierung verwendet wurden.
* Der Unterordner *evaluation/answers/* enthält die Resultate der Evaluierung.
