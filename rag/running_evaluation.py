import pandas as pd
from rag.agent_iteration2 import generate_response_list


#Read excel with question catalogue
prompts = pd.read_excel("evaluation/questions/questions.xlsx")

#Shuffle questions
prompts = prompts.sample(frac=1)

#Generate responses from langchain agent
df = generate_response_list(prompts)

#Reorder questions and answers to original order
df = df[["Index", "Question", "Erwartete Antwort", "output", "intermediate_steps"]].sort_values("Index")

#Save to excel
df.to_excel("evaluation/answers/output_questions_2_2.xlsx")
