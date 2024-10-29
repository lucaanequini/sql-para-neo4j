# Importações padrão
import os
import json

# Bibliotecas de terceiros
from dotenv import load_dotenv
from pymongo import MongoClient

# Carregar variáveis de ambiente
load_dotenv()

conexao_mongo = MongoClient(os.environ['MONGODB_URL'])
banco_mongo = conexao_mongo.projeto_db

# Função para buscar o histórico acadêmico do aluno com RA fornecido
def buscar_historico_aluno():
    print("Recuperando histórico do aluno com RA: 241220555")

    consulta_pipeline = [
        {"$match": {"student_id": "241220555"}}, 
        {"$lookup": {
            "from": "subj",         
            "localField": "subj_id",
            "foreignField": "id", 
            "as": "detalhes_disciplina"  
        }},
        {"$unwind": "$detalhes_disciplina"}, 
        {"$project": {
            "student_id": 1,
            "subj_id": 1,
            "detalhes_disciplina.title": 1, 
            "semester": 1, 
            "year": 1,
            "grade": 1,
            "sala_disciplina": 1
        }}
    ]

    resultado = list(banco_mongo["takes"].aggregate(consulta_pipeline))

    for i, linha in enumerate(resultado):
        resultado[i]["_id"] = str(linha["_id"])
        resultado[i]["codigo_disciplina"] = linha["detalhes_disciplina"]["title"]
        resultado[i].pop("detalhes_disciplina")

    with open('./resultados/historico-escolar.json', 'w') as f:
        json.dump(resultado, f, ensure_ascii=False)

# Retorna o histórico de disciplinas ministradas pelo professor com o ID selecionado
def disciplinas_professor():
    print("Buscando as disciplinas ministradas pelo professor de ID P010")

    pipeline = [
        {"$match": {"professor_id": "P010"}}, 
    ]

    resultado = list(banco_mongo["teaches"].aggregate(pipeline))

    for i, linha in enumerate(resultado):
        resultado[i]["_id"] = str(linha["_id"])

    with open('./resultados/disc-professores.json', 'w') as f:
        json.dump(resultado, f, ensure_ascii=False)