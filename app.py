# Importações padrão
import os
from typing import Any
from decimal import Decimal

# Bibliotecas
import psycopg2
from dotenv import load_dotenv
from pymongo import MongoClient
import pandas as pd

# Carregar variáveis de ambiente
load_dotenv()

# Importação das queries
from queries import buscar_historico_aluno, disciplinas_professor, alunos_formados, chefes_departamento, grupo_de_tcc

# Configuração das conexões com os bancos
postgres = psycopg2.connect(os.getenv('SQL_URL'))
mongo = MongoClient(os.getenv('MONGODB_URL'))
banco_mongo = mongo.sqlparadocs

# Retorna um lista com o nome de todas as tabelas disponíveis no PostgreSQL
def listar_tabelas() -> list[str]:
    print("Listando tabelas disponíveis no PostgreSQL...")
    with postgres.cursor() as db_context:
        db_context.execute("SHOW TABLES;")
        resultado = db_context.fetchall()
        postgres.commit()
        return [tabela[1] for tabela in resultado]

# Busca todos os registros de uma tabela especificada
def buscar_todos_registros(nome_tabela: str):
    print(f"Buscando todos os registros de '{nome_tabela}'...")
    with postgres.cursor() as db_context:
        db_context.execute(f'SELECT * FROM "{nome_tabela}";')
        registros = db_context.fetchall()
        postgres.commit()
        return registros

# Obtem o nome de todas as colunas de uma tabela especificada
def obter_colunas(nome_tabela: str):
    print(f"Obtendo colunas da tabela '{nome_tabela}'...")
    with postgres.cursor() as db_context:
        db_context.execute(f"SELECT * FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME='{nome_tabela}';")
        resposta = db_context.fetchall()
        postgres.commit()
        return [table[3] for table in resposta]

def migrar_dados():
    tabelas = listar_tabelas()

    for tabela in tabelas:
        colunas = obter_colunas(tabela)
        registros = buscar_todos_registros(tabela)

        # Criar um DataFrame para facilitar a conversão
        df = pd.DataFrame(registros, columns=colunas)

        # Converter Decimals automaticamente para floats
        # Verifique e converta apenas as colunas necessárias
        for coluna in df.select_dtypes(include=['object']):  # 'object' cobre Decimal
            df[coluna] = df[coluna].map(lambda x: float(x) if isinstance(x, Decimal) else x)

        # Inserir no MongoDB
        banco_mongo[tabela].insert_many(df.to_dict('records'))

    print("Migração de dados concluída com sucesso!")

# Limpa todas as coleções do MongoDB
def limpar_colecoes():
    colecoes = banco_mongo.list_collection_names()
    if colecoes:
        print("Removendo todas as coleções do MongoDB...")
        for colecao in colecoes:
            banco_mongo[colecao].drop()
            print(f"Coleção '{colecao}' removida.")
    else:
        print("Nenhuma coleção encontrada para remover.")

if __name__ == '__main__':
    limpar_colecoes()
    migrar_dados()

    print("Saídas estarão disponíveis na pasta './resultados'.")

    if not os.path.exists('./resultados'):
        os.makedirs('./resultados')

    buscar_historico_aluno()
    disciplinas_professor()
    alunos_formados()
    chefes_departamento()
    grupo_de_tcc()
