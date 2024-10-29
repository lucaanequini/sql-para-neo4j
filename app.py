# Importações padrão
import os
from typing import Any
from decimal import Decimal

# Bibliotecas de terceiros
import psycopg2
from dotenv import load_dotenv
from pymongo import MongoClient

# Carregar variáveis de ambiente
load_dotenv()

#import das queries
from queries import buscar_historico_aluno, disciplinas_professor

# Configuração das conexões com os bancos
postgres = psycopg2.connect(os.getenv('SQL_URL'))
mongo = MongoClient(os.getenv('MONGODB_URL'))
banco_mongo = mongo.banco_projeto

def listar_tabelas() -> list[str]:
    """Retorna uma lista com os nomes das tabelas do PostgreSQL."""
    print("Listando tabelas disponíveis no PostgreSQL...")
    with postgres.cursor() as db_context:
        db_context.execute("SHOW TABLES;")
        resultado = db_context.fetchall()
        postgres.commit()
        return [tabela[1] for tabela in resultado]

def buscar_todos_registros(nome_tabela: str):
    """Busca todos os registros da tabela informada."""
    print(f"Buscando todos os registros de '{nome_tabela}'...")
    with postgres.cursor() as db_context:
        db_context.execute(f'SELECT * FROM "{nome_tabela}";')
        registros = db_context.fetchall()
        postgres.commit()
        return registros

def obter_colunas(nome_tabela: str):
    """Obtém os nomes das colunas para uma tabela especificada."""
    print(f"Obtendo colunas da tabela '{nome_tabela}'...")
    with postgres.cursor() as db_context:
        db_context.execute(f"SELECT * FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME='{nome_tabela}';")
        resposta = db_context.fetchall()
        postgres.commit()
        return [table[3] for table in resposta]

def migrar_dados():
    """Transfere dados de tabelas do PostgreSQL para o MongoDB."""
    tabelas = listar_tabelas()

    for tabela in tabelas:
        colunas = obter_colunas(tabela)
        registros = buscar_todos_registros(tabela)
        documentos = []

        for registro in registros:
            dados_formatados = []
            for valor in registro:
                if isinstance(valor, Decimal):
                    dados_formatados.append(float(valor))
                else:
                    dados_formatados.append(valor)
            documentos.append(dict(zip(colunas, dados_formatados)))

        try:
            print(f"Criando coleção '{tabela}' no MongoDB...")
            banco_mongo.create_collection(tabela)
        except:
            print(f"A coleção '{tabela}' já existe no MongoDB.")
        
        print(f"Inserindo registros na coleção '{tabela}'...")
        banco_mongo[tabela].insert_many(documentos)

    print("Migração de dados concluída com sucesso!")

def limpar_colecoes():
    """Remove todas as coleções existentes no MongoDB."""
    colecoes = banco_mongo.list_collection_names()
    if colecoes:
        print("Removendo todas as coleções do MongoDB...")
        for colecao in colecoes:
            banco_mongo[colecao].drop()
            print(f"Coleção '{colecao}' removida.")
    else:
        print("Nenhuma coleção encontrada para remover.")

if __name__ == '__main__':
    # Limpeza de coleções antigas no MongoDB
    limpar_colecoes()
    # Migração dos dados
    migrar_dados()

    print("Saídas estarão disponíveis na pasta './resultados'.")

    if not os.path.exists('./resultados'):
        os.makedirs('./resultados')

    buscar_historico_aluno()
    disciplinas_professor()
