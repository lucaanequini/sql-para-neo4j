# Importações padrão
import os
from typing import Any
from decimal import Decimal

# Bibliotecas
import psycopg2
from dotenv import load_dotenv
from neo4j import GraphDatabase

# Carregar variáveis de ambiente
load_dotenv()

# Importação das queries
from queries import buscar_historico_aluno, disciplinas_professor, alunos_formados, chefes_departamento, grupo_de_tcc

# Configuração das conexões com os bancos
postgres = psycopg2.connect(os.getenv('SQL_URL'))
neo4j = GraphDatabase.driver(os.getenv('NEO4J_URI'), auth=(os.getenv('NEO4J_USERNAME'), os.getenv('NEO4J_PASSWORD')))

# Funções auxiliares (mantidas iguais)
def listar_tabelas() -> list[str]:
    print("Listando tabelas disponíveis no PostgreSQL...")
    with postgres.cursor() as db_context:
        db_context.execute("""
            SELECT table_name
            FROM information_schema.tables
            WHERE table_schema = 'public' 
            AND table_type = 'BASE TABLE';
        """)
        resultado = db_context.fetchall()
        postgres.commit()
        return [tabela[0] for tabela in resultado]


def buscar_todos_registros(nome_tabela: str):
    print(f"Buscando todos os registros de '{nome_tabela}'...")
    with postgres.cursor() as db_context:
        db_context.execute(f'SELECT * FROM "{nome_tabela}";')
        registros = db_context.fetchall()
        postgres.commit()
        return registros

def obter_colunas(nome_tabela: str):
    print(f"Obtendo colunas da tabela '{nome_tabela}'...")
    with postgres.cursor() as db_context:
        db_context.execute(f"SELECT column_name FROM information_schema.columns WHERE table_name = '{nome_tabela}';")
        resposta = db_context.fetchall()
        postgres.commit()
        return [coluna[0] for coluna in resposta]

def migrar_dados():
    tabelas = listar_tabelas()
    tabelas_terceiras = ['takes', 'tcc_group', 'teaches', 'req', 'graduate']
    with neo4j.session() as session:
        for tabela in tabelas:
            colunas = obter_colunas(tabela)
            registros = buscar_todos_registros(tabela)
            if tabela not in tabelas_terceiras:
                for registro in registros:
                    propriedades_node = {
                        k: float(v) if isinstance(v, Decimal) else v
                        for k, v in zip(colunas, registro)
                    }
                    query = f"CREATE (n:{tabela.capitalize()} {{{', '.join([f'{k}: ${k}' for k in propriedades_node])}}})"
                    session.run(query, **propriedades_node)
            else:
                for registro in registros:
                        propriedades_node = {colunas[i]: (float(value) if isinstance(value, Decimal) else value) for i, value in enumerate(registro)}
                        if tabela == 'takes':
                            data = {k: v for k, v in propriedades_node.items() if k not in ['student_id', 'subj_id']}
                            query = f"""
                            MATCH (student:Student {{id: $student_id}}), (subj:Subj {{id: $subj_id}})
                            CREATE (student)-[:relacao_estudante {{ {', '.join([f'{k}: ${k}' for k in data])} }}]->(subj)
                            """
                            session.run(query, **{**{'student_id': registro[colunas.index('student_id')], 'subj_id': registro[colunas.index('subj_id')]}, **data})
                        elif tabela == 'teaches':
                            data = {k: v for k, v in propriedades_node.items() if k not in ['subj_id', 'professor_id']}
                            query = f"""
                            MATCH (professor:Professor {{id: $professor_id}}), (subj:Subj {{id: $subj_id}})
                            CREATE (professor)-[:ensina {{ {', '.join([f'{k}: ${k}' for k in data])} }}]->(subj)
                            """
                            session.run(query, **{**{'subj_id': registro[colunas.index('subj_id')], 'professor_id': registro[colunas.index('professor_id')]}, **data})
                        elif tabela == 'req':
                            data = {k: v for k, v in propriedades_node.items() if k not in ['subj_id', 'course_id']}
                            query = f"""
                            MATCH (subj:Subj {{id: $subj_id}}), (course:Course {{id: $course_id}})
                            CREATE (subj)-[:requisito {{ {', '.join([f'{k}: ${k}' for k in data])} }}]->(course)
                            """
                            session.run(query, **{**{'subj_id': registro[colunas.index('subj_id')], 'course_id': registro[colunas.index('course_id')]}, **data})
                        elif tabela == 'graduate':
                            data = {k: v for k, v in propriedades_node.items() if k not in ['student_id', 'course_id']}
                            query = f"""
                            MATCH (student:Student {{id: $student_id}}), (course:Course {{id: $course_id}})
                            CREATE (student)-[:graduado {{ {', '.join([f'{k}: ${k}' for k in data])} }}]->(course)
                            """
                            session.run(query, **{**{'student_id': registro[colunas.index('student_id')], 'course_id': registro[colunas.index('course_id')]}, **data}) 
                        elif tabela == 'tcc_group':
                            data = {k: v for k, v in propriedades_node.items() if k not in ['group_id']}
                            query = f"""
                            MATCH (student:Student {{group_id: $group_id}}), (prof:Professor {{id: $professor_id}})
                            CREATE (student)-[:mentoria_por {{ {', '.join([f'{k}: ${k}' for k in data])} }}]->(prof)
                            """
                            session.run(query, **{**{'group_id': registro[colunas.index('id')], 'professor_id': registro[colunas.index('professor_id')]}, **data})

        colunas_dpt = obter_colunas('department')
        registros_dpt = buscar_todos_registros('department')

        for registro in registros_dpt:
            node_data = {colunas_dpt[i]: (float(value) if isinstance(value, Decimal) else value) for i, value in enumerate(registro)}
            query = f"""
            MATCH (p:Professor {{id: $boss_id}}), (d:Department {{dept_name: $dept_name}}) 
            CREATE (p)-[:chefes]->(d)"""
            session.run(query, **{**{'boss_id': registro[colunas_dpt.index('boss_id')], 'dept_name': registro[colunas_dpt.index('dept_name')]}})

    print("Migração de dados (com relacionamentos) concluída com sucesso!")

# Limpar o banco Neo4j (cuidado: apaga todos os dados!)
def limpar_neo4j():
    with neo4j.session() as session:
        session.run("MATCH (n) DETACH DELETE n")
        print("Limpeza concluída.")

if __name__ == '__main__':
    limpar_neo4j()
    migrar_dados()

    print("Consultas no Neo4j:")

    if not os.path.exists('./resultados_neo4j'):
        os.makedirs('./resultados_neo4j')

    buscar_historico_aluno()
    disciplinas_professor()
    alunos_formados ()
    chefes_departamento()
    grupo_de_tcc()