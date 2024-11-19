# Standard
import os
import json

# Third Party
from dotenv import load_dotenv
from neo4j import GraphDatabase

load_dotenv()

neo4j_driver = GraphDatabase.driver(os.environ['NEO4J_URI'], auth=(os.environ['NEO4J_USERNAME'], os.environ['NEO4J_PASSWORD']))

def buscar_historico_aluno():
    print("Buscando o histórico escolar do aluno de RA 241220555")
    query = """
    MATCH (student:Student {id: '241220555'})-[r:relacao_estudante]->(subj:Subj)
    RETURN student.id AS student_id, subj.id AS subj_id, subj.title AS nome_disciplina,
           r.semester AS semestre, r.year AS ano, r.grade AS media
    """
    resultado = neo4j_driver.session().run(query)
    registros = [r.data() for r in resultado]

    with open('./resultados_neo4j/historico-escolar.json', 'w') as f:
        json.dump({ "student_id": "241220555", "historico": registros }, f, ensure_ascii=False)

def disciplinas_professor():
    print("Buscando o histórico de disciplinas ministradas pelo professor de ID P010")
    query = """
    MATCH (professor:Professor {id: 'P010'})-[e:ensina]->(subj:Subj)
    RETURN professor.id AS professor_id, subj.id AS subj_id, subj.title AS nome_disciplina, 
           e.semester AS semestre, e.year AS ano
    """

    resultado = neo4j_driver.session().run(query)
    registros = [r.data() for r in resultado]

    with open('./resultados_neo4j/disc-professor.json', 'w') as f:
        json.dump({ "professor_id": "P010", "leciona": registros }, f, ensure_ascii=False)

def alunos_formados():
    print("Buscando os alunos que se formaram no segundo semestre de 2018")
    query = """
    MATCH (student:Student)-[:graduado {semester: 2, year: 2018}]->(course:Course)
    RETURN student.id AS student_id, student.name AS nome
    """

    resultado = neo4j_driver.session().run(query)
    registros = [r.data() for r in resultado]

    with open('./resultados_neo4j/alunos-formados.json', 'w') as f:
        json.dump({ "semester": 2, "year": 2018, "formados": registros }, f, ensure_ascii=False)

def chefes_departamento():
    print("Buscando os professores que são chefes de departamento")
    query = """
    MATCH (professor:Professor)-[:chefes]->(dept:Department)
    RETURN professor.id AS professorrofessor_id, professor.name AS nome_professor, dept.dept_name AS nome_departamento, dept.budget AS orcamento
    """
    
    resultado = neo4j_driver.session().run(query)
    registros = [r.data() for r in resultado]

    with open('./resultados_neo4j/prof-chefes.json', 'w') as f:
        json.dump(registros, f, ensure_ascii=False)

def grupo_de_tcc():
    print("Buscando os alunos que formaram o grupo de TCC de ID CC1234567")
    query = """
    MATCH (student:Student {group_id: 'CC1234567'})-[:mentoria_por]->(professor:Professor)
    RETURN student.id AS student_id, student.name AS nome_aluno, professor.name AS nome_professor
    """

    resultado = neo4j_driver.session().run(query)
    registros = [r.data() for r in resultado]

    with open('./resultados_neo4j/grupo-tcc.json', 'w') as f:
        json.dump(registros, f, ensure_ascii=False)