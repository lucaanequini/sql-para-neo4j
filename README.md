# Banco de Dados SQL para Graph Database

## Integrantes:

- Luca Anequini Antoniazzi -> R.A: 22.224.031-9
- Gustavo das Flores Lima -> R.A: 22.221.041-1

### Instalação

Crie um novo ambiente virtual env de sua preferência.

Clone o repositório:

```
git clone git@github.com:lucaanequini/sql-para-docs.git
```

Ative o ambiente virtual.

- Instale as dependências:

  ```
  pip install psycopg2 python-dotenv neo4j
  ```

Configure suas variaveis de ambiente em um arquivo `.env` na raiz do projeto:

```
SQL_URL=""
NEO4J_URI=""
NEO4J_USERNAME=""
NEO4J_PASSWORD=""
```

### Execução

Para executar a aplicação:

```
python app.py
```

Ao decorrer da execução, a aplicação irá criar as collections e inserir os dados no MongoDB. Você pode verificar os resultados na pasta `./resultados_neo4j` que será´criada automaticamente na raiz de seu projeto ao encerrar a execução.

1. `historico-escolar.json`: Historíco acadêmico de um aluno, com todas as disciplinas cursadas, semestre, ano, notas e média final.
2. `disc-professor.json`: Histórico de disciplinas ministradas por um professor específico, com o id da disciplina, nome da disciplina, semestre e ano.
3. `alunos-formados.json`: Lista de alunos graduados em um determinado semestre e ano.
4. `prof-chefes.json`: Lista de todos os professores que são chefes de departamento.
5. `grupo_tcc.json`: Lista de alunos que pertecem a um grupo de TCC específico e o nome do professor orientador.
