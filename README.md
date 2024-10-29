# Banco de Dados SQL para DOCS

## Integrantes:

- Luca Anequini Antoniazzi -> R.A:
- Gustavo das Flores Lima -> R.A:

### Instalação

Crie um novo virtual env com seu gerenciador de ambientes virtuais de sua preferência

Clone o repositório:

```
git clone git@github.com:lucaanequini/sql-para-docs.git
```

Ative o virtualenv em sua pasta da aplicação.

- Instale as dependências:

  ```
  pip install psycopg2 python-dotenv pymongo
  ```

Configure suas variav~eos de ambiente em um arquivo `.env` na raiz do projeto:

```
POSTGRES_URL=""
MONGO_URL=""
```

### Execução

Para executar a aplicação:

```
python app.py
```

Ao decorrer da execução, a aplicação irá criar as collections e inserir os dados no MongoDB. Você pode verificar os resultados na pasta `./saidas` que será´criada automaticamente na raiz de seu projeto ao encerrar a execução.

1. Historíco acadêmico de um aluno, com todas as disciplinas cursadas, notas.
2. Histórico de disciplinas ministradas por um professor específico.
3. Lista de alunos graduados em um determinado curso.
4. Lista de todos os professores que são chefes de departamento.
5. Lista de alunos que pertecem a um grupo de TCC específico, juntamente com seu orientador.
