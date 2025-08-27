# Projeto ETL de Vendas

Exemplo de pipeline **Extract-Transform-Load** implementado em Python para
inserir registros de vendas em um banco PostgreSQL.

## Pré-requisitos
- Python 3.10+
- Banco PostgreSQL acessível
- Dependências listadas em `requirements.txt`

Instale os pacotes necessários:

```bash
pip install -r requirements.txt
```

## Estrutura do projeto
- `data/`: CSV de exemplo com as vendas
- `sql/`: script SQL para criar a tabela `vendas`
- `src/etl_vendas.py`: funções `extract`, `transform` e `load`

### Etapas do ETL
- **Extract**: leitura de dados de um arquivo CSV
- **Transform**: tratamento de dados nulos e criação do campo `valor_total`
- **Load**: inserção dos dados em lote na tabela PostgreSQL usando
  `psycopg2.extras.execute_values`

## Como executar
1. Crie um banco PostgreSQL local e rode `sql/create_tables.sql`
2. Ajuste as credenciais de acesso no dicionário `DB_CONFIG` dentro de
   `src/etl_vendas.py`
3. Execute o ETL a partir da raiz do projeto. O script procura o arquivo CSV
   padrão em `data/vendas_raw.csv`, mas você pode informar um caminho
   diferente como argumento:

```bash
# usando o CSV padrão
python src/etl_vendas.py

# ou apontando para outro CSV
python src/etl_vendas.py caminho/para/seu_arquivo.csv
```

## Dados de exemplo
O arquivo `data/vendas_raw.csv` contém um conjunto pequeno de vendas usado para
testar o pipeline.
