import pandas as pd
import psycopg2
import sys
from pathlib import Path

# Configurações do banco
DB_CONFIG = {
    "host": "localhost",
    "dbname": "db_nome",
    "user": "seu_user",
    "password": "sua_password"
}

def extract(path):
    return pd.read_csv(path, sep=",", encoding="utf-8")

def transform(df):
    df["data_venda"] = pd.to_datetime(df["data_venda"], errors="coerce")
    df["quantidade"] = pd.to_numeric(df["quantidade"], errors="coerce").fillna(0)
    df["preco_unitario"] = pd.to_numeric(df["preco_unitario"], errors="coerce").fillna(0)
    df["valor_total"] = df["quantidade"] * df["preco_unitario"]
    df = df.dropna(subset=["data_venda"])
    return df

def load(df, table_name="vendas", page_size=10_000, conflict_do_nothing=False):
    """
    Insere o DataFrame em massa usando INSERT ... VALUES %s (execute_values).
    - page_size: tamanho dos lotes (5k–20k geralmente é ótimo)
    - conflict_do_nothing=True: adiciona ON CONFLICT DO NOTHING (se houver chave única)
    """

    # Normaliza NaN/NaT -> None (vira NULL no Postgres)
    import pandas as pd
    df = df.where(pd.notnull(df), None)

    # Nada para inserir?
    if df.empty:
        return 0

    # Ordem das colunas exatamente como estão no DF
    cols = list(df.columns)

    # Converte linhas para tuplas
    rows = [tuple(r) for r in df.itertuples(index=False, name=None)]

    # Query segura com identificadores
    base_query = sql.SQL("INSERT INTO {table} ({fields}) VALUES %s").format(
        table=sql.Identifier(table_name),
        fields=sql.SQL(", ").join(map(sql.Identifier, cols))
    )

    if conflict_do_nothing:
        base_query = base_query + sql.SQL(" ON CONFLICT DO NOTHING")

    # Usa sua configuração existente (DB_CONFIG) — mantém transação e commit automáticos
    with psycopg2.connect(**DB_CONFIG) as conn:
        with conn.cursor() as cur:
            execute_values(cur, base_query, rows, page_size=page_size)

    return len(rows)


if __name__ == "__main__":
    PROJECT_ROOT = Path(__file__).resolve().parents[1]
    DEFAULT_CSV = PROJECT_ROOT / "data" / "vendas_raw.csv"

    # permite passar o caminho do CSV via argumento; senão usa o padrão
    csv_path = Path(sys.argv[1]) if len(sys.argv) > 1 else DEFAULT_CSV

    if not csv_path.exists():
        raise FileNotFoundError(
            f"CSV não encontrado em: {csv_path}\n"
            f"Dica: rode 'python src/etl_vendas.py' a partir da raiz do projeto "
            f"ou passe o caminho: 'python src/etl_vendas.py data/vendas_raw.csv'"
        )

    df_raw = extract(csv_path)
    df_clean = transform(df_raw)
    load(df_clean)
