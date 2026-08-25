import requests
import pandas as pd
import sqlite3
from prefect import task, flow

@task(name="Extrair e Transformar Issues", retries=2)
def extract_and_transform_issues():
    url = "https://api.github.com/repos/fastapi/fastapi/issues"
    params = {"state": "open", "per_page": 10} 
    
    response = requests.get(url, params=params)
    if response.status_code != 200:
        return None
        
    raw_data = response.json()
    df = pd.DataFrame(raw_data)
    df['author'] = df['user'].apply(lambda x: x['login'] if isinstance(x, dict) else None)
    df_clean = df[['id', 'number', 'title', 'state', 'author', 'created_at']].copy()
    
    if df_clean['id'].isnull().any():
        df_clean = df_clean.dropna(subset=['id'])
        
    df_clean = df_clean.drop_duplicates(subset=['id'])
    return df_clean

@task(name="Carga Incremental no SQLite")
def load_incremental(df):
    conn = sqlite3.connect('agile_data.db')
    cursor = conn.cursor()
    
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS github_issues (
            id BIGINT PRIMARY KEY,
            number INTEGER,
            title TEXT,
            state TEXT,
            author TEXT,
            created_at TEXT
        )
    ''')
    
    upsert_query = '''
        INSERT INTO github_issues (id, number, title, state, author, created_at)
        VALUES (?, ?, ?, ?, ?, ?)
        ON CONFLICT(id) DO UPDATE SET
            state = excluded.state,
            title = excluded.title
    '''
    
    for index, row in df.iterrows():
        cursor.execute(upsert_query, (
            row['id'], row['number'], row['title'], 
            row['state'], row['author'], row['created_at']
        ))
        
    conn.commit()
    conn.close()

@flow(name="Agile Analytics Ingestion Pipeline")
def agile_analytics_pipeline():
    df_final = extract_and_transform_issues()
    
    if df_final is not None and not df_final.empty:
        load_incremental(df_final)

if __name__ == "__main__":
    agile_analytics_pipeline()