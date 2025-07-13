import pandas as pd
from airflow.providers.postgres.hooks.postgres import PostgresHook
from sqlalchemy import (
    Table, MetaData, Column, Integer, String, DateTime, Float,
    UniqueConstraint, inspect
)

def create_table():
    hook = PostgresHook('destination_db')
    engine = hook.get_sqlalchemy_engine()
    metadata = MetaData()
    users_churn = Table(
        'users_churn',
        metadata,
        Column('id', Integer, primary_key=True, autoincrement=True),
        Column('customer_id', String),
        Column('begin_date', DateTime),
        Column('end_date', DateTime),
        Column('type', String),
        Column('paperless_billing', String),
        Column('payment_method', String),
        Column('monthly_charges', Float),
        Column('total_charges', Float),
        Column('internet_service', String),
        Column('online_security', String),
        Column('online_backup', String),
        Column('device_protection', String),
        Column('tech_support', String),
        Column('streaming_tv', String),
        Column('streaming_movies', String),
        Column('gender', String),
        Column('senior_citizen', Integer),
        Column('partner', String),
        Column('dependents', String),
        Column('multiple_lines', String),
        Column('target', Integer),
        UniqueConstraint('customer_id', name='uq_customer_id')
    )
    if not inspect(engine).has_table('users_churn'):
        metadata.create_all(engine)

def extract():
    hook = PostgresHook('source_db')
    conn = hook.get_conn()
    sql = """
        SELECT
            c.customer_id, c.begin_date, c.end_date, c.type, c.paperless_billing, c.payment_method,
            c.monthly_charges, c.total_charges,
            i.internet_service, i.online_security, i.online_backup, i.device_protection,
            i.tech_support, i.streaming_tv, i.streaming_movies,
            p.gender, p.senior_citizen, p.partner, p.dependents,
            ph.multiple_lines
        FROM contracts AS c
        LEFT JOIN internet AS i ON i.customer_id = c.customer_id
        LEFT JOIN personal AS p ON p.customer_id = c.customer_id
        LEFT JOIN phone AS ph ON ph.customer_id = c.customer_id
    """
    df = pd.read_sql(sql, conn)
    conn.close()
    return df

def transform(df: pd.DataFrame):
    df['target'] = (df['end_date'] != 'No').astype(int)
    df['end_date'] = df['end_date'].replace('No', pd.NaT)
    return df

def load(df: pd.DataFrame):
    hook = PostgresHook('destination_db')
    engine = hook.get_sqlalchemy_engine()
    df.to_sql(
        name='users_churn',
        con=engine,
        if_exists='replace',
        index=False
    )