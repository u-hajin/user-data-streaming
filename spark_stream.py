import logging

from cassandra.auth import PlainTextAuthProvider
from cassandra.cluster import Cluster


def create_cassandra_connection():
    try:
        auth_provider = PlainTextAuthProvider(username='cassandra', password='cassandra')
        cluster = Cluster(['localhost'], auth_provider=auth_provider)
        cas_session = cluster.connect()
        return cas_session
    except Exception as e:
        logging.error(f'Could not create Cassandra. {e}')
        return None


def create_keyspace(session):
    session.execute("""
        CREATE KEYSPACE IF NOT EXISTS spark_streams
        WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};
    """)

    print('Keyspace created successfully!')


def create_table(session):
    session.execute("""
    CREATE TABLE IF NOT EXISTS spark_streams.users (
        id TEXT PRIMARY KEY,
        gender TEXT,
        first_name TEXT,
        last_name TEXT,
        address TEXT,
        postcode TEXT,
        email TEXT,
        username TEXT,
        dob TEXT,
        registered_date TEXT,
        phone TEXT,
        picture TEXT
    );
    """)

    print('Table created successfully!')


if __name__ == '__main__':
    cas_session = create_cassandra_connection()

    if cas_session is not None:
        create_keyspace(cas_session)
        create_table(cas_session)
