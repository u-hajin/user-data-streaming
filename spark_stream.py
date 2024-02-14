def create_cassandra_connection():
    try:
        cluster = Cluster(['localhost'])
        cas_session = cluster.connect()
        return cas_session
    except Exception as e:
        return None


def create_keyspace(session):
    session.execute("""
        CREATE KEYSPACE IF NOT EXISTS spark_streams
        WITH replication = {'class': 'SimpleStrategy'. 'replication_factor': '1'};
    """)

    print('Keyspace created successfully!')
