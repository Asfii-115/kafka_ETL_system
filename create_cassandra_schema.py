from sqlalchemy import create_engine, MetaData
from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider


mysql_url = "mysql+pymysql://app_user:Test_Pass%40123@localhost:3306/my_db"


cassandra_host = ['127.0.0.1']
cassandra_keyspace = 'test_keyspace'
auth_provider = PlainTextAuthProvider(username='cassandra', password='cassandra')

def get_mysql_schema():
    engine = create_engine(mysql_url)
    metadata = MetaData()
    metadata.reflect(bind=engine)
    return metadata.tables['ledgers']

def mysql_to_cassandra_type(mysql_type):
    type_mapping = {
        'INTEGER': 'int',
        'BIGINT': 'bigint',
        'TINYINT': 'int',
        'VARCHAR': 'text',
        'TEXT': 'text',
        'DECIMAL': 'decimal',
        'DATE': 'bigint',
        'TIMESTAMP': 'timestamp',
        'BOOLEAN': 'boolean',
        'UUID': 'uuid'
    }
    
    mysql_type_str = str(mysql_type).upper()
    for mysql_pattern, cassandra_type in type_mapping.items():
        if mysql_pattern in mysql_type_str:
            return cassandra_type
    return 'text'  

def create_cassandra_table():
    
    mysql_table = get_mysql_schema()
    
    columns = []
    primary_key = "id"  
    
    for column in mysql_table.columns:
        cassandra_type = mysql_to_cassandra_type(column.type)
        if column.name == primary_key:
            columns.append(f"{column.name} {cassandra_type} PRIMARY KEY")
        else:
            columns.append(f"{column.name} {cassandra_type}")
    
    create_table_stmt = f"""
    CREATE TABLE IF NOT EXISTS {mysql_table.name} (
        {',\n        '.join(columns)}
    );
    """
    
    
    try:
        cluster = Cluster(cassandra_host, auth_provider=auth_provider)
        session = cluster.connect()
        session.set_keyspace(cassandra_keyspace)
        
        print("Creating Cassandra table...")
        print(create_table_stmt)
        session.execute(create_table_stmt)
        print("Table created successfully!")
        
    except Exception as e:
        print(f"Error creating table: {e}")
    
    finally:
        if 'cluster' in locals():
            cluster.shutdown()

if __name__ == "__main__":
    create_cassandra_table()