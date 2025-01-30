from sqlalchemy import create_engine, MetaData
from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider
import os
import time

# MySQL connection configuration from environment variables
mysql_user = os.getenv('MYSQL_USER', 'app_user')
mysql_password = os.getenv('MYSQL_PASSWORD', 'Test_Pass@123')
mysql_host = os.getenv('MYSQL_HOST', 'localhost')
mysql_port = os.getenv('MYSQL_PORT', '3306')
mysql_database = os.getenv('MYSQL_DATABASE', 'my_db')

mysql_url = f"mysql+pymysql://{mysql_user}:{mysql_password}@{mysql_host}:{mysql_port}/{mysql_database}"

# Cassandra connection configuration from environment variables
cassandra_host = [os.getenv('CASSANDRA_HOST', '127.0.0.1')]
cassandra_keyspace = os.getenv('CASSANDRA_KEYSPACE', 'test_keyspace')
cassandra_user = os.getenv('CASSANDRA_USER', 'cassandra')
cassandra_password = os.getenv('CASSANDRA_PASSWORD', 'cassandra')

auth_provider = PlainTextAuthProvider(
    username=cassandra_user,
    password=cassandra_password
)

def get_mysql_schema():
    max_retries = 5
    retry_delay = 10  # seconds
    
    for attempt in range(max_retries):
        try:
            engine = create_engine(mysql_url)
            metadata = MetaData()
            metadata.reflect(bind=engine)
            return metadata.tables['ledgers']
        except Exception as e:
            if attempt < max_retries - 1:
                print(f"Failed to connect to MySQL (attempt {attempt + 1}/{max_retries}): {e}")
                print(f"Retrying in {retry_delay} seconds...")
                time.sleep(retry_delay)
            else:
                raise Exception(f"Failed to connect to MySQL after {max_retries} attempts: {e}")

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

def create_cassandra_keyspace(session):
    """Create the keyspace if it doesn't exist"""
    create_keyspace_query = f"""
    CREATE KEYSPACE IF NOT EXISTS {cassandra_keyspace}
    WITH replication = {{'class': 'SimpleStrategy', 'replication_factor': 1}};
    """
    session.execute(create_keyspace_query)

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
    
    max_retries = 5
    retry_delay = 10  # seconds
    
    for attempt in range(max_retries):
        try:
            cluster = Cluster(cassandra_host, auth_provider=auth_provider)
            session = cluster.connect()
            
            # Create keyspace if it doesn't exist
            create_cassandra_keyspace(session)
            
            # Use the keyspace
            session.set_keyspace(cassandra_keyspace)
            
            print("Creating Cassandra table...")
            print(create_table_stmt)
            session.execute(create_table_stmt)
            print("Table created successfully!")
            break
            
        except Exception as e:
            if attempt < max_retries - 1:
                print(f"Failed to create Cassandra table (attempt {attempt + 1}/{max_retries}): {e}")
                print(f"Retrying in {retry_delay} seconds...")
                time.sleep(retry_delay)
            else:
                print(f"Error creating table after {max_retries} attempts: {e}")
        
        finally:
            if 'cluster' in locals():
                cluster.shutdown()

if __name__ == "__main__":
    create_cassandra_table()