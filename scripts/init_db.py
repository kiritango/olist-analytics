from utils import get_client

def init_databases():
    client = get_client()
    for db in ['raw', 'staging', 'marts']:
        client.execute(f'CREATE DATABASE IF NOT EXISTS {db}')
        print(f'✅ {db}')

if __name__ == '__main__':
    init_databases()