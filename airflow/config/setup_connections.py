"""
Airflow Connections Configuration

This script creates the necessary connections for Airflow to interact with:
- ClickHouse (for data sensing)
- dbt CLI (for running transformations)

Usage:
    python setup_connections.py

Or set connections via Airflow UI or CLI:
    airflow connections add 'clickhouse_default' \
        --conn-uri 'http://admin:admin@clickhouse:8123/?database=air_quality'
"""

from airflow.models import Connection
from airflow import settings
import os


def create_connections():
    """Create Airflow connections for the Vietnam Air Quality Data Platform."""
    
    session = settings.Session()
    
    # Check if connections already exist
    existing_conns = session.query(Connection).all()
    existing_conn_ids = [conn.conn_id for conn in existing_conns]
    
    connections_to_create = [
        {
            'conn_id': 'clickhouse_default',
            'conn_type': 'http',
            'host': os.getenv('CLICKHOUSE_HOST', 'clickhouse'),
            'port': int(os.getenv('CLICKHOUSE_PORT', '8123')),
            'login': os.getenv('CLICKHOUSE_USER', 'admin'),
            'password': os.getenv('CLICKHOUSE_PASSWORD', 'admin'),
            'schema': os.getenv('CLICKHOUSE_DB', 'air_quality'),
            'description': 'ClickHouse connection for Airflow sensors',
        },
    ]
    
    for conn_params in connections_to_create:
        conn_id = conn_params['conn_id']
        
        if conn_id in existing_conns:
            print(f"Connection '{conn_id}' already exists. Skipping...")
            continue
        
        conn = Connection(**conn_params)
        session.add(conn)
        print(f"Created connection: {conn_id}")
    
    session.commit()
    print("All connections created successfully!")
    
    session.close()


if __name__ == '__main__':
    create_connections()

