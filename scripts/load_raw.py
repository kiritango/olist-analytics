import pandas as pd
from utils import get_client

file_config = {
    'data/olist_orders_dataset.csv': {
        'table': 'raw.orders',
        'datetime_cols': [
            'order_purchase_timestamp',
            'order_approved_at',
            'order_delivered_carrier_date',
            'order_delivered_customer_date',
            'order_estimated_delivery_date'
        ]
    },
    'data/olist_order_payments_dataset.csv': {
        'table': 'raw.payments',
        'datetime_cols': []
    },
    'data/olist_order_reviews_dataset.csv': {
        'table': 'raw.order_reviews',
        'datetime_cols': [
            'review_creation_date',
            'review_answer_timestamp'
        ]
    },
    'data/olist_customers_dataset.csv': {
        'table': 'raw.customers',
        'datetime_cols': [],
        'str_cols': ['customer_zip_code_prefix'] 
    },
    'data/olist_geolocation_dataset.csv': {
        'table': 'raw.geolocations',
        'datetime_cols': [],
        'str_cols': ['geolocation_zip_code_prefix']
    },
    'data/olist_products_dataset.csv': {
        'table': 'raw.products',
        'datetime_cols': [],
        'str_cols': ['product_name_lenght', 'product_description_lenght',
                     'product_photos_qty', 'product_weight_g',
                     'product_length_cm', 'product_height_cm',
                     'product_width_cm'],
        'nullable_int_cols': ['product_name_lenght', 'product_description_lenght',
                             'product_photos_qty', 'product_weight_g',
                             'product_length_cm', 'product_height_cm', 'product_width_cm']
    },
    'data/olist_sellers_dataset.csv': {
        'table': 'raw.sellers',
        'datetime_cols': [],
        'str_cols': ['seller_zip_code_prefix']
    },
    'data/olist_order_items_dataset.csv': {
        'table': 'raw.order_items',
        'datetime_cols': ['shipping_limit_date']
    },
}

def load_file(client, file_path, config):
    table = config['table']
    df = pd.read_csv(file_path)
    for col in config.get('datetime_cols', []):
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors='coerce')
            df[col] = df[col].astype(object).where(df[col].notna(), None)
    for col in config.get('str_cols', []):
        if col in df.columns:
            df[col] = df[col].astype(str).replace('nan', None)
    df = df.where(pd.notna(df), None)
    for col in config.get('nullable_int_cols', []):
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce').astype('Int64')

    records = df.to_dict('records')
    client.execute(f'TRUNCATE TABLE {table}')
    client.execute(f'INSERT INTO {table} VALUES', records)
    print(f'✅ {table}: загружено {len(records)} строк')

def load_all():
    client = get_client()
    for file_path, config in file_config.items():
        try:
            load_file(client, file_path, config)
        except Exception as e:
            print(f'❌ {config["table"]}: {e}')

if __name__ == '__main__':
    load_all()