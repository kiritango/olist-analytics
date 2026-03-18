from utils import get_client

# SQL выносим в отдельные строки — легко читать и редактировать
CREATE_RAW_ORDERS = """
CREATE TABLE IF NOT EXISTS raw.orders (
    order_id          String,
    customer_id       String,
    order_status      LowCardinality(String),
    order_purchase_timestamp    DateTime,
    order_approved_at   Nullable(DateTime),
    order_delivered_carrier_date    Nullable(DateTime),
    order_delivered_customer_date   Nullable(DateTime),
    order_estimated_delivery_date   Nullable(DateTime)
) ENGINE = MergeTree()
PARTITION BY toYYYYMM(order_purchase_timestamp)
ORDER BY (customer_id, order_purchase_timestamp)
"""

CREATE_RAW_PAYMENTS = """
CREATE TABLE IF NOT EXISTS raw.payments (
    order_id          String,
    payment_sequential       UInt8,
    payment_type	LowCardinality(String),
    payment_installments UInt8,
    payment_value Decimal(10, 2)
) ENGINE = MergeTree()
ORDER BY (order_id, payment_sequential)
"""

CREATE_RAW_ORDER_REVIEWS = """
CREATE TABLE IF NOT EXISTS raw.order_reviews (
    review_id          String,
    order_id          String,
    review_score       UInt8,
    review_comment_title	Nullable(String),
    review_comment_message	Nullable(String),
    review_creation_date	DateTime,
    review_answer_timestamp	DateTime
) ENGINE = MergeTree()
PARTITION BY toYYYYMM(review_creation_date)
ORDER BY (order_id, review_id)
"""

CREATE_RAW_ORDER_CUSTOMERS = """
CREATE TABLE IF NOT EXISTS raw.customers (
    customer_id          String,
    customer_unique_id       String,
    customer_zip_code_prefix      String,
    customer_city LowCardinality(String),
    customer_state LowCardinality(String)
) ENGINE = MergeTree()
ORDER BY (customer_id)
"""

CREATE_RAW_ORDER_GEOLOCATIONS = """
CREATE TABLE IF NOT EXISTS raw.geolocations (
    geolocation_zip_code_prefix          String,
    geolocation_lat       Float64,
    geolocation_lng      Float64,
    geolocation_city LowCardinality(String),
    geolocation_state LowCardinality(String)
) ENGINE = MergeTree()
ORDER BY (geolocation_zip_code_prefix)
"""

CREATE_RAW_ORDER_ORDER_ITEMS = """
CREATE TABLE IF NOT EXISTS raw.order_items (
    order_id          String,
    order_item_id       UInt8,
    product_id	String,
    seller_id	String,
    shipping_limit_date DateTime,
    price Decimal(10, 2),
    freight_value Decimal(10, 2)
) ENGINE = MergeTree()
ORDER BY (order_id, order_item_id, product_id)
"""

CREATE_RAW_ORDER_PRODUCTS = """
CREATE TABLE IF NOT EXISTS raw.products (
    product_id          String,
    product_category_name      LowCardinality(Nullable(String)),
    product_name_lenght	Nullable(UInt8),
    product_description_lenght Nullable(UInt16),
    product_photos_qty	Nullable(UInt8),
    product_weight_g 	Nullable(UInt16),
    product_length_cm	Nullable(UInt8),
    product_height_cm	Nullable(UInt8),
    product_width_cm	Nullable(UInt8)
) ENGINE = MergeTree()
ORDER BY (product_id)
"""

CREATE_RAW_ORDER_SELLERS = """
CREATE TABLE IF NOT EXISTS raw.sellers (
    seller_id          String,
    seller_zip_code_prefix      String,
    seller_city		LowCardinality(String),
    seller_state 	LowCardinality(String)
) ENGINE = MergeTree()
ORDER BY (seller_id)
"""

def init_tables():
    client = get_client()
    tables = [
        ('raw.orders',   CREATE_RAW_ORDERS),
        ('raw.payments', CREATE_RAW_PAYMENTS),
        ('raw.order_reviews', CREATE_RAW_ORDER_REVIEWS),
        ('raw.customers', CREATE_RAW_ORDER_CUSTOMERS),
        ('raw.geolocations', CREATE_RAW_ORDER_GEOLOCATIONS),
        ('raw.order_items', CREATE_RAW_ORDER_ORDER_ITEMS),
        ('raw.products', CREATE_RAW_ORDER_PRODUCTS),
        ('raw.sellers', CREATE_RAW_ORDER_SELLERS)
    ]
    for name, sql in tables:
        client.execute(sql)
        print(f'✅ {name}')

if __name__ == '__main__':
    init_tables()