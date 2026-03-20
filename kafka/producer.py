import json
import time
import uuid
import random
from datetime import datetime, timedelta
from kafka import KafkaProducer
from clickhouse_driver import Client
from dotenv import load_dotenv
from pathlib import Path
import os

load_dotenv(Path(__file__).parent.parent / '.env')
KAFKA_BROKER = os.getenv('KAFKA_BROKER', 'localhost:9092')

# Подключаемся к ClickHouse чтобы брать реальные ID
client = Client(
    host=os.getenv('CLICKHOUSE_HOST', 'localhost'),
    port=int(os.getenv('CLICKHOUSE_PORT', 9000)),
    user=os.getenv('CLICKHOUSE_USER'),
    password=os.getenv('CLICKHOUSE_PASSWORD')
)

producer = KafkaProducer(
    bootstrap_servers=KAFKA_BROKER,
    value_serializer=lambda v: json.dumps(v, default=str).encode('utf-8')
)

# Загружаем реальные ID из Olist чтобы события были правдоподобными
print('Загружаем справочники из ClickHouse...', flush=True)
customer_ids = [r[0] for r in client.execute('SELECT customer_id FROM raw.customers')]
product_ids  = [r[0] for r in client.execute('SELECT product_id FROM raw.products')]
seller_ids   = [r[0] for r in client.execute('SELECT seller_id FROM raw.sellers')]
payment_types = ['credit_card', 'boleto', 'voucher', 'debit_card']
statuses = ['created', 'approved', 'shipped', 'delivered']
print(f'Готово: {len(customer_ids)} покупателей, {len(product_ids)} товаров', flush=True)

def generate_order():
    order_id = str(uuid.uuid4())
    now = datetime.now()
    status = random.choice(statuses)

    # Даты зависят от статуса
    order_approved_at = None
    order_delivered_carrier_date = None
    order_delivered_customer_date = None
    order_estimated_delivery_date = None

    if status in ['approved', 'shipped', 'delivered']:
        order_approved_at = (now + timedelta(hours=random.randint(1, 24))).isoformat()

    if status in ['shipped', 'delivered']:
        order_delivered_carrier_date = (now + timedelta(days=random.randint(1, 3))).isoformat()
        order_estimated_delivery_date = (now + timedelta(days=random.randint(7, 15))).isoformat()

    if status == 'delivered':
        order_delivered_customer_date = (now + timedelta(days=random.randint(3, 10))).isoformat()

    order = {
        'event_type': 'order',
        'order_id': order_id,
        'customer_id': random.choice(customer_ids),
        'order_status': status,
        'order_purchase_timestamp': now.isoformat(),
        'order_approved_at': order_approved_at,
        'order_delivered_carrier_date': order_delivered_carrier_date,
        'order_delivered_customer_date': order_delivered_customer_date,
        'order_estimated_delivery_date': order_estimated_delivery_date
    }

    # Позиции заказа (1-4 товара)
    items = []
    n_items = random.randint(1, 4)
    for i in range(1, n_items + 1):
        price = round(random.uniform(10, 500), 2)
        freight = round(random.uniform(5, 50), 2)
        items.append({
            'event_type': 'order_item',
            'order_id': order_id,
            'order_item_id': i,
            'product_id': random.choice(product_ids),
            'seller_id': random.choice(seller_ids),
            'shipping_limit_date': now,
            'price': price,
            'freight_value': freight
        })

    # Платёж
    total = sum(i['price'] + i['freight_value'] for i in items)
    payment = {
        'event_type': 'payment',
        'order_id': order_id,
        'payment_type': random.choice(payment_types),
        'payment_value': round(total, 2)
    }

    return order, items, payment

def stream(orders_per_second=1):
    print(f'Стриминг запущен: {orders_per_second} заказ/сек. Ctrl+C для остановки.', flush=True)
    count = 0

    while True:
        order, items, payment = generate_order()

        # Отправляем в разные топики
        producer.send('olist.orders', order)
        for item in items:
            producer.send('olist.order_items', item)
        producer.send('olist.payments', payment)

        count += 1
        if count % 10 == 0:
            print(f'Отправлено заказов: {count} | Последний: {order["order_id"][:8]}...', flush=True)

        time.sleep(1 / orders_per_second)

if __name__ == '__main__':
    stream(orders_per_second=1)