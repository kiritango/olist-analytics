# Olist E-Commerce Analytics

Аналитический DWH на основе публичного датасета бразильского маркетплейса Olist.

## Стек
- **ClickHouse** — аналитическая база данных
- **dbt** — трансформации и тестирование данных
- **Apache Kafka** — потоковая передача данных
- **PySpark** — обработка стрима
- **Docker** — контейнеризация окружения
- **Python** — загрузка исторических данных и генерация стрима

## Архитектура

### Batch (исторические данные)
CSV (Olist dataset) → Python → ClickHouse raw → dbt → staging → marts

### Stream (живые данные)
Producer (Docker) → Kafka → Spark Streaming → ClickHouse raw_stream → dbt → marts

## Структура проекта
```
olist-analytics/
├── docker-compose.yml        # ClickHouse + Kafka + Spark + Producer
├── .env.example              # Шаблон переменных окружения
├── requirements.txt          # Python зависимости
├── scripts/
│   ├── utils.py              # ClickHouse клиент
│   ├── init_db.py            # Создание баз данных
│   ├── init_tables.py        # Создание таблиц
│   └── load_raw.py           # Загрузка CSV в ClickHouse
├── kafka/
│   ├── Dockerfile            # Образ для producer
│   └── producer.py           # Генератор событий → Kafka
├── spark/
│   └── consumer.py           # Spark Streaming: Kafka → ClickHouse
├── olist_dbt/
│   └── models/
│       ├── staging/          # Очистка и типизация данных
│       └── marts/            # Бизнес-метрики
└── docs/
    └── graph.png             # dbt lineage graph
```

[Lineage Graph]
![dbt lineage graph](docs/graph.png)

## Витрины

| Витрина | Описание |
|---|---|
| mart_revenue | Ежемесячная выручка, средний чек, ARPU |
| mart_dau | Ежедневная активность покупателей |
| mart_cohorts | Когортный анализ и retention |
| mart_funnel | Воронка заказов по этапам |
| mart_top_products | Топ категорий товаров по выручке |

## Запуск проекта

**1. Клонировать репозиторий**
```bash
git clone https://github.com/kiritango/olist-analytics.git
cd olist-analytics
```

**2. Создать .env из шаблона**
```bash
cp .env.example .env
# заполнить значения
```

**3. Поднять окружение**
```bash
docker-compose up -d
```

Producer запустится автоматически и начнёт генерировать заказы в Kafka.

**4. Установить зависимости**
```bash
python -m venv .venv
.venv\Scripts\activate
pip install -r requirements.txt
```

**5. Загрузить исторические данные**
```bash
python scripts/init_db.py
python scripts/init_tables.py
python scripts/load_raw.py
```

**6. Запустить Spark Streaming**
```bash
docker exec -it olist_spark /opt/spark/bin/spark-submit \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.13:4.0.0,com.github.housepower:clickhouse-native-jdbc-shaded:2.7.1 \
  /opt/spark-apps/consumer.py
```

Проверить что данные поступают:
```sql
SELECT count() FROM raw.orders_stream;
SELECT count() FROM raw.order_items_stream;
SELECT count() FROM raw.payments_stream;
```

**7. Запустить dbt**
```bash
cd olist_dbt
dbt run
dbt test
```