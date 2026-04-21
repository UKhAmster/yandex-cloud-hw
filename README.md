# Yandex Cloud — Обработка и анализ данных

**Дисциплина:** Семинар наставника  
**Тема:** Обработка и анализ данных в Yandex Cloud: от загрузки до визуализации  
**Преподаватель:** Владислав Шевченко  
**Дедлайн:** 21.04.2026

---

## Структура репозитория

```
.
├── task1_dataproc/
│   ├── 01_create_tables.sql      # DDL для Hive/Spark: transactions_v2, logs_v2
│   ├── 02_aggregations.sql       # Агрегирующие SQL-запросы (HiveQL / Spark SQL)
│   └── task1_notebook.json       # Zeppelin-ноутбук (PySpark DataFrame API)
│
├── task2_clickhouse/
│   └── clickhouse_queries.sql    # DDL + 5 агрегаций для ClickHouse
│
├── task4_airflow/
│   └── hive_to_clickhouse_dag.py # Airflow DAG: репликация Hive → ClickHouse
│
└── README.md
```

---

## Задание 1 — Yandex Data Proc (Hive / Spark SQL)

### Таблицы
| Таблица | Источник | Формат |
|---|---|---|
| `transactions_v2` | Object Storage (CSV) | `transaction_id, user_id, amount, currency, transaction_date, is_fraud` |
| `logs_v2` | Object Storage (TXT, TSV) | `log_id, transaction_id, event_time, category, message` |

### Агрегации (`02_aggregations.sql` / `task1_notebook.json`)
1. **По валютам** — суммарный объём транзакций USD/EUR/RUB, кол-во и средний чек.
2. **Фрод vs норма** — `is_fraud`: количество, суммарная сумма, средний чек.
3. **Ежедневная динамика** — `tx_count`, `daily_total`, `daily_avg` по датам.
4. **Временные интервалы** — разбивка по месяцу и дню недели.
5. **JOIN logs_v2** — логов на транзакцию, топ категорий, фрод по категориям.

### Запуск ноутбука в Zeppelin
1. Импортировать `task1_notebook.json` через **Import Note**.
2. Установить интерпретатор `%spark.pyspark`.
3. Заменить `<your-bucket>` на имя своего бакета в Object Storage.
4. Запустить ячейки последовательно.

---

## Задание 2 — ClickHouse

### Таблицы
| Таблица | Ключ сортировки |
|---|---|
| `orders` | `(order_date, order_id)` |
| `order_items` | `(order_id, item_id)` |

### Агрегации (`clickhouse_queries.sql`)
1. Группировка по `payment_status`: кол-во заказов, сумма, средний чек.
2. JOIN `orders × order_items`: общая статистика по позициям.
3. Ежедневная динамика заказов.
4. Топ-10 пользователей по сумме заказов.
5. Топ-10 продуктов по выручке (только `paid`-заказы).

### Загрузка данных из Object Storage
```sql
INSERT INTO orders
SELECT * FROM s3(
    'https://storage.yandexcloud.net/<bucket>/data/orders.csv',
    '<access_key>', '<secret_key>',
    'CSV', 'order_id String, user_id String, order_date Date,
             payment_status String, total_amount Float64'
);
```

---

## Задание 3 — DataLens (визуализация)

Дашборд строится поверх ClickHouse-коннектора. Рекомендуемые чарты:

| Чарт | Источник | Тип |
|---|---|---|
| Динамика транзакций по датам | `agg_daily` (Hive) или `orders` (CH) | Line chart |
| Распределение по валютам | `agg_currency` | Pie / Bar |
| Статистика фрода | `agg_fraud` | Bar |
| Топ пользователей | `orders` GROUP BY user_id | Horizontal bar |

**Фильтры:** по `order_date` (date range), `payment_status`, `user_id`.

---

## Задание 4* — Airflow DAG (репликация Hive → ClickHouse)

**Файл:** `task4_airflow/hive_to_clickhouse_dag.py`

### Шаги DAG
```
write_spark_script → spark_export_hive_agg → clickhouse_load
```

1. `write_spark_script` — сохраняет Spark-скрипт на Driver-узел.
2. `spark_export_hive_agg` — PySpark читает `transactions_v2`, агрегирует по датам, сохраняет CSV в HDFS `/tmp/airflow_export/tx_daily_agg`.
3. `clickhouse_load` — читает CSV из HDFS, создаёт таблицу `tx_daily_agg` в ClickHouse (если не существует), вставляет данные.

### Настройка
```bash
# Зависимости
pip install apache-airflow-providers-apache-spark clickhouse-driver pandas

# Переменная с паролем ClickHouse
airflow variables set ch_password '<your-password>'

# Airflow Connection для Spark
airflow connections add spark_default \
  --conn-type spark \
  --conn-host yarn \
  --conn-extra '{"queue": "default"}'
```

**Расписание:** `0 3 * * *` (ежедневно в 03:00 UTC).

---

## Зависимости

```
pyspark>=3.3
clickhouse-driver>=0.2
pandas>=2.0
apache-airflow>=2.8
apache-airflow-providers-apache-spark
```
