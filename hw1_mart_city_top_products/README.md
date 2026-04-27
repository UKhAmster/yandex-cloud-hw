# Витрина mart_city_top_products

Решение домашнего задания по теме «Яндекс Cloud»: сборка витрины Top‑2 товаров по выручке в каждом городе на PySpark в Apache Zeppelin.

## Постановка

На входе три датафрейма, создаются прямо в Zeppelin: `users(user_id, city)`, `orders(order_id, user_id, product_id, qty, price)`, `products(product_id, product_name)`. Нужно посчитать выручку, обогатить заказы, агрегировать по городу и товару, отобрать Top‑2 по выручке в каждом городе, сложить результат в parquet в HDFS и в S3 (с `overwrite`) и прочитать обратно для проверки.

## Окружение

- Yandex Data Processing 1.4 (Apache Spark 2.x + Apache Zeppelin 0.8).
- Сервисный аккаунт `hw-sa` с ролью `storage.editor` на бакете.
- Object Storage, бакет `hw-hse1`, под результат используется существующая папка `tmp/`.
- До UI Zeppelin шёл через SSH‑тоннель: на мастер‑ноде Zeppelin слушает `127.0.0.1:8080`, поэтому пробрасывал порт командой `ssh -i ~/.ssh/id_ed25519 -L 8890:localhost:8080 ubuntu@<master_ip>` и открывал `http://localhost:8890`. Интерпретатор — `%spark.pyspark`.

## Логика витрины

Ноутбук разбит на 6 содержательных параграфов:

1. Создание `users / orders / products` (данные взяты из условия задания).
2. `withColumn("revenue", qty * price)`, затем два inner join: `orders` ⨝ `users` по `user_id` и ⨝ `products` по `product_id`.
3. `groupBy("city", "product_id", "product_name").agg(count("order_id"), sum("qty"), sum("revenue"))` → метрики `orders_cnt / qty_sum / revenue_sum`.
4. Top‑2 через окно: `Window.partitionBy("city").orderBy(F.col("revenue_sum").desc(), F.col("product_id").asc())` + `row_number()` + фильтр `rn <= 2`. Тай‑брейкер по `product_id` нужен потому, что в Berlin у `p1 (Ring VOLA)` и `p2 (Ring POROG)` одинаковая выручка `30.0` — без него порядок был бы недетерминированным.
5. Запись parquet с `overwrite` в два места:
   - HDFS: `/tmp/sandbox_zeppelin/mart_city_top_products/`
   - S3:   `s3a://hw-hse1/tmp/sandbox_zeppelin/mart_city_top_products/`
   Креды S3 явно не передаются — Spark берёт их через сервисный аккаунт ноды.
6. `spark.read.parquet(HDFS_PATH).orderBy("city", revenue_sum desc).show()` — проверка, что читается обратно.

## Результат

| city    | product_id | product_name | orders_cnt | qty_sum | revenue_sum |
|---------|------------|--------------|------------|---------|-------------|
| Berlin  | p3         | Ring TISHINA | 1          | 5       | 35.0        |
| Berlin  | p1         | Ring VOLA    | 2          | 3       | 30.0        |
| Hamburg | p1         | Ring VOLA    | 1          | 10      | 100.0       |
| Munich  | p2         | Ring POROG   | 1          | 3       | 90.0        |
| Munich  | p3         | Ring TISHINA | 1          | 1       | 7.0         |

## Структура папки

- `notebooks/mart_city_top_products.zpln` — экспорт ноутбука из Zeppelin (с outputs).
- `src/mart_city_top_products.py` — тот же код в виде PySpark‑скрипта, под `spark-submit`.
- `.gitignore` — стандартные исключения для Python/Spark.
