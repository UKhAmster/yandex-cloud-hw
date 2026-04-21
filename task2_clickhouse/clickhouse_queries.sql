-- ============================================================
-- Задание 2: ClickHouse — создание таблиц и агрегации
-- ============================================================

-- ----------------------------------------------------------
-- DDL: таблица заказов
-- ----------------------------------------------------------
CREATE TABLE IF NOT EXISTS orders
(
    order_id       String,
    user_id        String,
    order_date     Date,
    payment_status String,        -- 'paid' | 'pending' | 'cancelled'
    total_amount   Float64
)
ENGINE = MergeTree()
ORDER BY (order_date, order_id);

-- Загрузка из Object Storage (S3)
-- INSERT INTO orders
-- SELECT * FROM s3(
--     'https://storage.yandexcloud.net/<bucket>/data/orders.csv',
--     '<access_key>', '<secret_key>',
--     'CSV', 'order_id String, user_id String, order_date Date,
--              payment_status String, total_amount Float64'
-- );

-- ----------------------------------------------------------
-- DDL: таблица позиций заказов
-- ----------------------------------------------------------
CREATE TABLE IF NOT EXISTS order_items
(
    item_id    String,
    order_id   String,
    product_id String,
    quantity   UInt32,
    unit_price Float64
)
ENGINE = MergeTree()
ORDER BY (order_id, item_id);

-- INSERT INTO order_items
-- SELECT * FROM s3(
--     'https://storage.yandexcloud.net/<bucket>/data/order_items.csv',
--     '<access_key>', '<secret_key>',
--     'CSV', 'item_id String, order_id String, product_id String,
--              quantity UInt32, unit_price Float64'
-- );

-- ============================================================
-- Агрегации
-- ============================================================

-- ----------------------------------------------------------
-- Аг. 1. Группировка по payment_status
-- ----------------------------------------------------------
SELECT
    payment_status,
    count()                          AS order_count,
    round(sum(total_amount), 2)      AS total_sum,
    round(avg(total_amount), 2)      AS avg_order_amount
FROM orders
GROUP BY payment_status
ORDER BY total_sum DESC;

-- ----------------------------------------------------------
-- Аг. 2. JOIN orders × order_items — общая статистика по товарам
-- ----------------------------------------------------------
SELECT
    o.payment_status,
    count(DISTINCT o.order_id)           AS order_count,
    sum(oi.quantity)                     AS total_qty,
    round(sum(oi.quantity * oi.unit_price), 2) AS items_total,
    round(avg(oi.unit_price), 2)         AS avg_unit_price
FROM orders AS o
INNER JOIN order_items AS oi ON o.order_id = oi.order_id
GROUP BY o.payment_status
ORDER BY items_total DESC;

-- ----------------------------------------------------------
-- Аг. 3. Ежедневная статистика заказов
-- ----------------------------------------------------------
SELECT
    order_date,
    count()                         AS order_count,
    round(sum(total_amount), 2)     AS daily_total
FROM orders
GROUP BY order_date
ORDER BY order_date;

-- ----------------------------------------------------------
-- Аг. 4. Топ-10 самых активных пользователей (по сумме и кол-ву)
-- ----------------------------------------------------------
SELECT
    user_id,
    count()                         AS order_count,
    round(sum(total_amount), 2)     AS total_spent,
    round(avg(total_amount), 2)     AS avg_order
FROM orders
GROUP BY user_id
ORDER BY total_spent DESC
LIMIT 10;

-- ----------------------------------------------------------
-- Аг. 5. Топ-10 самых популярных продуктов (по выручке)
-- ----------------------------------------------------------
SELECT
    oi.product_id,
    sum(oi.quantity)                          AS total_qty,
    round(sum(oi.quantity * oi.unit_price), 2) AS product_revenue
FROM order_items AS oi
INNER JOIN orders AS o ON oi.order_id = o.order_id
WHERE o.payment_status = 'paid'
GROUP BY oi.product_id
ORDER BY product_revenue DESC
LIMIT 10;
