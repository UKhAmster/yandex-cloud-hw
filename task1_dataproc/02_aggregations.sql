-- ============================================================
-- Задание 1: Агрегации по таблицам transactions_v2 и logs_v2
-- ============================================================

-- ----------------------------------------------------------
-- 1. Суммарный объём транзакций по «хорошим» валютам
-- ----------------------------------------------------------
SELECT
    currency,
    COUNT(*)          AS tx_count,
    ROUND(SUM(amount), 2) AS total_amount,
    ROUND(AVG(amount), 2) AS avg_amount
FROM transactions_v2
WHERE currency IN ('USD', 'EUR', 'RUB')
GROUP BY currency
ORDER BY total_amount DESC;

-- ----------------------------------------------------------
-- 2. Статистика по мошенническим / нормальным транзакциям
-- ----------------------------------------------------------
SELECT
    is_fraud,
    COUNT(*)              AS tx_count,
    ROUND(SUM(amount), 2) AS total_amount,
    ROUND(AVG(amount), 2) AS avg_check
FROM transactions_v2
GROUP BY is_fraud;

-- ----------------------------------------------------------
-- 3. Ежедневная динамика транзакций
-- ----------------------------------------------------------
SELECT
    transaction_date,
    COUNT(*)              AS tx_count,
    ROUND(SUM(amount), 2) AS daily_total,
    ROUND(AVG(amount), 2) AS daily_avg
FROM transactions_v2
GROUP BY transaction_date
ORDER BY transaction_date;

-- ----------------------------------------------------------
-- 4. Анализ по временным интервалам (месяц / день недели)
-- ----------------------------------------------------------
SELECT
    MONTH(transaction_date) AS tx_month,
    DAYOFWEEK(transaction_date) AS day_of_week,
    COUNT(*)              AS tx_count,
    ROUND(SUM(amount), 2) AS total_amount
FROM transactions_v2
GROUP BY
    MONTH(transaction_date),
    DAYOFWEEK(transaction_date)
ORDER BY tx_month, day_of_week;

-- ----------------------------------------------------------
-- 5. JOIN с logs_v2: количество логов на транзакцию
--    и самые частые категории событий
-- ----------------------------------------------------------

-- 5a. Логов на одну транзакцию
SELECT
    t.transaction_id,
    t.amount,
    t.currency,
    COUNT(l.log_id) AS log_count
FROM transactions_v2 t
LEFT JOIN logs_v2 l ON t.transaction_id = l.transaction_id
GROUP BY
    t.transaction_id,
    t.amount,
    t.currency
ORDER BY log_count DESC
LIMIT 20;

-- 5b. Топ-10 категорий логов по числу событий
SELECT
    l.category,
    COUNT(*) AS event_count
FROM logs_v2 l
GROUP BY l.category
ORDER BY event_count DESC
LIMIT 10;

-- 5c. Категории логов по мошенническим транзакциям
SELECT
    l.category,
    t.is_fraud,
    COUNT(*) AS event_count
FROM logs_v2 l
JOIN transactions_v2 t ON l.transaction_id = t.transaction_id
GROUP BY l.category, t.is_fraud
ORDER BY is_fraud DESC, event_count DESC;
