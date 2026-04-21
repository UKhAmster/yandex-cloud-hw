-- ============================================================
-- Задание 1: Создание таблиц в Hive / Spark SQL
-- ============================================================

-- Таблица транзакций
CREATE TABLE IF NOT EXISTS transactions_v2 (
    transaction_id   STRING,
    user_id          STRING,
    amount           DOUBLE,
    currency         STRING,
    transaction_date DATE,
    is_fraud         INT
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
TBLPROPERTIES ("skip.header.line.count"="1");

-- Загрузка данных из Object Storage (S3)
-- LOAD DATA INPATH 's3a://<your-bucket>/data/transactions_v2.csv'
-- OVERWRITE INTO TABLE transactions_v2;

-- Таблица логов
CREATE TABLE IF NOT EXISTS logs_v2 (
    log_id         STRING,
    transaction_id STRING,
    event_time     TIMESTAMP,
    category       STRING,
    message        STRING
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\t'
STORED AS TEXTFILE
TBLPROPERTIES ("skip.header.line.count"="1");

-- LOAD DATA INPATH 's3a://<your-bucket>/data/logs_v2.txt'
-- OVERWRITE INTO TABLE logs_v2;
