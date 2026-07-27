-- Создаем отдельную базу данных для нашего DWH проекта
CREATE DATABASE dwh;
GRANT ALL PRIVILEGES ON DATABASE dwh TO airflow;

-- Подключаемся к ней и нарезаем слои (схемы)
\c dwh;
CREATE SCHEMA staging; -- Сюда польются грязные данные из API
CREATE SCHEMA core;    -- Тут будут лежать чистые таблицы фактов и измерений
CREATE SCHEMA mart;    -- Тут будут готовые витрины для DS моделей
