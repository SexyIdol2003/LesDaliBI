-- Ускорение v_field_agro_input_cost_direct (42.5 с -> 0.6 с).
-- Колонка nomenklatura_id имеет тип uuid, а подзапрос цены закупки сравнивает
-- значения как ::text, поэтому обычный индекс по uuid не использовался.
-- Выполнять вне транзакции (CONCURRENTLY).
CREATE INDEX CONCURRENTLY IF NOT EXISTS ix_r1c_purchase_lines_nomenklatura_txt
    ON raw.r1c_purchase_lines ((nomenklatura_id::text));
ANALYZE raw.r1c_purchase_lines;
