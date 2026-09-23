-- 27_diagnose_putevoy_pole_id_linkage.sql
-- Диагностика (2026-09-15): mart.fact_putevoy_rabota.pole_id всегда NULL,
-- потому что табличная часть Document_АпкПутевойЛистТракториста_ВыполненныеРаботы
-- физически не содержит поле "Поле_Key" (подтверждено в коде dag_extract_putevoy_list.py,
-- см. _line_to_row: "None, # pole_id — в этой табличной части поля нет").
--
-- Это READ-ONLY скрипт для ручного расследования. Он НЕ меняет данные и НЕ должен
-- запускаться из Airflow — только вручную через psql/pgAdmin аналитиком.
--
-- Цель: проверить, можно ли восстановить pole_id для путевых листов через:
--   1) КлючСвязи строки путевого листа -> совпадение с doc_id другого документа,
--      у которого Поле_Key реально есть (raw.r1c_dvizhenie_produkcii_lines,
--      raw.r1c_tok_vzveshivanie_talony);
--   2) совпадение по технике (tehnika_id / oborudovanie_id) и дате (den_raboty)
--      с записями, где pole_id известен, как менее точный fallback.
--
-- Как использовать: выполнить блоки по очереди, посмотреть на match_count.
-- Если все три блока дают match_count = 0 (или близко к 0) — восстановить pole_id
-- программно невозможно, нужно доставать поле из 1С другим способом
-- (например, через отдельный регистр "Задание на работу" или "АктВыполненныхРабот",
-- если такой существует в конфигурации — уточнять через $metadata/curl к OData).

-- Блок 0: общий объём строк путевых листов за всё время
SELECT count(*) AS total_putevoy_lines,
       count(*) FILTER (WHERE klyuch_svyazi IS NOT NULL) AS lines_with_klyuch_svyazi
FROM raw.r1c_putevoy_list_lines;

-- Блок 1: пробуем сматчить КлючСвязи строки путевого листа с doc_id движения продукции
-- (там Поле_Key реально есть в raw.r1c_dvizhenie_produkcii_lines.pole_id)
SELECT count(*) AS match_count_dvizhenie
FROM raw.r1c_putevoy_list_lines pl
JOIN raw.r1c_dvizhenie_produkcii_lines dl
  ON dl.doc_id::text = pl.klyuch_svyazi
 AND dl.pole_id IS NOT NULL;

-- Блок 2: пробуем сматчить КлючСвязи с doc_id талонов взвешивания на току
-- (там Поле_Key есть в raw.r1c_tok_vzveshivanie_talony.pole_id)
SELECT count(*) AS match_count_tok_vzveshivanie
FROM raw.r1c_putevoy_list_lines pl
JOIN raw.r1c_tok_vzveshivanie_talony tt
  ON tt.doc_id::text = pl.klyuch_svyazi
 AND tt.pole_id IS NOT NULL;

-- Блок 3: fallback по технике + дате (менее точный, может давать ложные совпадения,
-- если несколько полей обрабатывались одной техникой в один день)
SELECT count(*) AS fallback_match_by_tehnika_date
FROM raw.r1c_putevoy_list_lines pl
JOIN raw.r1c_putevoy_list doc
  ON doc._id = pl.doc_id
JOIN raw.r1c_tok_vzveshivanie_talony tt
  ON tt.mehanizator_id = doc.voditel_id
 AND tt.pole_id IS NOT NULL
 AND date_trunc('day', pl.den_raboty) = date_trunc('day', tt.doc_id::date)
WHERE pl.den_raboty IS NOT NULL;

-- Блок 4: примеры значений КлючСвязи для ручного сопоставления вручную в 1С
-- (взять несколько ключей и найти их через консоль 1С или curl к OData
-- по разным сущностям, чтобы понять, на какой документ они реально ссылаются)
SELECT doc_id, line_number, klyuch_svyazi, den_raboty, vid_raboty_id, oborudovanie_id
FROM raw.r1c_putevoy_list_lines
WHERE klyuch_svyazi IS NOT NULL
ORDER BY den_raboty DESC NULLS LAST
LIMIT 20;

-- 2026-09-23: итог проверки моста через raw.r1c_dvizhenie_produkcii_lines.
-- raw.r1c_putevoy_list_lines.klyuch_svyazi имеет тип uuid и заполнен.
-- Сопоставление p.klyuch_svyazi::text с d._id, d.doc_id, d.pole_id
-- и d.nomenklatura_id вернуло 0 совпадений во всех вариантах.
-- Следовательно, r1c_dvizhenie_produkcii_lines не является владельцем
-- ключа связи путевого листа и не может использоваться для восстановления pole_id.
