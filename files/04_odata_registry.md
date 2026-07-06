# Реестр OData-объектов 1С — Лесные дали

> Обновлено: 2026-07-06  
> Источник: 1С:ERP Агропромышленный комплекс

## Найденные объекты (5 ключевых)

| № | Тип | Имя объекта в 1С | OData endpoint | RAW-таблица | Режим выгрузки |
|---|-----|-------------------|----------------|-------------|----------------|
| 1 | Справочник | УпаковкиЕдиницыИзмерения | `Catalog_УпаковкиЕдиницыИзмерения` | `raw.r1c_upakovki` | Полная, 1×/день |
| 2 | Справочник | АпкМоделиСХТехники | `Catalog_АпкМоделиСХТехники` | `raw.r1c_sh_tehnika` | Полная, 1×/день |
| 3 | Справочник | АпкПоля | `Catalog_АпкПоля` | `raw.r1c_polya` | Полная, 1×/день |
| 4 | Документ | ДвижениеПродукцииИМатериалов | `Document_ДвижениеПродукцииИМатериалов` | `raw.r1c_dvizhenie_produkcii` + `_lines` | Инкрементальная по `Date` |
| 5 | Документ | АпкПутевойЛистТракториста | `Document_АпкПутевойЛистТракториста` | `raw.r1c_putevoy_list` + `_lines` | Инкрементальная по `Date` |

## OData URL-шаблоны

### Справочники (полная выгрузка)
```
GET http://<server>/<base>/odata/standard.odata/Catalog_УпаковкиЕдиницыИзмерения?$format=json&$select=Ref_Key,DeletionMark,Description,Code
GET http://<server>/<base>/odata/standard.odata/Catalog_АпкМоделиСХТехники?$format=json&$select=Ref_Key,DeletionMark,Description
GET http://<server>/<base>/odata/standard.odata/Catalog_АпкПоля?$format=json&$select=Ref_Key,DeletionMark,Description,Parent_Key,АпкПлощадьПоляГа
```

### Документы (инкрементальная выгрузка по дате)
```
GET http://<server>/<base>/odata/standard.odata/Document_ДвижениеПродукцииИМатериалов
  ?$format=json
  &$filter=Date ge datetime'2026-01-01T00:00:00'
  &$expand=ТоварыТабличнаяЧасть
  &$select=Ref_Key,DeletionMark,Posted,Number,Date,Операция,Склад_Key

GET http://<server>/<base>/odata/standard.odata/Document_АпкПутевойЛистТракториста
  ?$format=json
  &$filter=Date ge datetime'2026-01-01T00:00:00'
  &$expand=РаботыТабличнаяЧасть
  &$select=Ref_Key,DeletionMark,Posted,Number,Date,Техника_Key,Водитель_Key
```

## Связи между таблицами

```
r1c_dvizhenie_produkcii_lines.pole_id    → r1c_polya._id
r1c_dvizhenie_produkcii_lines.edinica_id → r1c_upakovki._id
r1c_dvizhenie_produkcii_lines.nomenklatura_id → r1c_nomenclature._id

r1c_putevoy_list.model_tehniki_id        → r1c_sh_tehnika._id
r1c_putevoy_list_lines.pole_id           → r1c_polya._id
r1c_putevoy_list_lines.edinica_id        → r1c_upakovki._id
```

## Разбивка Document_ДвижениеПродукцииИМатериалов по Операции

| Значение поля `operaciya` | mart-таблица | Смысл |
|---------------------------|-------------|-------|
| `СписаниеМатериалов` | `mart.fact_spisanie_materialov` | Списание семян, удобрений, ядохимикатов |
| `ОприходованиеУрожая` | `mart.fact_vypusk_urozhaya` | Приход урожая с поля на склад |
| Прочие | `mart.fact_prochee_dvizhenie` | Прочие движения |

## Следующие шаги — DAG'и Airflow

1. **DAG `dag_extract_catalogs`** — ежедневно, 3 справочника параллельно
2. **DAG `dag_extract_dvizhenie`** — каждые 2 часа, инкремент за последние N часов
3. **DAG `dag_extract_putevoy`** — ежедневно ночью, инкремент за день
4. **DAG `dag_transform_mart`** — после загрузки, split по Операции → mart-таблицы
