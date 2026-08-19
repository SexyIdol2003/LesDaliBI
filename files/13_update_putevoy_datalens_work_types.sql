CREATE OR REPLACE VIEW mart.v_putevoy_rabota_datalens AS
SELECT
    r.doc_date::date AS work_date,
    DATE_TRUNC('month', r.doc_date)::date AS month,
    r.doc_number AS document_no,
    r.line_number,

    r.tehnika_id::text AS equipment_id,
    COALESCE(eq.name, r.tehnika_id::text) AS equipment,
    COALESCE(eq.eq_type, 'Не определён') AS equipment_type,

    r.voditel_id::text AS employee_id,
    COALESCE(emp.description, r.voditel_id::text) AS employee,

    r.agr_operaciya_id::text AS operation_id,

    r.obem_rabot_ga AS work_area_ha,
    r.norma_vyrabotki AS work_norm_ha,

    CASE
        WHEN r.norma_vyrabotki > 0
        THEN ROUND(100.0 * r.obem_rabot_ga / r.norma_vyrabotki, 1)
    END AS norm_completion_pct,

    r.doc_id::text AS source_doc_id,
    r._updated_at AS source_updated_at,

    COALESCE(op.description, r.agr_operaciya_id::text) AS operation_name,
    op.operation_kind AS operation_code,
    op.parent_id AS operation_group_id,
    COALESCE(parent_op.description, 'Без группы') AS operation_group

FROM mart.fact_putevoy_rabota r
LEFT JOIN mart.dim_equipment eq
    ON eq.code_1c = r.tehnika_id::text
LEFT JOIN raw.r1c_employees emp
    ON emp.individual_id = r.voditel_id::text
LEFT JOIN raw.r1c_tech_operations op
    ON op._id = r.agr_operaciya_id::text
LEFT JOIN raw.r1c_tech_operations parent_op
    ON parent_op._id = op.parent_id
WHERE r.obem_rabot_ga > 0;

GRANT SELECT ON mart.v_putevoy_rabota_datalens TO datalens_ro;
