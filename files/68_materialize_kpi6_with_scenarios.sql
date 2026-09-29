BEGIN;

DROP MATERIALIZED VIEW IF EXISTS mart.mv_field_season_kpi6_with_scenarios;

CREATE MATERIALIZED VIEW mart.mv_field_season_kpi6_with_scenarios AS
SELECT * FROM mart.v_field_season_kpi6_with_scenarios
WITH DATA;

CREATE UNIQUE INDEX ux_mv_kpi6_with_scenarios
    ON mart.mv_field_season_kpi6_with_scenarios (field_sk, season_year);
CREATE INDEX ix_mv_kpi6_with_scenarios_year_status
    ON mart.mv_field_season_kpi6_with_scenarios (season_year, coverage_status);

GRANT SELECT ON mart.mv_field_season_kpi6_with_scenarios TO datalens_ro;

COMMIT;
ANALYZE mart.mv_field_season_kpi6_with_scenarios;
