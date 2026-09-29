BEGIN;

DROP MATERIALIZED VIEW IF EXISTS mart.mv_field_agro_input_cost_per_ha_season;

CREATE MATERIALIZED VIEW mart.mv_field_agro_input_cost_per_ha_season AS
SELECT * FROM mart.v_field_agro_input_cost_per_ha_season
WITH DATA;

CREATE UNIQUE INDEX ux_mv_agro_cost_per_ha_season
    ON mart.mv_field_agro_input_cost_per_ha_season (field_sk, season_year);
CREATE INDEX ix_mv_agro_cost_per_ha_season_year
    ON mart.mv_field_agro_input_cost_per_ha_season (season_year);

GRANT SELECT ON mart.mv_field_agro_input_cost_per_ha_season TO datalens_ro;

COMMIT;
ANALYZE mart.mv_field_agro_input_cost_per_ha_season;
