-- show view for pipeline health
CREATE OR REPLACE VIEW tennis_raw.vw_pipeline_health AS
SELECT
    table_id,
    row_count,
    


-- show view for load history
CREATE OR REPLACE VIEW tennis_raw.vw_load_history AS

SELECT 'dim_players' AS table_name, loaded_at, COUNT(*) AS loaded
FROM tennis_raw.dim_players
GROUP BY loaded_at

UNION ALL

