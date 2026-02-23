-- show view for pipeline health
CREATE OR REPLACE VIEW tennis_raw.vw_pipeline_health AS
SELECT
    table_id,
    row_count,
    ROUND(size_bytes / (1024 * 1024), 2) AS size_mb,
    last_modified,
    TIMESTAMP_DIFF(CURRENT_TIMESTAMP(), last_modified, MINUTE) AS minutes_since_update,

    CASE 
        WHEN TIMESTAMP_DIFF(CURRENT_TIMESTAMP(), last_modified, HOUR) > 24 THEN 'STALE'  
        ELSE 'FRESH'
    END AS freshness_status,

    CASE 
        WHEN row_count = 0 THEN 'EMPTY'
        WHEN row_count < expected_min THEN 'LOW'
        ELSE 'OK'
    END AS row_count_status,

    expected_min

FROM (
    SELECT
        table_id,
        row_count,
        size_bytes,
        TIMESTAMP_MILLIS(last_modified_time) AS last_modified,

        -- minimum rows
        CASE table_id
            WHEN 'dim_players' THEN 1000
            WHEN 'dim_tournaments' THEN 10
            WHEN 'fact_matches' THEN 100
            WHEN 'fact_match_stats' THEN 100
            WHEN 'fact_rankings' THEN 1000
            ELSE 0
        END AS expected_min

    FROM `tennis-etl-pipeline.tennis_raw.__TABLES__`
    WHERE table_id IN (
        'dim_players',
        'dim_tournaments',
        'fact_matches',
        'fact_match_stats',
        'fact_rankings'
    )
);


-- generate view for data quality summary
CREATE OR REPLACE VIEW tennis_raw.vw_data_quality_summary AS 

-- check roughly the same amount of winners and losers
SELECT 
    'fact_match_stats' AS table_name,
    'player_role_balance' AS check_name,
    CASE 
        WHEN winner_count = 0 OR loser_count = 0 THEN 'FAIL'
        WHEN SAFE_DIVIDE(
            LEAST(winner_count, loser_count), 
            GREATEST(winner_count, loser_count)
        ) < 0.9 THEN 'WARN'
        ELSE 'PASS'
    END AS status,
    CONCAT(
        'winner=', CAST(winner_count AS STRING),
        ', loser=', CAST(loser_count AS STRING)
    ) AS detail

FROM (
    SELECT
        COUNTIF(player_role='winner') AS winner_count,
        COUNTIF(player_role='loser') AS loser_count
    FROM tennis_raw.fact_match_stats
)

UNION ALL

-- check no NULL foreign keys
SELECT
    'fact_matches',
    'null_foreign_keys',
    CASE WHEN null_winners + null_losers > 0 THEN 'FAIL' ELSE 'PASS' END,
    CONCAT(
        'null winner_id=', CAST(null_winners AS STRING),
        ', null loser_id=', CAST(null_losers AS STRING)
    )
FROM (
    SELECT
        COUNTIF(winner_id IS NULL) AS null_winners,
        COUNTIF(loser_id IS NULL) AS null_losers
    FROM tennis_raw.fact_matches
)

UNION ALL

-- check no duplicate players
SELECT
    'dim_players'       AS table_name,
    'duplicate_ids'     AS check_name,
    CASE WHEN dupe_count > 0 THEN 'FAIL'
    ELSE 'PASS'
    END AS status,
    CONCAT('duplicate_player_ids=', CAST(dupe_count AS STRING)) AS detail

FROM (
    SELECT COUNT(*) - COUNT(DISTINCT player_id) AS dupe_count
    FROM tennis_raw.dim_players
)

UNION ALL

-- check fact_rankings should have no (ranking_date, player_id) duplicates
SELECT
    'fact_rankings'             AS table_name,
    'duplicate_date_player'     AS check_name,
    CASE WHEN dupe_count > 0 THEN 'WARN' ELSE 'PASS' END AS status,
    CONCAT('duplicate (date, player) pairs=', CAST(dupe_count AS STRING)) AS detail

FROM (
    SELECT COUNT(*) - COUNT(
        DISTINCT CONCAT(CAST(ranking_date AS STRING), '_', CAST(player_id AS STRING))
    ) AS dupe_count
    FROM tennis_raw.fact_rankings
)

UNION ALL

-- check missing scores
SELECT
    'fact_matches',
    'missing_scores',
    CASE 
        WHEN missing > 0 AND SAFE_DIVIDE(missing, total) > 0.05  
        THEN 'WARN'
        ELSE 'PASS'
    END,
    CONCAT(CAST(missing AS STRING), ' of ', CAST(total AS STRING), ' matches missing scores')
FROM (
    SELECT
        COUNTIF(score IS NULL) AS missing,
        COUNT(*) AS total
    FROM tennis_raw.fact_matches
)

UNION ALL

-- check missing names
SELECT
    'dim_players',
    'missing_names',
    CASE 
        WHEN SAFE_DIVIDE(missing_names, total) > 0.05 THEN 'WARN'  
        ELSE 'PASS'
    END,
    CONCAT(
        CAST(missing_names AS STRING), ' of ', CAST(total AS STRING),
        ' players missing full_name (',
        CAST(ROUND(SAFE_DIVIDE(missing_names, total) *100, 1) AS STRING), '%)'
    )
FROM (
    SELECT
        COUNTIF(full_name IS NULL OR full_name = '') AS missing_names,
        COUNT(*) AS total
    FROM tennis_raw.dim_players
);


-- show view for load history.
-- shows the number of rows loaded per job, using the loaded_at column
CREATE OR REPLACE VIEW tennis_raw.vw_load_history AS

SELECT 'dim_players' AS table_name, loaded_at, COUNT(*) AS rows_loaded
FROM tennis_raw.dim_players
GROUP BY loaded_at

UNION ALL

SELECT 'dim_tournaments', loaded_at, COUNT(*)
FROM tennis_raw.dim_tournaments
GROUP BY loaded_at

UNION ALL

SELECT 'fact_matches', loaded_at, COUNT(*)
FROM tennis_raw.fact_matches
GROUP BY loaded_at

UNION ALL

SELECT 'fact_match_stats', loaded_at, COUNT(*)
FROM tennis_raw.fact_match_stats
GROUP BY loaded_at

UNION ALL

SELECT 'fact_rankings', loaded_at, COUNT(*)
FROM tennis_raw.fact_rankings
GROUP BY loaded_at


ORDER BY loaded_at DESC;