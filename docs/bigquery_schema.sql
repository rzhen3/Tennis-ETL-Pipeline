-- create schema --

CREATE SCHEMA IF NOT EXISTS tennis_raw
OPTIONS(
    location='US',
    description='Tennis data silver layer - cleaned and structured'
);


-- =================
-- dimension tables
-- =================

CREATE TABLE IF NOT EXISTS tennis_raw.dim_players (
    -- primary KEY
    player_id INT64 NOT NULL OPTIONS(description = 'Unique player identifier from ATP'),

    -- player identity info
    name_first STRING OPTIONS(description='first name'),
    name_last STRING OPTIONS(description='last name'),
    full_name STRING OPTIONS(description='computed: first + last name'),

    -- player attributes
    hand STRING OPTIONS(description = 'handedness: R=right, L=left, A=ambidextrous, U=unknown'),
    dob DATE OPTIONS(description='date of birth'),
    country_code STRING OPTIONS(description='IOC 3-letter country code'),
    height_cm INT64 OPTIONS(description='height in cm'),

    source_file STRING OPTIONS(description='source CSV filename'),
    loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP() OPTIONS(description='timestamp of initial load'),
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP() OPTIONS(description'timestamp of latest update')
)
CLUSTER BY player_id
OPTIONS(description='player dimension - SCD type 1');

CREATE TABLE IF NOT EXISTS tennis_raw.dim_tournaments(
    -- primary key
    tourney_id STRING NOT NULL OPTIONS(description='unique tournament identifier (YYYY-NNNN)'),

    -- tournament attributes
    tourney_name STRING NOT NULL OPTIONS(description='tournament name'),
    tourney_level STRING OPTIONS(description='tournament level: G=grand slam, M=masters, A=other ATP Tour events, C=challengers, F=tour finals, D=davis cup'),
    typical_surface STRING OPTIONS(description='most common surface: hard, clay, grass, carpet'),
    typical_draw_size INT64 OPTIONS(description='most common draw size'),

    -- metadata (for later)
    location STRING OPTIONS(description='tournament location/city'),
    country STRING OPTIONS(description='tournament country'),

    -- audit columns
    first_seen_date DATE OPTIONS(description='first date of tournament appearance'),
    last_seen_date DATE OPTIONS(description='most recent date of tournament appearance'),
    loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP() OPTIONS(description='timestamp of initial load'),
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP() OPTIONS(description='timestamp of latest update')
)
CLUSTER BY tourney_id
OPTIONS(
    description='tournament master dimension - built from match data'
);

-- =================
-- fact tables
-- =================
CREATE TABLE IF NOT EXISTS tennis_raw.fact_matches (
    -- composite keys
    tourney_id STRING NOT NULL OPTIONS(description='foreign key in dim_tournaments'),
    match_num INT64 NOT NULL OPTIONS(description='match number within tournament'),
    tourney_date DATE NOT NULL OPTIONS(description='tournament date(for partitioning)'),

    -- foreign keys
    winner_id INT64 NOT NULL OPTIONS(description='foreign key to dim_players (winner)'),
    loser_id INT64 NOT NULL OPTIONS(description='foreign key to dim_players (loser)'),
    
    -- match context
    surface STRING OPTIONS(description='surface type: hard, clay, grass, carpet'),
    draw_size INT64 OPTIONS(description='tournament draw size'),
    tourney_level STRING OPTIONS(description='tournament level: G, M, A, F, D, etc'),
    match_round STRING OPTIONS(description='match round: F, SF, QF, R16, R32, etc'),
    best_of STRING OPTIONS(description='best of N sets(BO3, BO5)'),

    -- match results
    score STRING OPTIONS(description='final score'),
    match_minutes INT64 OPTIONS(description='match duration in minutes'),

    -- winner metadata (at time of match)
    winner_seed INT64 OPTIONS(description='winners tournament seed'),
    winner_entry STRING OPTIONS(description='winners entry type: WildCard, Qualifier, LL, etc.'),
    winner_rank INT64 OPTIONS(description='winners ATP ranking at time of match'),
    winner_rank_points INT64 OPTIONS(description='winners ranking points'),
    winner_age INT64 OPTIONS(description='winners age at time of match'),
    winner_ht INT64 OPTIONS(description='winners height in cm'),
    winner_hand INT64 options(description='winner handedness'),
    winner_ioc STRING OPTIONS(description='winner country code'),

    -- loser metadata (at time of match)
    loser_seed INT64 OPTIONS(description='losers tournament seed'),
    loser_entry STRING OPTIONS(description='losers entry type'),
    loser_rank INT64 OPTIONS(description='losers ATP ranking at time of match'),
    loser_rank_points INT64 OPTIONS(description='losers ranking points'),
    loser_age FLOAT64 OPTIONS(description='losers age at time of match'),
    loser_ht INT64 OPTIONS(description='losers height in cm'),
    loser_hand STRING OPTIONS(description='losers handedness'),
    loser_ioc STRING OPTIONS(description='losers country code'),

    -- audit columns
    source_file STRING OPTIONS(description='source CSV filename (e.g. atp_matches_2024)'),
    loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP() OPTIONS(description='timestamp of data load')
)
PARTITION BY tourney_date
CLUSTER BY winner_id, loser_id
OPTIONS(
    description='match results fact table - one row per match'
);

CREATE TABLE IF NOT EXISTS tennis_raw.fact_match_stats(
    -- foreign keys (composite key back to fact_matches)
    tourney_id STRING NOT NULL OPTIONS(description='links to fact_matches.tourney_id'),
    match_num STRING NOT NULL OPTIONS(description='links to fact_matches.match_num'),
    tourney_date DATE NOT NULL OPTIONS(description='links to fact_matches.tourney_date'),
    player_id INT64 NOT NULL OPTIONS(description='foreign key to dim_players'),
    player_role STRING NOT NULL OPTIONS(description='winner/loser'),

    -- serve stats
    aces INT64 OPTIONS(description='number of aces'),
    double_faults INT64 OPTIONS(description='number of double faults'),
    service_points INT64 OPTIONS(description='total service points played'),
    first_serves_in INT64 OPTIONS(description='first serves in'),
    first_serves_won INT64 OPTIONS(description='first serve points won'),
    second_serves_won INT64 OPTIONS(description='second serve points won'),
    service_games INT64 OPTIONS(description='service games played'),

    -- break point stats
    break_points_saved INT64 OPTIONS(description='break points saved'),
    break_points_faced INT64 OPTIONS(description='break points faced'),

    -- audit columns
    loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP() OPTIONS(description='timestamp of initial load')
)
PARTITION BY tourney_date
CLUSTER BY player_id
OPTIONS(
    description='detailed match stats - two rows per match (for winner and loser)'
);

CREATE TABLE IF NOT EXISTS tennis_raw.fact_rankings(
    -- composite keys
    ranking_date DATE NOT NULL OPTIONS(description='monday of ranking week'),
    player_id INT64 NOT NULL OPTIONS(description='foreign key to dim_players'),

    -- ranking metrics
    rank INT64 NOT NULL OPTIONS(description='ATP ranking position (1-based)'),
    points INT64 NOT NULL OPTIONS(description='ATP ranking points'),

    -- duplicate handling
    ranking_sequence INT64 DEFAULT 1 OPTIONS(description='Sequence number for rare duplicates'),

    -- audit columns
    source_file STRING OPTIONS(description='source CSV filename'),
    loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP() OPTIONS(description='timestamp of initial load')
)
PARTITION BY ranking_date
CLUSTER BY player_id
OPTIONS(
    description='weekly ATP ranking snapshots - complete rankings per update week'
);

-- ========================
-- views (for convenience)
-- ========================

-- CREATE OR REPLACE VIEW tennis_raw.vw_player_matches AS
-- SELECT
--     m.tourney_date,
--     m.tourney_id,
--     t.tourney_name,
--     m.surface,
--     m.round,

--     -- winner details
--     m.winner_id,
--     pw.full_name AS winner_name,
--     pw.country_code AS winner_country,
--     m.winner_rank,

--     -- loser details
--     m.loser_id,
--     pl.full_name AS loser_name,
--     pl.country_code AS loser_country,
--     m.loser_rank,

--     -- match details
--     m.score,
--     m.minutes,
--     m.best_of

-- FROM tennis_raw.fact_matches m
-- LEFT JOIN tennis_raw.dim_players pw ON m.winner_id = pw.player_id
-- LEFT JOIN tennis_raw.dim_players pl ON m.loser_id = pl.player_id
-- LEFT JOIN tennis_raw.dim_tournaments t ON m.toruney_id = t.tourney_id;