-- OPT-2 sub-item b.1: Add 16 dropped fields to skillcorner_matches bronze table.
-- ALTER TABLE ADD COLUMNS is idempotent on Databricks (existing rows get NULL).
ALTER TABLE soccer_analytics.bronze.skillcorner_matches
ADD COLUMNS (
    start_time STRING,
    end_time STRING,
    minutes_played DOUBLE,
    start_frame BIGINT,
    end_frame BIGINT,
    minutes_tip DOUBLE,
    minutes_otip DOUBLE,
    yellow_card BIGINT,
    red_card BIGINT,
    injured BOOLEAN,
    goal BIGINT,
    own_goal BIGINT,
    trackable_object BIGINT,
    birthday STRING,
    gender STRING,
    team_player_id BIGINT
);
