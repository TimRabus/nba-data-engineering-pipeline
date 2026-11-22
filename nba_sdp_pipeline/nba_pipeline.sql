--------------
-- Bronze
--------------


CREATE OR REFRESH STREAMING TABLE bronze_games_sdp_training
AS
SELECT *
FROM STREAM read_files(
    '/Volumes/nba/default/nba_data/',
    format => 'json',
    multiLine => true
);



--------------
-- Silver 
--------------


CREATE OR REFRESH STREAMING TABLE silver_games_sdp_training
(
    CONSTRAINT valid_game_id
        EXPECT (id IS NOT NULL)
        ON VIOLATION FAIL UPDATE,

    CONSTRAINT valid_team_names
        EXPECT (
            home_team_name IS NOT NULL
            AND home_team_name <> ''
            AND visitor_team_name IS NOT NULL
            AND visitor_team_name <> ''
        )
        ON VIOLATION DROP ROW,

    CONSTRAINT valid_scores
        EXPECT (
            home_team_score > 0
            AND visitor_team_score > 0
        )
        ON VIOLATION DROP ROW
)
AS
SELECT
    id,
    date,
    status,
    home_team.full_name AS home_team_name,
    visitor_team.full_name AS visitor_team_name,
    home_team_score,
    visitor_team_score,
    home_team_score + visitor_team_score AS total_points
FROM STREAM(bronze_games_sdp_training);



--------------
-- Gold 
--------------


CREATE OR REFRESH MATERIALIZED VIEW gold_team_scoring_sdp
AS
SELECT
    home_team_name,
    COUNT(*) AS total_games,
    ROUND(AVG(total_points), 2) AS avg_total_points,
    MAX(total_points) AS max_total_points,
    SUM(
        CASE
            WHEN total_points >= 150 THEN 1
            ELSE 0
        END
    ) AS high_scoring_games
FROM silver_games_sdp_training
GROUP BY home_team_name;
