# NBA Data Engineering – Spark Declarative Pipelines

## Project Overview

This project extends my existing NBA Data Engineering project with a SQL-based implementation using **Databricks Lakeflow Spark Declarative Pipelines (SDP)**.

The objective is to demonstrate declarative ETL development, incremental file ingestion, data quality validation, and analytical transformations using the Medallion Architecture.

The original PySpark pipeline remains available as a separate implementation for comparison.

## Architecture

**NBA JSON Files → Bronze Streaming Table → Silver Streaming Table → Gold Materialized View**

| Layer | Technology | Purpose |
|---|---|---|
| Bronze | Streaming Table / Auto Loader | Incrementally ingest NBA JSON files from a Unity Catalog Volume |
| Silver | Streaming Table / Data Quality Expectations | Validate records, extract team names, and calculate total points |
| Gold | Materialized View | Generate scoring statistics grouped by home team |

Spark Declarative Pipelines automatically derives dependencies between datasets from their SQL definitions.

## Bronze Layer – Data Ingestion

The Bronze layer ingests NBA game data from JSON files stored in a Unity Catalog Volume.

**Key features:**
- Incremental file ingestion with Auto Loader (`read_files`)
- Support for multiline JSON
- Preservation of nested source fields
- Streaming Table managed by Databricks

**Validated result: 125 records ingested.**

## Silver Layer – Data Quality and Transformation

The Silver layer transforms the Bronze data into a structured dataset.

Transformations include:
- Extracting home and visitor team names from nested JSON structures
- Calculating combined game points (`total_points`)
- Applying declarative Data Quality Expectations

| Expectation | Validation rule | Action |
|---|---|---|
| `valid_game_id` | Game ID must not be NULL | FAIL UPDATE |
| `valid_team_names` | Both team names must be non-null and non-empty | DROP ROW |
| `valid_scores` | Both team scores must be greater than zero | DROP ROW |

**Validated results:**
- Bronze records: 125
- Silver records: 59
- Records excluded from Silver: 66
- Unique Silver game IDs: 59

The Bronze data quality analysis identified 66 records containing missing or non-positive scores.

## Gold Layer – Analytical Aggregations

The Gold layer uses a Materialized View to aggregate game statistics by home team.

The following metrics are calculated:

| Metric | Description |
|---|---|
| `total_games` | Number of games per home team |
| `avg_total_points` | Average combined game points |
| `max_total_points` | Maximum combined game points |
| `high_scoring_games` | Number of games with at least 150 combined points |

The high-scoring threshold is a configurable analytical definition rather than an official NBA classification.

## Data Validation

A separate validation notebook contains SQL checks covering:

- Record counts across Bronze, Silver, and Gold
- Game ID uniqueness
- Missing or non-positive source scores
- Low-scoring records requiring further investigation
- Gold aggregation results
- Consistency of the derived `total_points` column

The validation process confirmed 59 unique game IDs in the Silver dataset.

### Known Data Limitations

The observed Silver data contains combined scores ranging from 13 to 187 points.

These values suggest that some source records may contain incomplete or otherwise unexpected scoring information, even when their status is marked as final.

The current pipeline applies structural and numerical data quality checks. It does not guarantee that every retained record represents a complete NBA final score.

Further source-level validation would be required for authoritative sports analysis.

## Comparison: PySpark vs. Spark Declarative Pipelines

| Original PySpark Pipeline | Spark Declarative Pipeline |
|---|---|
| PySpark notebooks | Declarative SQL definitions |
| Workflow orchestration with Lakeflow Jobs | Automatic dataset dependency management |
| Custom Python data quality checks | SQL Data Quality Expectations |
| Explicit Delta Lake writes | Streaming Tables and Materialized Views |
| Manual transformation logic | Declarative pipeline definitions |

Both implementations demonstrate the Medallion Architecture while highlighting different development and orchestration approaches.

## Technologies

- Databricks Free Edition
- Apache Spark SQL
- Delta Lake
- Unity Catalog
- Auto Loader
- Lakeflow Spark Declarative Pipelines
- Streaming Tables
- Materialized Views
- Data Quality Expectations

## Project Files

| File | Description |
|---|---|
| `nba_pipeline.sql` | Complete Bronze-Silver-Gold SDP implementation |
| `NBA_SDP_Validation.ipynb` | SQL-based data quality and pipeline validation |

## Future Improvements

- Investigate incomplete NBA game scores at the source
- Implement change data capture for corrected game records
- Introduce a quarantine dataset for invalid records
- Add automated data quality monitoring

---

**Project focus:** Practical application of Databricks Lakeflow Spark Declarative Pipelines, SQL transformations, data quality validation, and the Medallion Architecture.
