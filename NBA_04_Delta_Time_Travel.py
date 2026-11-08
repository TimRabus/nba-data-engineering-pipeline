# Databricks notebook source
# MAGIC %sql
# MAGIC
# MAGIC /*
# MAGIC DESCRIBE HISTORY nba.default.gold_teamstatistik
# MAGIC Limit 3
# MAGIC */
# MAGIC
# MAGIC -- Vergleich Version 9 und 8
# MAGIC SELECT *
# MAGIC FROM nba.default.gold_teamstatistik VERSION AS OF 9
# MAGIC
# MAGIC EXCEPT ALL
# MAGIC
# MAGIC SELECT *
# MAGIC FROM nba.default.gold_teamstatistik VERSION AS OF 8;
# MAGIC
# MAGIC
# MAGIC
# MAGIC -- Vergleich Version 8 und 9
# MAGIC SELECT *
# MAGIC FROM nba.default.gold_teamstatistik VERSION AS OF 8
# MAGIC
# MAGIC EXCEPT ALL
# MAGIC
# MAGIC SELECT *
# MAGIC FROM nba.default.gold_teamstatistik VERSION AS OF 9;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC /* Test Tabelle Erstellen
# MAGIC
# MAGIC CREATE OR REPLACE TABLE nba.default.test_time_travel
# MAGIC USING DELTA
# MAGIC AS
# MAGIC SELECT 'Lakers' AS Team, 100 AS Punkte
# MAGIC UNION ALL
# MAGIC SELECT 'Celtics', 95;
# MAGIC */
# MAGIC
# MAGIC -- SELECT * FROM nba.default.test_time_travel
# MAGIC
# MAGIC -- Version History von Tabelle
# MAGIC DESCRIBE HISTORY nba.default.test_time_travel;
# MAGIC
# MAGIC /* Datenänderung
# MAGIC UPDATE nba.default.test_time_travel
# MAGIC SET Punkte = 110
# MAGIC WHERE Team = 'Lakers';
# MAGIC */
# MAGIC
# MAGIC WITH 
# MAGIC version_2 AS (
# MAGIC     SELECT * 
# MAGIC     FROM nba.default.test_time_travel VERSION AS OF 2
# MAGIC ),
# MAGIC version_1 AS (
# MAGIC     SELECT * 
# MAGIC     FROM nba.default.test_time_travel VERSION AS OF 1
# MAGIC )
# MAGIC /*
# MAGIC SELECT * FROM version_2 
# MAGIC EXCEPT All 
# MAGIC SELECT * FROM version_1
# MAGIC */
# MAGIC
# MAGIC SELECT * FROM version_1
# MAGIC EXCEPT All 
# MAGIC SELECT * FROM version_2
# MAGIC
# MAGIC -- SELECT * FROM nba.default.test_time_travel 

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC /*
# MAGIC RESTORE TABLE nba.default.test_time_travel
# MAGIC TO VERSION AS OF 1;
# MAGIC
# MAGIC SELECT * FROM nba.default.test_time_travel;
# MAGIC */
# MAGIC
# MAGIC DESCRIBE HISTORY nba.default.test_Time_travel;