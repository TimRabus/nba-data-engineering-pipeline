# Databricks notebook source
# Imports und Konfiguration

from pyspark.sql import functions as F
import logging 

logger = logging.getLogger("NBA_Silver")
logger.setLevel(logging.INFO)


# Tabellenbezeichnungen zentral definieren
BRONZE_TABLE = "nba.default.bronze_spiele"
SILVER_TABLE = "nba.default.silver_spiele"



# COMMAND ----------

# Bronze-Daten einlesen

logger.info("Bronze: Datenverarbeitung gestartet")

df_bronze = spark.read.table(BRONZE_TABLE)

anzahl_spiele = df_bronze.count()

logger.info(
    "Bronze: %s Datensätze eingelesen",
    anzahl_spiele
)
    

# COMMAND ----------

# Daten transformieren


df_silver = (
    df_bronze
    .filter(
        F.col("status") == "Final"
    )
    .select(
        F.col("home_team.full_name").alias("Heimteam"),
        F.col("home_team_score").alias("Heimpunkte"),
        F.col("visitor_team.full_name").alias("Gastteam"),
        F.col("visitor_team_score").alias("Gastpunkte")
    )
    .filter(
        (F.col("Heimpunkte") > 0) &
        (F.col("Gastpunkte") > 0)
    )
)

anzahl_spiele = df_silver.count()

logger.info(
        "Silver: %s Datensätze nach Transformation",
        anzahl_spiele
    )


# COMMAND ----------

# Daten prüfen

anzahl_spiele = df_silver.count()

if anzahl_spiele == 0:
    raise ValueError(
        "Datenqualitätsprüfung fehlgeschlagen: "
        "Silver enthält keine Spiele."
    )

# Fehlende Teamnamen prüfen
anzahl_null = (df_silver.filter(
    F.col("Heimteam").isNull() |
    F.col("Gastteam").isNull()
    ).count()
)


if anzahl_null > 0:
    raise ValueError(
        f"Datenqualitätsprüfung fehlgeschlagen: "
        f"{anzahl_null} Spiele mit fehlenden Teamnamen."
    )

logger.info(
    "Silver: Datenqualität erfolgreich geprüft | "
    "Spiele=%s | Fehlende_Teamnamen=%s",
    anzahl_spiele,
    anzahl_null
 )




# COMMAND ----------

# Daten speichern
 
df_silver.writeTo(
    SILVER_TABLE
).createOrReplace()

df_kontrolle = spark.read.table(SILVER_TABLE)

anzahl_gespeichert = df_kontrolle.count()

logger.info(
    "Silver erfolgreich gespeichert: %s Spiele",
    anzahl_gespeichert
)

# COMMAND ----------

# Testdaten: absichtlich leerer DataFrame
# df_test = df_silver.limit(0)

# anzahl_test = df_test.count()

# if anzahl_test == 0:
#     raise ValueError("TEST: Keine gültigen NBA-Spiele vorhanden!")

# print("Datenprüfung erfolgreich!")