# Databricks notebook source
# Import / JSON-Pfad

from pyspark.sql import functions as F
import logging

logger = logging.getLogger("NBA_Bronze")
logger.setLevel(logging.INFO)

SOURCE_PATH = "/Volumes/nba/default/nba_data/alle_spiele.json"
BRONZE_TABLE = "nba.default.bronze_spiele"
  


# COMMAND ----------

# Daten einlesen

try:
    logger.info("Bronze: Datenaufnahme gestartet")

    df_bronze = (
        spark.read
        .option("multiLine", "true")
        .json(SOURCE_PATH)
    )

    anzahl_spiele = df_bronze.count()

    logger.info(
            "Bronze: %s Datensätze eingelesen",
            anzahl_spiele
        )

except Exception as e:
    logger.error(
        "Bronze: Datenaufnahme fehlgeschlagen: %s",
        e
    )
    raise


  


# COMMAND ----------


# Daten prüfen

anzahl_spiele = df_bronze.count()

if anzahl_spiele == 0:
    raise ValueError(
        "Bronze-Validierung fehlgeschlagen: Keine Spiele vorhanden."
    )

anzahl_fehlende_ids = (
    df_bronze
    .filter(F.col("id").isNull())
    .count()
)

if anzahl_fehlende_ids > 0:
    raise ValueError(
        f"Bronze-Validierung fehlgeschlagen: "
        f"{anzahl_fehlende_ids} Spiele ohne ID."
    )

anzahl_unterschiedlicher_ids = (
    df_bronze
    .select(F.col("id"))
    .distinct()
    .count()
)

if anzahl_unterschiedlicher_ids != anzahl_spiele:
    raise ValueError(
        f"Bronze-Validierung fehlgeschlagen: "
        f"{anzahl_unterschiedlicher_ids} unterschiedliche IDs."
    )
    
anzahl_duplikate = (
    anzahl_spiele - anzahl_unterschiedlicher_ids
)

if anzahl_duplikate > 0:
    raise ValueError(
        f"Bronze-Validierung fehlgeschlagen: "
        f"{anzahl_duplikate} zusätzliche Einträge "
        f"mit doppelter Spiel-ID."
    )

# 4. Erfolgsmeldung
logger.info(
    "Bronze: Datenqualität erfolgreich geprüft | "
    "Spiele=%s | Fehlende_IDs=%s | Duplikate=%s",
    anzahl_spiele,
    anzahl_fehlende_ids,
    anzahl_duplikate
)
  

# COMMAND ----------


# Daten speichern und kontrollieren

df_bronze.writeTo(
    BRONZE_TABLE
).createOrReplace()

df_kontrolle = spark.read.table(BRONZE_TABLE)

anzahl_gespeichert = df_kontrolle.count()

if anzahl_gespeichert != anzahl_spiele:
    raise ValueError(
        f"Abweichende Datensatzanzahl: "
        f"Erwartet {anzahl_spiele}, "
        f"gespeichert {anzahl_gespeichert}."
    )

logger.info(
    "Bronze erfolgreich gespeichert: %s Spiele",
    anzahl_gespeichert
)
  