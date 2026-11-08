# Databricks notebook source
# Imports und Konfiguration

from pyspark.sql import functions as F

# Tabellenbezeichnungen

SILVER_TABLE = "nba.default.silver_spiele"
GOLD_TABLE = "nba.default.gold_teamstatistik"


# COMMAND ----------

# Daten einlesen

df_silver = spark.read.table(SILVER_TABLE)


# COMMAND ----------

# Daten transformieren

df_heim = (
    df_silver
    .select(
        F.col("Heimteam").alias("Team"),
        F.col("Heimpunkte").alias("Punkte")
    )
)

df_gast = (
    df_silver
    .select(
        F.col("Gastteam").alias("Team"),
        F.col("Gastpunkte").alias("Punkte")
    )
)


df_gold = (
    df_heim
    .unionByName(df_gast)
    .groupBy("Team")
    .agg(
        F.count("Team").alias("Gesamtspiele"),
        F.sum("Punkte").alias("Gesamtpunkte"),
        F.avg("Punkte").alias("Durchschnittspunkte")
    )
)


# COMMAND ----------

# Daten prüfen

# 1. Prüfen, ob Teamstatistiken vorhanden sind
anzahl_teams = df_gold.count()

if anzahl_teams == 0:
    raise ValueError(
        "Gold-Validierung fehlgeschlagen: "
        "Keine Teamstatistiken vorhanden."
    )

# 2. Fehlende Teamnamen prüfen
anzahl_fehlende_teams = (
    df_gold
    .filter(F.col("Team").isNull())
    .count()
)

if anzahl_fehlende_teams > 0:
    raise ValueError(
        "Gold-Validierung fehlgeschlagen: "
        f"{anzahl_fehlende_teams} fehlende Teamnamen."
    )

# 3. Teams ohne Spiele prüfen
anzahl_teams_ohne_spiele = (
    df_gold
    .filter(F.col("Gesamtspiele") <= 0)
    .count()
)

if anzahl_teams_ohne_spiele > 0:
    raise ValueError(
        "Gold-Validierung fehlgeschlagen: "
        f"{anzahl_teams_ohne_spiele} Teams ohne Spiele."
    )


anzahl_silver_spiele = df_silver.count()
anzahl_gold_spiele = df_gold.agg(F.sum("Gesamtspiele")).first()[0]

if anzahl_gold_spiele != 2 * anzahl_silver_spiele:
    raise ValueError(
        "Gold-Validierung fehlgeschlagen: "
        f"Erwartet: {2 * anzahl_silver_spiele} Teamergebnisse in Gold, "
        f"Tatsächlich: {anzahl_gold_spiele}.")

print(
    f"Gold-Validierung erfolgreich: "
    f"{anzahl_teams} Teams, "
    f"{anzahl_fehlende_teams} fehlende Teamnamen, "
    f"{anzahl_teams_ohne_spiele} Teams ohne Spiele, "
    f"{anzahl_silver_spiele} Spiele in Silver, "
    f"{anzahl_gold_spiele} Teamergebnisse in Gold."
)


# COMMAND ----------

# Daten speichern

df_gold.writeTo(
    GOLD_TABLE
).createOrReplace()

df_kontrolle = spark.read.table(GOLD_TABLE)

anzahl_gespeichert = df_kontrolle.count()

if anzahl_gespeichert != anzahl_teams:
    raise ValueError(
        f"Gold-Speicherprüfung fehlgeschlagen: "
        f"Erwartet {anzahl_teams} Teams, "
        f"gespeichert {anzahl_gespeichert}."
    )

print(f"Gold erfolgreich gespeichert: {anzahl_gespeichert} Teams.")