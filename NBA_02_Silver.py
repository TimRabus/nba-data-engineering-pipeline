# Databricks notebook source
# Imports und Konfiguration

from pyspark.sql import functions as F

# Tabellenbezeichnungen zentral definieren
BRONZE_TABLE = "nba.default.bronze_spiele"
SILVER_TABLE = "nba.default.silver_spiele"


# COMMAND ----------

# Bronze-Daten einlesen
df_bronze = spark.read.table(BRONZE_TABLE)


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


# COMMAND ----------

# Daten prüfen

anzahl_spiele = df_silver.count()

if anzahl_spiele == 0:
    raise ValueError(
        "Datenqualitätsprüfung fehlgeschlagen: "
        "Silver enthält keine Spiele."
    )

# Fehlende Teamnamen prüfen
df_null_check = df_silver.filter(
    F.col("Heimteam").isNull() |
    F.col("Gastteam").isNull()
)

anzahl_null = df_null_check.count()

if anzahl_null > 0:
    raise ValueError(
        f"Datenqualitätsprüfung fehlgeschlagen: "
        f"{anzahl_null} Spiele mit fehlenden Teamnamen."
    )

print(
    f"Datenqualitätsprüfung erfolgreich: "
    f"{anzahl_spiele} Spiele, "
    f"{anzahl_null} fehlende Teamnamen."
)




# COMMAND ----------

# Daten speichern
 
df_silver.writeTo(
    "nba.default.silver_spiele"
).createOrReplace()

df_kontrolle = spark.read.table("nba.default.silver_spiele")

df_kontrolle.show(5, truncate=False)

print("Gespeicherte Spiele: ", df_kontrolle.count())

# COMMAND ----------

# Testdaten: absichtlich leerer DataFrame
# df_test = df_silver.limit(0)

# anzahl_test = df_test.count()

# if anzahl_test == 0:
#     raise ValueError("TEST: Keine gültigen NBA-Spiele vorhanden!")

# print("Datenprüfung erfolgreich!")