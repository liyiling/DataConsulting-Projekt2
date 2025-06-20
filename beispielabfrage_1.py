'''
-- ACTUAL QUERIES TO TEST AT MongoDB
SELECT DATA_SOURCE, ADDRESS,
        COUNT(*), AVG(VALUE),
        PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY VALUE) AS MEDIAN,
        PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY VALUE) AS FIRST_QUARTILE
FROM MEASUREMENTS
WHERE TIMESTAMP BETWEEN '2020-03-01 00:00:00' AND '2020-03-31 00:00'
GROUP BY DATA_SOURCE, ADDRESS;

Authorin： Yiling LI (108980) von Gruppe4
'''


from datetime import datetime
from pymongo import MongoClient
import pprint
from pymongo.server_api import ServerApi

# Verbindung MongoDB
uri = "mongodb://root:password@localhost:27017/sensors_db?authSource=admin"
client = MongoClient(uri, server_api=ServerApi('1'))
db = client["sensors_db"]
collection = db['measurements']   # Collection Name

# Definition des Zeitraums (März 2020)
start_date = datetime(2020, 3, 1, 0, 0, 0)
end_date = datetime(2020, 3, 31, 0, 0, 0)

# Erstellung der Aggregationspipeline
pipeline =  [
    # 1. Zeitliche Eingrenzung:
    # Nur Dokumente innerhalb des gewünschten Zeitraums (März 2020) werden berücksichtigt.
    {
        "$match": {
            "timestamp": {
                "$gte": start_date,  #  datetime(2020, 3, 1, 0, 0, 0)
                "$lte": end_date     #  datetime(2020, 3, 31, 0, 0, 0)
            }
        }
    },

    # 2. Gruppierung und Berechnung von Basisstatistiken:
    # Gruppiere die Dokumente nach data_source und address.
    # Berechne für jede Gruppe:
    # - die Anzahl der Dokumente (count),
    # - den Durchschnittswert (avg_value),
    # - und sammle alle Werte in einer Liste (values) zur späteren Perzentilberechnung.
    {
        "$group": {
            "_id": {
                "data_source": "$data_source",
                "address": "$address"
            },
            "count": { "$sum": 1 },
            "avg_value": { "$avg": "$value" },
            "values": { "$push": "$value" }
        }
    },

    # 3. Berechnung von Median (50%) und erstem Quartil (25%):
    # Im $project-Abschnitt:
    # - Sortiere die Werte pro Gruppe mit $sortArray.
    # - Berechne Median und erstes Quartil über Positionen in der sortierten Liste.
    {
        "$project": {
            "data_source": "$_id.data_source",
            "address": "$_id.address",
            "count": 1,
            "avg_value": 1,

            # Sortiere die gesammelten Werte in aufsteigender Reihenfolge
            "sorted": { "$sortArray": { "input": "$values", "sortBy": 1 } },

            # Median (50 Perzentil)
            "median": {
                "$let": { # Lokale Variablen für die Positionsberechnung definieren
                    "vars": {
                        # Bei ungerader Anzahl: beide Positionen zeigen auf denselben Wert

                        # Berechnung des Index für die niedrigere Mitte (bei ungerader Anzahl identisch zur höheren Mitte)
                        # Beispiel: Wenn count = 5 → midLow = floor(5/2) = 2
                        "midLow": { "$floor": { "$divide": ["$count", 2] } },

                        # Berechnung des Index für die höhere Mitte
                        # Beispiel: count = 6 → ceil(6 / 2) = 3, aber Index beginnt bei 0 → tatsächlicher Index = 2
                        # Wir wollen also Wert an Position 2 (midLow) und 3 (midHigh) mitteln
                        "midHigh": { "$subtract": [
                            { "$ceil": { "$divide": ["$count", 2] } }, 1
                        ]}
                    },
                    "in": {
                        # Der Median ist der Durchschnitt der beiden mittleren Werte
                        # Bei ungerader Anzahl sind beide Positionen identisch → exakt der mittlere Wert
                        # Bei gerader Anzahl → Mittelwert der zwei mittleren Werte
                        "$avg": [
                            { "$arrayElemAt": ["$sorted", "$$midLow"] },
                            { "$arrayElemAt": ["$sorted", "$$midHigh"] }
                        ]
                    }
                }
            },

            # Erstes Quartil (25 Perzentil)
            "first_quartile": {
                "$let": { # Lokale Variablen für die Positionsberechnung definieren
                    "vars": {
                        # Index des unteren Quartilswerts berechnen
                        # floor(count * 0.25) rundet ab, um die "untere" Position zu bestimmen
                        "qLow": { "$floor": { "$multiply": ["$count", 0.25] } },

                        # Index des oberen Quartilswerts berechnen
                        # ceil(count * 0.25) rundet auf, um die "obere" Position zu bestimmen
                        "qHigh": { "$ceil": { "$multiply": ["$count", 0.25] } }
                    },
                    "in": {
                        # Durchschnitt der Werte an qLow- und qHigh-Position
                        # Bei exaktem Quartilwert (wenn qLow = qHigh) wird nur dieser Wert genommen,
                        # ansonsten wird der Durchschnitt der beiden benachbarten Werte berechnet
                        "$avg": [
                            { "$arrayElemAt": ["$sorted", "$$qLow"] },
                            { "$arrayElemAt": ["$sorted", "$$qHigh"] }
                        ]
                    }
                }
            }
        }
    },

    # 4. Optionale Sortierung des Endergebnisses:
    # Sortiere die Ausgabe nach Datenquelle und Adresse.
    {
        "$sort": {
            "data_source": 1,
            "address": 1
        }
    }
]

# durchführen abfragen
results = collection.aggregate(pipeline)

# print Ergebnisse und
pp = pprint.PrettyPrinter(indent=2)
print(f"Ergbnisse (2020-03-01 bis 2020-03-31):")

num=0

for doc in results:
    num+=1
    pp.pprint(doc)

print(num)
