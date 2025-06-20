'''
-- ACTUAL QUERIES TO TEST AT MongoDB
SELECT  DAY_INTERVAL, DATA_SOURCE, ADDRESS,
        COUNT(*), AVG(VALUE),
        PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY VALUE) AS MEDIAN,
        PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY VALUE) AS FIRST_QUARTILE
FROM GENERATE_SERIES('2020-03-01 00:00:00', '2020-03-31 00:00', INTERVAL '1 DAY') DAY_INTERVAL
INNER JOIN MEASUREMENTS M ON M.TIMESTAMP > DAY_INTERVAL AND M.TIMESTAMP <= DAY_INTERVAL + INTERVAL '1 DAY'
GROUP BY DAY_INTERVAL, DATA_SOURCE, ADDRESS;

Authorin： Yiling LI (108980) von Gruppe4
'''


import uri
from pymongo import MongoClient
from datetime import datetime, timedelta
import pprint
from pymongo.server_api import ServerApi

# Verbindung zu MongoDB herstellen
uri = "mongodb://root:password@127.0.0.1:27017/sensors_db?authSource=admin"
client = MongoClient(uri, server_api=ServerApi('1'))
db = client['sensors_db']
collection = db['measurements']

# Definieren des Zeitraums: 1. März 2020 bis (exklusiv) 1. April 2020
start_date = datetime(2020, 3, 1)
end_date = datetime(2020, 4, 1)  # Enddatum exklusiv, entspricht SQL-Bereich [start_date, end_date)

# Aggregations-Pipeline erstellen
pipeline = [
    # 1. Zeitliche Eingrenzung: Filtere Dokumente, bei denen "timestamp" im gewünschten Zeitraum liegt
    {
        "$match": {
            "timestamp": {
                "$gte": start_date,  # größer gleich Startdatum
                "$lt": end_date      # kleiner als Enddatum
            }
        }
    },

    # 2. Datum auf Tagesniveau runden: Erzeuge neues Feld "date_group" mit abgerundetem Tag (entspricht SQL DATE_TRUNC('day', timestamp))
    {
        "$addFields": {
            "date_group": {
                "$dateTrunc": {
                    "date": "$timestamp",
                    "unit": "day"
                }
            }
        }
    },

    # 3. Gruppierung nach Tag, Datenquelle und Adresse
    {
        "$group": {
            "_id": {
                "day": "$date_group",
                "data_source": "$data_source",
                "address": "$address"
            },
            "count": {"$sum": 1},            # Anzahl der Dokumente in der Gruppe zählen
            "avg_value": {"$avg": "$value"}, # Durchschnitt des Feldes "value" berechnen
            "values": {"$push": "$value"}    # Alle "value"-Werte in einem Array sammeln für spätere Perzentilberechnung
        }
    },

    # 4. Berechnung von Median und erstem Quartil
    {
        "$project": {
            "day": "$_id.day",
            "data_source": "$_id.data_source",
            "address": "$_id.address",
            "count": 1,
            "avg_value": 1,

            # Medianberechnung: Sortiere Werte und berechne mittlere Position(en)
            "median": {
                "$let": {
                    "vars": {
                        "sorted": {"$sortArray": {"input": "$values", "sortBy": 1}},   # Sortiere Werte aufsteigend
                        "n": "$count",                                                # Anzahl der Werte
                        "is_even": {"$eq": [{"$mod": ["$count", 2]}, 0]},             # Prüfe, ob Anzahl gerade ist
                        "mid_index": {"$floor": {"$divide": ["$count", 2]}},          # Mittlerer Index (unten gerundet)
                        "prev_index": {"$subtract": [{"$floor": {"$divide": ["$count", 2]}}, 1]}  # Vorheriger Index für gerade Anzahl
                    },
                    "in": {
                        "$cond": {
                            "if": "$$is_even",                                        # Wenn gerade Anzahl Werte
                            "then": {
                                "$avg": [                                            # Durchschnitt der zwei mittleren Werte
                                    {"$arrayElemAt": ["$$sorted", "$$prev_index"]},
                                    {"$arrayElemAt": ["$$sorted", "$$mid_index"]}
                                ]
                            },
                            "else": {
                                "$arrayElemAt": ["$$sorted", "$$mid_index"]           # Bei ungerader Anzahl: mittlerer Wert
                            }
                        }
                    }
                }
            },

            # Berechnung des ersten Quartils (25% Perzentil)
            "first_quartile": {
                "$let": {
                    "vars": {
                        "sorted": {"$sortArray": {"input": "$values", "sortBy": 1}},  # Werte sortieren
                        "n": "$count",
                        "q_index": {"$multiply": ["$count", 0.25]},                   # Position des 25%-Quantils
                        "floor_index": {"$floor": {"$multiply": ["$count", 0.25]}},  # Abrunden des Index
                        "ceil_index": {"$ceil": {"$multiply": ["$count", 0.25]}},    # Aufrunden des Index
                        "is_integer": {                                               # Prüfen, ob q_index ganzzahlig ist
                            "$eq": [
                                {"$mod": [{"$multiply": ["$count", 0.25]}, 1]},
                                0
                            ]
                        }
                    },
                    "in": {
                        "$cond": {
                            "if": "$$is_integer",                                    # Wenn Index ganzzahlig
                            "then": {
                                "$arrayElemAt": ["$$sorted", {"$toInt": "$$q_index"}]  # Direkt Wert an Index holen
                            },
                            "else": {
                                "$avg": [                                            # Sonst Mittelwert der Nachbarwerte
                                    {"$arrayElemAt": ["$$sorted", "$$floor_index"]},
                                    {"$arrayElemAt": ["$$sorted", "$$ceil_index"]}
                                ]
                            }
                        }
                    }
                }
            }
        }
    },

    # 5. Sortierung der Ergebnisse nach Tag, Datenquelle und Adresse für bessere Übersichtlichkeit
    {
        "$sort": {
            "day": 1,
            "data_source": 1,
            "address": 1
        }
    }
]

# Aggregation ausführen
results = collection.aggregate(pipeline)

# Ausgabe der Ergebnisse
print("Tägliche Statistiken für März 2020:")
num = 0
for doc in results:
    num += 1
    pprint.pprint(doc)

print("Insgesamt Ergebnisse:", num)
