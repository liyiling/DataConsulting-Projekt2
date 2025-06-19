import uri
from pymongo import MongoClient
from datetime import datetime, timedelta
import pprint

from pymongo.server_api import ServerApi

# 连接到 MongoDB
uri = "mongodb://root:password@127.0.0.1:27017/sensors_db?authSource=admin"
client = MongoClient(uri, server_api=ServerApi('1'))
db = client['sensors_db']
collection = db['measurements']

# 定义日期范围
start_date = datetime(2020, 3, 1)
end_date = datetime(2020, 4, 1)  # 结束日期为4月1日（不包括）

# 创建聚合管道
pipeline = [
    # 步骤1: 过滤时间范围
    {
        "$match": {
            "timestamp": {
                "$gte": start_date,
                "$lt": end_date
            }
        }
    },

    # 步骤2: 创建日期分组键 (按天)
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

    # 步骤3: 分组统计
    {
        "$group": {
            "_id": {
                "day": "$date_group",
                "data_source": "$data_source",
                "address": "$address"
            },
            "count": {"$sum": 1},
            "avg_value": {"$avg": "$value"},
            "values": {"$push": "$value"}  # 收集值用于百分位数计算
        }
    },

    # 步骤4: 计算百分位数
    {
        "$project": {
            "day": "$_id.day",
            "data_source": "$_id.data_source",
            "address": "$_id.address",
            "count": 1,
            "avg_value": 1,
            "median": {
                "$let": {
                    "vars": {
                        "sorted": {"$sortArray": {"input": "$values", "sortBy": 1}},
                        "count": "$count",
                        "mid": {"$floor": {"$multiply": ["$count", 0.5]}},
                        "next": {"$ceil": {"$multiply": ["$count", 0.5]}}
                    },
                    "in": {
                        "$avg": [
                            {"$arrayElemAt": ["$$sorted", "$$mid"]},
                            {"$arrayElemAt": ["$$sorted", "$$next"]}
                        ]
                    }
                }
            },
            "first_quartile": {
                "$let": {
                    "vars": {
                        "sorted": {"$sortArray": {"input": "$values", "sortBy": 1}},
                        "count": "$count",
                        "pos": {"$floor": {"$multiply": ["$count", 0.25]}},
                        "next": {"$ceil": {"$multiply": ["$count", 0.25]}}
                    },
                    "in": {
                        "$avg": [
                            {"$arrayElemAt": ["$$sorted", "$$pos"]},
                            {"$arrayElemAt": ["$$sorted", "$$next"]}
                        ]
                    }
                }
            }
        }
    },

    # 步骤5: 排序结果
    {
        "$sort": {
            "day": 1,
            "data_source": 1,
            "address": 1
        }
    }
]

# 执行查询
results = collection.aggregate(pipeline)

# 输出结果
print("Tägliche Statistiken für März 2020:")
for doc in results:
    pprint.pprint(doc)
