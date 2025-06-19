-- ACTUAL QUERIES TO TEST AT OTHER DATABASE
SELECT DATA_SOURCE, ADDRESS, 
        COUNT(*), AVG(VALUE),
        PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY VALUE) AS MEDIAN,
        PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY VALUE) AS FIRST_QUARTILE
FROM MEASUREMENTS
WHERE TIMESTAMP BETWEEN '2020-03-01 00:00:00' AND '2020-03-31 00:00'
GROUP BY DATA_SOURCE, ADDRESS;


from datetime import datetime
from pymongo import MongoClient
import pprint
from pymongo.server_api import ServerApi

# 连接到本地 MongoDB
uri = "mongodb://root:password@localhost:27017/sensors_db?authSource=admin"
client = MongoClient(uri, server_api=ServerApi('1'))
db = client["sensors_db"]
collection = db['measurements']   # 集合名

# 定义时间范围 (2020年3月)
start_date = datetime(2020, 3, 1, 0, 0, 0)
end_date = datetime(2020, 3, 31, 0, 0, 0)

# 创建聚合管道查询
pipeline = [
    # 步骤1: 时间范围过滤=where
    {
        "$match": {
            "timestamp": {
                "$gte": start_date,
                "$lte": end_date
            }
        }
    },

    # 步骤2: 分组统计=Group by
    {
        "$group": {
            "_id": {
                "data_source": "$data_source",
                "address": "$address"
            },
            "count": {"$sum": 1},  # 计算总数
            "avg_value": {"$avg": "$value"},  # 计算平均值
            "values": {"$push": "$value"}  # 收集所有值用于计算百分位数
        }
    },

    # 步骤3: 计算统计指标
    {
        "$project": {
            "data_source": "$_id.data_source",
            "address": "$_id.address",
            "count": 1,
            "avg_value": 1,
            # 计算中位数 (50%)
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
            # 计算第一四分位数 (25%)
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

    # 步骤4: 排序 (可选)
    {
        "$sort": {
            "data_source": 1,
            "address": 1
        }
    }
]

# 执行查询
results = collection.aggregate(pipeline)

# 打印结果
pp = pprint.PrettyPrinter(indent=2)
print(f"Ergbnisse (2020-03-01 bis 2020-03-31):")
for doc in results:
    pp.pprint(doc)
