import pymongo
from pymongo import MongoClient
import csv
import pickle
from bson import ObjectId
import json

def connect_db():
    client = MongoClient()
    db = client["db2024"]
    print(db.Jobs)
    return db.Jobs

def read_csv(path):
    data=[]
    with open (path, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f, delimiter=';')
        for row in reader:
            data.append({
                'job': row['job'],
                'salary': int(row['salary']),
                'id': int(row['id']),
                'city': row['city'],
                'year': int(row['year']),
                'age': int(row['age'])
            })
    return data

def read_pkl(path):
    with open(path, "rb") as f:
        return pickle.load(f)


def read_text(file):
    with open(file, 'r', encoding='utf-8') as f:
        content = f.read()
    blocks = content.strip().split("=====\n")
    items = []
    for block in blocks:
        item = {}
        for line in block.split("\n"):
            if "::" in line:
                key, value = line.split("::", 1)
                key = key.strip()
                value = value.strip()

                # Преобразование типов
                if key == "id":
                    item[key] = int(value)  # Преобразуем в int
                elif key == "age":
                    item[key] = int(value)  # Преобразуем в int
                elif key == "salary":
                    item[key] = int(value)  # Преобразуем в int
                elif key == "year":
                    item[key] = int(value)  # Преобразуем в int
                else:
                    item[key] = value  # Оставляем как строку для остальных полей
        items.append(item)
    return items

collection = connect_db()
#collection.insert_many(read_text("task_3_item.text"))
#collection.insert_many(read_pkl("task_2_item.pkl"))
#collection.insert_many(read_csv("task_1_item.csv"))

def save_to_file(data, filename):
    def convert_objectid(obj):
        if isinstance(obj, ObjectId):
            return str(obj)

    with open(filename, 'w', encoding='utf-8') as f:
        json.dump(data, f, ensure_ascii=False, indent=4, default=convert_objectid)

def sort_by_salary(collection):
    results = list(collection.find().sort("salary", pymongo.DESCENDING).limit(10))
    save_to_file(results, "Results1/sort_by_salary.json")
    return results


def filter_by_age(collection):
    results = list(collection.find({"age": {"$lt": 30}})
                   .sort("salary", pymongo.DESCENDING)
                   .limit(15))
    save_to_file(results, "Results1/filter_by_age.json")
    return results

def complex_filter(collection):
    results = list(collection.find({
        "city": "Мадрид",
        "job": {"$in": ["Оператор", "Менеджер", "Программист"]}
    }).sort("age", pymongo.ASCENDING).limit(10))
    save_to_file(results, "Results1/complex_filter.json")
    return results

def fourth_filter(collection):
    results = collection.count_documents({
        "age": {"$gt": 25, "$lt": 40},
        "year": {"$gte": 2019, "$lte": 2022},
        "$or": [
            {"salary": {"$gt": 50000, "$lte": 75000}},
            {"salary": {"$gt": 125000, "$lt": 150000}}
        ]
    })
    save_to_file(results, "Results1/fourth_filter.json")
    return results

def get_salary_agg(collection):
    query = [
        {
            "$group": {
                "_id": "result",
                "max_salary": {"$max": "$salary"},
                "min_salary": {"$min": "$salary"},
                "avg_salary": {"$avg": "$salary"}
            }
        }
    ]
    results = list(collection.aggregate(query))
    save_to_file(results, "Results2/get_salary_agg.json")
    return results

def get_freq_job(collection):
    q = [
        {
            "$group": {
                "_id":"$job",
                "count": {"$sum": 1}
            }
        }
    ]
    results = list(collection.aggregate(q))
    save_to_file(results, "Results2/get_freq_job.json")
    return results

def get_salary_stat_by_city(collection):
    q = [
        {
            "$group": {
                "_id":"$city",
                "max_salary": {"$max": "$salary"},
                "min_salary": {"$min": "$salary"},
                "avg_salary": {"$avg": "$salary"}
            }
        }
    ]
    results = list(collection.aggregate(q))
    save_to_file(results, "Results2/get_salary_stat_by_city.json")
    return results

def get_salary_stat_by_job(collection):
    q = [
        {
            "$group": {
                "_id":"$job",
                "max_salary": {"$max": "$salary"},
                "min_salary": {"$min": "$salary"},
                "avg_salary": {"$avg": "$salary"}
            }
        }
    ]
    results = list(collection.aggregate(q))
    save_to_file(results, "Results2/get_salary_stat_by_job.json")
    return results

def get_stat_by_custom_key(collection, group_by_key, agg_key):
    q = [
        {
            "$group": {
                "_id":f"${group_by_key}",
                "max": {"$max": f"${agg_key}"},
                "min": {"$min": f"${agg_key}"},
                "avg": {"$avg": f"${agg_key}"}
            }
        }
    ]
    results = list(collection.aggregate(q))
    save_to_file(results, "Results2/get_stat_by_custom_key.json")
    return results

def get_max_salary_by_min_age_sort(collection):
    results = list(collection.find(limit=1).sort({"age": pymongo.ASCENDING, "salary": pymongo.DESCENDING}))
    save_to_file(results, "Results2/get_max_salary_by_min_age_sort.json")
    return results

def get_min_salary_by_max_age_sort(collection):
    results = list(collection.find(limit=1).sort({"salary": pymongo.ASCENDING, "age": pymongo.DESCENDING}))
    save_to_file(results, "Results2/get_min_salary_by_max_age_sort.json")
    return results

def get_age_stat_by_city_with_match(collection):
    q = [
        {
            "$match": {
                "salary": {"$gt":50_000}
            }

        },
        {
            "$group": {
                "_id":"$city",
                "max": {"$max": "$age"},
                "min": {"$min": "$age"},
                "avg": {"$avg": "$age"}
            }
        },
        {
            "$sort": {
                "avg": pymongo.DESCENDING
            }
        }
    ]
    results = list(collection.aggregate(q))
    save_to_file(results, "Results2/get_age_stat_by_city_with_match.json")
    return results

def custom_query(collection):
    q = [
        {
            "$match": {
                "city": {"$in": ["Хихон","Гранада","Таллин","Бишкек"]},
                "job": {"$in": ["Программист", "Инженер"]},
                "$or": [
                    {"age": {"$gt":18, "$lt":25}},
                    {"age": {"$gt": 50, "$lt": 65}}
                ]
            }

        },
        {
            "$group": {
                "_id":"result",
                "max": {"$max": "$salary"},
                "min": {"$min": "$salary"},
                "avg": {"$avg": "$salary"}
            }
        }
    ]
    results = list(collection.aggregate(q))
    save_to_file(results, "Results2/custom_query.json")
    return results

def custom_query_2 (collection):
    q = [
        {
            "$match": {
                "year": {"$gt":2000}
            }

        },
        {
            "$group": {
                "_id":"$job",
                "max": {"$max": "$salary"},
                "min": {"$min": "$salary"},
                "avg": {"$avg": "$salary"}
            }
        },
        {
            "$sort": {
                "min": pymongo.ASCENDING
            }
        }
    ]
    results = list(collection.aggregate(q))
    save_to_file(results, "Results2/custom_query_2.json")
    return results

def delete_by_salary(collection):
    return collection.delete_many({
        "$or": [
            {"salary": {"$lt": 25_000}},
            {"salary": {"$gt": 175_000}}
        ]
    })

def inc_age(collection):
    return collection.update_many({}, {
        "$inc": {
            "age": 1
        }
    })

def inc_salary_for_job(collection):
    return collection.update_many({
        "job": {"$in": ["Программист", "Инженер"]},
    }, {
        "$mul": {
            "salary": 1.05
        }
    })


def inc_salary_for_city(collection):
    return collection.update_many({
        "city": {"$in": ["Хихон", "Гранада", "Таллин", "Бишкек"]},
    }, {
        "$mul": {
            "salary": 1.07
        }
    })

def inc_salary_for_3(collection):
    return collection.update_many({
        "job": {"$in": ["Программист", "Инженер"]},
        "city": {"$in": ["Прага", "Ташкент", "Москва"]},
        "$or": [
            {"age": {"$gt": 18, "$lt": 25}},
            {"age": {"$gt": 50, "$lt": 65}}
        ]
    }, {
        "$mul": {
            "salary": 1.1
        }
    })

def delete_by_salary_2(collection):
    return collection.delete_many({
        "$or": [
            {"year": {"$lt": 2020}},
            {"year": {"$gt": 2010}}
        ]
    })
#1ое задание
sort_by_salary(collection)
filter_by_age(collection)
complex_filter(collection)
fourth_filter(collection)
#2ое задание
get_salary_agg(collection)
get_freq_job(collection)
get_salary_stat_by_city(collection)
get_salary_stat_by_job(collection)
get_stat_by_custom_key(collection, 'city', 'age')
get_max_salary_by_min_age_sort(collection)
get_min_salary_by_max_age_sort(collection)
get_age_stat_by_city_with_match(collection)

custom_query(collection)
custom_query_2(collection)
delete_by_salary(collection)
inc_age(collection)
inc_salary_for_job(collection)
inc_salary_for_city(collection)
inc_salary_for_3(collection)
delete_by_salary_2(collection)

