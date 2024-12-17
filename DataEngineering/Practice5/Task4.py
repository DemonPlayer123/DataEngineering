import pymongo
from pymongo import MongoClient
import csv
import pickle
from bson import ObjectId
import json

def connect_db():
    client = MongoClient()
    db = client["db2025"]
    print(db.Jobs)
    return db.Jobs

def read_csv(path):
    data=[]
    with open (path, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            data.append({
                'Record Date': row['\ufeff"Record Date"'].strip(),
                'Country - Currency Description': row['Country - Currency Description'].strip(),
                'Exchange Rate': float(row['Exchange Rate'].replace('"','').strip()),
                'Effective Date': row['Effective Date'],
            })
    return data

def read_json(path):
    with open(path, "r", encoding='utf-8') as f:
        return json.load(f)

collection = connect_db()
collection.insert_many(read_csv("dataset/RprtRateXchg_20240701_20240930.csv"))
collection.insert_many(read_json("dataset/RprtRateXchg_20240701_20240930.json"))

def save_to_file(data, filename):
    def convert_objectid(obj):
        if isinstance(obj, ObjectId):
            return str(obj)

    with open(filename, 'w', encoding='utf-8') as f:
        json.dump(data, f, ensure_ascii=False, indent=4, default=convert_objectid)

def sort_by_Exchange_Rate(collection):
    results = list(collection.find().sort("Exchange Rate", pymongo.DESCENDING).limit(10))
    save_to_file(results, "Results4/Viborka/sort_by_Exchange_Rate.json")
    return results

def filter_by_Exchange_Rate(collection):
    results = list(collection.find({"Exchange Rate": {"$lt": 30}})
                   .sort("Exchange Rate", pymongo.DESCENDING)
                   .limit(15))
    save_to_file(results, "Results4/Viborka/filter_by_age.json")
    return results

def complex_filter(collection):
    results = list(collection.find({
        "Effective Date": "2024-09-30",
        "Country - Currency Description": {"$in": ["Bahrain-Dinar", "Bermuda-Dollar", "Brazil-Real"]}
    }).sort("Exchange Rate", pymongo.ASCENDING).limit(10))
    save_to_file(results, "Results4/Viborka/complex_filter.json")
    return results

def Country_start_w_A_filter(collection):
    results = list(collection.find({ "Country - Currency Description": { "$regex": "^A" } }).sort("Exchange Rate", pymongo.ASCENDING).limit(10))
    save_to_file(results, "Results4/Viborka/Country_start_w_A_filter.json")
    return results

def fourth_filter(collection):
    results = collection.count_documents({
        "Exchange Rate": {"$gt": 50, "$lt": 100},
        "Effective Date": "2024-09-30",
        "Country - Currency Description": {"$in": ["Gambia-Dalasi", "Dominican Republic-Peso", "Mongolia-Tugrik"]}
    })
    save_to_file(results, "Results4/Viborka/fourth_filter.json")
    return results

def get_Exchange_Rate_agg(collection):
    query = [
        {
            "$group": {
                "_id": "result",
                "max_Exchange_Rate": {"$max": "$Exchange Rate"},
                "min_Exchange_Rate": {"$min": "$Exchange Rate"},
                "avg_Exchange_Rate": {"$avg": "$Exchange Rate"}
            }
        }
    ]
    results = list(collection.aggregate(query))
    save_to_file(results, "Results4/Agregacia/get_Exchange_Rate_agg.json")
    return results

# Агрегация для группировки валют по диапазонам курсов
def get_Exchange_Rate_stat(collection):
    q = [
        {
            "$bucket": {
                "groupBy": "$Exchange Rate",
                "boundaries": [0, 10, 100, 1000],
                "default": "1000+",
                "output": {
                    "count": { "$sum": 1 }
                }
            }
        }
    ]
    results = list(collection.aggregate(q))
    save_to_file(results, "Results4/Agregacia/get_Exchange_Rate_stat.json")
    return results

# Агрегация для подсчета валют, начинающихся с буквы "A"
def get_Country_start_w_A(collection):
    q = [
        {
            "$match": {
                "Country - Currency Description": {"$regex": "^A"}
            }
        },
        {
            "$count": "count"
        }
    ]
    results = list(collection.aggregate(q))
    save_to_file(results, "Results4/Agregacia/get_Country_start_w_A.json")
    return results

# Агрегация для группировки валют по странам и подсчета количества валют в каждой стране
def get_Exchange_Rate_by_Country(collection):
    q = [
        {
            "$group": {
                "_id": {
                    "country": {"$substr": ["$Country - Currency Description", 0,
                                            {"$indexOfBytes": ["$Country - Currency Description", "-"]}]},
                    "currency": "$Country - Currency Description"
                },
                "count": {"$sum": 1}
            }
        },
        {
            "$sort": {"count": -1}  # Сортировка по убыванию количества
        }
    ]
    results = list(collection.aggregate(q))
    save_to_file(results, "Results4/Agregacia/get_Exchange_Rate_by_Country.json")
    return results

# Агрегация для получения списка валют с курсом выше 100 и сортировка по курсу
def get_sort_Exchange_Rate(collection):
    q = [
        {
            "$match": {
                "Exchange Rate": {"$gt": 100}
            }
        },
        {
            "$sort": {"Exchange Rate": -1}  # Сортировка по убыванию курса
        }
    ]
    results = list(collection.aggregate(q))
    save_to_file(results, "Results4/Agregacia/get_sort_Exchange_Rate.json")
    return results

# Обновление курса для всех валют с курсом ниже 10
def update_by_Exchange_Rate(collection):
    return collection.update_many(
        { "Exchange Rate": { "$lt": 10 } },
        { "$inc": { "Exchange Rate": 5 } }
    )

# Обновление курса для "Afghanistan-Afghani"
def update_by_Afghanistan_Afghani(collection):
    return collection.update_one(
        {"Country - Currency Description": "Afghanistan-Afghani"},
        {"$set": {"Exchange Rate": 70.00}}
    )

# Удаление документа для "Antigua & Barbuda-E. Caribbean Dollar"
def delete_by_Antigua(collection):
    return collection.delete_one(
        { "Country - Currency Description": "Antigua & Barbuda-E. Caribbean Dollar" }
    )

# Удаление всех валют с курсом ниже 5
def delete_by_Afghanistan_Afghani(collection):
    return collection.delete_many(
        { "Exchange Rate": { "$lt": 5 } }
    )

# Увеличение курса для всех валют с курсом от 50 до 100 на 5
def inc_by_Exchange_Rate(collection):
    return collection.update_many(
        {"Exchange Rate": {"$gte": 50, "$lte": 100}},
        {"$inc": {"Exchange Rate": 5}}
    )

#1ое задание
sort_by_Exchange_Rate(collection)
filter_by_Exchange_Rate(collection)
complex_filter(collection)
fourth_filter(collection)
Country_start_w_A_filter(collection)
#2ое задание
get_Exchange_Rate_agg(collection)
get_Exchange_Rate_stat(collection)
get_Exchange_Rate_by_Country(collection)
get_Country_start_w_A(collection)
get_sort_Exchange_Rate(collection)
#3е задание
inc_by_Exchange_Rate(collection)
delete_by_Afghanistan_Afghani(collection)
delete_by_Antigua(collection)
update_by_Exchange_Rate(collection)
update_by_Afghanistan_Afghani(collection)