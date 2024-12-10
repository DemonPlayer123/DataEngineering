import json
import sqlite3

db_path = 'addresses.db'
conn = sqlite3.connect(db_path)
cursor = conn.cursor()

cursor.execute("""
CREATE TABLE IF NOT EXISTS addresses (
    id INTEGER PRIMARY KEY,
    name TEXT,
    street TEXT,
    city TEXT,
    zipcode INTEGER,
    floors INTEGER,
    year INTEGER,
    parking BOOLEAN,
    prob_price INTEGER,
    views INTEGER
)
""")

def parse_file(file_path):
    with open(file_path, 'r', encoding='utf-8') as f:
        data = json.load(f)
    return data

file_path = '1-2/item.json'

records = parse_file(file_path)

cursor.executemany("""
INSERT INTO addresses (id, name, street, city, zipcode, floors, year, parking, prob_price, views)
VALUES (:id, :name, :street, :city, :zipcode, :floors, :year, :parking, :prob_price, :views)
""", records)

conn.commit()

VAR = 40
LIMIT = VAR + 10

query_1 = f"""
SELECT * FROM addresses
ORDER BY prob_price ASC
LIMIT {LIMIT}
"""
cursor.execute(query_1)
results_1 = cursor.fetchall()

columns = [desc[0] for desc in cursor.description]
json_output_1 = [dict(zip(columns, row)) for row in results_1]

with open('query1_output.json', 'w', encoding='utf-8') as f:
    json.dump(json_output_1, f, ensure_ascii=False, indent=4)

print("Запрос 1: Данные сохранены в файл query1_output.json")

query_2 = """
SELECT 
    SUM(views) AS total,
    MIN(views) AS minimum,
    MAX(views) AS maximum,
    AVG(views) AS average
FROM addresses
"""
cursor.execute(query_2)
stats_2 = cursor.fetchone()
print(f"Запрос 2: Сумма: {stats_2[0]}, Минимум: {stats_2[1]}, Максимум: {stats_2[2]}, Среднее: {stats_2[3]:.2f}")

query_3 = """
SELECT city, COUNT(*) AS frequency
FROM addresses
GROUP BY city
ORDER BY frequency DESC
"""
cursor.execute(query_3)
frequency_3 = cursor.fetchall()
print("Запрос 3: Частота встречаемости категорий:")
for row in frequency_3:
    print(f"City: {row[0]}, Frequency: {row[1]}")

query_4 = f"""
SELECT * FROM addresses
WHERE floors > 4
ORDER BY year DESC
LIMIT {LIMIT}
"""
cursor.execute(query_4)
results_4 = cursor.fetchall()

json_output_4 = [dict(zip(columns, row)) for row in results_4]

with open('query4_output.json', 'w', encoding='utf-8') as f:
    json.dump(json_output_4, f, ensure_ascii=False, indent=4)

print("Запрос 4: Данные сохранены в файл query4_output.json")

conn.close()
