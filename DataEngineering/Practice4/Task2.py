import sqlite3
import csv

db_path = 'addresses.db'
conn = sqlite3.connect(db_path)
cursor = conn.cursor()

cursor.execute("""
CREATE TABLE IF NOT EXISTS buildings (
    name TEXT,
    rating FLOAT,
    security INTEGER,
    FOREIGN KEY (name) REFERENCES tournaments (name)
)
""")

# Функция для чтения данных из CSV
def parse_buildings(file_path):
    items = []
    with open(file_path, 'r', encoding='utf-8') as f:
        reader = csv.reader(f, delimiter=';')
        reader.__next__()
        for row in reader:
            if len(row) == 0: continue
            item = {
                'name': row[0],
                'rating': float(row[1]),
                'convenience': int(row[2]),
                'security': int(row[3]),
                'functionality': int(row[4]),
                'comment': row[5]
            }
            items.append(item)
        return items

# Путь к файлу CSV
file_path = '1-2/subitem.csv'
buildings_items = parse_buildings(file_path)

# Преобразование данных для вставки в базу
buildings_data = [
    (item['name'], float(item['rating']), int(item['security']))
    for item in buildings_items
]

# Вставка данных в таблицу
cursor.executemany("""
INSERT INTO buildings (name, rating, security)
VALUES (?, ?, ?)
""", buildings_data)

conn.commit()

# Проверка количества записей в таблице
cursor.execute("SELECT COUNT(*) FROM buildings")
print(f"Количество записей в таблице buildings: {cursor.fetchone()[0]}")

# Запросы, использующие связь между таблицами

# Запрос 1: Средний рейтинг зданий по городам
query_1 = """
SELECT a.city, AVG(b.rating) AS avg_rating
FROM buildings b
JOIN addresses a ON b.name = a.name
GROUP BY a.city
ORDER BY avg_rating DESC
"""
cursor.execute(query_1)
print("Запрос 1: Средний рейтинг зданий по городам")
for row in cursor.fetchall():
    print(f"City: {row[0]}, Average rating: {row[1]}")

# Запрос 2: Здания с высоким уровнем безопасности и наличием парковки
query_2 = """
SELECT b.name, b.security, a.parking
FROM buildings b
JOIN addresses a ON b.name = a.name
WHERE b.security > 8 AND a.parking = 1
ORDER BY b.security DESC
"""
cursor.execute(query_2)
print("\nЗапрос 2: Здания с высоким уровнем безопасности и парковкой")
for row in cursor.fetchall():
    print(f"Name: {row[0]}, Security: {row[1]}, Parking: {row[2]}")

# Запрос 3: Города с самым большим количеством зданий
query_3 = """
SELECT a.city, COUNT(*) AS building_count
FROM buildings b
JOIN addresses a ON b.name = a.name
GROUP BY a.city
ORDER BY building_count DESC
"""
cursor.execute(query_3)
print("\nЗапрос 3: Города с самым большим количеством зданий")
for row in cursor.fetchall():
    print(f"City: {row[0]}, Building Count: {row[1]}")

conn.close()
