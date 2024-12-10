import sqlite3
import msgpack

from common import connect_to_db


def read_msgpack_file(path):
    with open(path, 'rb') as file:
        reader = msgpack.unpack(file, raw=False)

        items = []
        for item in list(reader):
            item['price'] = float(item['price'])
            item['quantity'] = int(item['quantity'])
            item['category'] = item.get('category', None)
            item['isAvailable'] = bool(item['isAvailable'])
            if item['views'] is not None:
                item['views'] = int(item['views'])
                items.append(item)
            for item in items:
                if 'category' not in item:
                    item['category'] = None
        print(items)
        return items


def read_upd(path):
    with open(path, 'r', encoding='utf-8') as file:
        lines = file.readlines()
        updates = []
        update = {}
        for line in lines:
            line = line.strip()
            if line == '=====':
                if update:
                    updates.append(update)
                    update = {}
                continue

            if '::' in line:
                pair = line.split("::")
                if len(pair) == 2:
                    update[pair[0].strip()] = pair[1].strip()

        for update in updates:
            if update.get('method') == 'available':
                update['param'] = bool(update['param'])
            elif update.get('method') != 'remove':
                update['param'] = float(update['param'])

        return updates


def insert_data(db, items):
    cursor = db.cursor()
    cursor.executemany("""
        INSERT INTO product (
        name, price, quantity, category, fromCity, isAvailable, views)
        VALUES (:name, :price, :quantity, :category, :fromCity, :isAvailable, :views) 

    """, items)
    db.commit()


def create_product_table(db):
    cursor = db.cursor()

    cursor.execute("""
         CREATE TABLE IF NOT EXISTS product(
            id INTEGER PRIMARY KEY,
            name TEXT,
            price REAL,
            quantity INTEGER,
            fromCity TEXT,
            isAvailable INTEGER,
            category TEXT,
            views INTEGER,
            version INTEGER default 0                   
        )
    """)


def handle_remove(db, name):
    cursor = db.cursor()
    cursor.execute("DELETE FROM product WHERE name = ?", [name])
    db.commit()


def handle_price_percent(db, name, param):
    cursor = db.cursor()
    cursor.execute("""
        UPDATE product 
        SET price = ROUND(price * ( 1 + ?), 2),
            version = version + 1  
        WHERE name = ?""",
                   [param, name]
                   )
    db.commit()


def handle_price_abs(db, name, param):
    cursor = db.cursor()
    cursor.execute("""
        UPDATE product 
        SET price = price * + ?,
            version = version + 1  
        WHERE name = ?""",
                   [param, name]
                   )
    db.commit()


def handle_quantity(db, name, param):
    cursor = db.cursor()
    cursor.execute("""
        UPDATE product 
        SET quantity = quantity + ?,
            version = version + 1  
        WHERE name = ?""",
                   [param, name]
                   )
    db.commit()


def handle_available(db, name, param):
    cursor = db.cursor()
    cursor.execute("""
        UPDATE product 
        SET isAvailable = ?,
            version = version + 1  
        WHERE name = ?""",
                   [param, name]
                   )
    db.commit()


def handle_updates(db, updates):
    for update in updates:
        if update['method'] == 'remove':
            handle_remove(db, update['name'])
        elif update['method'] == 'price_percent':
            handle_price_percent(db, update['name'], update['param'])
        elif update['method'] == 'price_abs':
            handle_price_abs(db, update['name'], update['param'])
        elif update['method'] == 'quantity_add':
            handle_quantity(db, update['name'], update['param'])
        elif update['method'] == 'quantity_sub':
            handle_quantity(db, update['name'], update['param'])
        elif update['method'] == 'available':
            handle_available(db, update['name'], update['param'])


db = connect_to_db("fourth.db")
create_product_table(db)
path = read_msgpack_file(
    '4/_product_data.msgpack')
updates = read_upd("4/_update_data.text")
insert_data(db, path)
handle_updates(db, updates)


def get_top_updated_products(db):
    cursor = db.cursor()
    cursor.execute("""
        SELECT name, version 
        FROM product 
        ORDER BY version DESC 
        LIMIT 10
    """)
    return cursor.fetchall()

def analyze_price_groups(db):
    cursor = db.cursor()
    cursor.execute("""
        SELECT category, 
               COUNT(*) AS count, 
               SUM(price) AS total_price, 
               MIN(price) AS min_price, 
               MAX(price) AS max_price, 
               AVG(price) AS avg_price 
        FROM product 
        GROUP BY category
    """)
    return cursor.fetchall()

def analyze_quantity_groups(db):
    cursor = db.cursor()
    cursor.execute("""
        SELECT category, 
               COUNT(*) AS count, 
               SUM(quantity) AS total_quantity, 
               MIN(quantity) AS min_quantity, 
               MAX(quantity) AS max_quantity, 
               AVG(quantity) AS avg_quantity 
        FROM product 
        GROUP BY category
    """)
    return cursor.fetchall()

def custom_query(db):
    cursor = db.cursor()
    cursor.execute("""
        SELECT * FROM product WHERE price > 100
    """)
    return cursor.fetchall()

# Выполнение запросов и вывод результатов
top_updated_products = get_top_updated_products(db)
print("Топ-10 самых обновляемых товаров:")
for product in top_updated_products:
    print(f"Название: {product['name']}, Версия: {product['version']}")

price_analysis = analyze_price_groups(db)
print("\nАнализ цен товаров:")
for row in price_analysis:
    print(f"Категория: {row['category']}, Количество: {row['count']}, "
          f"Сумма: {row['total_price']}, Минимум: {row['min_price']}, "
          f"Максимум: {row['max_price']}, Среднее: {row['avg_price']}")

quantity_analysis = analyze_quantity_groups(db)
print("\nАнализ остатков товаров:")
for row in quantity_analysis:
    print(f"Категория: {row['category']}, Количество: {row['count']}, "
          f"Сумма: {row['total_quantity']}, Минимум: {row['min_quantity']}, "
          f"Максимум: {row['max_quantity']}, Среднее: {row['avg_quantity']}")

arbitrary_results = custom_query(db)
print("\nРезультаты произвольного запроса (товары с ценой больше 100):")
for row in arbitrary_results:
    print(f"Название: {row['name']}, Цена: {row['price']}, Остаток: {row['quantity']}, "
          f"Категория: {row['category']}")


