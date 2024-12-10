import csv
import sqlite3
import json


def create_and_populate_db(csv_filepath, json_filepath, db_filepath):
    conn = sqlite3.connect(db_filepath)
    cursor = conn.cursor()

    # Создание таблиц
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS Countries (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            country_name TEXT UNIQUE
        )
    ''')
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS Currencies (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            currency_description TEXT UNIQUE
        )
    ''')
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS ExchangeRates (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            record_date TEXT,
            country_id INTEGER,
            currency_id INTEGER,
            exchange_rate REAL,
            effective_date TEXT,
            FOREIGN KEY (country_id) REFERENCES Countries (id),
            FOREIGN KEY (currency_id) REFERENCES Currencies (id)
        )
    ''')

    # Заполнение таблиц из CSV
    with open(csv_filepath, "r", encoding="utf-8-sig") as file:
        reader = csv.DictReader(file)
        reader.fieldnames = [field.strip() for field in reader.fieldnames]
        for row in reader:
            full_description = row['Country - Currency Description'].strip()
            if '-' in full_description:
                country, currency = full_description.split('-', 1)
            else:
                country = full_description
                currency = "Unknown Currency"

            cursor.execute('INSERT OR IGNORE INTO Countries (country_name) VALUES (?)', (country.strip(),))
            cursor.execute('INSERT OR IGNORE INTO Currencies (currency_description) VALUES (?)', (currency.strip(),))
            cursor.execute('''
                INSERT INTO ExchangeRates (record_date, country_id, currency_id, exchange_rate, effective_date)
                VALUES (?, (SELECT id FROM Countries WHERE country_name = ?), 
                            (SELECT id FROM Currencies WHERE currency_description = ?), ?, ?)
            ''', (
                row['Record Date'],
                country.strip(),
                currency.strip(),
                row['Exchange Rate'],
                row['Effective Date']
            ))

    # Заполнение таблиц из JSON
    with open(json_filepath, "r", encoding="utf-8") as f:
        json_data = json.load(f)
        for item in json_data['data']:
            full_description = item['country_currency_desc'].strip()
            if '-' in full_description:
                country, currency = full_description.split('-', 1)
            else:
                country = full_description
                currency = "Unknown Currency"

            cursor.execute('INSERT OR IGNORE INTO Countries (country_name) VALUES (?)', (country.strip(),))
            cursor.execute('INSERT OR IGNORE INTO Currencies (currency_description) VALUES (?)', (currency.strip(),))
            cursor.execute('''
                INSERT INTO ExchangeRates (record_date, country_id, currency_id, exchange_rate, effective_date)
                VALUES (?, (SELECT id FROM Countries WHERE country_name = ?), 
                            (SELECT id FROM Currencies WHERE currency_description = ?), ?, ?)
            ''', (
                item['record_date'],
                country.strip(),
                currency.strip(),
                item['exchange_rate'],
                item['effective_date']
            ))

    conn.commit()
    conn.close()


def json_save(data, json_filepath):
    with open(json_filepath, 'w', encoding='utf-8') as f:
        json.dump(data, f, ensure_ascii=False, indent=4)


def analyze_exchange_rates(db_filepath):
    conn = sqlite3.connect(db_filepath)
    cursor = conn.cursor()

    # ТОП-10 стран с самым высоким обменным курсом
    cursor.execute('''
        SELECT c.country_name, cu.currency_description, e.exchange_rate
        FROM ExchangeRates AS e
        JOIN Countries AS c ON e.country_id = c.id
        JOIN Currencies AS cu ON e.currency_id = cu.id
        ORDER BY e.exchange_rate DESC
        LIMIT 10
    ''')
    top_rates = cursor.fetchall()
    json_save([{'country_name': row[0], 'currency_description': row[1], 'exchange_rate': row[2]} for row in top_rates],
              "top_10_highest_rates.json")

    # Количество записей обменных курсов по каждой стране
    cursor.execute('''
        SELECT c.country_name, COUNT(e.id) AS record_count
        FROM ExchangeRates AS e
        JOIN Countries AS c ON e.country_id = c.id
        GROUP BY c.country_name
        ORDER BY record_count DESC
    ''')
    record_counts = cursor.fetchall()
    json_save(
        [{'country_name': row[0], 'record_count': row[1]} for row in record_counts],
        "record_counts_by_country.json"
    )

    # Страны с обменным курсом ниже 10
    threshold = 10
    cursor.execute('''
        SELECT c.country_name, cu.currency_description, e.exchange_rate
        FROM ExchangeRates AS e
        JOIN Countries AS c ON e.country_id = c.id
        JOIN Currencies AS cu ON e.currency_id = cu.id
        WHERE e.exchange_rate < ?
        ORDER BY e.exchange_rate ASC
    ''', (threshold,))
    low_rates = cursor.fetchall()
    json_save(
        [{'country_name': row[0], 'currency_description': row[1], 'exchange_rate': row[2]} for row in low_rates],
        "low_exchange_rates.json"
    )

    conn.close()

    conn.close()


# Указание путей к файлам
csv_filepath = "dataset/RprtRateXchg_20240701_20240930.csv"
json_filepath = "dataset/RprtRateXchg_20240701_20240930.json"
db_filepath = "exchange_rates.db"

# Выполнение функций
create_and_populate_db(csv_filepath, json_filepath, db_filepath)
analyze_exchange_rates(db_filepath)
