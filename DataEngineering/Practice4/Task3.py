import json
import pickle
from common import connect_to_db


def load_pkl(filename):
    with open(filename, 'rb') as f:
        return pickle.load(f)


def load_json(filename):
    with open(filename, 'r', encoding='utf-8') as f:
        data = json.load(f)
    return data

var = 40

def create_songs_table(db):
    cursor = db.cursor()

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS songs (
            id integer primary key, 
            artist text, 
            song text, 
            duration_ms integer, 
            year integer, 
            tempo real,
            genre text
        )
    """)


def insert_data(db, items):
    cursor = db.cursor()
    try:
        cursor.executemany("""
            INSERT INTO songs (artist, song, duration_ms, year, tempo, genre)
            VALUES (:artist, :song, :duration_ms, :year, :tempo, :genre)
        """, items)
        db.commit()
    except Exception as e:
        print(f"Error inserting data: {e}")


def first_query(db, var):
    cursor = db.cursor()
    res = cursor.execute(f"""
        SELECT *
        FROM songs
        ORDER BY year
        LIMIT {var + 10}
    """)

    items = []
    for row in res.fetchall():
        items.append(dict(row))

    return items


def second_query(db):
    cursor = db.cursor()
    res = cursor.execute("""
        SELECT 
            SUM(tempo) as sum_tempo,
            MIN(duration_ms) as min_duration_ms,
            MAX(duration_ms) as max_duration_ms,
            ROUND(AVG(tempo), 2) as avg_tempo
        FROM songs
    """)

    return dict(res.fetchone())


def third_query(db):
    cursor = db.cursor()
    res = cursor.execute("""
        SELECT 
            COUNT(*) as count,
            artist
        FROM songs
        GROUP BY artist
    """)

    items = []
    for row in res.fetchall():
        items.append(dict(row))

    return items


def fourth_query(db, var):
    cursor = db.cursor()
    res = cursor.execute(f"""
        SELECT *
        FROM songs
        WHERE year < 2002
        ORDER BY year DESC
        LIMIT {var + 15}
    """)

    items = []
    for row in res.fetchall():
        items.append(dict(row))

    return items


# Main execution flow
db = connect_to_db('third.db')

# STEP 1. CREATE TABLE
create_songs_table(db)

# STEP 2. INSERT INTO TABLE

insert_data(db, load_pkl('3/_part_1.pkl'))
insert_data(db, load_json('3/_part_2.json'))



with open('third_result_1.json', 'w', encoding='utf-8') as f:
    json.dump(first_query(db, var), f, ensure_ascii=False, indent=4)

with open('third_result_2.json', 'w', encoding='utf-8') as f:
    json.dump(second_query(db), f, ensure_ascii=False, indent=4)

with open('third_result_3.json', 'w', encoding='utf-8') as f:
    json.dump(third_query(db), f, ensure_ascii=False, indent=4)

with open('third_result_4.json', 'w', encoding='utf-8') as f:
    json.dump(fourth_query(db, var), f, ensure_ascii=False, indent=4)