import json
import os
from bs4 import BeautifulSoup
from collections import Counter

def handle_file(path):
    with open(path, 'r', encoding='utf-8') as file:
        xml_content = file.read()

    star = BeautifulSoup(xml_content, "xml").star
    item = {}
    for el in star:
        if el.name is None:
            continue

        item[el.name] = el.get_text().strip()

    item['radius'] = int(item['radius'])
    item['rotation'] = float(star.rotation.get_text().replace(' days', '').strip())
    item['age'] = float(star.age.get_text().replace(' billion years', '').strip())
    item['distance'] = float(star.distance.get_text().replace(' million km', '').strip())
    item['absolute-magnitude'] = float(star.find('absolute-magnitude').get_text().replace(' million km', '').strip())

    return item

# Путь к папке с XML файлами
folder_path = "C:/Users/User/PycharmProjects/pythonProject2/DataEngineering/Practice3/3"

# Получаем список всех XML файлов в папке
xml_files = [f for f in os.listdir(folder_path) if f.endswith('.xml')]

# Список для хранения данных всех звезд
all_star_data = []

# Парсим каждый файл и добавляем данные в общий список
for xml_file in xml_files:
    file_path = os.path.join(folder_path, xml_file)
    star_data = handle_file(file_path)
    all_star_data.append(star_data)

# Сохраняем данные в JSON
with open('result3.json', 'w', encoding='utf-8') as json_file:
    json.dump(all_star_data, json_file, ensure_ascii=False, indent=4)

# Сортировка по радиусу
sorted_star_data = sorted(all_star_data, key=lambda x: x['radius'])

# Фильтрация по возрасту (например, звезды старше 2 миллиардов лет)
filtered_star_data = [data for data in all_star_data if data['age'] > 2.0]

# Вычисление статистических характеристик для радиуса
radius_values = [data['radius'] for data in all_star_data]
radius_sum = sum(radius_values)
radius_min = min(radius_values) if radius_values else 0
radius_max = max(radius_values) if radius_values else 0
radius_average = radius_sum / len(radius_values) if radius_values else 0

# Подсчет частоты меток для поля 'name'
name_frequency = Counter(data['name'] for data in all_star_data)

# Вывод результатов
print("Sorted Star Data:", sorted_star_data)
print("Filtered Star Data:", filtered_star_data)
print("Radius Sum:", radius_sum)
print("Radius Min:", radius_min)
print("Radius Max:", radius_max)
print("Radius Average:", radius_average)
print("Name Frequency:", name_frequency)