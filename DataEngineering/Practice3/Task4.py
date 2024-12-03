import json
import os
from bs4 import BeautifulSoup
from collections import Counter

def handle_file(path):
    with open(path, 'r', encoding='utf-8') as file:
        xml_content = file.read()

    clothings = BeautifulSoup(xml_content, "xml").find_all('clothing')
    items = []
    for clothing in clothings:
        item = {}
        item['id'] = int(clothing.id.get_text().strip())
        item['name'] = clothing.find_all('name')[0].get_text().strip()
        item['category'] = clothing.category.get_text().strip()
        item['size'] = clothing.size.get_text().strip()
        item['color'] = clothing.color.get_text().strip()
        item['material'] = clothing.material.get_text().strip()
        item['price'] = float(clothing.price.get_text().strip())
        item['rating'] = float(clothing.rating.get_text().strip())
        if clothing.sporty is not None:
            item['sporty'] = clothing.sporty.get_text().strip() == "yes"
        if clothing.new is not None:
            item['new'] = clothing.new.get_text().strip() == "+"
        if clothing.exclusive is not None:
            item['exclusive'] = clothing.exclusive.get_text().strip() == "yes"

        items.append(item)
    return items

# Путь к папке с XML файлами
folder_path = "C:/Users/User/PycharmProjects/pythonProject2/DataEngineering/Practice3/4"

# Получаем список всех XML файлов в папке
xml_files = [f for f in os.listdir(folder_path) if f.endswith('.xml')]

# Список для хранения данных всех предметов одежды
all_clothing_data = []

# Парсим каждый файл и добавляем данные в общий список
for xml_file in xml_files:
    file_path = os.path.join(folder_path, xml_file)
    clothing_data = handle_file(file_path)
    all_clothing_data.extend(clothing_data)

# Сохраняем данные в JSON
with open('result4.json', 'w', encoding='utf-8') as json_file:
    json.dump(all_clothing_data, json_file, ensure_ascii=False, indent=4)

# Сортировка по цене
sorted_clothing_data = sorted(all_clothing_data, key=lambda x: x['price'])

# Фильтрация по sporty
filtered_clothing_data = [data for data in all_clothing_data if data.get('sporty')]

# Вычисление статистических характеристик для цены
price_values = [data['price'] for data in all_clothing_data]
price_sum = sum(price_values)
price_min = min(price_values) if price_values else 0
price_max = max(price_values) if price_values else 0
price_average = price_sum / len(price_values) if price_values else 0

# Подсчет частоты меток для поля 'name'
name_frequency = Counter(data['name'] for data in all_clothing_data)

# Вывод результатов
print("Sorted Clothing Data:", sorted_clothing_data)
print("Filtered Clothing Data:", filtered_clothing_data)
print("Price Sum:", price_sum)
print("Price Min:", price_min)
print("Price Max:", price_max)
print("Price Average:", price_average)
print("Name Frequency:", name_frequency)