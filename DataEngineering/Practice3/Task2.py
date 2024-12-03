from bs4 import BeautifulSoup
import json
from collections import Counter
import os

def handle_file(path):
    with open(path, 'r', encoding='utf-8') as file:
        html_content = file.read()

        soap = BeautifulSoup(html_content, "html.parser")
        products = soap.find_all("div", attrs={'class': 'product-item'})

        items = []
        for product in products:
            item = {}
            item['id'] = int(product.a['data-id'])
            item['link'] = product.find_all('a')[1]['href']
            item['img'] = product.img['src']
            item['title'] = product.span.get_text().strip().replace('\"', '')
            item['price'] = float(product.price.get_text().replace(' ₽', '').replace(' ', '').strip())
            item['bonus'] = int(product.strong.get_text()
                                .replace("+ начислим", "")
                                .replace(" бонусов", "")
                                .strip())
            properties = product.ul.find_all("li")
            for prop in properties:
                item[prop['type']] = prop.get_text().strip()

            items.append(item)

        return items

# Путь к папке с файлами
folder_path = 'C:/Users/User/PycharmProjects/pythonProject2/DataEngineering/Practice3/2'
html_files = sorted([f for f in os.listdir(folder_path) if f.endswith('.html')])

all_items = []

# Парсинг всех HTML файлов в папке
for file_name in html_files:
    file_path = os.path.join(folder_path, file_name)
    items = handle_file(file_path)
    all_items.extend(items)

# Сортировка по цене
sorted_items = sorted(all_items, key=lambda x: x['price'])

# Фильтрация по наличию
filtered_items = [item for item in all_items if item['bonus'] > 1000]

# Статистические характеристики для цены
prices = [item['price'] for item in all_items]
price_sum = sum(prices)
price_min = min(prices) if prices else 0
price_max = max(prices) if prices else 0
price_avg = price_sum / len(prices) if prices else 0

# Частота iPhone
iphone_frequency = Counter(item['title'] for item in all_items if 'iPhone' in item['title'])

# Результаты
result = {
    'sorted_items': sorted_items,
    'filtered_items': filtered_items,
    'price_statistics': {
        'sum': f"{price_sum} руб",
        'min': f"{price_min} руб",
        'max': f"{price_max} руб",
        'average': f"{price_avg:.2f} руб"
    },
    'iphone_frequency': iphone_frequency
}

# Запись в JSON
with open('result2.json', 'w', encoding='utf-8') as f:
    json.dump(result, f, ensure_ascii=False, indent=4)

print(json.dumps(result, ensure_ascii=False, indent=4))