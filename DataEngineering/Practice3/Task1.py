from bs4 import BeautifulSoup
import json
from collections import Counter
import os

def parse_product(html_content):
    soup = BeautifulSoup(html_content, "html.parser")
    product = soup.find("div", attrs={'class': 'product-wrapper'})

    item = {}
    article_temp = product.span.get_text().split("Артикул:")[1].split("Наличие:")
    item['article'] = article_temp[0].strip()
    item['availability'] = article_temp[1].strip()
    item['id'] = product.h1['id']
    item['type'] = product.h1.get_text().split(":")[1].strip()
    address_temp = product.p.get_text().split("Город:")[1].split("Цена:")
    item['city'] = address_temp[0].strip()
    item['price'] = int(address_temp[1].strip().replace(' руб', ''))
    item['color'] = product.find_all("span", attrs={'class': 'color'})[0].get_text().split(":")[1].strip()
    item['quantity'] = int(product.find_all("span", attrs={'class': 'quantity'})[0].get_text().split(":")[1].strip().replace(' шт', ''))
    item['size'] = product.find_all("span", attrs={'class': ''})[1].get_text().split("Размеры:")[1].strip()
    spans = product.find_all("span", attrs={'class': ''})
    item['rating'] = float(spans[2].get_text().split(":")[1])
    item['views'] = int(spans[3].get_text().split(":")[1])
    item['img'] = product.img['src']

    return item

# Путь к папке с файлами
folder_path = 'C:/Users/User/PycharmProjects/pythonProject2/DataEngineering/Practice3/1'
html_files = sorted([f for f in os.listdir(folder_path) if f.endswith('.html')])

items = []

# Парсинг всех HTML файлов в папке
for file_name in html_files:
    file_path = os.path.join(folder_path, file_name)
    with open(file_path, 'r', encoding='utf-8') as file:
        html_content = file.read()
        items.append(parse_product(html_content))

# Сортировка по цене
sorted_items = sorted(items, key=lambda x: x['price'])

# Фильтрация по наличию
available_items = [item for item in items if item['availability'] == 'Есть в наличии']

# Статистические характеристики для цены
prices = [item['price'] for item in items]
price_sum = sum(prices)
price_min = min(prices) if prices else 0
price_max = max(prices) if prices else 0
price_avg = price_sum / len(prices) if prices else 0

# Частота цветов
colors = [item['color'] for item in items]
color_frequency = Counter(colors)

# Результаты
result = {
    'sorted_items': sorted_items,
    'available_items': available_items,
    'price_statistics': {
        'sum': f"{price_sum} руб",
        'min': f"{price_min} руб",
        'max': f"{price_max} руб",
        'average': f"{price_avg:.2f} руб"
    },
    'color_frequency': color_frequency
}


with open('result1.json', 'w', encoding='utf-8') as json_file:
    json.dump(result, json_file, ensure_ascii=False, indent=4)

print(json.dumps(result, ensure_ascii=False, indent=4))