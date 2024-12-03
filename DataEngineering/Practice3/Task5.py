from bs4 import BeautifulSoup
import json
import os
import statistics


# Функция для парсинга страницы
def handle_file(path):
    with open(path, 'r', encoding='utf-8') as file:
        html_content = file.read()
        soup = BeautifulSoup(html_content, "html.parser")

        # Поиск всех товаров
        product_blocks = soup.find_all("div", class_="description")

        items = []
        for product_block in product_blocks:
            item = {}

            # Извлечение названия и ссылки
            name_tag = product_block.find("a", class_="description_name")
            if name_tag:
                item['name'] = name_tag.find("span").get_text(strip=True) if name_tag.find("span") else None
                item['link'] = 'https://www.ismart-store.ru' + name_tag['href']
            else:
                item['name'] = None
                item['link'] = None

            # Извлечение цены
            price_tag = product_block.find("div", class_="description_price")
            if price_tag:
                price_span = price_tag.find("span")
                if price_span:
                    try:
                        item['price'] = float(price_span.get_text(strip=True).replace(' руб.', '').replace(' ', ''))
                    except ValueError:
                        item['price'] = None
                else:
                    item['price'] = None
            else:
                item['price'] = None

            # Добавляем товар в список
            items.append(item)

        return items


# Основной код
folder_path = 'C:/Users/User/PycharmProjects/pythonProject2/DataEngineering/Practice3/5'
html_files = sorted([f for f in os.listdir(folder_path) if f.endswith('.html')])

all_items = []

# Парсинг всех HTML файлов в папке
for file_name in html_files:
    file_path = os.path.join(folder_path, file_name)
    items = handle_file(file_path)
    all_items.extend(items)
# Сохранение данных в JSON
output_file = 'result5.json'
with open(output_file, 'w', encoding='utf-8') as f:
    json.dump(all_items, f, ensure_ascii=False, indent=4)

# Сортировка по цене
sorted_data = sorted(all_items, key=lambda x: x['price'] if x['price'] is not None else float('inf'))
with open('sorted_result5.json', 'w', encoding='utf-8') as f:
    json.dump(sorted_data, f, ensure_ascii=False, indent=4)

# Фильтрация по цене
filtered_data = [item for item in all_items if item['price'] and item['price'] < 50000]
with open('filtered_result5.json', 'w', encoding='utf-8') as f:
    json.dump(filtered_data, f, ensure_ascii=False, indent=4)

# Статистика по ценам
prices = [item['price'] for item in all_items if item['price'] is not None]
if prices:
    mean_price = statistics.mean(prices)
    median_price = statistics.median(prices)
    print(f"Mean price: {mean_price}, Median price: {median_price}")

# Частота меток
name_frequency = {}
for item in all_items:
    name = item['name']
    if name:
        name_frequency[name] = name_frequency.get(name, 0) + 1

print("Name frequency:", name_frequency)

print(f"Parsed data: {all_items}")
print(f"Data saved to {output_file}")
