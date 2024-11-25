import pickle
import json

with open("C:/Users/Григорий/PycharmProjects/pythonProject2/DataEngineering/Practice2/fourth_task_products.json", "rb") as file:
    products = pickle.load(file)

with open("C:/Users/Григорий/PycharmProjects/pythonProject2/DataEngineering/Practice2/fourth_task_updates.json", "r", encoding='utf-8') as file:
    updates = json.load(file)

product_map = {}

for product in products:
    product_map[product['name']] = product

methods = {
    'percent-': lambda price, param: price * (1- param),
    'percent+': lambda price, param: price * (1+ param),
    'add': lambda price, param: price + param,
    'sub': lambda price, param: price - param

}

for update in updates:
    product = product_map[update['name']]
    product['price'] = methods[update['method']](product['price'], update['param'])

products = list(product_map.values())
print(products)
with open("fourth_task.pkl", 'wb') as file:
    pickle.dump(products, file)

