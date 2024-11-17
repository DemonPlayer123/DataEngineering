import csv

def read_csv(path):
    data=[]
    with open (path, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            data.append({
                'product_id': int(row['product_id']),
                'name': row['name'],
                'price': float(row['price']),
                'quantity': int(row['quantity']),
                'category': row['category'],
                'description': row['description'],
                'production_date':row['production_date'],
                #'expiration_date':row['expiration_date'], remove
                'rating':float(row['rating']),
                'status': row['status']
            })
    return data


data = read_csv("C:/Users/Григорий/PycharmProjects/pythonProject2/DataEngineering/Practice1/fourth_task.txt")

size = len(data)
avg_rating = 0
max_quantity = data[0]['quantity']
min_rating = data[0]['rating']

filtered_data = []

for item in data:
    avg_rating+=item['rating']
    if max_quantity < item['quantity']:
        max_quantity = item['quantity']
    if min_rating > item['rating']:
        min_rating = item['rating']

    if item['category'] == 'Фрукты':
        filtered_data.append(item)

avg_rating/=size

with open("fourth_task_result.txt", "w", encoding="utf-8") as f:
    f.write(f"{avg_rating}\n")
    f.write(f"{max_quantity}\n")
    f.write(f"{min_rating}\n")

with open("fourth_task_result.csv", "w", encoding="utf-8", newline="") as file:
    writer = csv.DictWriter(file, filtered_data[0].keys())
    writer.writeheader()
    for row in filtered_data:
        writer.writerow(row)
