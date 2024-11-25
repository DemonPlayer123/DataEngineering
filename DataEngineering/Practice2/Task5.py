import json
import msgpack
import os
import csv


def read_csv(path):
    with open(path, mode='r', encoding='utf-8') as file:
        reader = csv.DictReader(file)
        return list(reader)


def calculate_statistics(data, numeric_fields, categorical_fields):
    stats = {}

    # Для числовых полей
    for field in numeric_fields:
        values = []
        for item in data:
            try:
                # Пробуем преобразовать значение в float
                value = float(item[field])
                values.append(value)
            except (ValueError, TypeError):
                # Игнорируем значения, которые не могут быть преобразованы ('NA' etc.)
                continue



        if values:
            max_value = max(values)
            min_value = min(values)
            sum_value = sum(values)
            mean_value = sum_value / len(values)

            # Стандартное отклонение
            variance = sum((x - mean_value) ** 2 for x in values) / (len(values) - 1) if len(values) > 1 else 0
            std_dev_value = variance ** 0.5

            stats[field] = {
                'max': max_value,
                'min': min_value,
                'mean': mean_value,
                'sum': sum_value,
                'std_dev': std_dev_value
            }

    # Для категориальных полей
    for field in categorical_fields:
        frequency = {}
        for item in data:
            value = item[field]
            if value:
                frequency[value] = frequency.get(value, 0) + 1
        stats[field] = frequency


    return stats


# Загрузка данных
data = read_csv("C:/Users/Григорий/PycharmProjects/pythonProject2/DataEngineering/Practice2/XAU_15m_data_2004_to_2024-20-09.csv")

# Отбор полей для анализа
numeric_fields = ['Open', 'High', 'Low', 'Close', 'Volume']
categorical_fields = ['Date', 'Time']

# Расчет статистики
statistics_results = calculate_statistics(data, numeric_fields, categorical_fields)

# Сохранение результатов в JSON
with open("statistics_results.json", "w", encoding='utf-8') as file:
    json.dump(statistics_results, file, ensure_ascii=False)

# Сохранение набора данных в разных форматах
# CSV
with open("filtered_data.csv", "w", encoding='utf-8', newline='') as file:
    writer = csv.DictWriter(file, fieldnames=data[0].keys())
    writer.writeheader()
    writer.writerows(data)

# JSON
with open("filtered_data.json", "w", encoding='utf-8') as file:
    json.dump(data, file, ensure_ascii=False)

# Msgpack
with open("filtered_data.msgpack", "wb") as file:
    msgpack.pack(data, file)

# Pickle
import pickle

with open("filtered_data.pkl", "wb") as file:
    pickle.dump(data, file)

# Сравнение размеров файлов
csv_size = os.path.getsize("filtered_data.csv")
json_size = os.path.getsize("filtered_data.json")
msgpack_size = os.path.getsize("filtered_data.msgpack")
pkl_size = os.path.getsize("filtered_data.pkl")

print(f"CSV size: {csv_size} bytes")
print(f"JSON size: {json_size} bytes")
print(f"Msgpack size: {msgpack_size} bytes")
print(f"Pickle size: {pkl_size} bytes")

# Вывод разницы
print(f"CSV - JSON: {csv_size - json_size} bytes")
print(f"CSV - Msgpack: {csv_size - msgpack_size} bytes")
print(f"CSV - Pickle: {csv_size - pkl_size} bytes")