import pandas as pd
import json
import matplotlib.pyplot as plt
import seaborn as sns

def read_file(file_name):
    return pd.read_csv(file_name, compression='zip')
    # return next()


def get_memory_stat_by_column(df):
    memory_usage_stat = df.memory_usage(deep=True)
    total_memory_usage = memory_usage_stat.sum()
    print(f"file in memory size = {total_memory_usage // 1024:10} КБ")
    column_stat = list()
    for key in df.dtypes.keys():
        column_stat.append({
            "column_name": key,
            "memory_abs": int(memory_usage_stat[key] // 1024),
            "memory_per": round(memory_usage_stat[key] / total_memory_usage * 100, 4),
            "dtype": str(df.dtypes[key])
        })
    column_stat.sort(key=lambda x: x['memory_abs'], reverse=True)
    for column in column_stat:
        print(
            f"{column['column_name']:30}: {column['memory_abs']:10} КБ: {column['memory_per']:10}% : {column['dtype']}")
    return column_stat


def save_memory_stat_to_json(column_stat, output_file):
    # Добавляем метаданные
    data_to_save = {
        "description": "Статистика по набору данных без применения оптимизаций",
        "columns": column_stat
    }

    # Сохраняем в JSON файл
    with open(output_file, 'w', encoding='utf-8') as json_file:
        json.dump(data_to_save, json_file, ensure_ascii=False, indent=4)



def mem_usage(pandas_obj):
    if isinstance(pandas_obj, pd.DataFrame):
        usage_b = pandas_obj.memory_usage(deep=True).sum()
    else:  # предположим, что если это не датафрейм, то серия
        usage_b = pandas_obj.memory_usage(deep=True)
    usage_mb = usage_b / 1024 ** 2  # преобразуем байты в мегабайты
    return "{:03.2f} MB".format(usage_mb)

def opt_obj(df):
    converted_obj = pd.DataFrame()
    dataset_obj = df.select_dtypes(include=['object']).copy()

    for col in dataset_obj.columns:
        num_unique_values = len(dataset_obj[col].unique())
        num_total_values = len(dataset_obj[col])
        if num_unique_values / num_total_values < 0.5:
            converted_obj.loc[:, col] = dataset_obj[col].astype('category')
        else:
            converted_obj.loc[:, col] = dataset_obj[col]

    print(mem_usage(dataset_obj))
    print(mem_usage(converted_obj))
    return converted_obj

def opt_int(df):
    dataset_int = df.select_dtypes(include=['int'])
    """
    downcast:
            - 'integer' or 'signed': smallest signed int dtype (min.: np.int8)
            - 'unsigned': smallest unsigned int dtype (min.: np.uint8)
            - 'float': smallest float dtype (min.: np.float32)
    """
    converted_int = dataset_int.apply(pd.to_numeric, downcast='unsigned')
    print(mem_usage(dataset_int))
    print(mem_usage(converted_int))
    #
    compare_ints = pd.concat([dataset_int.dtypes, converted_int.dtypes], axis=1)
    compare_ints.columns = ['before', 'after']
    compare_ints.apply(pd.Series.value_counts)
    print(compare_ints)

    return converted_int


def opt_float(df):
    # # =======================================================================
    # # выполняем понижающее преобразование
    # # для столбцов типа float
    dataset_float = df.select_dtypes(include=['float'])
    converted_float = dataset_float.apply(pd.to_numeric, downcast='float')

    print(mem_usage(dataset_float))
    print(mem_usage(converted_float))

    compare_floats = pd.concat([dataset_float.dtypes, converted_float.dtypes], axis=1)
    compare_floats.columns = ['before', 'after']
    compare_floats.apply(pd.Series.value_counts)
    print(compare_floats)

    return converted_float


# steps 1-3
file_name = "archive.zip"
dataset = read_file(file_name)
get_memory_stat_by_column(dataset)
column_stat = get_memory_stat_by_column(dataset)
save_memory_stat_to_json(column_stat, 'save_memory_stat_to_json.json')

# steps 4-6
optimized_dataset = dataset.copy()

converted_obj = opt_obj(dataset)
converted_int = opt_int(dataset)
converted_float = opt_float(dataset)
#
optimized_dataset[converted_obj.columns] = converted_obj
optimized_dataset[converted_int.columns] = converted_int
optimized_dataset[converted_float.columns] = converted_float

# 7
get_memory_stat_by_column(dataset)
print(mem_usage(dataset))
print(mem_usage(optimized_dataset))
optimized_dataset.info(memory_usage='deep')

# 8
# отобрать свои 10 колонок
need_column = dict()
column_names = ['Job Id', 'Salary Range', 'Country',
                 'latitude', 'longitude', 'Company Size',
                 'Job Posting Date', 'Preference', 'Contact', 'Work Type']
opt_dtypes = optimized_dataset.dtypes
for key in column_names:
    if key in optimized_dataset.columns:
        need_column[key] = optimized_dataset.dtypes[key]
        print(f"{key}: {optimized_dataset.dtypes[key]}")

with open("dtypes_2.json", mode="w") as file:
    dtype_json = need_column.copy()
    for key in dtype_json.keys():
        dtype_json[key] = str(dtype_json[key])

    json.dump(dtype_json, file)

# 9. Чтение и оптимизация данных
read_and_optimized = pd.read_csv(file_name, usecols=lambda x: x in column_names, dtype=need_column)


#1. Гистограмма частоты средней зарплаты
# Преобразование диапазона зарплат в числовые значения
read_and_optimized['Salary Range'] = read_and_optimized['Salary Range'].str.replace('K', '').str.replace('$', '').str.split('-').apply(lambda x: (int(x[0]), int(x[1])))

# Создание нового столбца со средней зарплатой
read_and_optimized['Average Salary'] = read_and_optimized['Salary Range'].apply(lambda x: (x[0] + x[1]) / 2)

# Построение гистограммы
plt.figure(figsize=(10, 6))
plt.hist(read_and_optimized['Average Salary'], bins=30, color='blue', alpha=0.7)
plt.title('Распределение средней зарплаты')
plt.xlabel('Средняя зарплата тыс.$')
plt.ylabel('Частота')
plt.grid(axis='y')
plt.show()
#2. Столбчатая диаграмма по странам

# Подсчет количества вакансий по странам
country_counts = read_and_optimized['Country'].value_counts()

# Построение столбчатой диаграммы
plt.figure(figsize=(16, 8))
country_counts.plot(kind='bar', color='orange')
plt.title('Количество вакансий по странам')
plt.xlabel('Страна')
plt.ylabel('Количество вакансий')
plt.xticks(rotation=90, fontsize=6)
plt.show()
#3. Круговая диаграмма по типам работы

# Подсчет количества вакансий по типам работы
work_type_counts = read_and_optimized['Work Type'].value_counts()

# Построение круговой диаграммы
plt.figure(figsize=(8, 8))
work_type_counts.plot(kind='pie', autopct='%1.1f%%', startangle=90, colors=['lightblue','brown', 'lightgreen', 'lightcoral', 'yellow'])
plt.title('Распределение вакансий по типам работы')
plt.ylabel('')
plt.show()
#4. Корреляция между зарплатой и размером компании

# Построение графика рассеяния
plt.figure(figsize=(10, 6))
sns.scatterplot(data=read_and_optimized, x='Work Type', y='Company Size', alpha=0.6)
plt.title('Корреляция между размером компании и типом работы')
plt.xlabel('Тип работы')
plt.ylabel('Размер компании')
plt.grid()
plt.show()
#5. Линейный график по датам публикации вакансий

# Преобразование даты в формат datetime
read_and_optimized['Job Posting Date'] = pd.to_datetime(read_and_optimized['Job Posting Date'])

# Подсчет количества вакансий по датам
date_counts = read_and_optimized['Job Posting Date'].value_counts().sort_index()

# Построение линейного графика
plt.figure(figsize=(12, 6))
date_counts.plot(kind='line', color='purple')
plt.title('Количество вакансий по датам публикации')
plt.xlabel('Дата публикации')
plt.ylabel('Количество вакансий')
plt.grid()
plt.show()
#6. График распределения вакансий по предпочтениям

# Подсчет количества вакансий по предпочтениям
preference_counts = read_and_optimized['Preference'].value_counts()

# Построение столбчатой диаграммы
plt.figure(figsize=(10, 6))
preference_counts.plot(kind='bar', color='teal')
plt.title('Количество вакансий по предпочтениям')
plt.xlabel('Предпочтение')
plt.ylabel('Количество вакансий')
plt.xticks(rotation=0)
plt.show()

