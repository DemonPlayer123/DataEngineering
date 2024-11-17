
def read_file():
    with open("C:/Users/Григорий/PycharmProjects/pythonProject2/DataEngineering/Practice1/second_task.txt", encoding="utf-8") as file:
        lines = file.readlines()
        table =[]
        for line in lines:
            words = line.strip().split(" ")
            table.append(list(map(int, words)))
        return table

def first_operation(table):
    result = [
        sum(abs(num) for num in row if num**2 < 100000)
        for row in table
    ]

    return result

def second_operation(result):
    if result:
        return sum(result)/len(result)
    return 0

def write_to_file(column, avg):
    with open("second_task_result.txt", "w", encoding="utf-8") as file:
        for num in column:
            file.write(f"{num}\n")

        file.write(f"\nСреднее арифметическое: {avg}\n")


table = read_file()
result = first_operation(table)
avg = second_operation(result)
print(first_operation(table))
print(second_operation(result))
write_to_file(result, avg)