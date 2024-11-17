import requests
from bs4 import BeautifulSoup

# URL публичного API GitHub для получения информации о пользователе
url = "https://api.github.com/users"

# Получаем данные в формате JSON
json_data = requests.get(url).json()

# Создаем объект BeautifulSoup для формирования HTML
soup = BeautifulSoup("", "html.parser")

# Создаем список для хранения элементов
items_list = soup.new_tag('ul', id='users')

# Обрабатываем данные и добавляем их в HTML
for user in json_data:
    li = soup.new_tag('li')
    li.string = f"Username: {user['login']}, ID: {user['id']}, URL: {user['html_url']}"
    items_list.append(li)

# Добавляем список пользователей в HTML
soup.append(items_list)

# Записываем полученный HTML в файл
with open('github_users.html', 'w', encoding='utf-8') as f:
    f.write(soup.prettify())