from bs4 import BeautifulSoup

with open  ('C:/Users/Григорий/PycharmProjects/pythonProject2/DataEngineering/Practice3/1.html','r',encoding='utf-8') as file:
    html_content = file.read()

soup = BeautifulSoup(html_content, "html.parser")
print(soup)

builds = soup.find_all("div", attrs={'class':'build-wrapper'})
items = []
for build in builds:
    item={}
    item['city']=build.find_all("span")[0].get_text().split(":")[1].strip()
    item['id']=build.h1['id']
    item['type']=build.h1.get_text().split(":")[1].strip()
    address_temp=build.p.get_text().split("Улица:")[1].split("Индекс:")
    item['address']=address_temp[0]
    item['index']=address_temp[1]
    item['floors']=build.find_all("span", attrs={'class':'floors'})[0].get_text().split(":")[1].strip()
    item['year']=int(build.find_all())


    print(item)