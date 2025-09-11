'''

from selenium import webdriver
from selenium.webdriver.common.by import By
import pandas as pd
import time

url = "https://www.appaweb.appa.pr.gov.br/appaweb/pesquisa.aspx?WCI=relLineUpRetroativo"

driver = webdriver.Chrome()

driver.get(url)

time.sleep(10)


tabelas = driver.find_elements(By.TAG_NAME, "table")

data = []
for i, tabela in enumerate(tabelas, start=3):
    table = []
    linhas = tabela.find_elements(By.TAG_NAME, "tr")
    for linha in linhas:
        table.append([c.text for c in linha.find_elements(By.TAG_NAME, "td")])
    data.append(table)

driver.quit()

df = pd.DataFrame(data)
df.to_csv('tabela1.csv', index=False, encoding="utf-8")

'''

from bs4 import BeautifulSoup
import requests
import pandas as pd


url_paranagua = 'https://www.appaweb.appa.pr.gov.br/appaweb/pesquisa.aspx?WCI=relLineUpRetroativo'
response = requests.get(url_paranagua)

soup = BeautifulSoup(response.text, 'html.parser')
tabelas = soup.find_all('table', class_='table table-bordered table-striped table-hover')
print(len(tabelas))
dados = []
for tabela in tabelas:
    linhas = tabela.find_all('tr')
    tabela_dados = []
    for linha in linhas:
        celulas = linha.find_all('td')
        if celulas:
            tabela_dados.append([celula.get_text(strip=True) for celula in celulas[1:]])
    dados.append(tabela_dados)

dados2 = {"produto": [], "sentido": [], "porto": []}
for i in dados:
    print(len(i))
    for j in i:
        print(len(j))
        if (len(j)) < 21:
            dados2["produto"].append(j[0])
            dados2["sentido"].append(j[3])
        else:
            dados2["produto"].append(j[8])
            dados2["sentido"].append(j[11])
        dados2["porto"].append('Paranaguá')
print(dados2)
df = pd.DataFrame(dados2)
df.to_csv('tabela3.csv', index=False, encoding="utf-8")

'''soup = BeautifulSoup(response.text, 'html.parser')
tabelas = soup.find('table', class_='table table-bordered table-striped table-hover')

data = {
    "produto": [],
    "sentido": [],
    "porto": []
    }
b = []
for i in tabelas:
    linhas = tabelas.find_all('tr')
    for linha in linhas:
        celulas = linha.find_all('td')
        for celula in celulas:
            dado = celulas[i].get_text(strip=True)
            b.append(dado)
dados_bronze = pd.DataFrame(data)
dados_bronze.to_csv('tabela1.csv', index=False, encoding="utf-8")'''