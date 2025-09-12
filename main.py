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


tabela_correta = {"agencia": [], "produto": [], "sentido": [], "porto": [], "volume": [], "data_chegada": []}

# def ColetaDadosParanagua():
url_paranagua = 'https://www.appaweb.appa.pr.gov.br/appaweb/pesquisa.aspx?WCI=relLineUpRetroativo'
response = requests.get(url_paranagua)

soup = BeautifulSoup(response.text, 'html.parser')
dados = soup.find('form')
tabelas = []
count = 0
for i in dados:
    count += 1
    if count == 3:
        break
    tabelas.append(i)
print(tabelas[0])
dados = []
'''for i in tabelas:
    linhas = i.find_all('tr')
    informacao = []
    for linha in linhas:
        celulas = linha.find_all('td')
        if celulas:
            informacao.append([celula.get_text(strip=True) for celula in celulas[:]])
    dados.append(informacao)

dataframe_bronze = pd.DataFrame(dados)
dataframe_bronze.to_csv('paranagua_lineup.csv', index=False, encoding="utf-8")
print(dados)

print(len(dados))
for j in dados[1]:
    tabela_correta["agencia"].append(j[8])
    tabela_correta["produto"].append(j[7])
    tabela_correta["sentido"].append(j[10])
    tabela_correta["volume"].append(j[14])
    tabela_correta["data_chegada"].append(j[12])
    tabela_correta["porto"].append('Paranaguá')
df = pd.DataFrame(tabela_correta)
df.to_csv('tabela3.csv', index=False, encoding="utf-8")'''

# def ColetaDadosSantos():  # https://www.portodesantos.com.br/informacoes-operacionais/operacoes-portuarias/navegacao-e-movimento-de-navios/navios-esperados-carga
'''
url_santos = "https://www.portodesantos.com.br/informacoes-operacionais/operacoes-portuarias/navegacao-e-movimento-de-navios/navios-esperados-carga"
import urllib3
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
response = requests.get(url_santos, verify=False)
soup = BeautifulSoup(response.text, 'html.parser')
tabelas = soup.find_all('table', id='esperados')
dados = []
for i in tabelas[:4]:
    linhas = i.find_all('tr')
    informacao = []
    for linha in linhas:
        celulas = linha.find_all('td')
        if celulas:
            informacao.append([celula.get_text(strip=True) for celula in celulas[:]])
    dados.append(informacao)
print(len(dados))
dataframe_bronze = pd.DataFrame(dados)
dataframe_bronze.to_csv('santos_lineup.csv', index=False, encoding="utf-8")

for i in dados:
    for j in i:
        tabela_correta["agencia"].append(j[6])
        tabela_correta["produto"].append(j[8])
        tabela_correta["sentido"].append(j[7])
        tabela_correta["porto"].append('Santos')
        tabela_correta["volume"].append(j[9])
        tabela_correta["data_chegada"].append(j[4])
df = pd.DataFrame(tabela_correta)
df.to_csv('tabela_correta.csv', index=False, encoding="utf-8")
'''


'''tabela = soup.find_all('table', class_='padrao font12 table table-bordered table-hover')
print(tabela)
dados = []'''
'''for i in tabela:
    linhas = i.find_all('tr')
    informacao = []
    for linha in linhas:
        celulas = linha.find_all('td')
        if celulas:
            informacao.append(celula.get_text(strip=True) for celula in celulas)
    dados.append(informacao)
print(dados)'''
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
dados_bronze.to_csv('tabela1.csv', index=False, encoding="utf-8")
'''