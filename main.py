from bs4 import BeautifulSoup
import requests
import pandas as pd
from datetime import datetime
import urllib3 
import os

data = datetime.now()
data_formatada = data.strftime('%Y-%m-%d')

def coleta_dados_paranagua():
    url_paranagua = 'https://www.appaweb.appa.pr.gov.br/appaweb/pesquisa.aspx?WCI=relLineUpRetroativo'
    response = requests.get(url_paranagua)
    soup = BeautifulSoup(response.text, 'html.parser')

    # extração das tabelas da página
    tabelas = soup.find('table', class_='table table-bordered table-striped table-hover')
    dados = []
    for tabela in tabelas:
        linhas = tabela.find_all('tr')
        for linha in linhas:
            celulas = linha.find_all('td')
            if celulas:
                celula_texto = ([celula.get_text(strip=True) for celula in celulas[1:]])

                if len(celula_texto) == 21:
                    dados.append(celula_texto)
                elif len(celula_texto) > 1:  # formata as linhas que não foram extraídas corretamente
                    for i in range(8):
                        celula_texto.insert(i, '')
                    dados.append(celula_texto)
    df_bronze = pd.DataFrame(
        dados,
        columns=["Programação", "DUV", "Berço", "Embarcação", "IMO", "LOA", "DWT", "Bordo", "Sentido", "Agência", "Operador", "Mercadoria", "Atracação", "Chegada", "Janela Operacional", "	Prancha", "Tons/Dia", "Previsto", "Realizado", 'Saldo Operador', 'Saldo Total'] 
    )

    if not os.path.exists('camada_bronze'): # cria a pasta se não existir
        os.makedirs('camada_bronze')
    df_bronze.to_csv(f'camada_bronze/paranagua_lineup_{data_formatada}.csv', index=False, encoding="utf-8")

    # filtragem das colunas desejadas e adição da coluna Porto
    colunas_desejadas = ["Agência", "Mercadoria", "Sentido", "Saldo Total", "Chegada"]
    df_filtrado = df_bronze[colunas_desejadas]
    df_filtrado["Porto"] = "Paranaguá"
    df_filtrado.to_csv('camada_bronze/lineup_paranagua_filtrado.csv', index=False, encoding="utf-8")

def coleta_dados_santos(): 
    url_santos = "https://www.portodesantos.com.br/informacoes-operacionais/operacoes-portuarias/navegacao-e-movimento-de-navios/navios-esperados-carga"

    # desabilita o aviso de certificado SSL para requisições HTTPS
    urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
    response = requests.get(url_santos, verify=False)
    soup = BeautifulSoup(response.text, 'html.parser')

    # extração das tabelas da página
    tabelas = soup.find_all('table', id='esperados')
    dados = []
    for i in tabelas[:5]:
        linhas = i.find_all('tr')
        for linha in linhas:
            celulas = linha.find_all('td')
            if celulas:
                dados.append([celula.get_text(strip=True) for celula in celulas[:]])
    df_bronze = pd.DataFrame(
        dados,
        columns=["Navio", "Bandeira", "Com/Len", "Nav", "Chegada", "Carimbo", "Agência", "Sentido", "Mercadoria", "Peso", "Viagem", "DUV", "P", "Terminal", "IMO", "LOA"] 
    )
    if not os.path.exists('camada_bronze'):
        os.makedirs('camada_bronze')
    df_bronze.to_csv(f'camada_bronze/santos_lineup_{data_formatada}.csv', index=False, encoding="utf-8")

    # filtragem das colunas desejadas e adição da coluna Porto
    df_santos_filtrado = df_bronze[["Agência", "Mercadoria", "Sentido", "Peso", "Chegada"]]
    df_santos_filtrado["Porto"] = "Santos"
    df_santos_filtrado = df_santos_filtrado.drop(5)
    df_santos_filtrado.to_csv('camada_bronze/lineup_santos_filtrado.csv', index=False, encoding="utf-8")

def processar_dados_prata():
    df_paranagua = pd.read_csv('camada_bronze/lineup_paranagua_filtrado.csv', encoding="utf-8")
    df_santos = pd.read_csv('camada_bronze/lineup_santos_filtrado.csv', encoding="utf-8")
    
    # padronização dos dados
    padroes_importacao = ['Imp', 'DESC']
    padroes_exportacao = ['Exp', 'EMB']
    padroes_exp_imp = ['Imp/Exp', 'EMBDESC']
    df_paranagua['Sentido'] = df_paranagua['Sentido'].replace(padroes_importacao, 'Imp')
    df_paranagua['Sentido'] = df_paranagua['Sentido'].replace(padroes_exportacao, 'Exp')
    df_paranagua['Sentido'] = df_paranagua['Sentido'].replace(padroes_exp_imp, 'Exp/Imp')
    df_santos['Sentido'] = df_santos['Sentido'].replace(padroes_importacao, 'Imp')
    df_santos['Sentido'] = df_santos['Sentido'].replace(padroes_exportacao, 'Exp')
    df_santos['Sentido'] = df_santos['Sentido'].replace(padroes_exp_imp, 'Exp/Imp')

    df_santos.rename(columns={
        'Peso': 'Volume',
        'Chegada': 'Data_chegada',
        'Agência': 'Agencia',
        'Mercadoria': 'Produto',
        'Sentido': 'Sentido',
        'Porto': 'Porto'
    }, inplace=True)

    df_paranagua.rename(columns={
        'Saldo Total': 'Volume',
        'Chegada': 'Data_chegada',
        'Agência': 'Agencia',
        'Mercadoria': 'Produto',
        'Sentido': 'Sentido',
        'Porto': 'Porto'
    }, inplace=True)

    df_final = pd.concat([df_paranagua, df_santos], ignore_index=True)
    df_final.to_csv('prata_dados_lineup.csv', index=False, encoding="utf-8")
    df_prata = pd.read_csv('prata_dados_lineup.csv', encoding="utf-8")
    df_prata['Volume'] = df_prata['Volume'].str.replace(' Tons', '').str.replace(' Tons.', '').str.replace(' Movs', '').str.replace('.', '').str.replace(',', '.').astype(float) # para futuras operações

    if not os.path.exists('camada_prata'): # cria a pasta se não existir
        os.makedirs('camada_prata')
    df_prata.to_csv(f'camada_prata/dados_processados_lineup_{data_formatada}.csv', index=False, encoding="utf-8")

def carregar_dados_prata(caminho_prata):
    lista_arquivos = [os.path.join(caminho_prata, f) for f in os.listdir(caminho_prata) if f.endswith('.csv')]
    lista_df = [pd.read_csv(f) for f in lista_arquivos] # carrega todos os arquivos CSV da camada prata
    df_prata_completo = pd.concat(lista_df, ignore_index=True)
    df_prata_completo.drop_duplicates(inplace=True) # remove as duplicatas, atualizando o dataframe e preservando o historico
    
    return df_prata_completo # retorna o dataframe preservado e atualizado

def agregar_dados_ouro(df_prata):
    df_ouro = df_prata.groupby(
        ['Produto', 'Sentido', 'Porto']
    ).agg(
        volume_total=('Volume', 'sum')  # soma a coluna 'Volume' de acordo com produto, sentido e porto
    ).reset_index()

    return df_ouro
    
def processar_dados_ouro():
    caminho_prata = 'camada_prata'
    caminho_ouro = 'camada_ouro'

    if not os.path.exists(caminho_ouro): # cria a pasta se não existir
        os.makedirs(caminho_ouro)

    df_prata = carregar_dados_prata(caminho_prata) # carrega todos os arquivos CSV da camada prata, atualiza e preserva o historico
    df_ouro = agregar_dados_ouro(df_prata) # agrega os dados e atualiza o volume transportado 

    nome_arquivo_ouro = 'volume_diario_agregado.csv' 
    caminho_arquivo_ouro = os.path.join(caminho_ouro, nome_arquivo_ouro) 
    
    df_ouro.to_csv(caminho_arquivo_ouro, index=False)

coleta_dados_paranagua()
coleta_dados_santos()
processar_dados_prata()
processar_dados_ouro()

