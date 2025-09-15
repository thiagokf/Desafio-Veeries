from bs4 import BeautifulSoup # extração de dados de HTML
import requests # requisições HTTP
import pandas as pd # manipulação de dados
from datetime import datetime
import urllib3 
import os # manipulação de diretórios e arquivos

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
                dados.append([celula.get_text(strip=True) for celula in celulas[1:]])

    df_bronze = pd.DataFrame(
        dados,
        columns=["Programação", "DUV", "Berço", "Embarcação", "IMO", "LOA", "DWT", "Bordo", "Sentido", "Agência", "Operador", "Mercadoria", "Atracação", "Chegada", "Janela Operacional", "	Prancha", "Tons/Dia", "Previsto", "Realizado", 'Saldo Operador', 'Saldo Total'] 
    )

    # mudanças em linhas especificas que tiveram erro na extração
    df_bronze.loc[2, 'Chegada'] = '14/09/2025 20:50'

    df_bronze.loc[13, 'Agência'] = 'FORTENAVE'
    df_bronze.loc[13, 'Mercadoria'] = 'CLORETOS DE POTASSIO'
    df_bronze.loc[13, 'Sentido'] = 'Imp'
    df_bronze.loc[13, 'Saldo Total'] = '10.690,050 Tons'

    df_bronze.loc[16, 'Agência'] = 'AGENCIA MARITIMA NAABSA'
    df_bronze.loc[16, 'Mercadoria'] = 'UREIA'
    df_bronze.loc[16, 'Sentido'] = 'Imp'
    df_bronze.loc[16, 'Saldo Total'] = '5.553,700 Tons.'

    df_bronze.loc[18, 'Agência'] = 'ZPORT'
    df_bronze.loc[18, 'Mercadoria'] = 'SULFATO DE AMONIO'
    df_bronze.loc[18, 'Sentido'] = 'Imp'
    df_bronze.loc[18, 'Saldo Total'] = '15.209,380 Tons.'
    df_bronze.loc[18, 'Chegada'] = '10/09/2025 03:15'

    if not os.path.exists('camada_bronze'): # cria a pasta se não existir
        os.makedirs('camada_bronze')
    df_bronze.to_csv(f'camada_bronze/paranagua_lineup_{data_formatada}.csv', index=False, encoding="utf-8")

    # Filtragem das colunas desejadas e adição da coluna Porto
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

    # tratamento de linhas específicas que tiveram erro na extração
    df_bronze.loc[12, 'Sentido'] = 'EMB'
    df_bronze.loc[12, 'Peso'] = '2400'
    df_bronze.loc[12, 'Mercadoria'] = 'ETHANOL'
    
    nova_linha = {
        "Navio": "CASTILLO DE TEBRA (REB)",
        "Bandeira": "BRASILEIRA",
        "Com/Len": "1206.5",
        "Nav": "Cabo",
        "Chegada": "14/09/2025 05:00:00",
        "Carimbo": "",
        "Agência": "ORION OPERACOES PORTUARIAS LTDA",
        "Sentido": "DESC",
        "Mercadoria": "PRODUTOS QUIMICOS",
        "Peso": "6210",
        "Viagem": "4209-72025",
        "DUV": "410842025",
        "P": "B",
        "Terminal": "ILHA",
        "IMO": "09753636",
        "LOA": "",
    }
    df_bronze = pd.concat([df_bronze, pd.DataFrame([nova_linha])], ignore_index=True)
    df_bronze.loc[60, 'Sentido'] = 'EMB'
    df_bronze.loc[60, 'Peso'] = '8000'
    df_bronze.loc[60, 'Mercadoria'] = 'FERTILIZANTES'
    nova_linha = {
        "Navio": "RESOLUTE BAY",
        "Bandeira": "ILHA DE MAN",
        "Com/Len": "18610",
        "Nav": "Long",
        "Chegada": "05/09/2025 01:30:00",
        "Carimbo": "3935-7",
        "Agência": "ALPHAMAR AGÊNCIA MARÍTIMA  LTDA.",
        "Sentido": "DESC",
        "Mercadoria": "FERTILIZANTES",
        "Peso": "24900",
        "Viagem": "3966-92025",
        "DUV": "391782025",
        "P": "B",
        "Terminal": "35/37",
        "IMO": "09626314",
        "LOA": "",
    }
    df_bronze = pd.concat([df_bronze, pd.DataFrame([nova_linha])], ignore_index=True)
    # Filtragem das colunas desejadas e adição da coluna Porto
    df_santos_filtrado = df_bronze[["Agência", "Mercadoria", "Sentido", "Peso", "Chegada"]]
    df_santos_filtrado["Porto"] = "Santos"
    df_santos_filtrado = df_santos_filtrado.drop(5)
    df_santos_filtrado.to_csv('camada_bronze/lineup_santos_filtrado.csv', index=False, encoding="utf-8")


def unir_dados():
    df_paranagua = pd.read_csv('camada_bronze/lineup_paranagua_filtrado.csv', encoding="utf-8")
    df_santos = pd.read_csv('camada_bronze/lineup_santos_filtrado.csv', encoding="utf-8")

    # Padronização dos dados
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
    df_prata['Volume'] = df_prata['Volume'].str.replace(' Tons', '').str.replace(' Tons.', '').str.replace('.', '').str.replace(',', '.').astype(float) # para futuras operações

    if not os.path.exists('camada_prata'): # cria a pasta se não existir
        os.makedirs('camada_prata')
    df_prata.to_csv(f'camada_prata/dados_processados_lineup_{data_formatada}.csv', index=False, encoding="utf-8")

def carregar_dados_prata(caminho_prata):

    lista_arquivos = [os.path.join(caminho_prata, f) for f in os.listdir(caminho_prata) if f.endswith('.csv')]
    if not lista_arquivos:
        print("Nenhum arquivo encontrado na camada Prata.")
        return pd.DataFrame()
        
    lista_df = [pd.read_csv(f) for f in lista_arquivos]
    df_prata_completo = pd.concat(lista_df, ignore_index=True)
    df_prata_completo.drop_duplicates(inplace=True)
    
    return df_prata_completo

def agregar_dados_ouro(df_prata):
    df_ouro = df_prata.groupby(
        ['Produto', 'Sentido', 'Porto']
    ).agg(
        # soma a coluna 'Volume' de acordo com produto, sentido e porto
        volume_total=('Volume', 'sum')
    ).reset_index()

    return df_ouro
    
def executar_camada_ouro():
    caminho_prata = 'camada_prata'
    caminho_ouro = 'camada_ouro'

    if not os.path.exists(caminho_ouro): # cria a pasta se não existir
        os.makedirs(caminho_ouro)

    # carrega os dados da camada prata
    df_prata = carregar_dados_prata(caminho_prata)
    # agrega os dados e atualiza o volume transportado
    df_ouro = agregar_dados_ouro(df_prata)

    nome_arquivo_ouro = 'volume_diario_agregado.csv'
    caminho_arquivo_ouro = os.path.join(caminho_ouro, nome_arquivo_ouro)
    
    df_ouro.to_csv(caminho_arquivo_ouro, index=False)

coleta_dados_paranagua()
coleta_dados_santos()
unir_dados()
executar_camada_ouro()

