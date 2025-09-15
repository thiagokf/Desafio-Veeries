# Desafio-Veeries
O desafio consiste em desenvolver um processo em python que colete e atualize diariamente uma base de
dados com os volumes diários transportados pelos navios, de acordo com o produto, sentido
(exportação e importação) e porto (Paranaguá, Santos), preservando o historico dos dados.

## Ferramentas
Para esse desafio, utilizei as bibliotecas:
> - requests, para requisição do URL;
> - BeautifulSoup, para extração dos dados de HTML;
> - pandas, para manipulação dos dados;
> - urllib3, para desabilita o aviso de certificado SSL para requisição do URL de Santos;
> - os, para organizar os arquivos;
Essas foram as ferramentas que eu identifiquei serem uteis para o desafio e que eu tenho mais afinidade.

## Explicação das etapas da solução
### Extração dos dados (**Bronze**)
Nessa camada, utilizei do **requests** e do **BeatifulSoup** para conseguir acesso e extrair os dados das tabelas presentes nos sites.
Depois as organizei nas colunas devidas e coloquei em um arquivo .csv.

### Organização dos dados (**Prata**)
A função *processar_dados_prata()* e as funções de coleta de dados são responsáveis por essa camada. Nelas, eu filtro os principais
dados pedidos para o desafio, adiciono a coluna 'Porto', padronizo as colunas e os dados e, por fim, uno as tabelas em uma nova base de dados, agora com as colunas pedidas e os dados processados.

### Enriquecimento dos dados (**Ouro**)
Nessa camada, participa as três ultimas funções implementadas. A função *carregar_dados_prata()*, carrega e une as tabelas .csv de nivel prata coletadas diariamente, 
depois elimina as duplicatas. Assim, preservando os dados já salvos e atualizando com os novos. 
A função *agregar_dados()* é a que atualiza o volume da base de dados de acordo com os produto, sentido e porto. Ela agrega os dados em comum das três colunas
e faz a soma dos volumes desses dados. Obtendo, assim, o volume transportado para cada ocasião.
Por fim, a função *processar_dados_ouro()* conecta as funções anteriores e organiza os dados eriquecidos em uma tabela .csv.

## Dificuldades
No desafio, tive dificuldade principalmente na importação das tabelas dos URLs. As tabelas do porto de Paranaguá tinham formatações diferentes, o que
dificultava na extração e organização dos dados. Por isso, optei por pegar os dados apenas de uma das tabelas e fazer o tratamento apenas dela. 
Também, nos dados de Santos, algumas linhas representavam dois movimentos de uma mesma agência, o que fazia com que a celula extraída tivesse valor duplicado.
Infelizmente, não encontrei uma forma de tratar isso, portanto deixei assim no resultado final.

## Considerações finais
A realização deste desafio permitiu-me aplicar e consolidar conhecimentos em ETL e aprender mais sobre manipulação e analise de dados.
Como melhorias futuras, a solução poderia incluir a automação da rotina de coleta e processamento diário utilizando ferramentas como o cronjob ou o Apache Airflow, que fariam
o processamento automaticamente.