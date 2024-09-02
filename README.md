# Desafio de Consumo de Dados
## Pipeline de Dados

### 1. **Star Wars Data Pipeline API**

### 2. **Descrição**
   ```
   Esta aplicação é uma API desenvolvida com Flask que consome dados da API pública de Star Wars (SWAPI: https://swapi.dev/), faz o armazenamento deles em um banco de dados SQLite e expõe rotas para acessar algumas informações: 
   - planetas com os climas mais quentes
   - personagens que mais aparecem
   - naves mais rápidas do universo Star Wars. 
   Informações essas retiradas dos dados da propria API SWAPI.
   ```

### 3. **Estrutura do Projeto**
   ```
├── starwars-data-pipeline/
│   ├── api/
│   │   ├── __init__.py
│   │   ├── app.py
│   ├── data_pipeline/
│   │   ├── __init__.py
│   │   ├── pipeline.py
│   │   ├── tests.py
│   │   ├── starwars.db*
│   ├── .gitignore
│   ├── Makefile
│   ├── requirements.txt
│   ├── venv/*
├── README.md

*arquivos de criação

   ```
### 4. **Pré-requisitos**
   ```
   - Python 3.8 ou superior
   - venv (opcional, mas recomendado)
   - Git (para clonar o repositório)
   - recomendado rodar com WSL, caso seja usuario Windows
   ```

### 5. **Instalação**

   1. Clone o repositório:
      ```
      git clone git@github.com:alecsevla/api-swapi.git

      ```
   2. Crie e ative um ambiente virtual:
      ```
      python -m venv venv
      source venv/bin/activate  # No Windows: venv\Scripts\activate
      ```
   3. Instale as dependências:
      ```
      pip install -r requirements.txt
      ```

### 6. **Executando a Pipeline de Dados**
   ```
   python data_pipeline/pipeline.py
   ```
   Esse script irá buscar dados da SWAPI, criar as tabelas no banco de dados SQLite e popular com as informações.
   Garanta que o pipeline.py seja rodado antes das proximas execuções, para que o banco esteja devidamente preenchido.

### 7. **Executando o teste dos Dados**
   ```
   python data_pipeline/tests.py
   ```
   Esse script irá validar se há dados da SWAPI nas tabelas criadas no banco de dados SQLite.

### 8. **Executando a API**
   Como rodar a API Flask:
   ```
   python api/app.py
   ```
   A API estará disponível em `http://127.0.0.1:5000/`.

### 9. **Rotas da API**
   Liste as rotas disponíveis e o que cada uma faz:
   - `GET /` - Página inicial da API com links para as rotas.
   - `GET /fastest_ships` - Retorna as 3 naves mais rápidas.
   - `GET /appears_most` - Retorna os 5 personagens que mais aparecem.
   - `GET /hottest_planet` - Retorna os 3 planetas mais quentes.

### 10. **Estrutura do Banco de Dados**
   - **planets**   : Armazena informações sobre os planetas.
   - **characters**: Armazena informações sobre os personagens.
   - **starships** : Armazena informações sobre as naves.
   - **films**     : Armazena informações sobre os filmes.
   - **species**   : Armazena informações sobre as espécies.
   - **vehicles**  : Armazena informações sobre os veículos.

### 11. **Erros Comuns e Soluções**
   - **Erro: Database file not found**: Verifique se o caminho para o banco de dados está correto e se o banco de dados foi gerado na etapa de pipeline.
   - **Problema: Consulta retorna vazio ou não encontra dados esperados**: Verifique a existência dos dados no banco. 

### 12. **Considerações Finais**
   - Para customizações adicionais ou melhorias, explore o código nas pastas `api` e `data_pipeline`.
   - Usuarios Unix/Linux tem a possibilidade de usar o Makefile. Caso não seja um usuario Unix/Linux você pode: fazer uso do WSL(https://learn.microsoft.com/pt-br/windows/wsl/install), ou usar o Git Bash(https://gitforwindows.org/) .
