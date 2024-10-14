# Função para buscar concelho e distrito
import pandas as pd
import requests
import mysql.connector
from flask import Flask, jsonify


# Função para buscar concelho e distrito
def buscar_info(codigo_postal):
    url = f'https://www.cttcodigopostal.pt/api/v1/d53a021d2af0453b96415111a5b62348/{codigo_postal}'  # API endpoint

    # Substitua com a chave correta, se necessário
    headers = {
        'Authorization': 'Bearer d53a021d2af0453b96415111a5b62348'  # Adicione a chave de API, se necessário
    }

    response = requests.get(url, headers=headers)

    # Imprimir a resposta para depuração
    print(f"Response for {codigo_postal}: {response.text}")

    if response.status_code == 200:
        try:
            data = response.json()
            # Verifique se os campos 'concelho' e 'distrito' estão no JSON retornado
            if 'concelho' in data and 'distrito' in data:
                return data['concelho'], data['distrito']
            else:
                print(f"Erro: Campos 'concelho' ou 'distrito' não encontrados para {codigo_postal}.")
                return None, None
        except ValueError:
            print("Erro ao decodificar JSON.")
            return None, None
    else:
        print(f"Erro: {response.status_code} - {response.reason}")
        return None, None


# Conexão ao banco de dados
def conectar_db():
    return mysql.connector.connect(
        host='localhost',
        user='root',
        password='«03u29ywhj934ygj<w39ohj40<~32q',
        database='codigospostais'
    )


# Criar tabela no MySQL
def criar_tabela():
    conn = conectar_db()
    cursor = conn.cursor()
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS codigos_postais (
            id INT AUTO_INCREMENT PRIMARY KEY,
            codigo_postal VARCHAR(10),
            concelho VARCHAR(255),
            distrito VARCHAR(255)
        )
    ''')
    conn.commit()
    cursor.close()
    conn.close()


# Enriquecer dados e armazenar no banco
def enriquecer_dados(csv_file):
    df = pd.read_csv(csv_file)

    # Renomear a coluna para 'codigo_postal' para facilitar o acesso
    df.rename(columns={'cp7': 'codigo_postal'}, inplace=True)

    # Exibir as primeiras linhas do DataFrame para depuração
    print(df.head())

    conn = conectar_db()
    cursor = conn.cursor()

    for index, row in df.iterrows():
        codigo_postal = row['codigo_postal']  # Agora acessamos como 'codigo_postal'
        concelho, distrito = buscar_info(codigo_postal)

        if concelho and distrito:
            cursor.execute('''
                INSERT INTO codigos_postais (codigo_postal, concelho, distrito) 
                VALUES (%s, %s, %s)
            ''', (codigo_postal, concelho, distrito))

    conn.commit()
    cursor.close()
    conn.close()


# Criar API Flask
app = Flask(__name__)


@app.route('/codigos_postais', methods=['GET'])
def get_codigos_postais():
    conn = conectar_db()
    cursor = conn.cursor(dictionary=True)
    cursor.execute('SELECT * FROM codigos_postais')
    results = cursor.fetchall()
    cursor.close()
    conn.close()
    return jsonify(results)


if __name__ == '__main__':
    criar_tabela()
    enriquecer_dados('..//..//..//Desktop//codigos_postais.csv')  # Substitua pelo caminho do seu CSV
    app.run(debug=True)

df.to_csv('dados_enriquecidos.csv', index=False)