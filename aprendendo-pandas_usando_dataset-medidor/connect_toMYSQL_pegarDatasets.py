import pymysql
import pandas as pd
import json

import pymysql.cursors

TABLE_BACKUP = 'back_up'


def connect():
    try:
        connection = pymysql.connect(
            host="database-medidor.cnq0qmuscd2t.us-east-1.rds.amazonaws.com",
            user='LINK',
            password='Wsl131287@',
            database="db_medidor"
        )

        return connection

    except pymysql.MySQLError as error:
        return error


def viewdatabase(n_rows):
    try:
        connection = connect()

    except pymysql.MySQLError as error:
        print(f'Erro de conexão ao banco de dados: ({error}).')

    with connection:
        with connection.cursor() as cursor:
            query = f'SELECT * FROM {TABLE_BACKUP} LIMIT {n_rows};'
            cursor.execute(query)

            rows = cursor.fetchall()

        return rows


def getDataframeByDate(date):

    try:
        connection = connect()

    except pymysql.MySQLError as error:
        print(f'Erro de conexão ao banco de dados: ({error}).')

    with connection:
        with connection.cursor() as cursor:
            query = f'SELECT * FROM {TABLE_BACKUP} WHERE data = %s;'
            cursor.execute(query, date)
            datas = cursor.fetchall()
            column_names = [desc[0] for desc in cursor.description]

    # pegar cada tupla do conjunto de tuplas (datas) e aplicar um zip que contém
    # o nome da coluna com o seu valor, converter este objeto zip para um dict
    # e cada uma dessas conversões vai pra lista, que ao final temos uma lista
    # de dicionários, o que é facilmente convertido em json
    dados_completos = [dict(zip(column_names, data)) for data in datas]

    # aqui convertemos o dict (dados_completos) para uma string json, default = str
    # força que todo e qualquer dado não serealizável vire uma string,
    # no nosso caso o objeto datetime da hora de inserção entra nessa
    # isso garantirá que na conversão para o dataframe pandas, este campo volte
    # a ser um datetime contendo data e hora

    json_data = json.dumps(dados_completos, default=str)

    return json_data


if __name__ == "__main__":
    print(getDataframeByDate('22/1/2025'))
