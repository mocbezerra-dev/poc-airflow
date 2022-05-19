from datetime import datetime, timedelta
from airflow import DAG

from airflow.operators.bash import BashOperator
from airflow.operators.python_operator import PythonOperator

from bs4 import BeautifulSoup
import requests

import pandas as pd
import psycopg2

def _get_smartphones_links():
    URL_CELULARES = 'https://www.buscape.com.br/celular/smartphone?page='
    total_smartphones_links = []

    acc = 1

    while True:
        acc += 1

        url = URL_CELULARES + str(acc)
        response = requests.get(url)
        soup = BeautifulSoup(response.text, 'html.parser')

        links = soup.find_all('a')

        smart_phone_links = [
            link['href']
            for link in links
            if '/celular/smartphone-' in link['href'] or '/celular/pre-venda-smartphone' in link['href']
        ]

        total_smartphones_links += smart_phone_links

        if not smart_phone_links:
            break
    
    return total_smartphones_links

def _get_smartphone_detalhes(url_smartphone):
    url = 'https://www.buscape.com.br' + url_smartphone
    response = requests.get(url)
    soup = BeautifulSoup(response.text, 'html.parser')
    print(url)
    preco = soup.select('div[class*="Price_Price"]')[0]
    preco = preco.find('strong').text
    marca = soup.find('th', text = 'Marca').parent.find('td').find('a')['title']
    modelo = soup.find('th', text = 'Série').parent.find('td').find('a')['title']

    return { 'url': url, 'preco': preco, 'marca': marca, 'modelo': modelo }

def _scrapy_smartphones():
    smartphones_links = _get_smartphones_links()
    smartphones = []

    for link in smartphones_links:
        try:
            smartphones.append(_get_smartphone_detalhes(link))
        except:
            pass
    
    return smartphones

def _validate_data(ti):
    smartphones = ti.xcom_pull(task_ids='scrapy_smartphones')

    df = pd.DataFrame(smartphones)

    df['marca'] = df['marca'].str.upper()

    df['preco'] = df['preco'].str.replace('R$ ', '', regex=False)
    df['preco'] = df['preco'].str.replace('.', '', regex=False)
    df['preco'] = df['preco'].str.replace(',', '.', regex=False)
    df['preco'] = df['preco'].astype(float)

    return df.to_dict('records')

def _get_conn():
    con = psycopg2.connect('postgres://mppgfjhdhjfydi:d4bc0d85c13f94769360bbecfa6281ee1c3f2c77c15bd803a4b45bb2788629d8@ec2-52-3-2-245.compute-1.amazonaws.com:5432/d4fc0dlithqnt7')

    return con

def _insert_update_dw(ti):
    smartphones = ti.xcom_pull(task_ids='validate_data')
    con = _get_conn()

    cur = con.cursor()

    cur.execute('SELECT * FROM dim_marca')
    marcas = cur.fetchall()

    for m in set([s['marca'] for s in smartphones]):
        if m not in [marca[1] for marca in marcas]:
            cur.execute(f"INSERT INTO dim_marca (nome) VALUES ('{m}')")

    cur.execute('SELECT * FROM dim_marca')
    marcas = cur.fetchall()

    cur.execute('SELECT url FROM fato_smartphone')

    urls = cur.fetchall()
    urls = [u[0] for u in urls]

    for smartphone in smartphones:
        if smartphone['url'] not in urls:
            marca_id = [marca[0] for marca in marcas if marca[1] == smartphone['marca']][0]
            url = smartphone['url']
            modelo = smartphone['modelo']
            preco = smartphone['preco']
            urls.append(url)
            cur.execute(f"INSERT INTO fato_smartphone (marca_fk, url, preco, modelo) VALUES ({marca_id}, '{url}', {preco}, '{modelo}')")

    con.commit()

    cur.close()
    con.close()

with DAG(
    'smartphones',
    default_args={
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),
    },
    description='A simple tutorial DAG',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2021, 1, 1),
    catchup=False,
    tags=['example'],
) as dag:

    scrapy_smartphones = PythonOperator(task_id='scrapy_smartphones', python_callable=_scrapy_smartphones)
    validate_data = PythonOperator(task_id='validate_data', python_callable=_validate_data)
    insert_update_dw = PythonOperator(task_id='insert_update_dw', python_callable=_insert_update_dw)


    scrapy_smartphones >> validate_data
    validate_data >> insert_update_dw
