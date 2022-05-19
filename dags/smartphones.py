from datetime import datetime, timedelta
from airflow import DAG

from airflow.operators.bash import BashOperator
from airflow.operators.python_operator import PythonOperator

from bs4 import BeautifulSoup
import requests

def get_smartphones_links():
    URL_CELULARES = 'https://www.buscape.com.br/celular/smartphone?page='
    total_smartphones_links = []

    acc = 18

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

def get_smartphone_detalhes(url_smartphone):
    url = 'https://www.buscape.com.br' + url_smartphone
    response = requests.get(url)
    soup = BeautifulSoup(response.text, 'html.parser')
    print(url)
    preco = soup.select('div[class*="Price_Price"]')[0]
    preco = preco.find('strong').text
    marca = soup.find('th', text = 'Marca').parent.find('td').find('a')['title']
    modelo = soup.find('th', text = 'Série').parent.find('td').find('a')['title']

    return { 'url': url, 'preco': preco, 'marca': marca, 'modelo': modelo }


def test_xcom(ti):
    smartphones = ti.xcom_pull(task_ids='scrapy_smartphones')

    print('SMARTPHONES')
    print(smartphones)



def get_smartphones(ti):
    smartphones_links = get_smartphones_links()
    smartphones = []

    for l in smartphones_links:
        try:
            smartphones.append(get_smartphone_detalhes(l))
        except:
            pass
    
    return smartphones

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
    scrapy_smartphones = PythonOperator(task_id='scrapy_smartphones', python_callable=get_smartphones)
    print_smartphones = PythonOperator(task_id='print_smartphones', python_callable=test_xcom)


    scrapy_smartphones >> print_smartphones
