from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago
import requests
from bs4 import BeautifulSoup
import csv
from datetime import datetime

default_args = {
    'owner': 'Jawad',
}

dag = DAG(
    'extract_and_store_data',
    default_args=default_args,
    description='A DAG to extract links, titles, and descriptions from articles and store the data',
    tags=['Web Scraping']
)


    def extract_links_and_metadata():
    """
    Function to extract links from sources, and get title and description of articles.
    """
    sources = [
        'https://www.dawn.com','https://www.theguardian.com/international'
    ]
    data = []
    for source in sources:
        response = requests.get(source)
        if response.status_code == 200:
            soup = BeautifulSoup(response.text, 'html.parser')
            articles = soup.find_all('article')
            for article in articles:
                link = article.find('a', href=True)
                if link:
                    url = link['href']
                    title = article.find('h2').get_text() if article.find('h2') else ""
                    description = article.find('p').get_text() if article.find('p') else ""
                    data.append({'url': url, 'title': title, 'description': description, 'source': source})
    return data


def store_data_in_csv(**kwargs):
    """
    Function to store extracted data in a CSV file.
    """
    data = kwargs['ti'].xcom_pull(task_ids='extract_links')
    if not data:
        raise ValueError("No data received from extract_links task.")
    
    csv_file = './data.csv'  # Change this to the desired path
    try:
        with open(csv_file, 'w', newline='', encoding='utf-8') as csvfile:
            fieldnames = ['url', 'title', 'description', 'source']
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            writer.writeheader()
            for item in data:
                writer.writerow(item)
        print(f"Data successfully stored in CSV file: {csv_file}")
    except Exception as e:
        print(f"Error occurred while writing to CSV file: {e}")
        raise e

def push_to_dvc():
    os.system('dvc add ./data.csv')
    os.system('dvc push')

 
def git_push():
    os.system('git status')
    os.system('git pull')
    os.system('git add .')
    os.system('git commit -m "Updated data.csv file."')
    os.system('git push origin main')
    os.system('git status')

task1 = PythonOperator(
    task_id='extract_links',
    python_callable=extract_links_and_metadata,
    dag=dag,
)

task2 = PythonOperator(
    task_id='store_data_in_csv',
    python_callable=store_data_in_csv,
    provide_context=True,
    dag=dag,
)

task3 = PythonOperator(
    task_id='push_to_dvc',
    python_callable=push_to_dvc,
    provide_context=True,
    dag=dag,
)


task4 = PythonOperator(
    task_id='push_to_github',
    python_callable=git_push,
    provide_context=True,
    dag=dag,
)



task1 >> task2 >> task3 >> task4

