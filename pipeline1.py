from datetime import datetime, timedelta

from airflow.models.dag import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

import os
import zipfile
import requests
from bs4 import BeautifulSoup

from airflow.sensors.filesystem import FileSensor

import pandas as pd
import pickle
import apache_beam as beam

import geopandas
from geodatasets import get_path
import matplotlib.pyplot as plt

from PIL import Image



## SETTING REQUIRED VARIABLES
url = "https://www.ncei.noaa.gov/data/local-climatological-data/access/"
years = [2001] #Change years required accordingly
## SELECT NUMBER OF STATIONS YOU NEED TO FETCH DATA
num_stations = 25 ## Change as required
required_fields = ['DATE','HourlyWindSpeed'] ##Add required fields
root = '/opt/airflow/dags/weatherdata/' #root directory to store everything

with DAG(
    "pipeline1",
    
    default_args={
        "depends_on_past" : False,
        'start_date': datetime(2024, 2, 27),
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 0,
    },
    
    description = "A pipeline to extract the heatmap of weather details",
    schedule_interval = None
    
) as dag:
    
    ## Looping through every year
    
    for year in years:
        
        #######################################################
        ##TASK-1 FETCH CONTENTS FROM URL

        fetch_page_task = BashOperator(
            task_id=f'fetch_page_{year}',
            bash_command=f"curl -f {url}{year}/  -o {root}{year} && echo 'Page fetched successfully'",
        )
        
        #######################################################
        ## EXTRACT CSV FILES LINKS AND SAVE THEM AS A FILE
        
        def extract_links_from_html(html_file_path,output_file_path):
            with open(html_file_path, 'r') as file:
                html_content = file.read()

            soup = BeautifulSoup(html_content, 'html.parser')

            links = [link.get('href') for link in soup.find_all('a', href=True) if link.get('href').endswith('.csv')]

            links = links[:num_stations]

            with open(output_file_path, 'w') as output_file:
                for link in links:
                    output_file.write(f"{link}\n")


        select_files_task = PythonOperator(
            task_id = f'select_files_{year}',
            python_callable = extract_links_from_html,
            op_kwargs = {'html_file_path':f'{root}{year}','output_file_path':f'{root}{year}csvfilelist'},
        )

        #######################################################
        ## FETCH CSV FILES
        
        def fetch_datafiles(filelist_path,output_dirpath):

            with open(filelist_path, 'r') as file:
                csv_files = file.read().splitlines()

            for csv_file_url in csv_files:
                total_csv_url = f"{url}{year}/{csv_file_url}"
                response = requests.get(total_csv_url)

                if response.status_code == 200:
                    csv_filename = csv_file_url.split('/')[-1]
                    csv_file_path = f"{output_dirpath}{csv_filename[:-4]}_{year}.csv"

                    with open(csv_file_path, 'w', newline='') as csvfile:
                        csvfile.write(response.text)
                    print(f"Saved data from {csv_file_url} to {csv_file_path}")
                else:
                    print(f"Failed to fetch data from {csv_file_url}")


        fetch_data_task = PythonOperator(
            task_id = f'fetch_data_{year}',
            python_callable = fetch_datafiles,
            op_kwargs = {'filelist_path':f'{root}{year}csvfilelist','output_dirpath':root}
        )

        #######################################################
        ## ZIP CSV FILES
        
        def zip_csv_files(input_dir, output_zip_file):
            csv_files = [f for f in os.listdir(input_dir) if f.endswith(f'{year}.csv')]
            with zipfile.ZipFile(output_zip_file, 'w') as zipf:
                for csv_file in csv_files:
                    file_path = os.path.join(input_dir, csv_file)
                    zipf.write(file_path, arcname=csv_file)

        zip_files_task = PythonOperator(
            task_id = f'zip_files_{year}',
            python_callable = zip_csv_files,
            op_kwargs = {'input_dir':root,'output_zip_file':f'{root}zip_{year}.zip'}
        )

        fetch_page_task >> select_files_task >> fetch_data_task >> zip_files_task 