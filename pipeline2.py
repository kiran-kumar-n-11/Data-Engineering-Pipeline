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
    "pipeline2",
    
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
        ## Check if Zip exists

        wait_for_archive_task = FileSensor(
            task_id=f'wait_for_archive_{year}',
            poke_interval = 1,
            timeout=60,      # Timeout after 300 seconds (5 minutes)
            mode = 'poke',
            filepath=f'{root}zip_{year}.zip',
        )

        #######################################################
        ## CHECK FOR VALIDITY OF ZIP FILE AND UNZIP
        def unzip_file():
            zip_file_path = f'{root}zip_{year}.zip'
            with zipfile.ZipFile(zip_file_path, 'r') as zip_ref:
                is_valid = zip_ref.testzip() is None
                if is_valid:
                    os.mkdir(f'{root}extracted_{year}')
                    zip_ref.extractall(f'{root}extracted_{year}')
                    print("ZIP file extracted successfully")
                else:
                    raise Exception("Invalid ZIP file")


        unzip_archive_task = PythonOperator(
            task_id = f'unzip_archive_{year}',
            python_callable = unzip_file,
        )
    
        #######################################################
        ## PROCESS CSV FILES
        
        def process_with_beam():

            def process_csv(csv_file):
                df = pd.read_csv(csv_file)
                filtered_df = df[required_fields].copy()
                lat,lon = df['LATITUDE'].iloc[0],df['LONGITUDE'].iloc[0]
                filtered_df['DATE'] = pd.to_datetime(filtered_df['DATE'], format='%Y-%m-%dT%H:%M:%S')
                filtered_df['DATE'] = filtered_df['DATE'].dt.strftime('%m-%Y')
                for field in required_fields:
                    if field!='DATE':
                        filtered_df[field] = pd.to_numeric(filtered_df[field], errors='coerce')
                filtered_df = filtered_df.groupby(['DATE']).mean()
                filtered_df['DATE'] = filtered_df.index
                filtered_df = filtered_df.reset_index(drop=True)
                data_tuple = (lat,lon,filtered_df.values.tolist())
                file_name = csv_file.split('/')[-1]
                with open(f'{root}extracted_{year}/{file_name[:-4]}.pickle', 'wb') as f:
                    pickle.dump(data_tuple, f)


            csv_files = []
            for r,dir,files in os.walk(f'{root}extracted_{year}/'):
                for file in files:
                    if file.endswith('.csv'):
                        csv_files.append(os.path.join(r,file))

            with beam.Pipeline() as pipeline:
                grouped_data = (
                    pipeline
                    | 'Create'>> beam.Create(csv_files)
                    | 'Process'>> beam.Map(process_csv)
                )


        process_csv_task = PythonOperator(
            task_id = f'process_csv_{year}',
            python_callable = process_with_beam,
        )
    
        ######################################################
        ## GROUPING A/C TO MONTH
        
        def group_with_beam():

            def extract_df(file):
                with open(file, 'rb') as f:
                    loaded_tuple = pickle.load(f)
                lat = loaded_tuple[0]
                lon = loaded_tuple[1]
                data = loaded_tuple[2]
                col_list = []
                for field in required_fields:
                    if field!='DATE':
                        col_list.append(field)
                col_list.append('DATE')
                df = pd.DataFrame(data,columns=col_list)
                df['Latitude'] = lat
                df['Longitude'] = lon
                print(f"Extracted DataFrame: {df.head()}")
                return df


            def group_dfs(dfs):
                dfs_list = []

                for df in dfs:    
                    dfs_list.append(df)
                merged_df = pd.concat(dfs_list,ignore_index=True)
                grouped = merged_df.groupby('DATE')
                for name, grp in grouped:
                    df = grp
                    with open(f'{root}extracted_{year}/{name}_grouped.pickle', 'wb') as f:
                        pickle.dump(df, f)
                return merged_df

            pickle_files = []
            for r,dir,files in os.walk(f'{root}extracted_{year}/'):
                for file in files:
                    if file.endswith('.pickle'):
                        pickle_files.append(os.path.join(r,file))

            with beam.Pipeline() as pipeline:
                group_df = (
                    pipeline
                    | 'Create'>> beam.Create(pickle_files)
                    | 'Process1'>> beam.Map(extract_df)
                    | 'Process2'>> beam.CombineGlobally(group_dfs)
                )

        group_csv_task = PythonOperator(
            task_id = f'group_csv_{year}',
            python_callable = group_with_beam,
        )
        #####################################################
        ##MAKE PLOTS
        
        def create_plots_with_beam():

            def create_plots(file):
                with open(file, 'rb') as f:
                    df = pickle.load(f)
                gdf = geopandas.GeoDataFrame(
                    df, geometry=geopandas.points_from_xy(df.Longitude, df.Latitude), crs="EPSG:4326"
                )
                
                world = geopandas.read_file(geopandas.datasets.get_path("naturalearth_lowres"))
                ax = world.clip([-100, 25, 100, 90]).plot(color="green", edgecolor="black")
                # We can now plot our ``GeoDataFrame``.
                
                
                for field in required_fields:
                    if field!='DATA':
                        file_name = file.split('/')[-1]
#                        plt.figure()
                        gdf.plot(column=field, ax=ax, cmap='coolwarm')
#                        cbar = plt.colorbar(ax.get_children()[1], ax=ax, orientation='vertical')
#                        cbar.set_label(f'{field}')
                        plt.xlabel('Longitude')
                        plt.ylabel('Latitude')
                        plt.title(f'{field} : Period {file_name[:-15]}')
                        plt.savefig(f'{file[:-15]}.png',bbox_inches="tight")


            grp_pickles = []
            for r,dir,files in os.walk(f'{root}extracted_{year}/'):
                for file in files:
                    if file.endswith('grouped.pickle'):
                        grp_pickles.append(os.path.join(r,file))

            with beam.Pipeline() as pipeline:
                plotfigs = (
                    pipeline
                    | 'Create'>> beam.Create(grp_pickles)
                    | 'Plot'>> beam.Map(create_plots)
                )

        create_plots_task = PythonOperator(
            task_id=f'create_plots_{year}',
            python_callable=create_plots_with_beam,
        )


        ######################################################
        ## CREATING GIF
        
        def create_gif_animation():

            #define output file name
            output_filename = os.path.join(root,f'animation{year}.gif')

            # Get list of image files
            image_files = [f for f in os.listdir(f'{root}extracted_{year}/') if f.endswith(".png")]

            # Open first image to get format and size
            first_image = Image.open(os.path.join(f'{root}extracted_{year}/', image_files[0]))
            image_format = first_image.format
            image_size = first_image.size

            # Create a list of images
            images = []
            for filename in image_files:
                image = Image.open(os.path.join(f'{root}extracted_{year}/', filename))
                images.append(image)

            # Save as GIF
            images[0].save(output_filename,save_all=True, append_images=images[1:],optimize=False, duration=400,loop=0)

        # Create BashOperator task
        create_gif_task = PythonOperator(
            task_id=f'create_gif_task_{year}',
            python_callable=create_gif_animation,
        )

        #####################################################
        # DELETING UNNECESSARY FILES

        def delete_except_gif(directory):
            print('Entered!!')
            print(directory)
            print()
            for filename in os.listdir(directory):
                filepath = os.path.join(directory, filename)

                if os.path.isfile(filepath):
                    if not filepath.endswith(".gif"):
                        os.remove(filepath)
                        print(f"Deleted: {filename}")
                elif os.path.isdir(filepath):
                    # Recursively delete contents of subdirectories
                    delete_except_gif(f'{filepath}/')
                    os.removedirs(filepath)  # Remove empty subdirectory
                    print(f"Deleted directory: {filename}")


        delete_task = PythonOperator(
            task_id=f'delete_{year}',
            python_callable=delete_except_gif,
            op_args=[root],
        )

        wait_for_archive_task >> unzip_archive_task >> process_csv_task >> group_csv_task >> create_plots_task >> create_gif_task >> delete_task
