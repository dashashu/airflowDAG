import datetime
import os
import airflow
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator

import pandas as pd 
import os
from pandas import ExcelWriter
from os import listdir
from os.path import isfile, join

import time
import sys
import numpy as np

YESTERDAY = datetime.datetime.now() - datetime.timedelta(days=1)

default_args = {
    'owner': 'Composer Example',
    'depends_on_past': False,
    'email': [''],
    'email_on_failure': "ashutosh.dash3@vodafone.com",
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': datetime.timedelta(minutes=5),
    'start_date': YESTERDAY,
}
def ModifyInputfiles():
    def get_excel_files(path):
        return [f for f in listdir(path) if isfile(join(path, f))]

    

    dirpath = '/home/airflow/gcs/data/' 
    print("Dirpath:"+dirpath)
    if not os.path.exists(os.path.join(dirpath,'in-data')):
        os.mkdir(os.path.join(dirpath,'in-data'))
    OutputFolder = os.path.join(dirpath,'in-data')
    
    inputfilepath = '/home/airflow/gcs/data/input'
    mappingFilePath = '/home/airflow/gcs/data/map'

    mapping = get_excel_files(mappingFilePath)
    inputfilelist = get_excel_files(inputfilepath)
   
    for map_file in mapping: #if in future we need to take multiple files
        print("running file:" + map_file)
        mapping_exten = map_file.split('.')[1]
        if mapping_exten == 'csv':
            mapping_df = pd.read_csv(os.path.join(mappingFilePath,map_file),sep=';')
        elif mapping_exten == 'xlsx':
            mapping_df = pd.read_excel(os.path.join(mappingFilePath,map_file))
    mapping_df = mapping_df.loc[:,mapping_df.columns.intersection(['VM','VNF Name','VNF Program name','VNF vendor name','Site Name'])]
    mapping_df =mapping_df.rename(columns = {'VNF Program name':'Program Name' ,'VNF vendor name' :'Vendor'})   
    writer = ExcelWriter(os.path.join(OutputFolder,'FS_Input_'+time.strftime("%Y_%m_%d")+'.xlsx'))
    final_input_df = pd.DataFrame()
    for input in inputfilelist: #if in future we need to take multiple files
        print("Running file: "+input)
        input_exten = input.split('.')[1]
        file_name = input.split('.')[0]
        site_name = file_name.split('_')[0]
        if input_exten == 'csv':
            input_df = pd.read_csv(os.path.join(inputfilepath , input),sep=';')
        elif input_exten == 'xlsx':
            input_df = pd.read_excel(os.path.join(inputfilepath , input))
        input_df = input_df.rename(columns=lambda x: x.strip())

        input_df['Disk Average Read Bytes Per Second (kB/s)'] = pd.to_numeric(input_df['Disk Average Read Bytes Per Second (kB/s)'].astype(str).replace('.',''), errors='coerce')
        input_df['Disk Average Read Bytes Per Second (kB/s)'] = pd.to_numeric(input_df['Disk Average Read Bytes Per Second (kB/s)'], downcast="float")
        input_df['Disk Average Write Bytes Per Second (kB/s)'] = pd.to_numeric(input_df['Disk Average Write Bytes Per Second (kB/s)'].astype(str).replace('.',''), errors='coerce')
        input_df['Disk Average Write Bytes Per Second (kB/s)'] = pd.to_numeric(input_df['Disk Average Write Bytes Per Second (kB/s)'], downcast="float")
        #input_df['Site Name'] = site_name
        
        input_df = input_df.rename(columns=lambda x: x.lower())
        final_input_df = final_input_df.append(input_df)
        
    final_input_df =final_input_df.rename(columns = {'site name':'SiteName','start time':'timestamp','vm':'VM','cpu usage %':'CPU usage (%)','memory usage %':'Memory Usage (%)','disk usage %':'Disk Usage (%)','disk average read bytes per second (kb/s)':'Disk Average Read Bytes Per Second (kB/s)','disk average write bytes per second (kb/s)':'Disk Average Write Bytes Per Second (kB/s)'})   
    final_input_df = pd.merge(final_input_df,mapping_df, on='VM', how='left')       
    
    final_input_df['Disk Usage (%)'] = final_input_df['Disk Usage (%)'].convert_objects(convert_numeric=True)
    final_input_df['Memory Usage (%)'] = final_input_df['Memory Usage (%)'].convert_objects(convert_numeric=True)
    final_input_df['CPU usage (%)'] = final_input_df['CPU usage (%)'].convert_objects(convert_numeric=True)
    
    final_input_df['Disk Usage (%)'] = np.round(final_input_df['Disk Usage (%)'].astype(float) ,3)
    final_input_df['Memory Usage (%)'] = np.round(final_input_df['Memory Usage (%)'].astype(float),3) 
    final_input_df['CPU usage (%)'] = np.round(final_input_df['CPU usage (%)'].astype(float) ,3)
    final_input_df.to_excel(writer,index=False)
    writer.save()
    writer.close()
    print('end of process')
with airflow.DAG(
        'composer_FSR_RO_dag',
        'catchup=False',
        default_args=default_args,
        schedule_interval='*/5 * * * *') as dag:
        OPCO = 'ro'
        date = datetime 
        
        
        path = "/home/airflow/gcs/data/input"
        
        if os.path.exists(path) and not os.path.isfile(path): 
            run_command0 = "rm -rv /home/airflow/gcs/data/map/*" "/home/airflow/gcs/data/input/* /home/airflow/gcs/data/scripts/OPCO_OUTPUT/*"
            if os.listdir(path): #if not empty direcctory
                t1 = BashOperator(
                        task_id= 'cleaning_previous_files',
                        bash_command= run_command0,
                        dag=dag)
        else:
            run_command0 = "mkdir /home/airflow/gcs/data/map/ /home/airflow/gcs/data/input/"
            t1 = BashOperator(
                        task_id= 'creating_the_paths',
                        bash_command= run_command0,
                        dag=dag)
            
        run_command2 = "gsutil cp gs://fushionsphere/MappingFiles/ro/ro_mapping.csv /home/airflow/gcs/data/map &&" " gsutil cp gs://fushionsphere/InputFile/ro/* /home/airflow/gcs/data/input" 
        
        t2 = BashOperator(
                task_id= 'getting_map_input_data',
                bash_command= run_command2,
                dag=dag)
        
        run_command3 = "python3 ~airflow/dags/ModifyInput.py /home/airflow/gcs/data/input /home/airflow/gcs/data/map"
        
        #t3    = PythonOperator(task_id='input_data_engineering', python_callable=ModifyInputfiles)
        t3 = BashOperator(
                task_id= 'input_data_engineering',
                bash_command= run_command3,
                dag=dag)
        run_command4 = "python /home/airflow/gcs/data/scripts/FSRScript.py /home/airflow/gcs/data/in-data ${OPCO}"
        
        t4 = BashOperator(
                task_id= 'FS_report_run',
                bash_command= run_command4,
                dag=dag)
        
        run_command5 =  "tar -czvf ${OPCO}_FSR_Monthly_$date.tar.gz /home/airflow/gcs/data/scripts/OPCO_OUTPUT/* &&" "gsutil cp /home/airflow/gcs/data/scripts/OPCO_OUTPUT/${OPCO}_FSR_Monthly_$date.tar.gz gs://fushionsphere/Output/"
        
        t5 = BashOperator(
                task_id= 'uploading_report_to_GCP',
                bash_command= run_command5,
                dag=dag)
        t2 >> t3 >> t4 >> t5