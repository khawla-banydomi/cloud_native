import os
import pandas
from pandas import DataFrame
import csv
from datetime import timedelta
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago
import requests


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': days_ago(2),
    'email': ['kkaldomi07@cit.just.edu.jo'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
}


#Initialization
dag = DAG(
    'practice_2_cc',
    default_args=default_args,
    description='Deployment of a Cloud Native service for humidity and temperature prediction',
     schedule_interval=timedelta(days=1),
)

# prepare_environment
Preparenvironment = BashOperator(
    task_id='prepare_environment',
    depends_on_past=False,
    bash_command='mkdir -p /tmp/workflow/',
    dag=dag,
)

#capture_dataA
DataA = BashOperator(
    task_id='Download_Data_Humidity',
    depends_on_past=True,
    bash_command='curl -o /tmp/workflow/humidity.csv.zip https://raw.githubusercontent.com/manuparra/MaterialCC2020/master/humidity.csv.zip',
    dag=dag,
)

#capture_dataB
DataB = BashOperator(
    task_id='Download_Data_Temperature',
    depends_on_past=True,
    bash_command='curl -o /tmp/workflow/temperature.csv.zip https://raw.githubusercontent.com/manuparra/MaterialCC2020/master/temperature.csv.zip',
    dag=dag,
)

#unzip_data_A
DescompressA = BashOperator(
    task_id='unzip_data_Humidity',
    depends_on_past=True,
    bash_command='unzip -od /tmp/workflow/ /tmp/workflow/humidity.csv.zip',
    dag=dag,
)

#unzip_data_B
DescompressB = BashOperator(
    task_id='unzip_data_Temperature',
    depends_on_past=True,
    bash_command='unzip -od /tmp/workflow/ /tmp/workflow/temperature.csv.zip',
    dag=dag,
)

#Humidity_drop
CleanA = BashOperator(
    task_id='clean_data_Humidity',
    depends_on_past=True,
    bash_command='cut -d "," -f 1,4 /tmp/workflow/humidity.csv >> /tmp/workflow/humidity-clean.csv',
    dag=dag,
)

#Temperature_drop
CleanB = BashOperator(
    task_id='clean_data_Termerature',
    depends_on_past=True,
    bash_command='cut -d "," -f 1,4 /tmp/workflow/temperature.csv >> /tmp/workflow/temperature-clean.csv',
    dag=dag,
)

#unified_data_A_and_data_B
#BindAB = BashOperator(
#   task_id='bind_temperature_Humidity',
#   depends_on_past=True,
#    bash_command='join -t "," /tmp/workflow/temperature-clean.csv /tmp/workflow/humidity-clean.csv >> 
#/tmp/workflow/dataset_BIND.csv',
#   dag=dag,
#)


#########

#DataBase_Image
#DownloadImageAB = BashOperator(
#    task_id='download_image_sqlite3',
#    depends_on_past=True,
#    bash_command="docker pull sqlite3:latest",
#    dag=dag,
#)
#########
#it is not necessary to create container
#CreatecontainerAB = BashOperator(
#    task_id='create_sqlite3_container',
#    depends_on_past=True,
#    bash_command="docker run -d -p 28000:27017 sqlite3:latest",
#    dag=dag,
#)
#########
#ImportDataAB = BashOperator(
#    task_id='import_data_to_DB_container',
#    depends_on_past=True,
#    bash_command="sqliteimport -d BD1 -c sanFrancisco --file /tmp/workflow/dataset_BIND.csv --type csv --drop 
#--port 28000 -f DATE,HUM,TEMP --#host localhost",
#    dag=dag,
#)

#########

#
#
##in the following way we can read and merge two datasets.
##df1 = pd.DataFrame({'employee': ['Bob', 'Jake', 'Lisa', 'Sue'],group': ['Accounting', 'Engineering', 
#'Engineering', 'HR']})
##df2 = pd.DataFrame({'employee': ['Lisa', 'Bob', 'Jake', 'Sue'],hire_date': [2004, 2008, 2012, 2014]})
##display('df1', 'df2') #### df3 = pd.merge(df1, df2); df3
#
#




#####New Function For treat data after capturing it from 
def frame_merge_dataset(path_file):
#read_data_1
	read_data1 = pandas.read_csv(path_file+'humidity.csv', sep=',',index_col=0)
	read_data1 = read_data1.rename(columns={'San Francisco':'humidity'})
	read_data1= read_data1.drop(read_data1.columns[[0,1,3,4,5,6,7,8,9,10,11,12,13 ,14,15,16,17,18,19,20,21,22,
  23,24,25,26,27,28,29,30,31,32,33,34,35]], axis='columns')
	read_data1= read_data1[1:1500]
#read_data2
	read_data2 = pandas.read_csv(path_file+'temperature.csv', sep=',',index_col=0)
	read_data2 = read_data2.rename(columns={'San Francisco':'temperature'})
	read_data2= read_data2.drop(read_data2.columns[[0,1,3,4,5,6,7,8,9,10,11,12,13 ,14,15,16,17,18,19,20,21,22,
  23,24,25,26,27,28,29,30,31,32,33,34,35]], axis='columns')
	read_data2= read_data2[1:1500]
#####################################################################################################
#####################################################################################################
	
	
##merge Both data files
	read_data_AB = pandas.merge(read_data1,read_data2,on='datetime')
	read_data_AB = read_data_AB.dropna()# method allows the user to analyze and drop Rows/Columns with Null. 
	read_data_AB.to_csv(path_file+'sanFrancisco.csv')
	

#######################################################################################################
#######################################################################################################

#choose_HUM_TEMP_DATE
#SettopointAB = BashOperator(
#    task_id='set_to_point_Dataset',
#    depends_on_past=True,
#    bash_command="sed 's/datetime,San Francisco,San Francisco/DATE,HUM,TEMP/g' /tmp/workflow/dataset_BIND.csv>>/tmp/workflow/#dataset_final.csv",
 #   dag=dag
#)

#clean and merge data after capturing it.
clean_bind_AB = PythonOperator(
		task_id='clean_from_null_bind_AB',
		depends_on_past=True,
		python_callable=frame_merge_dataset,
		op_args={'/tmp/workflow/'},
		dag=dag
		)


######################################################################################

#Generate mongo container..related to the data base.
create_mongo_container = BashOperator(
    task_id='GenerateMongoContainer',
    depends_on_past=True,
    bash_command="docker run -d -p 28900:27017 mongo:latest",
    dag=dag,
)


#Import Data from the main database
Import_data_from_database = BashOperator(
    task_id='Import_data',
    depends_on_past=True,
    bash_command="mongoimport -d BD1 -c sanFrancisco --type csv --file /tmp/workflow/sanFrancisco.csv--headerline --port 28900 --host localhost",
    dag=dag,
)


##clone the whole airflow to the git repository
Clone_to_Git = BashOperator(
    task_id='GitClone',
    depends_on_past=True,
    bash_command="cd /tmp/workflow && git clone git@github.com:khawla-banydomi/Airflow-REST-API-Prediction",
    dag=dag,
)

##add the first version of the application

Git_application_V1 = BashOperator(
    task_id='Gitappv1',
    depends_on_past=True,
    bash_command="cd /tmp/workflow/Airflow-REST-API-Prediction && git checkout application_V1",
    dag=dag,
)
##build the first version of the application


Build_application_V1 = BashOperator(
    task_id='Buildappav1',
    depends_on_past=True,
    bash_command='cd /tmp/workflow/Airflow-REST-API-Prediction && docker build -f Dockerfile -t "cloud_naive_airflow" .',
    dag=dag,
)

##Run the first version of the application

Run_application_V1 = BashOperator(
    task_id='RunV1',
    depends_on_past=True,
    bash_command="cd /tmp/workflow/Airflow-REST-API-Prediction && docker run -p 8000:8000 --name cloud_native_dock -t cloud_naive_airflow",
    dag=dag,
)
##Git the second version of the application
Git_application_V2 = BashOperator(
    task_id='GitappV2',
    depends_on_past=True,
    bash_command="cd /tmp/workflow/cloud_native_airflow",
    dag=dag,
)

####Build the second version of the application
Build_application_V2 = BashOperator(
    task_id='BuildappV2',
    depends_on_past=True,
    bash_command='cd /tmp/workflow/Airflow-REST-API-Prediction && docker build -f Dockerfile -t "khawla-banydomi:cloud_native_airflow2" .',
    dag=dag,
)

##Run the second version of the appliacion.
Run_application_V2 = BashOperator(
    task_id='RunappV2',
    depends_on_past=True,
    bash_command="cd /tmp/workflow/Airflow-REST-API-Prediction && docker run -p 5000:5000 --name cloud_native_dock -t khawla-banydomi:cloud_native_V2",
    dag=dag,
)



#Dependencies of the tasks:
Preparenvironment >> [DataA , DataB] >> DescompressA >> DescompressB >> clean_bind_AB >> create_mongo_container >> Import_data_from_database >> Clone_to_Git >> Git_application_V1 >>  Build_application_V1 >> Run_application_V1
