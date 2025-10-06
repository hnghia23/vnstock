from datetime import datetime, timedelta  

from airflow import DAG  
from airflow.operators.python import PythonOperator


from src.batch.etl_vnstock import extract_data, transform_data, append_data

import pandas as pd 



# symbol = pd.read_csv('../symbol.csv')[["symbol"]].iloc[:, 0].tolist()

# path = '../batch_pipeline/storage'

# latest_day = pd.read_csv(f"../batch_pipeline/storage/{symbol}.csv").iloc[-1]['time']


# with DAG(
#     dag_id="vnstock",
#     start_date=latest_day,
#     schedule='@weekly',
#     catchup=False
# ) as dag:
#     extract=PythonOperator(
#         task='extract_data_from_vnstock',
#         python_callable=extract_data, 
#         kwargs=symbol,
#     )

#     transform=PythonOperator(
#         task='add_technical_indicators',
#         python_callable=transform_data
#     )

#     update_data = PythonOperator(
#         task='load_new_data',
#         python_callable=append_data,
#         kwargs=symbol,
#     )


# extract >> transform >> update_data   
