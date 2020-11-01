from airflow import DAG
from etl import load, transform, extract
from airflow.operators.python_operator import PythonOperator

def etl():
    best_song_per_week = extract()
    best_song_per_week = transform(best_song_per_week)
    load(best_song_per_week)

dag = DAG(dag_id='etl-log-dag',
          schedule_interval='0 0 0 * *'
          )

pip = PythonOperator(task_id='pipeline',
                     python_callable=etl,
                     )

