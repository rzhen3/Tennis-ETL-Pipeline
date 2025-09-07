from airflow.sdk import dag, task
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.sdk.bases.sensor import PokeReturnValue
from airflow.providers.standard.operators.python import PythonOperator
import requests

from extract_from_data_source import main as extract_main

@dag
def load_application():

    # create_table = SQLExecuteQueryOperator(
    #     task_id = "create_table",
    #     sql = """
    #     CREATE TABLE IF NOT EXISTS users (
    #         id INT PRIMARY KEY,
    #         firstname VARCHAR(255),
    #         lastname VARCHAR(255),
    #         email VARCHAR(255),
    #         created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP                          
    #     )                            
    #     """,
    #     conn_id = "postgres"                             
    # )

    @task
    def extract_data_from_api():
        extract_main()


    @task
    def upload_to_gcs():

        '''
            uploads python scripts to bucket
        '''
        pass


    @task
    def load_to_warehouse():
        pass
    
    
    extract_data_from_api >> upload_to_gcs >> load_to_warehouse
    # @task.sensor(poke_interval=30, timeout=300)
    # def is_api_available() -> PokeReturnValue:
    #     response = requests.get("https://raw.githubusercontent.com/marclamberti/datasets/refs/heads/main/fakeuser.json")
    #     print(response.status_code)

    #     if response.status_code == 200:
    #         condition = True
    #         fake_user = response.json()
    #     else:
    #         condition = False
    #         fake_user = None

    #     return PokeReturnValue(is_done = condition, xcom_value = fake_user)
    
    # @task
    # def extract_user(fake_user):
    #     # mocking the API request
    #     # import requests
    #     # response = requests.get("https://raw.githubusercontent.com/marclamberti/datasets/refs/heads/main/fakeuser.json")
    #     # fake_user = response.json()

    #     print(fake_user)

    #     return {
    #         "id": fake_user["id"],
    #         "firstname": fake_user["personalInfo"]["firstName"],
    #         "lastname": fake_user["personalInfo"]["lastName"],
    #         "email": fake_user["personalInfo"]["email"],
    #     }


load_application()