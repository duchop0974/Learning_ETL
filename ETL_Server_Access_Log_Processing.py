import requests
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.email_operator import EmailOperator
from airflow.operators.bash_operator import BashOperator

input_file = "web-server-access-log.txt"
extracted_file = "extracted-data.txt"
transformed_file = "transformed-data.txt"
output_file = "output-data.txt"

def download():
    print("Downloading file...")
    url = "https://cf-courses-data.s3.us.cloud-object-storage.appdomain.cloud/IBM-DB0250EN-SkillsNetwork/labs/Apache%20Airflow/Build%20a%20DAG%20using%20Airflow/web-server-access-log.txt"
    with requests.get(url, stream=True) as response:
        response.raise_for_status()
        with open(input_file, 'wb') as file:
            for chunk in response.iter_content(chunk_size=8192):
                file.write(chunk)
    print(f"File downloaded successfully: {input_file}")

def extract():
    global input_file
    print("Extracting data...")
    with open(input_file,"r") as infile:
        with open(extracted_file,"w") as outfile:
            for line in infile:
                fields = line.split("#")
                if len(fields) >= 4:
                    field_1 = fields[0]
                    field_4 = fields[3]
                    outfile.write(field_1 + "#"+ field_4 + "\n")
    print("Data extracted successfully")

def transform():
    global extracted_file, transformed_file
    print("Transforming data...")
    with open(extracted_file, "r") as infile:
        with open(transformed_file, "w") as outfile:
            for line in infile:
                capitalized_line = line.upper()
                outfile.write(capitalized_line)
    print("Data transformed successfully")

def load():
    global transformed_file, output_file
    print("Loading data...")
    with open(transformed_file, "r") as infile:
        with open(output_file, "w") as outfile:
            for line in infile:
                outfile.write(line)
    print("Data loaded successfully")

def check():
    global output_file
    print("Checking data...")
    with open(output_file, "r") as infile:
        for line in infile:
                print(line)

#if __name__ == "__main__":
#   download()
#    extract()
#    transform()
#    load()

default_args = {
    'owner': 'airflow',
    'start_date': dayago(0),
    'email':[duchop0974@gmail.com]
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}
