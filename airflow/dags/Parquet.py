import os
import pandas as pd
import pathlib

rootpath="/opt/airflow"
def convert_parquet(path):
    rootpath=os.getcwd()
    parquet_path = f"{rootpath}/parquet_files"
    for dir,subdir ,files in os.walk(path):
        for file in files:
            if is_csv(file):
                df = pd.read_csv(f"{path}/{file}", engine='python', error_bad_lines = False)
                df.to_parquet(f"{parquet_path}/{file[:-4]}.parquet")
                print(file)

def is_csv(file):
    file_extension = pathlib.Path(file).suffix
    if( file_extension == '.csv'):
        return True
    return False

