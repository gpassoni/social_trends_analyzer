from spark_processing.spark_processing import SparkProcessor
from dotenv import load_dotenv
import os
from pathlib import Path

load_dotenv()
data_path = Path(os.getenv("DATA_DIR"))

hadoop_home = r"C:\hadoop"

sp = SparkProcessor(data_path=data_path, hadoop_home=hadoop_home)
saved_folder = sp.process_and_save_posts()
saved_folder_comments = sp.process_and_save_comments()

print("Processing completato! Controlla i file in:", saved_folder)

sp.spark.stop()
