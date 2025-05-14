import pandas as pd
import matplotlib.pyplot as plt
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler
import time
import os

plot_log_file_name = './logs/plot_log.txt'
png_file_name = './logs/error_distribution.png'
 
csv_file_name = "./logs/metric_log.csv"
folder_name = csv_file_name 

def log(text):
    with open(plot_log_file_name, 'a') as log:
        log.write(text + '\n')

class CSVEventHandler(FileSystemEventHandler):
    def __init__(self, csv_file):
        self.csv_file = csv_file

    def on_modified(self, event):
        if event.src_path == self.csv_file:
            log(f"{self.csv_file} изменен. Перестраиваю гистограмму...")
            self.plot_histogram()

    def plot_histogram(self):
        # Чтение данных из CSV файла
        df = pd.read_csv(self.csv_file)

        # Построение гистограммы абсолютной ошибки
        plt.figure(figsize=(10, 6))
        plt.hist(df['absolute_error'], bins=20, color='blue', edgecolor="black",alpha=0.7)
        plt.title('Гистограмма абсолютной ошибки')
        plt.xlabel('Абсолютная ошибка')
        plt.ylabel('Частота')
        plt.grid(axis='y', alpha=0.75)
        
        # Сохранение гистограммы в файл
        plt.savefig(png_file_name)
        plt.close()
        log(f"Гистограмма сохранена в {png_file_name}")

def monitor_csv(csv_file):
    event_handler = CSVEventHandler(csv_file)
    observer = Observer()
    observer.schedule(event_handler, path=os.path.dirname(folder_name), recursive=False)
    observer.start()
    log(f"Начато отслеживание изменений в {folder_name}")

    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        observer.stop()
    observer.join()

with open(plot_log_file_name, 'w') as logfile:
        logfile.write('plot is alive \n')

while True:
    log("Plotter restarted")
    try:
        time.sleep(10)
        csv_file_path = csv_file_name
        monitor_csv(csv_file_path)
    except Exception as e:
        log("plot failed with {e}")
