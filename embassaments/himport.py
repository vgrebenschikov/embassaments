from textwrap import indent
import pandas as pd
import requests
import time
import click
import snappy
from .metrics_pb2 import Sample, TimeSeries


@click.command()
@click.argument('data')
@click.option('--verbose', '-v', is_flag=True, help='Enable verbose mode.')
@click.option('--url', default='http://localhost:9090/api/v1/write', help='URL of the prometheus.')
def import_historical_data(data, verbose, url):
    labels_df = pd.read_excel(data, sheet_name='Sheet1', header=None, skiprows=28, usecols='P:X', nrows=1)
    labels = labels_df.iloc[0].tolist()

    capacitys_df = pd.read_excel(data, sheet_name='Sheet1', header=None, skiprows=29, usecols='P:X', nrows=1)
    capacitys = capacitys_df.iloc[0].tolist()

    data_df = pd.read_excel(data, sheet_name='Sheet1', header=None, skiprows=32, usecols='A:AK')

    def generate_prometheus_payload(data_df, labels):
        time_now = int(time.time() * 1000)
        timeseries = []

        for index, row in data_df.iterrows():
            date_time = pd.to_datetime(row[0], format='%Y-%m-%d %H:%M:%S')
            timestamp = int(date_time.timestamp() * 1000)

            timeseries = TimeSeries()  # Создайте объект TimeSeries


            for i, label in enumerate(labels):
                capacity = float(capacitys[i])
                volume = float(row[i + 15])
                percent = (volume / capacity) * 100 if capacity > 0 else 0

                # metrics = [
                #     {"name": "reservoir_capacity", "value": capacity},
                #     {"name": "reservoir_volume", "value": volume},
                #     {"name": "reservoir_percent", "value": percent},
                # ]

                # for metric in metrics:
                #     timeseries.append({
                #         "labels": [
                #             {"name": "__name__", "value": metric["name"]},
                #             {"name": "name", "value": label}
                #         ],
                #         "samples": [{"value": metric["value"], "timestamp": int(timestamp)}]
                #     })

                for metric_name, value in [("reservoir_capacity", capacity), 
                                        ("reservoir_volume", volume), 
                                        ("reservoir_percent", percent)]:
                    sample = timeseries.samples.add()
                    sample.name = metric_name
                    sample.value = value
                    sample.timestamp = timestamp

        return timeseries.SerializeToString()


    def send_to_prometheus(data):
        headers = {
            "Content-Type": "application/x-protobuf",
            "Content-Encoding": "snappy",
        }
        compressed_data = snappy.compress(data)
        response = requests.post(url, data=compressed_data, headers=headers)

        if response.status_code == 200:
            print("Data sent to Prometheus")
        else:
            print(f"Error: {response.status_code}, {response.text}")

    payload = generate_prometheus_payload(data_df, labels)
    send_to_prometheus(payload)
