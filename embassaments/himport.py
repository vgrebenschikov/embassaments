from textwrap import indent
import pandas as pd
import requests
import time
import click
import snappy
from .metrics_pb2 import Sample, TimeSeries, WriteRequest


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
            timestamp = int(date_time.timestamp() * 1_000_000_000)

            request = WriteRequest()

            for i, label in enumerate(labels):
                capacity = float(capacitys[i])
                volume = float(row[i + 15])
                percent = (volume / capacity) * 100 if capacity > 0 else 0

                for metric_name, value in [("reservoir_capacity", capacity), 
                                        ("reservoir_volume", volume), 
                                        ("reservoir_percent", percent)]:
                    ts = request.timeseries.add()
                    ts.labels.add(name="__name__", value=metric_name)
                    ts.labels.add(name="name", value=label)

                    sample = ts.samples.add()
                    sample.value = value
                    sample.timestamp = timestamp

        return request.SerializeToString()


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
