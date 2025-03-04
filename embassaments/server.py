from prometheus_client import start_http_server, Gauge
import click
import io
import requests
import pdfplumber
import pandas as pd
import re
import time

# Define Prometheus metrics
capacity_gauge = Gauge("reservoir_capacity", "capacity of the reservoir", ["name"])
volume_gauge = Gauge("reservoir_volume", "Current filled volume of the reservoir", ["name"])
percent_gauge = Gauge("reservoir_percent", "Percentage filled of the reservoir", ["name"])
volume_1y_ago_gauge = Gauge('reservoir_volume_1y_ago', 'Filled volume of the reservoir 1 year ago', ['name'])
percent_1y_ago_gauge = Gauge('reservoir_percent_1y_ago', 'Percentage filled of the reservoir 1 year ago', ['name'])
volume_5y_avg_gauge = Gauge('reservoir_volume_5y_avg', 'Filled volume of the reservoir 5 years average', ['name'])
percent_5y_avg_gauge = Gauge('reservoir_percent_5y_avg', 'Percentage filled of the reservoir 5 years average', ['name'])


def extract_metrics_from_pdf(pdf_url, verbose=False):
    response = requests.get(pdf_url)
    response.raise_for_status() 

    with pdfplumber.open(io.BytesIO(response.content)) as pdf:
        page = pdf.pages[0]
        tables = page.extract_tables()

        df = pd.DataFrame(tables[0])

        date_time = pd.to_datetime(df.iloc[0, 0], format='Data última actualització: %d/%m/%Y %H:%M')
        timestamp = date_time.timestamp()

        if verbose:
            click.echo(date_time)

        df = pd.DataFrame(tables[1])

        if verbose:
            df_string = df.to_string(index=False)
            click.echo(df_string)

        for row in df.itertuples(index=False, name=None):
            if len(row) < 4:
                continue

            name = sfix(row[0])
            capacity = sfix(row[1])

            # Skip header and footer of table
            if name == "TOTAL" or not name or capacity == "Volum màxim":
                continue

            capacity = float(capacity)
            volume = float(sfix(row[2]))
            percent = float(sfix(row[3]))
            volume_1y_ago = float(sfix(row[4]))
            percent_1y_ago = float(sfix(row[5]))
            volume_5y_avg = float(sfix(row[6]))
            percent_5y_avg = float(sfix(row[7]))

            # Update Prometheus metrics
            capacity_gauge.labels(name=name).set(capacity)
            volume_gauge.labels(name=name).set(volume)
            percent_gauge.labels(name=name).set(percent)
            volume_1y_ago_gauge.labels(name=name).set(volume_1y_ago)
            percent_1y_ago_gauge.labels(name=name).set(percent_1y_ago)
            volume_5y_avg_gauge.labels(name=name).set(volume_5y_avg)
            percent_5y_avg_gauge.labels(name=name).set(percent_5y_avg)


def sfix(s):
    ret = (s or "").replace("\n", " ").replace(",", ".")
    ret = re.sub(r"\(.+?\)", "", ret)
    ret = re.sub(r"\s+$", "", ret)
    return ret


@click.command()
@click.option('--verbose', '-v', is_flag=True, help='Enable verbose mode.')
@click.option('--port', '-p', default=8000, help='Port to run the server on.')
@click.option('--url', default='https://info.aca.gencat.cat/ca/aca/informacio/informesdwh/dades_embassaments_ca.pdf', help='URL of the PDF file.')
@click.option('--address', '-a', default='0.0.0.0', help='Address to bind the server to.')
def main(verbose, port, url, address):
    if verbose:
        saddr = address if address != '0.0.0.0' else 'localhost'
        click.echo(f"Starting server on {address}:{port}")
        click.echo(f"URI http://{saddr}:{port}/metrics")
        click.echo(f"Fetching PDF from {url}")

    # Start up the server to expose the metrics.
    start_http_server(port, addr=address)
    
    # Generate some requests.
    while True:
        extract_metrics_from_pdf(url, verbose=verbose)
        if verbose:
            click.echo("Metrics updated")
        time.sleep(60*60)  # Sleep for 1 hour before updating metrics again

if __name__ == "__main__":
    main()