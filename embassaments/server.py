from prometheus_client import start_http_server, Gauge
import click
import io
import requests
import pdfplumber
import pandas as pd
import re
import time

# Define Prometheus metrics
g_capacity = Gauge("reservoir_capacity", "capacity of the reservoir", ["name"])
g_volume = Gauge("reservoir_volume", "Current filled volume of the reservoir", ["name"])
g_percent = Gauge("reservoir_percent", "Percentage filled of the reservoir", ["name"])

g_volume_1y_ago = Gauge('reservoir_volume_1y_ago', 'Filled volume of the reservoir 1 year ago', ['name'])
g_percent_1y_ago = Gauge('reservoir_percent_1y_ago', 'Percentage filled of the reservoir 1 year ago', ['name'])

g_volume_5y_avg = Gauge('reservoir_volume_5y_avg', 'Filled volume of the reservoir 5 years average', ['name'])
g_percent_5y_avg = Gauge('reservoir_percent_5y_avg', 'Percentage filled of the reservoir 5 years average', ['name'])

g_total_capacity = Gauge("reservoir_total_capacity", "All reservoirs capacity", ["name"])
g_total_volume = Gauge("reservoir_total_volume", "All  filled volumes", ["name"])
g_total_percent = Gauge("reservoir_total_percent", "All reservoirs filled percentage", ["name"])


def extract_metrics_from_pdf(pdf_url, verbose=False):
    response = requests.get(pdf_url)
    response.raise_for_status() 

    with pdfplumber.open(io.BytesIO(response.content)) as pdf:
        page = pdf.pages[0]
        tables = page.extract_tables()

        df = pd.DataFrame(tables[0])

        date_time = pd.to_datetime(df.iloc[0, 0], format='Data última actualització: %d/%m/%Y %H:%M')
        timestamp = int(date_time.timestamp())

        if verbose:
            click.echo(f'Time: {date_time}, timestamp={timestamp}')

        df = pd.DataFrame(tables[1])

        if verbose:
            df_string = df.to_string(index=False)
            click.echo(df_string)

            print(
                80 * '-' + '\n',
                f'{"Name":30} {"Capacity":>15} {"Volume":>15} {"Percent":>15} '
                f'{"volume_1y_ago":>15} {"percent_1y_ago":>15} '
                f'{"volume_5y_avg":>15} {"percent_5y_avg":>15} ',
                sep=''
            )

        for row in df.itertuples(index=False, name=None):
            if len(row) < 4:
                continue

            name = sfix(row[0])
            capacity = sfix(row[1])

            # Skip header and footer of table
            if not name or capacity == "Volum màxim":
                continue

            capacity = float(capacity)
            volume = float(sfix(row[2]))
            percent = float(sfix(row[3]))
            volume_1y_ago = float(sfix(row[4]))
            percent_1y_ago = float(sfix(row[5]))
            volume_5y_avg = float(sfix(row[6]))
            percent_5y_avg = float(sfix(row[7]))

            if verbose:
                print(
                    f'{name:30} {capacity:15} {volume:15} {percent:15} '
                    f'{volume_1y_ago:15} {percent_1y_ago:15} '
                    f'{volume_5y_avg:15} {percent_5y_avg:15} '
                )

            # Update Prometheus metrics
            if name == "TOTAL":
                g_total_capacity.labels(name=name).set(capacity)
                g_total_volume.labels(name=name).set(volume)
                g_total_percent.labels(name=name).set(percent)
            else:
                g_capacity.labels(name=name).set(capacity)
                g_volume.labels(name=name).set(volume)
                g_percent.labels(name=name).set(percent)
                g_volume_1y_ago.labels(name=name).set(volume_1y_ago)
                g_percent_1y_ago.labels(name=name).set(percent_1y_ago)
                g_volume_5y_avg.labels(name=name).set(volume_5y_avg)
                g_percent_5y_avg.labels(name=name).set(percent_5y_avg)

        if verbose:
            print(80 * '-' + '\n')



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