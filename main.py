# gcloud functions deploy exchange_rate --runtime python37 --trigger-http --region=asia-northeast1 --allow-unauthenticated
# poetry export --without-hashes -f requirements.txt > requirements.txt

import csv
import requests
from datetime import datetime
from google.cloud import bigquery

bigquery_database = 'taiwan_exchange_rate'
bigquery_table = 'exchange_rate'

def exchange_rate(request):
    # exhcange parser from Taiwan bank
    result = exchange_parser()

    # BigQuery schema create
    bq_create_dataset()
    bq_create_table()

    # data insert to BigQuery
    export_items_to_bigquery(result)

    return 'OK'

def exchange_parser():
    url = 'https://rate.bot.com.tw/xrt/flcsv/0/day?Lang=en-US'
    result = requests.get(url)
    text = (
        result.text.encode("utf-8-sig")
        .decode("utf-8-sig")
        .replace("\xef\xbb\xbf", "")
        .replace("\r", "")
    )
    csv_list = text.split("\n")
    header = csv_list[0].split(",")

    rate_data = {}
    now = datetime.now()

    result = []
    for row in csv_list[1:]:
        data = row.split(',')
        if len(data) <= 1:
            continue

        date = now.strftime('%Y-%m-%d')
        currency = data[0]
        cash_buying = float(data[2])
        cash_selling = float(data[12])
        cash_average = (cash_buying + cash_selling) / 2
        spot_buying = float(data[3])
        spot_selling = float(data[13])
        spot_average = (spot_buying + spot_selling) / 2

        result.append({
            'date': date,
            'currency': currency,
            'cash_buying': cash_buying,
            'cash_selling': cash_selling,
            'cash_average': cash_average,
            'spot_buying': spot_buying,
            'spot_selling': spot_selling,
            'spot_average': spot_average
        })

    return result

def bq_create_dataset():
    bigquery_client = bigquery.Client()
    dataset_ref = bigquery_client.dataset(bigquery_database)

    try:
        bigquery_client.get_dataset(dataset_ref)
    except Exception:
        dataset = bigquery.Dataset(dataset_ref)
        dataset = bigquery_client.create_dataset(dataset)
        print('Dataset {} created.'.format(dataset.dataset_id))

def bq_create_table():
    bigquery_client = bigquery.Client()
    dataset_ref = bigquery_client.dataset(bigquery_database)

    # Prepares a reference to the table
    table_ref = dataset_ref.table(bigquery_table)

    try:
        bigquery_client.get_table(table_ref)
    except Exception:
        schema = [
            bigquery.SchemaField('date', 'STRING', mode='REQUIRED', description='Exchange Date'),
            bigquery.SchemaField('currency', 'STRING', mode='REQUIRED', description='Currency'),
            bigquery.SchemaField('cash_buying', 'FLOAT', mode='REQUIRED', description='CashBuying'),
            bigquery.SchemaField('cash_selling', 'FLOAT', mode='REQUIRED', description='CashBuying'),
            bigquery.SchemaField('cash_average', 'FLOAT', mode='REQUIRED', description='CashAverage'),
            bigquery.SchemaField('spot_buying', 'FLOAT', mode='REQUIRED', description='SpotBuying'),
            bigquery.SchemaField('spot_selling', 'FLOAT', mode='REQUIRED', description='SpotSelling'),
            bigquery.SchemaField('spot_average', 'FLOAT', mode='REQUIRED', description='SpotAverage')
        ]
        table = bigquery.Table(table_ref, schema=schema)
        table = bigquery_client.create_table(table)
        print('table {} created.'.format(table.table_id))

def export_items_to_bigquery(result):
    # Instantiates a client
    bigquery_client = bigquery.Client()

    # Prepares a reference to the dataset
    dataset_ref = bigquery_client.dataset(bigquery_database)

    table_ref = dataset_ref.table(bigquery_table)
    table = bigquery_client.get_table(table_ref)  # API call

    errors = bigquery_client.insert_rows(table, result)  # API request
    if len(errors) > 0:
        print(errors)

if __name__ == '__main__':
    exchange_rate("")
