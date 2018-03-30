import zipfile
import sqlite3
from os import path

import requests

INFO_URL = 'http://www.bestchange.ru/bm/info.zip'
INFO_FILE_PATH = '/tmp/info.zip'
UNZIPPED_INFO_DIR = '/tmp/info'
CURRENCIES_FILENAME = 'bm_cy.dat'
EXCHANGERS_FILENAME = 'bm_exch.dat'
RATES_FILENAME = 'bm_rates.dat'

BEST_RATES_QUERY = \
    """
    SELECT
      from_currencies.name AS from_currency,
      to_currencies.name AS to_currency,
      exchangers.name AS exchanger,
      rates.give_amount AS give_amount,
      rates.receive_amount AS receive_amount
    FROM
      (
        SELECT
          from_id, to_id,
          MIN(give_amount / receive_amount) AS rate
        FROM rates
        GROUP BY from_id, to_id
      ) AS best_rates
    JOIN rates
      ON
        best_rates.from_id = rates.from_id
        AND best_rates.to_id = rates.to_id
        AND ABS(best_rates.rate - rates.give_amount / rates.receive_amount) < 1e-18
    JOIN currencies AS from_currencies
      ON rates.from_id = from_currencies.id
    JOIN currencies AS to_currencies
      ON rates.to_id = to_currencies.id
    JOIN exchangers
      ON rates.exchanger_id = exchangers.id
    """


def load_ticker_data():
    _download_file(INFO_URL, INFO_FILE_PATH)
    _unzip_file(INFO_FILE_PATH, UNZIPPED_INFO_DIR)
    with sqlite3.connect(':memory:') as conn:
        cursor = conn.cursor()
        _load_to_sqlite(
            cursor, 'currencies',
            path.join(UNZIPPED_INFO_DIR, CURRENCIES_FILENAME),
            (0, 'id', 'INTEGER'), (2, 'name', 'TEXT')
        )
        _load_to_sqlite(
            cursor, 'exchangers',
            path.join(UNZIPPED_INFO_DIR, EXCHANGERS_FILENAME),
            (0, 'id', 'INTEGER'), (1, 'name', 'TEXT')
        )
        _load_to_sqlite(
            cursor, 'rates', path.join(UNZIPPED_INFO_DIR, RATES_FILENAME),
            (0, 'from_id', 'INTEGER'), (1, 'to_id', 'INTEGER'),
            (2, 'exchanger_id', 'INTEGER'), (3, 'give_amount', 'REAL'),
            (4, 'receive_amount', 'REAL'), (5, 'available_amount', 'REAL')
        )
        cursor.execute(BEST_RATES_QUERY)
        return cursor.fetchall()


def _download_file(source_url, target_path):
    r = requests.get(source_url, stream=True)
    if r.status_code != 200:
        r.raise_for_status()
    with open(target_path, 'wb') as f:
        for chunk in r:
            f.write(chunk)


def _unzip_file(zip_path, target_dir):
    with zipfile.ZipFile(zip_path) as zip_ref:
        zip_ref.extractall(target_dir)


def _load_to_sqlite(cursor, table_name, source_filename, *columns):
    cursor.execute('DROP TABLE IF EXISTS {}'.format(table_name))
    cursor.execute(
        'CREATE TABLE {table_name} ({columns})'.format(
            table_name=table_name,
            columns=','.join(
                '{} {}'.format(name, type_) for _, name, type_ in columns
            )
        )
    )
    with open(source_filename, encoding='cp1251') as file:
        for row in file:
            row_values = row.split(';')
            result_values = []
            for idx, _, _ in columns:
                result_values.append(row_values[idx])
            cursor.execute(
                'INSERT INTO {table_name} VALUES ({values})'.format(
                    table_name=table_name,
                    values=','.join('"{}"'.format(v) for v in result_values)
                )
            )
