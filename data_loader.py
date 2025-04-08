import os
import re

import requests

from .auth import Auth

class DataLoader:
    def __init__(self, auth: Auth):
        self.auth = auth
        self.pattern = r"/(\d{4}-\d{2}-\d{2})/(\d{2}-\d{2}-\d{2})\.parquet"
        self.data_types = ['deltas', 'book_ticks', 'snapshots']
        self.exchange = ['binance']
        self.market = ['spot', 'coin', 'usd']

    def load_data(self, exchange, market , load_type, symbols, start_date, end_date):

        if exchange not in self.exchange:
            raise ValueError(f"Unsupported exchange: {exchange}. Available types: {list(self.exchange)}")

        if market not in self.market:
            raise ValueError(f"Unsupported market: {market}. Available types: {list(self.market)}")

        if load_type not in self.data_types:
            raise ValueError(f"Unsupported data type: {load_type}. Available types: {list(self.data_types)}")

        for symbol in symbols:
            url = f"{self.auth.base_url}/api/data/{exchange}/{market}/{load_type}/get-files-urls"
            params = {
                "code": symbol,
                "startTime": start_date,
                "endTime": end_date,
                "expirationInMinutes": 60
            }

            try:
                response = requests.get(
                    url,
                    headers=self.auth.get_headers(),
                    params=params,
                    timeout=10
                )
                response.raise_for_status()

                file_urls = response.json()
                for file_url in file_urls:
                    self.download_and_save(file_url, exchange, market, symbol, load_type)

            except requests.exceptions.RequestException as e:
                print(f"Error loading {load_type} data for {symbol}: {str(e)}")

    def download_and_save(self, file_url, exchange, market, symbol, data_type):
        response = requests.get(file_url)
        if response.status_code == 200:
            directory = f"data/{exchange}/{market}/{symbol}/{data_type}"
            os.makedirs(directory, exist_ok=True)

            match = re.search(self.pattern, file_url)
            file_path = f"{directory}/{symbol}_.parquet"
            if match:
                date = match.group(1)
                time = match.group(2)
                file_path = f"{directory}/{date}T{time}.parquet"

            with open(file_path, 'wb') as f:
                f.write(response.content)

            print(f"File saved to {file_path}")
        else:
            print(f"Failed to download file from {file_url}. Status code: {response.status_code}")