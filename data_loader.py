import os
import re

import requests

from .auth import Auth

class DataLoader:
    def __init__(self, auth: Auth):
        self.auth = auth
        self.pattern = r"/(\d{4}-\d{2}-\d{2})/(\d{2}-\d{2}-\d{2})\.parquet"
        self.data_types = ['deltas', 'book_ticks', 'snapshots']

    def load_data(self, data_type, exchange, symbols, start_date, end_date):
        """Загружает данные указанного типа с сервера"""
        endpoints = {
            'deltas': 'deltas/get-files-urls',
            'book_ticks': 'book_ticks/get-files-urls',
            'snapshots': 'snapshots/get-files-urls'
        }

        if data_type not in endpoints:
            raise ValueError(f"Unsupported data type: {data_type}. Available types: {list(endpoints.keys())}")

        for symbol in symbols:
            url = f"{self.auth.base_url}/api/{endpoints[data_type]}"
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
                    self.download_and_save(file_url, exchange, symbol, data_type)

            except requests.exceptions.RequestException as e:
                print(f"Error loading {data_type} data for {symbol}: {str(e)}")

    def download_and_save(self, file_url, exchange, symbol, data_type):
        response = requests.get(file_url)
        if response.status_code == 200:
            # Создаем директорию с учетом типа данных
            directory = f"data/{exchange}/{symbol}/{data_type}"
            os.makedirs(directory, exist_ok=True)

            match = re.search(self.pattern, file_url)
            file_path = f"{directory}/{symbol}_.parquet"
            if match:
                date = match.group(1)
                time = match.group(2)
                file_path = f"{directory}/{date}T{time}.parquet"

            # Сохраняем файл
            with open(file_path, 'wb') as f:
                f.write(response.content)

            print(f"File saved to {file_path}")
        else:
            print(f"Failed to download file from {file_url}. Status code: {response.status_code}")