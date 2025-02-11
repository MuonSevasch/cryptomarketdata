import os
import re

import requests

from .auth import Auth


class DataLoader:
    def __init__(self, auth: Auth):
        self.auth = auth
        self.pattern = r"/(\d{4}-\d{2}-\d{2})/(\d{2}-\d{2}-\d{2})\.parquet"

    def load_data(self, exchange, symbols, start_date, end_date):
        for symbol in symbols:
            # Запрос на сервер для получения ссылки на данные
            url = f"{self.auth.base_url}/api/deltas/get-files-urls"
            params = {
                "code": symbol,
                "startTime": start_date,
                "endTime": end_date,
                "expirationInMinutes": 60
            }

            response = requests.get(url, headers=self.auth.get_headers(), params=params)

            if response.status_code == 200:
                file_urls = response.json()
                for file_url in file_urls:
                    self.download_and_save(file_url, exchange, symbol)
            else:
                print(f"Failed to retrieve file URLs for {symbol}. Status code: {response.status_code}")


    def download_and_save(self, file_url, exchange, symbol):
        response = requests.get(file_url)
        if response.status_code == 200:

            # Создаем директорию для сохранения файла
            directory = f"data/{exchange}/{symbol}"
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
