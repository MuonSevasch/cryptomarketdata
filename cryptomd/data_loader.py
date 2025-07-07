import os
import re
from typing import Literal, Union, Sequence
from datetime import datetime, date

import requests

from .auth import Auth

ExchangeType = Literal["binance"]
MarketType   = Literal["spot", "coin", "usd"]
MDType       = Literal["deltas", "book_ticks", "snapshots"]


class DataLoader:
    def __init__(self, auth: Auth): #todo: custom save path
        self.auth = auth
        self.pattern = r"/(\d{4}-\d{2}-\d{2})/(\d{2}-\d{2}-\d{2})\.parquet"
        self.data_types = ['deltas', 'book_ticks', 'snapshots'] #todo: avoid duplicating
        self.exchanges = ['binance']
        self.markets = ['spot', 'coin', 'usd']


    def load_period(
        self,
        exchange: ExchangeType,
        market: MarketType,
        md_type: MDType,
        symbols: Union[str, Sequence[str]],
        start_date: Union[str, datetime, date],
        end_date:   Union[str, datetime, date],
    ) -> None:
        """
        Load market‐data files from `start_date` to `end_date` (inclusive).
        Accepts:
          - date‐only string "YYYY-MM-DD" → uses 00:00:00 for start, 23:00:00 for end
          - full ISO string "YYYY-MM-DDTHH:MM:SS" → honors the hour (must be on the hour)
          - datetime (must be on the hour)
          - date → treated like date‐only string
        """

        def _normalize(
            d: Union[str, datetime, date],
            is_end: bool,
            name: str
        ) -> str:
            # date‐only string
            if isinstance(d, str) and re.fullmatch(r"\d{4}-\d{2}-\d{2}", d):
                hour = 23 if is_end else 0
                return f"{d}T{hour:02d}:00:00"

            # datetime.date (but not datetime.datetime)
            if isinstance(d, date) and not isinstance(d, datetime):
                hour = 23 if is_end else 0
                dt = datetime(d.year, d.month, d.day, hour)
                return dt.strftime("%Y-%m-%dT%H:%M:%S")

            # full ISO‐string or datetime
            if isinstance(d, str):
                try:
                    dt = datetime.fromisoformat(d)
                except ValueError:
                    raise ValueError(f"{name} string not ISO‐format: {d!r}")
            elif isinstance(d, datetime):
                dt = d
            else:
                raise TypeError(f"{name} must be str|datetime|date, got {type(d).__name__}")

            # enforce no minutes/seconds/microseconds
            if dt.minute != 0 or dt.second != 0 or dt.microsecond != 0:
                raise ValueError(f"{name} must be exactly on the hour: {d!r}")

            return dt.strftime("%Y-%m-%dT%H:%M:%S")

        start_iso = _normalize(start_date, is_end=False, name="start_date")
        end_iso   = _normalize(end_date,   is_end=True,  name="end_date")

        # --- validate exchange, market, type ---
        if exchange not in self.exchanges:
            raise ValueError(f"Unsupported exchange: {exchange}. Available: {self.exchanges}")
        if market not in self.markets:
            raise ValueError(f"Unsupported market: {market}. Available: {self.markets}")
        if md_type not in self.data_types:
            raise ValueError(f"Unsupported data type: {md_type}. Available: {self.data_types}")

        # unify symbols list
        if isinstance(symbols, str):
            symbols = [symbols]

        # --- fetch URLs & download files ---
        for symbol in symbols:
            url = f"{self.auth.base_url}/api/data/{exchange}/{market}/{md_type}/get-files-urls"
            params = {
                "code": symbol,
                "startTime": start_iso,
                "endTime": end_iso,
                "expirationInMinutes": 60,
            }
            try:
                resp = requests.get(url, headers=self.auth.get_headers(), params=params, timeout=10)
                resp.raise_for_status()
                file_urls = resp.json()
                for file_url in file_urls:
                    self.load_by_url(file_url, exchange, market, symbol, md_type)
            except requests.RequestException as e:
                print(f"Error loading {md_type} data for {symbol}: {e}")


    def load_by_url(self, file_url, exchange, market, symbol, data_type):
        directory = f"data/{exchange}/{market}/{symbol}/{data_type}"

        match = re.search(self.pattern, file_url)
        if match:
            date = match.group(1)
            time = match.group(2)
            file_path = f"{directory}/{date}T{time}.parquet"
        else:
            print(f"Failed to download file from {file_url}")
            return
        if os.path.exists(file_path):
            print(f"File already exists at {file_path}")
            return
        response = requests.get(file_url)
        if response.status_code == 200:
            os.makedirs(directory, exist_ok=True)


            with open(file_path, 'wb') as f:
                f.write(response.content)

            print(f"File saved to {file_path}")
        else:
            print(f"Failed to download file from {file_url}. Status code: {response.status_code}")