import os


def save_parquet(df, exchange, symbol, data_type):
    directory = f"data/{exchange}/{symbol}/"
    os.makedirs(directory, exist_ok=True)

    file_path = f"{directory}/{symbol}_{data_type}.parquet"
    df.to_parquet(file_path)
    print(f"Saved Parquet file to {file_path}")
