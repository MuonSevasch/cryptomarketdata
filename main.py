import cryptomarketdata

def main():
    # Загрузка данных
    try:
        cryptomarketdata.load("binance", ["BTCUSDT", "ETHUSDT"],
                              "2024-01-01", "2024-02-01",
                              ["updates", "trades"])
        print("Data loaded successfully.")
    except Exception as e:
        print(f"An error occurred: {e}")

if __name__ == "__main__":
    main()