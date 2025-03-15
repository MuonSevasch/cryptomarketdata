from .auth import Auth
from .data_loader import DataLoader

def load(load_type, exchange, symbols, start_date, end_date, username, password):
    auth: Auth = Auth(base_url="http://localhost:8080")
    auth.login(username, password)

    loader = DataLoader(auth)
    loader.load_data(load_type, exchange, symbols, start_date, end_date)