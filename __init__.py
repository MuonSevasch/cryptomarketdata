from .auth import Auth
from .data_loader import DataLoader

def load(exchange, market , load_type, symbols, start_date, end_date, username, password):
    auth: Auth = Auth(base_url="http://194.87.140.9:30080")
    auth.login(username, password)

    loader = DataLoader(auth)
    loader.load_data(exchange, market , load_type, symbols, start_date, end_date)