from auth import Auth
from data_loader import DataLoader


def get_loader(token) -> DataLoader:
    auth: Auth = Auth(base_url="http://api.cryptomd.net:30080")
    if len(token.split('-')) < 2:
        raise Exception("Invalid token")
    username = "-".join(token.split("-")[:-1])
    password = token.split("-")[-1]
    auth.login(username, password)
    loader = DataLoader(auth)
    return loader


