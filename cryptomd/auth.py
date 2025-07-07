import requests


class Auth:
    def __init__(self, base_url):
        self.base_url = base_url
        self.token = None

    def login(self, username, password):
        login_url = f"{self.base_url}/login/sigin"
        login_payload = {"username": username, "password": password}

        response = requests.post(login_url, json=login_payload)
        if response.status_code == 200:
            self.token = response.text.strip()
            print("Login successful!")
        else:
            raise Exception(f"Login failed. Status code: {response.status_code}")

    def get_headers(self):
        if not self.token:
            raise Exception("Not authenticated. Please login first.")
        return {"Authorization": f"Bearer {self.token}"}
