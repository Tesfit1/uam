import requests
import time

class AxwayTokenManager:
    def __init__(self, auth_url, client_id, client_secret):
        self.auth_url = auth_url
        self.client_id = client_id
        self.client_secret = client_secret
        self.token = None
        self.expiry = 0

    def get_token(self):
        if self.token and time.time() < self.expiry:
            return self.token

        response = requests.post(self.auth_url, data={
            "grant_type": "client_credentials",
            "client_id": self.client_id,
            "client_secret": self.client_secret
        }, headers={
            "Content-Type": "application/x-www-form-urlencoded",
            "Accept": "application/json"
        })

        if response.status_code != 200:
            raise Exception(f"Token request failed: {response.text}")

        data = response.json()
        self.token = data["access_token"]
        self.expiry = time.time() + data.get("expires_in", 3600) - 60  # buffer before expiry
        return self.token