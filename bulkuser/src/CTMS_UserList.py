import json
from dotenv import load_dotenv
import requests
import os
import pandas as pd

load_dotenv()
CTMS_API_VERSION = os.getenv("CTMS_API_VERSION")
CTMS_URL = os.getenv("CTMS_URL")
CLIENT_ID = os.getenv("CLIENT_ID")
SESSION_FILE = "CTMSsession_id.txt"
with open(SESSION_FILE) as f:
    SESSION_ID = f.read().strip()
print(f"Session ID: {SESSION_ID}")

def retrieve_CTMS_users():
    users = []
    base_url = f"{CTMS_URL}/api/{CTMS_API_VERSION}/query"
    headers = {
        "Content-Type": "application/x-www-form-urlencoded",
        "Accept": "application/json",
        "X-VaultAPI-DescribeQuery": "true",
        "Authorization": f"Bearer {SESSION_ID}"
    }
    payload = {
        "q": "SELECT user_name__v, user_email__v FROM users "
    }

    url = base_url
    while url:
        if url == base_url:
            response = requests.post(url, data=payload, headers=headers)
        else:
            response = requests.get(url, headers=headers)
        response.raise_for_status()
        json_response = response.json()
        print(json.dumps(json_response, indent=4))  # Debug

        # Append users from this page
        users.extend(json_response.get("data", []))

        # Get next page URL if present
        next_page = json_response.get("responseDetails", {}).get("next_page")
        if next_page:
            url = CTMS_URL + next_page
            payload = None  # For GET requests, don't send payload
        else:
            url = None

    users_df = pd.DataFrame(users)
    print(f"Total users retrieved: {len(users_df)}")
    users_df.to_csv("ctms_user_list.csv", index=False)
    return users_df

retrieve_CTMS_users()