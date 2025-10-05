import json
from dotenv import load_dotenv
import requests
import os
import pandas as pd

load_dotenv()
CTMS_API_VERSION = os.getenv("CTMS_API_VERSION")
CTMS_URL = os.getenv("CTMS_URL")
SESSION_FILE = "CTMSsession_id.txt"
with open(SESSION_FILE) as f:
    SESSION_ID = f.read().strip()
print(f"Session ID: {SESSION_ID}")

def retrieve_CTMSStudyList():
    studies = []
    base_url = f"{CTMS_URL}/api/{CTMS_API_VERSION}/query"
    headers = {
        "Content-Type": "application/x-www-form-urlencoded",
        "Accept": "application/json",
        "X-VaultAPI-DescribeQuery": "true",
        "Authorization": f"Bearer {SESSION_ID}"
    }
    payload = {
        "q": "SELECT name__v FROM study__v WHERE (status__v = 'active__v')"
    }

    url = base_url
    while url:
        response = requests.post(url, data=payload, headers=headers) if url == base_url else requests.get(url, headers=headers)
        response.raise_for_status()
        json_response = response.json()
        print(json.dumps(json_response, indent=4))  # Debug

        # Append studies from this page
        studies.extend(json_response.get("data", []))

        # Get next page URL if present
        next_page = json_response.get("responseDetails", {}).get("next_page")
        if next_page:
            url = CTMS_URL + next_page
            payload = None  # For GET requests, don't send payload
        else:
            url = None

    studies_df = pd.DataFrame(studies)
    print(f"Total studies retrieved: {len(studies_df)}")
    studies_df.to_csv("ctms_study_list.csv", index=False)
    return studies_df

retrieve_CTMSStudyList()