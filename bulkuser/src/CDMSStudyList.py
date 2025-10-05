import json
from dotenv import load_dotenv
import requests
import os
import pandas as pd
from io import StringIO 

load_dotenv()
API_VERSION = os.getenv("API_VERSION")
BASE_URL = os.getenv("BASE_URL")
# SESSION_ID = os.getenv("SESSION_ID")
SESSION_FILE = "session_id.txt"
with open(SESSION_FILE) as f:
    SESSION_ID = f.read().strip()
print(f"Session ID: {SESSION_ID}")

# 
def retrieve_CDMSStudyList():
    studies = []
    base_url = f"{BASE_URL}/api/{API_VERSION}/query"
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
            # next_page is a relative URL, so prepend BASE_URL
            url = BASE_URL + next_page
            payload = None  # For GET requests, don't send payload
        else:
            url = None

    studies_df = pd.DataFrame(studies)
    print(f"Total studies retrieved: {len(studies_df)}")
    studies_df.to_csv("cdms_study_list.csv", index=False)
    return studies_df

retrieve_CDMSStudyList()