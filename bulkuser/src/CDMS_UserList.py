import json
from dotenv import load_dotenv
import requests
import os
import pandas as pd

load_dotenv()
API_VERSION = os.getenv("API_VERSION")
BASE_URL = os.getenv("BASE_URL")
study_name = os.getenv("Study_name")
SESSION_FILE = "session_id.txt"
with open(SESSION_FILE) as f:
    SESSION_ID = f.read().strip()
print(f"Session ID: {SESSION_ID}")

def retrieve_CDMSusers():
    users = []
    base_url = f"{BASE_URL}/api/{API_VERSION}/query"
 
    headers = {
        "Content-Type": "application/x-www-form-urlencoded",
        "Accept": "application/json",
        "X-VaultAPI-DescribeQuery": "true",
        "Authorization": f"Bearer {SESSION_ID}"
    }
    payload = {
        "q": "SELECT user_name__v, user_email__v   FROM users   "
    }

    url = base_url
    while url:
        response = requests.post(url, data=payload, headers=headers) if url == base_url else requests.get(url, headers=headers)
        response.raise_for_status()
        json_response = response.json()
        print(json.dumps(json_response, indent=4))  # Debug

        # Append studies from this page
        users.extend(json_response.get("data", []))

        # Get next page URL if present
        next_page = json_response.get("responseDetails", {}).get("next_page")
        if next_page:
            # next_page is a relative URL, so prepend BASE_URL
            url = BASE_URL + next_page
            payload = None  # For GET requests, don't send payload
        else:
            url = None
    users_df = pd.DataFrame(users)
    print(f"Total users retrieved: {len(users_df)}")
    users_df.to_csv("cdms_user_list.csv", index=False)
    return users_df

retrieve_CDMSusers()




# import json
# from dotenv import load_dotenv
# import requests
# import os
# import pandas as pd

# load_dotenv()
# API_VERSION = os.getenv("API_VERSION")
# BASE_URL = os.getenv("BASE_URL")
# study_name = os.getenv("Study_name")
# SESSION_FILE = "session_id.txt"
# with open(SESSION_FILE) as f:
#     SESSION_ID = f.read().strip()
# print(f"Session ID: {SESSION_ID}")

# def retrieve_CDMSusers():
#     users = []
#     base_url = f"{BASE_URL}/api/{API_VERSION}/app/cdm/users"
#     # Only add study_name if it is set and not empty
#     if study_name:
#         url = f"{base_url}?study_name={study_name}"
#     else:
#         url = base_url
#     headers = {
#         "Content-Type": "application/json",
#         "Accept": "application/json",
#         "Authorization": f"Bearer {SESSION_ID}",
#     }

#     while url:
#         response = requests.get(url, headers=headers)
#         response.raise_for_status()
#         json_response = response.json()
#         print(json.dumps(json_response, indent=4))  # Debug

#         # Append users from this page
#         users.extend(json_response.get("users", []))

#         # Get next page URL if present
#         next_page = json_response.get("responseDetails", {}).get("next_page")
#         if next_page:
#             url = BASE_URL + next_page
#         else:
#             url = None

#     users_df = pd.DataFrame(users)
#     print(f"Total users retrieved: {len(users_df)}")
#     users_df.to_csv("cdms_user_list.csv", index=False)
#     return users_df

# retrieve_CDMSusers()