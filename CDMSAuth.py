import os
from dotenv import load_dotenv
from requests.auth import HTTPBasicAuth
import requests

load_dotenv()

# Read environment variables
CDMS_CLIENT_ID = os.getenv("CDMS_CLIENT_ID")
CDMS_CLIENT_SECRET = os.getenv("CDMS_CLIENT_SECRET")
API_VERSION = os.getenv("API_VERSION")
BASE_URL = os.getenv("BASE_URL")

# Auth

# Define the URL and headers
url =  f"{BASE_URL}/api/{API_VERSION}/auth"
headers = {
    'Content-Type': 'application/x-www-form-urlencoded',
    'Accept': 'application/json',
}

# Define the data
data = {
    'username': CDMS_CLIENT_ID,
    'password': CDMS_CLIENT_SECRET
}

# Send the POST request
response = requests.post(url, headers=headers, data=data)

# Print the response
print(f"Authentication response: " + str(response.json()))
response_json = response.json()

# Store the session ID
if 'sessionId' in response_json:
    session_id = response_json['sessionId']

    # Use absolute path for shared session directory
    session_dir = "."
    try:
        os.makedirs(session_dir, exist_ok=True)
        session_file_path = os.path.join(session_dir, "CDMSsession_id.txt")
        with open(session_file_path, "w") as f:
            f.write(session_id)
        print(f"Session ID saved successfully at {session_file_path}.")
    except Exception as e:
        print(f"Error saving session ID: {e}")
else:
    print("Error: 'sessionId' not found in the response.")

