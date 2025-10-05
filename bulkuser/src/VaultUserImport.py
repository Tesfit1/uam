import os
import requests
from dotenv import load_dotenv

# Load environment variables
load_dotenv()
API_VERSION = os.getenv("API_VERSION")
BASE_URL = os.getenv("BASE_URL")
SESSION_FILE = "session_id.txt"
with open(SESSION_FILE) as f:
    SESSION_ID = f.read().strip()

# Paths
dir = os.path.dirname(os.path.abspath(__file__))
csv_file_path = os.path.join(dir, "user-import-template-24r2.csv")

# Prepare request
url = f"{BASE_URL}/api/{API_VERSION}/objects/users"
headers = {
     "Accept": "application/json",
    "Content-Type": "application/json",
    "Authorization": f"Bearer {SESSION_ID}"
}

# Send POST request with CSV file as binary
with open(csv_file_path, "rb") as file:
    response = requests.post(url, headers=headers, data=file)

# Check response
try:
    response.raise_for_status()
    # print("Import successful.")
    print(response.text)  # CSV response as text
except requests.exceptions.HTTPError as err:
    print("Import failed.")
    print(err)
    print(response.text)
