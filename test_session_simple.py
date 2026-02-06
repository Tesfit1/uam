"""Simple CDMS session test"""
import os
import requests
from dotenv import load_dotenv
import urllib3

# Disable SSL warnings
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

load_dotenv()

# Load session
with open('CDMSsession_id.txt') as f:
    session_id = f.read().strip()

print(f"Session ID length: {len(session_id)}")
print(f"Session ID: {session_id[:30]}...{session_id[-10:]}")

# Test 1: Simple query
url = f"{os.getenv('BASE_URL')}/api/{os.getenv('API_VERSION')}/query"
headers = {
    'Authorization': f'Bearer {session_id}',
    'Accept': 'application/json',
    'Content-Type': 'application/x-www-form-urlencoded'
}

query = "SELECT id, name__v FROM study__v LIMIT 1"

print(f"\nTest Query: {query}")
print(f"URL: {url}")
print("Sending request...")

try:
    response = requests.post(url, data={'q': query}, headers=headers, verify=False, timeout=10)

    print(f"Status: {response.status_code}")
    print(f"Response: {response.json()}")

    if response.status_code == 200:
        json_resp = response.json()
        if "errors" in json_resp:
            print(f"\n❌ Session INVALID - Errors: {json_resp['errors']}")
        else:
            print("\n✅ Session IS VALID!")
    else:
        print("\n❌ Session INVALID")
except requests.exceptions.Timeout:
    print("\n❌ Request timed out after 10 seconds")
except Exception as e:
    print(f"\n❌ Error: {e}")
