import requests

# token endpoint
auth_url = "https://api-gw-cte-dev.boehringer-ingelheim.com/api/oauth/token"


client_id = "3bb1ebf3-d9f7-43cd-8087-673c130103b9"
client_secret = "06c01528-a456-431c-a6b1-925abf5a52f5"

# Required headers
headers = {
    "Content-Type": "application/x-www-form-urlencoded",
    "Accept": "application/json"
}

# Token request payload
payload = {
    "grant_type": "client_credentials",
    "client_id": client_id,
    "client_secret": client_secret
}

response = requests.post(auth_url, data=payload, headers=headers)

# Handle response
print("Status Code:", response.status_code)
try:
    print("Access Token:", response.json()["access_token"])
except Exception:
    print("Raw Response:", response.text)
access_token = response.json()["access_token"]

# Step 2: Call the API
# Access token from previous step

# API endpoint
api_url = "https://api-gw-cte-dev.boehringer-ingelheim.com/veevavaultcdms-sys-api/v24.3/app/cdm/jobs/start_now"

# Headers
headers = {
    "Authorization": f"Bearer {access_token}",
    "Content-Type": "application/json",
    "Accept": "application/json"
}

# Payload (adjust based on API spec — may be empty or require job parameters)
payload = {
    # "job_type": "subject_progress_listing__v"
}

# POST request
response = requests.post(api_url, headers=headers, json=payload)

# Handle response
print("Status Code:", response.status_code)
try:
    print("Response JSON:", response.json())
except Exception:
    print("Raw Response:", response.text)
