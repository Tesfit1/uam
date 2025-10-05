import os
import json
import pandas as pd
import requests
import redis
from dotenv import load_dotenv

# ─── Load Environment Variables ─────────────────────────────
load_dotenv()
API_VERSION = os.getenv("API_VERSION")
BASE_URL = os.getenv("BASE_URL")

REDIS_HOST = os.getenv("REDIS_HOST", "localhost")
REDIS_PORT = int(os.getenv("REDIS_PORT", 6379))
REDIS_DB = int(os.getenv("REDIS_DB", 0))
REDIS_SESSION_KEY = "CDMS:session_id"
SESSION_FILE = "CDMSsession_id.txt"

# ─── Load Session ID from Redis or Fallback to File ─────────
def load_session_id():
    try:
        r = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, db=REDIS_DB, socket_connect_timeout=3)
        session_id = r.get(REDIS_SESSION_KEY)
        if session_id:
            print("✅ Session ID loaded from Redis.")
            return session_id.decode()
        else:
            print("⚠️ Redis key not found. Falling back to file.")
    except Exception as e:
        print(f"⚠️ Redis unavailable: {e}. Falling back to file.")

    try:
        with open(SESSION_FILE) as f:
            session_id = f.read().strip()
            if session_id:
                print("✅ Session ID loaded from file.")
                return session_id
            else:
                print("❌ Session ID file is empty.")
    except Exception as e:
        print(f"❌ Failed to load session ID from file: {e}")
    return None

# ─── Load Session ID ────────────────────────────────────────
SESSION_ID = load_session_id()
if not SESSION_ID:
    print("❌ No valid session ID. Aborting.")
    exit(1)

# ─── Load User Data from CSV ────────────────────────────────
dir = os.path.dirname(os.path.abspath(__file__))
template_path = os.path.join(dir, "user-import-template-24r2.csv")

try:
    df = pd.read_csv(template_path)
    df = df.fillna("")
except Exception as e:
    print(f"❌ Error reading CSV: {e}")
    exit(1)

users_to_import = [row.to_dict() for _, row in df.iterrows()]
if not users_to_import:
    print("⚠️ No users found in the template.")
    exit(0)

# ─── Prepare Payload and Send Request ───────────────────────
payload = {
    "append_site_country_access": True,
    "users": users_to_import
}
print(json.dumps(payload, indent=4))

url = f"{BASE_URL}/api/{API_VERSION}/app/cdm/users_json"
headers = {
    "Accept": "application/json",
    "Content-Type": "application/json",
    "Authorization": f"Bearer {SESSION_ID}"
}

try:
    response = requests.post(url, headers=headers, data=json.dumps(payload))
    response.raise_for_status()
    print("✅ Import response:")
    print(json.dumps(response.json(), indent=4))
except requests.exceptions.RequestException as e:
    print(f"❌ Import failed: {e}")