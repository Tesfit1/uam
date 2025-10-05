import os
import requests
import redis
from dotenv import load_dotenv

# ─── Load Environment Variables ─────────────────────────────
load_dotenv()
CLIENT_ID = os.getenv("CLIENT_ID")
CLIENT_SECRET = os.getenv("CLIENT_SECRET")
API_VERSION = os.getenv("API_VERSION")
BASE_URL = os.getenv("BASE_URL")

REDIS_HOST = os.getenv("REDIS_HOST", "localhost")
REDIS_PORT = int(os.getenv("REDIS_PORT", 6379))
REDIS_DB = int(os.getenv("REDIS_DB", 0))
REDIS_SESSION_KEY = "CDMS:session_id"
SESSION_FILE = "CDMSsession_id.txt"

# ─── Authenticate ──────────────────────────────────────────
url = f"{BASE_URL}/api/{API_VERSION}/auth"
headers = {
    'Content-Type': 'application/x-www-form-urlencoded',
    'Accept': 'application/json',
}
data = {
    'username': CLIENT_ID,
    'password': CLIENT_SECRET
}

try:
    response = requests.post(url, headers=headers, data=data)
    response.raise_for_status()
    response_json = response.json()
    print(f"🔐 Authentication response: {response_json}")
except requests.exceptions.RequestException as e:
    print(f"❌ Authentication failed: {e}")
    exit(1)

# ─── Store Session ID ──────────────────────────────────────
if 'sessionId' in response_json:
    session_id = response_json['sessionId']
    saved_to_redis = False
    saved_to_file = False

    # Try saving to Redis
    try:
        r = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, db=REDIS_DB, socket_connect_timeout=3)
        r.set(REDIS_SESSION_KEY, session_id)
        print(f"✅ Session ID saved to Redis under key '{REDIS_SESSION_KEY}'.")
        saved_to_redis = True
    except Exception as e:
        print(f"⚠️ Redis unavailable or failed to save: {e}")

    # Always save to file as fallback
    try:
        os.makedirs(".", exist_ok=True)
        with open(SESSION_FILE, "w") as f:
            f.write(session_id)
        print(f"✅ Session ID saved to file: {SESSION_FILE}")
        saved_to_file = True
    except Exception as e:
        print(f"❌ Failed to save session ID to file: {e}")

    # Final status
    if saved_to_redis and saved_to_file:
        print("🟢 Session ID saved to both Redis and file.")
    elif saved_to_redis:
        print("🟡 Session ID saved to Redis only.")
    elif saved_to_file:
        print("🟡 Session ID saved to file only.")
    else:
        print("🔴 Failed to save session ID anywhere.")
else:
    print("❌ Error: 'sessionId' not found in the response.")