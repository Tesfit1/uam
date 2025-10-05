import pandas as pd
import requests
import os
import time
import csv
from dotenv import load_dotenv

# ─── Load Environment Variables ─────────────────────────────
load_dotenv()
VAULT_DOMAIN = os.getenv("BASE_URL")
API_VERSION = os.getenv("API_VERSION")
SESSION_FILE = "session_id.txt"
STUDY_CSV = "study_output.csv"
FAILURE_LOG = "cdms_study_failures.csv"
ORGANIZATION_NAME = "Boehringer Ingelheim"
DELAY_SECONDS = int(os.getenv("CDMS_DELAY", 5))


# ─── Load CDMS Session ─────────────────────────────────────
def load_session_id():
    try:
        with open(SESSION_FILE) as f:
            session_id = f.read().strip()
            if not session_id:
                raise ValueError("Session ID file is empty.")
            print("🔐 CDMS session loaded.")
            return session_id
    except Exception as e:
        print(f"X Failed to load session ID: {e}")
        return None


# ─── Check Session Validity ────────────────────────────────
def is_session_valid(response):
    """Check if the API response indicates an expired session"""
    if response.status_code == 401:
        try:
            json_resp = response.json()
            if "errors" in json_resp and any(
                    err.get("type") == "INVALID_SESSION_ID" for err in json_resp.get("errors", [])):
                print("X ERROR: Session ID has expired. Please run CDMSAuth.py to refresh your session.")
                return False
        except:
            pass
    return True


# ─── Check Study Existence ─────────────────────────────────
def study_exists(session_id, study_name):
    url = f"{VAULT_DOMAIN}/api/{API_VERSION}/app/cdm/design/study_masters"
    headers = {
        "Authorization": f"Bearer {session_id}",
        "Accept": "application/json"  # This is needed to get JSON responses
    }
    params = {"study_master_name": study_name}

    try:
        response = requests.get(url, headers=headers, params=params)

        # Print response for debugging
        print(f"Study check response: {response.status_code}")
        print(f"Response content: {response.text[:200]}...")  # Print first 200 chars

        # Check for session expiration using the shared function
        if not is_session_valid(response):
            return False, "SESSION_EXPIRED"

        if response.status_code == 200:
            json_data = response.json()
            # The API returns study_masters array, not data array
            records = json_data.get("study_masters", [])
            print(f"Found {len(records)} matching studies")
            return len(records) > 0, None
        else:
            print(f" Study check failed for '{study_name}': {response.status_code}")
            return False, f"HTTP {response.status_code}"
    except requests.exceptions.RequestException as e:
        print(f" Error during study existence check: {e}")
        return False, str(e)
    except ValueError as e:
        print(f" Error parsing JSON response: {e}")
        print(f"Raw response: {response.text}")
        return False, str(e)


# ─── Submit Study Creation ─────────────────────────────────
def create_study(session_id, payload):
    url = f"{VAULT_DOMAIN}/api/{API_VERSION}/app/cdm/design/actions/create_study"
    headers = {
        "Authorization": f"Bearer {session_id}",
        "Content-Type": "application/json"
    }

    try:
        response = requests.post(url, headers=headers, json=payload)

        # Check for session expiration using the shared function
        if not is_session_valid(response):
            return False, "SESSION_EXPIRED"

        if response.status_code == 200:
            print(f" Study creation submitted for: {payload['study_master_name']}")
            return True, None
        else:
            print(f" Submission failed for '{payload['study_master_name']}'")
            print(f"Status: {response.status_code} → {response.text}")
            return False, f"HTTP {response.status_code}"
    except requests.exceptions.RequestException as e:
        print(f" Request error during creation: {e}")
        return False, str(e)


# ─── Log Failures to CSV ───────────────────────────────────
def log_failure(name, external_id, reason):
    header_needed = not os.path.exists(FAILURE_LOG)
    with open(FAILURE_LOG, mode="a", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        if header_needed:
            writer.writerow(["study_master_name", "external_id", "reason"])
        writer.writerow([name, external_id, reason])


# ─── Main Workflow ─────────────────────────────────────────
def process_study_list():
    session_id = load_session_id()
    if not session_id:
        return

    if not os.path.exists(STUDY_CSV):
        print(f" Missing CSV file: {STUDY_CSV}")
        return

    df = pd.read_csv(STUDY_CSV)
    required_fields = {"name__v", "external_id__v", "global_id__sys"}
    # required_fields = {"name__v", "external_id__v", "status__v", "global_id__sys"}
    if not required_fields.issubset(df.columns):
        print(" CSV is missing required headers.")
        return

    for i, row in df.iterrows():
        name = str(row["name__v"]).strip() if pd.notna(row["name__v"]) else ""
        external_id = str(row["external_id__v"]).strip() if pd.notna(row["external_id__v"]) else ""
        global_id = str(row["global_id__sys"]).strip() if pd.notna(row["global_id__sys"]) else ""

        if not name or not global_id:
            print(f" Row {i + 1} skipped due to missing name or external_id.")
            log_failure(name, external_id, "Missing name or external_id")
            continue

        payload = {
            "study_master_name": name,
            "organization_name": ORGANIZATION_NAME,
            "external_id": external_id
        }

        # Submit creation request
        submitted, error = create_study(session_id, payload)
        if error == "SESSION_EXPIRED":
            print(" Processing stopped: Session expired. Please refresh your session.")
            break

        time.sleep(3)  # Allow backend time to register study

        # Confirm registration
        exists, error = study_exists(session_id, name)
        if error == "SESSION_EXPIRED":
            print(" Processing stopped: Session expired. Please refresh your session.")
            break

        if exists:
            print(f" Verified: Study '{name}' exists in CDMS.")
        else:
            reason = "Created but not found" if submitted else "Creation request failed"
            print(f" {reason} → logging failure.")
            log_failure(name, external_id, reason)
            continue

        time.sleep(DELAY_SECONDS)  # Respect pacing


# ─── Entry Point ───────────────────────────────────────────
if __name__ == "__main__":
    process_study_list()