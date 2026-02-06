import json
import os
import requests
from dotenv import load_dotenv
import logging

# ─── Load Environment Variables ─────────────────────────────
load_dotenv()
API_VERSION = os.getenv("API_VERSION")
BASE_URL = os.getenv("BASE_URL")
SESSION_FILE = "CDMSsession_id.txt"
CTMS_STUDY_JSON = os.getenv("CTMS_STUDY_JSON", "clinicalstudylist.json")
FILTERED_STUDY_JSON = "filtered_studies.json"

# ─── Logging Setup ──────────────────────────────────────────
LOG_FILE = "cdms_study_check.log"
logging.basicConfig(
    filename=LOG_FILE,
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
logger = logging.getLogger()

# ─── Load Session ID ────────────────────────────────────────
def load_session_id():
    """Load session ID from file."""
    try:
        with open(SESSION_FILE) as f:
            session_id = f.read().strip()
            if session_id:
                print("✅ CDMS Session ID loaded from file.")
                logger.info("CDMS Session ID loaded from file.")
                return session_id
            else:
                print("❌ CDMS Session ID file is empty.")
                logger.error("CDMS Session ID file is empty.")
    except FileNotFoundError:
        print(f"❌ Session ID file not found: {SESSION_FILE}")
        logger.error(f"Session ID file not found: {SESSION_FILE}")
    except Exception as e:
        print(f"❌ Failed to load session ID from file: {e}")
        logger.error(f"Failed to load session ID from file: {e}")

    return None

# ─── Load Studies from JSON ─────────────────────────────────
def load_studies_from_json(json_file):
    """Load studies from the JSON file."""
    try:
        with open(json_file, 'r') as f:
            studies = json.load(f)
            print(f"📄 Loaded {len(studies)} studies from {json_file}")
            logger.info(f"Loaded {len(studies)} studies from {json_file}")
            return studies
    except FileNotFoundError:
        print(f"❌ JSON file not found: {json_file}")
        logger.error(f"JSON file not found: {json_file}")
        return []
    except json.JSONDecodeError as e:
        print(f"❌ Invalid JSON format: {e}")
        logger.error(f"Invalid JSON format: {e}")
        return []
    except Exception as e:
        print(f"❌ Error loading JSON: {e}")
        logger.error(f"Error loading JSON: {e}")
        return []

# ─── Build VQL Query ────────────────────────────────────────
def build_vql_query(study_names):
    """Build VQL query to check if studies exist in CDMS."""
    if not study_names:
        return None

    # Create OR conditions for each study name - escape single quotes
    escaped_names = [name.replace("'", "\\'") for name in study_names]
    conditions = " OR ".join([f"name__v = '{name}'" for name in escaped_names])
    query = f"SELECT name__v FROM study__v WHERE ({conditions})"

    return query

# ─── Check Studies in CDMS ──────────────────────────────────
def check_existing_studies(session_id, study_names):
    """Query CDMS to check which studies already exist."""
    if not study_names:
        print("⚠️ No study names to check.")
        return set()

    # Trim session ID
    session_id = session_id.strip()

    query = build_vql_query(study_names)
    if not query:
        return set()

    url = f"{BASE_URL}/api/{API_VERSION}/query"
    headers = {
        "Content-Type": "application/x-www-form-urlencoded",
        "Accept": "application/json",
        "Authorization": f"Bearer {session_id}"
    }
    payload = {"q": query}

    existing_studies = set()

    print("🔍 Checking which studies already exist in CDMS...")
    logger.info(f"Executing VQL query: {query}")

    try:
        while url:
            response = requests.post(url, data=payload, headers=headers) if payload else requests.get(url, headers=headers)

            if response.status_code == 401:
                json_resp = response.json()
                if "errors" in json_resp and any(err.get("type") == "INVALID_SESSION_ID" for err in json_resp.get("errors", [])):
                    print("❌ Session ID expired. Run CDMSAuth.py to refresh.")
                    logger.warning("Session ID expired.")
                    return existing_studies

            if response.status_code != 200:
                error_detail = response.text[:200] if response.text else "No details"
                print(f"❌ API error {response.status_code}: {error_detail}")
                logger.error(f"API error {response.status_code}: {error_detail}")
                break

            json_response = response.json()

            if "errors" in json_response:
                error_detail = json_response['errors']
                print(f"❌ API returned an error: {error_detail}")
                logger.error(f"API returned an error: {error_detail}")
                break

            # Extract existing study names
            data = json_response.get("data", [])
            for study in data:
                study_name = study.get("name__v")
                if study_name:
                    existing_studies.add(study_name)

            # Check for next page
            next_page = json_response.get("responseDetails", {}).get("next_page")
            url = f"{BASE_URL}{next_page}" if next_page else None
            payload = None

        if existing_studies:
            print(f"⚠️ Found {len(existing_studies)} studies that already exist in CDMS:")
            for name in existing_studies:
                print(f"   - {name}")
            logger.info(f"Found {len(existing_studies)} existing studies: {existing_studies}")
        else:
            print("✅ No existing studies found. All studies are new.")
            logger.info("No existing studies found in CDMS.")

        return existing_studies

    except requests.exceptions.RequestException as e:
        print(f"❌ Network error: {e}")
        logger.error(f"Network error: {e}")
        return existing_studies
    except Exception as e:
        print(f"❌ Error checking studies: {e}")
        logger.error(f"Error checking studies: {e}")
        return existing_studies

# ─── Filter Out Existing Studies ────────────────────────────
def filter_new_studies(studies, existing_study_names):
    """Remove studies that already exist in CDMS."""
    new_studies = [
        study for study in studies
        if study.get("name__v") not in existing_study_names
    ]

    removed_count = len(studies) - len(new_studies)

    if removed_count > 0:
        print(f"🗑️ Removed {removed_count} existing studies from the list.")
        logger.info(f"Removed {removed_count} existing studies.")

    return new_studies

# ─── Save Filtered Studies ──────────────────────────────────
def save_filtered_studies(studies, output_file):
    """Save filtered studies to JSON file."""
    try:
        with open(output_file, 'w') as f:
            json.dump(studies, f, indent=2)
        print(f"✅ Saved {len(studies)} new studies to {output_file}")
        logger.info(f"Saved {len(studies)} new studies to {output_file}")
    except Exception as e:
        print(f"❌ Error saving filtered studies: {e}")
        logger.error(f"Error saving filtered studies: {e}")

# ─── Main Execution ─────────────────────────────────────────
def main():
    logger.info("🔄 CDMS study check started.")
    print("🔄 CDMS study check started.")

    # Load CDMS session ID
    session_id = load_session_id()
    if not session_id:
        print("❌ No valid CDMS session ID. Run CDMSAuth.py first.")
        logger.error("No valid CDMS session ID. Aborting.")
        return

    # Load studies from JSON
    studies = load_studies_from_json(CTMS_STUDY_JSON)
    if not studies:
        print("❌ No studies to check.")
        logger.error("No studies loaded. Aborting.")
        return

    # Extract study names
    study_names = [study.get("name__v") for study in studies if study.get("name__v")]

    if not study_names:
        print("❌ No valid study names found in JSON.")
        logger.error("No valid study names found.")
        return

    print(f"📋 Checking {len(study_names)} studies against CDMS...")

    # Check which studies already exist in CDMS
    existing_studies = check_existing_studies(session_id, study_names)

    # Filter out existing studies
    new_studies = filter_new_studies(studies, existing_studies)

    # Save filtered studies
    if new_studies:
        save_filtered_studies(new_studies, FILTERED_STUDY_JSON)
        print(f"✅ {len(new_studies)} new studies ready for creation.")
        logger.info(f"{len(new_studies)} new studies ready for creation.")
    else:
        print("⚠️ No new studies to create. All studies already exist in CDMS.")
        logger.warning("No new studies to create.")

    print("✅ CDMS study check completed.")
    logger.info("✅ CDMS study check completed.")

if __name__ == "__main__":
    main()
