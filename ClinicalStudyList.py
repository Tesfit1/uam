import json
import os
import requests
from dotenv import load_dotenv
import redis
import logging

# ─── Load Environment Variables ─────────────────────────────
load_dotenv()
CTMS_API_VERSION = os.getenv("CTMS_API_VERSION")
CTMS_URL = os.getenv("CTMS_URL")
CDMS_API_VERSION = os.getenv("API_VERSION")
CDMS_BASE_URL = os.getenv("BASE_URL")
REDIS_SESSION_KEY = "CTMS:session_id"
SESSION_FILE = "CTMSsession_id.txt"
CDMS_SESSION_FILE = "CDMSsession_id.txt"
OUTPUT_JSON = os.getenv("CTMS_STUDY_JSON")
# PROCESSED_CSV = "processed_studies.csv"
FALLBACK_DATE = os.getenv("CTMS_FALLBACK_DATE", "2026-02-05T00:00:00.000Z")

REDIS_HOST = os.getenv("REDIS_HOST", "localhost")
REDIS_PORT = int(os.getenv("REDIS_PORT", 6379))
REDIS_DB = int(os.getenv("REDIS_DB", 0))
REDIS_KEY = "ctms:last_modified_date"

# ─── Logging Setup ──────────────────────────────────────────
LOG_FILE = "ctms_sync.log"
logging.basicConfig(
    filename=LOG_FILE,
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
logger = logging.getLogger()

# ─── Redis Utilities ───────────────────────────────────────
def get_last_modified_date():
    try:
        r = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, db=REDIS_DB, socket_connect_timeout=3)
        if not r.ping():
            raise ConnectionError("Redis ping failed.")
        value = r.get(REDIS_KEY)
        if value:
            date_str = value.decode()
            logger.info(f"Last modified date from Redis: {date_str}")
            print(f"🕒 Last modified date from Redis: {date_str}")
            return date_str
        else:
            logger.warning("No last modified date found in Redis. Using fallback.")
            return FALLBACK_DATE
    except Exception as e:
        logger.error(f"Redis unavailable: {e}")
        print(f"⚠️ Redis unavailable. Using fallback date: {FALLBACK_DATE}")
        return FALLBACK_DATE

def update_last_modified_date(studies_list):
    """Update Redis with the latest modified_date__v from the studies list."""
    if not studies_list:
        return
    try:
        # Extract all modified_date__v values
        modified_dates = [
            study.get('modified_date__v')
            for study in studies_list
            if study.get('modified_date__v')
        ]

        if modified_dates:
            latest_date = max(modified_dates)
            r = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, db=REDIS_DB)
            r.set(REDIS_KEY, latest_date)
            logger.info(f"Updated Redis with latest modified_date__v: {latest_date}")
            print(f"✅ Updated Redis with latest modified_date__v: {latest_date}")
    except Exception as e:
        logger.error(f"Failed to update Redis: {e}")
        print(f"❌ Failed to update Redis: {e}")

# ─── Build Query Dynamically ───────────────────────────────
def build_query(modified_date):
    return f"""
    SELECT name__v,id, global_id__sys, connect_to_vault_cdms__v,
        (SELECT
				study_organization_role__vr.name__v
				from selected_study_organization_roles__cr
				where study_organization_role__vr.name__v='IRT Services' OR  study_organization_role__vr.name__v='IRT Services (without Interface)' LIMIT 1)AS IRT_NON_IRT,
        (SELECT milestone_type__v FROM milestones__vr WHERE milestone_type__v = 'protocol_approved__c' )                
    FROM study__v 
    WHERE (connect_to_vault_cdms__v = false) AND ((milestone_master_set__v = 'OOW000000004010') OR (milestone_master_set__v = 'OOW000000000201') OR (milestone_master_set__v = 'OOW000000004001'))  AND (external_id__v = null) AND (status__v = 'active__v') and (state__v = 'planning_state__v') 

    AND id IN (SELECT milestone_type__v FROM milestones__vr WHERE milestone_type__v = 'protocol_approved__c' ) AND (modified_date__v > '{modified_date}')
    """

# ─── Load Session ID ────────────────────────────────────────
def load_session_id():
    # Try Redis first
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

    # Fallback to file
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

# ─── Load CDMS Session ID ───────────────────────────────────
def load_cdms_session_id():
    """Load CDMS session ID from file."""
    try:
        with open(CDMS_SESSION_FILE) as f:
            cdms_session_id = f.read().strip()
            if cdms_session_id:
                print(f"✅ CDMS Session ID loaded from file")
                logger.info("CDMS Session ID loaded from file.")
                return cdms_session_id
            else:
                print("⚠️ CDMS Session ID file is empty.")
                logger.warning("CDMS Session ID file is empty.")
    except FileNotFoundError:
        print(f"⚠️ CDMS Session ID file not found: {CDMS_SESSION_FILE}")
        logger.warning(f"CDMS Session ID file not found: {CDMS_SESSION_FILE}")
    except Exception as e:
        print(f"⚠️ Failed to load CDMS session ID from file: {e}")
        logger.warning(f"Failed to load CDMS session ID from file: {e}")

    return None

def retrieve_CTMSStudyList(session_id, query_str):
    studies = []
    base_url = f"{CTMS_URL}/api/{CTMS_API_VERSION}/query"
    headers = {
        "Content-Type": "application/x-www-form-urlencoded",
        "Accept": "application/json",
        "X-VaultAPI-DescribeQuery": "true",
        "Authorization": f"Bearer {session_id}"
    }
    payload = {"q": query_str}
    url = base_url

    print("📡 Fetching studies from ClinOps Vault...")
    try:
        while url:
            response = requests.post(url, data=payload, headers=headers) if url == base_url else requests.get(url, headers=headers)

            if response.status_code == 401:
                json_resp = response.json()
                if "errors" in json_resp and any(err.get("type") == "INVALID_SESSION_ID" for err in json_resp.get("errors", [])):
                    print("❌ Session ID expired. Run CTMSAuth.py to refresh.")
                    logger.warning("Session ID expired.")
                    return []

            if response.status_code != 200:
                print(f"❌ API error {response.status_code}: {response.text}")
                logger.error(f"API error {response.status_code}: {response.text}")
                break

            json_response = response.json()

            if "errors" in json_response:
                print(f"❌ API returned an error: {json_response['errors']}")
                logger.error(f"API returned an error: {json_response['errors']}")
                if len(studies) == 0:
                    return []
                break

            studies.extend(json_response.get("data", []))

            next_page = json_response.get("responseDetails", {}).get("next_page")
            url = f"{CTMS_URL}{next_page}" if next_page else None
            payload = None

    except requests.exceptions.RequestException as e:
        print(f"❌ Network error: {e}")
        logger.error(f"Network error: {e}")
        return []
    except Exception as e:
        print(f"❌ Error retrieving studies: {e}")
        logger.error(f"Error retrieving studies: {e}")
        return []

    if not studies:
        print("⚠️ No studies returned. Check session or permissions.")
        logger.warning("No studies returned.")
    return studies  # Return raw list instead of DataFrame


# ─── Transform Study Data ───────────────────────────────────
def transform_study_data(studies_list):
    """Transform nested JSON to flat structure with clean key-value pairs."""
    transformed_studies = []

    for study in studies_list:
        # Check IRT status
        irt_data = study.get('IRT_NON_IRT', {})
        if isinstance(irt_data, dict):
            irt_data_list = irt_data.get('data', [])
        else:
            irt_data_list = []

        irt_status = "IRT" if any(
            item.get('study_organization_role__vr.name__v') in ["IRT Services", "IRT Services (without Interface)"]
            for item in irt_data_list
        ) else "NON_IRT"

        # Check milestone status
        milestone_data = study.get('milestones__vr', {})
        if isinstance(milestone_data, dict):
            milestone_data_list = milestone_data.get('data', [])
        else:
            milestone_data_list = []

        milestone_status = "final ctp" if any(
            "protocol_approved__c" in item.get('milestone_type__v', [])
            for item in milestone_data_list
        ) else ""

        # Create cleaned structure
        cleaned_study = {
            "name__v": study.get('name__v'),
            "id": study.get('id'),
            "global_id__sys": study.get('global_id__sys'),
            "connect_to_vault_cdms__v": study.get('connect_to_vault_cdms__v'),
            "IRT_NON_IRT": irt_status,
            "milestones__vr": milestone_status
        }

        transformed_studies.append(cleaned_study)

    return transformed_studies


# ─── Check Studies in CDMS ──────────────────────────────────
def check_studies_in_cdms(studies_list, cdms_session_id):
    """Check which studies already exist in CDMS and filter them out."""
    if not studies_list or not cdms_session_id:
        return studies_list

    # Trim session ID
    cdms_session_id = cdms_session_id.strip()

    study_names = [study.get("name__v") for study in studies_list if study.get("name__v")]

    if not study_names:
        return studies_list

    # Build VQL query - escape single quotes
    escaped_names = [name.replace("'", "\\'") for name in study_names]
    conditions = " OR ".join([f"name__v = '{name}'" for name in escaped_names])
    query = f"SELECT name__v FROM study__v WHERE ({conditions})"

    # Query CDMS
    url = f"{CDMS_BASE_URL}/api/{CDMS_API_VERSION}/query"
    headers = {
        "Content-Type": "application/x-www-form-urlencoded",
        "Accept": "application/json",
        "Authorization": f"Bearer {cdms_session_id}"
    }
    payload = {"q": query}

    existing_studies = set()

    print("🔍 Checking CDMS for existing studies...")
    logger.info(f"Checking CDMS with query: {query}")
    logger.info(f"CDMS URL: {url}")
    logger.info(f"Session ID (first 20 chars): {cdms_session_id[:20]}...")

    try:
        while url:
            response = requests.post(url, data=payload, headers=headers) if payload else requests.get(url, headers=headers)

            logger.info(f"CDMS Response Status: {response.status_code}")
            logger.info(f"CDMS Response Headers: {dict(response.headers)}")

            if response.status_code == 401:
                json_resp = response.json()
                logger.error(f"401 Response: {json_resp}")
                if "errors" in json_resp and any(err.get("type") == "INVALID_SESSION_ID" for err in json_resp.get("errors", [])):
                    print("⚠️ CDMS session expired (401). Skipping duplicate check.")
                    logger.warning("CDMS session ID expired (401). Proceeding without filtering.")
                    return studies_list

            if response.status_code != 200:
                error_detail = response.text[:200] if response.text else "No details"
                logger.warning(f"CDMS check failed: {response.status_code} - {error_detail}. Proceeding without filtering.")
                print(f"⚠️ CDMS check failed: {response.status_code}. Saving all studies.")
                return studies_list  # Fail-safe: return all studies if check fails

            json_response = response.json()
            logger.info(f"CDMS Response JSON keys: {list(json_response.keys())}")

            if "errors" in json_response:
                error_detail = json_response['errors']
                logger.error(f"CDMS query returned errors in 200 response: {error_detail}")
                logger.error(f"Full response: {json_response}")
                print(f"⚠️ CDMS query error: {error_detail}. Saving all studies.")
                return studies_list

            # Collect existing study names
            for study in json_response.get("data", []):
                if study.get("name__v"):
                    existing_studies.add(study.get("name__v"))

            next_page = json_response.get("responseDetails", {}).get("next_page")
            url = f"{CDMS_BASE_URL}{next_page}" if next_page else None
            payload = None

    except Exception as e:
        logger.warning(f"CDMS check failed: {e}. Proceeding without filtering.")
        print(f"⚠️ CDMS check failed: {e}. Saving all studies.")
        return studies_list

    # Filter and log
    if existing_studies:
        logger.info(f"🗑️ Found {len(existing_studies)} existing studies in CDMS")
        print(f"🗑️ Skipping {len(existing_studies)} studies that already exist in CDMS:")
        for name in existing_studies:
            logger.info(f"   - Skipping existing study: {name}")
            print(f"   - {name}")

    # Return only new studies
    new_studies = [s for s in studies_list if s.get("name__v") not in existing_studies]

    if existing_studies:
        logger.info(f"✅ {len(new_studies)} new studies after filtering.")
        print(f"✅ {len(new_studies)} new studies will be saved.")
    else:
        print("✅ No existing studies found in CDMS. All studies are new.")
        logger.info("No existing studies found in CDMS.")

    return new_studies


# ─── Save Study List to JSON ────────────────────────────
def save_studies_to_json(studies_list, output_file):
    """Save cleaned study list to JSON file."""
    with open(output_file, 'w') as f:
        json.dump(studies_list, f, indent=2)
    print(f"✅ Saved {len(studies_list)} studies to {output_file}")
    logger.info(f"Saved {len(studies_list)} studies to {output_file}")


# ─── Main Execution ─────────────────────────────────────────
def main():
    logger.info("🔄 CTMS sync started.")
    print("🔄 CTMS sync started.")

    # Load CTMS session ID
    session_id = load_session_id()
    if not session_id:
        print("❌ No valid CTMS session ID. Run CTMSAuth.py.")
        logger.error("No valid CTMS session ID. Aborting.")
        return

    # Load CDMS session ID (optional - fail-safe if not available)
    cdms_session_id = load_cdms_session_id()
    if not cdms_session_id:
        print("⚠️ No CDMS session. Duplicate check will be skipped.")
        logger.warning("No CDMS session. Skipping duplicate check.")

    # Get last modified date from Redis
    modified_date = get_last_modified_date()
    query_str = build_query(modified_date)

    # Retrieve studies from CTMS
    all_studies = retrieve_CTMSStudyList(session_id, query_str)
    if not all_studies:
        print("⚠️ No studies found or unable to retrieve.")
        logger.warning("No studies retrieved.")
        return

    # Update Redis with latest modified_date__v (before transformation)
    update_last_modified_date(all_studies)

    # Transform studies to clean structure
    cleaned_studies = transform_study_data(all_studies)

    # **Check CDMS and filter out existing studies**
    if cdms_session_id:
        filtered_studies = check_studies_in_cdms(cleaned_studies, cdms_session_id)
    else:
        logger.warning("⚠️ No CDMS session. Saving all studies without duplicate check.")
        print("⚠️ Saving all studies without duplicate check.")
        filtered_studies = cleaned_studies

    # Save only new studies to JSON
    if filtered_studies:
        save_studies_to_json(filtered_studies, OUTPUT_JSON)
        print(f"✅ CTMS sync completed. {len(filtered_studies)} studies saved.")
        logger.info(f"✅ CTMS sync completed. {len(filtered_studies)} studies saved.")
    else:
        logger.info("⚠️ No new studies to save after filtering.")
        print("⚠️ No new studies to save. All studies already exist in CDMS.")
        # Create empty JSON file
        save_studies_to_json([], OUTPUT_JSON)

if __name__ == "__main__":
    main()