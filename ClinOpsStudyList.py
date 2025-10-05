import json
import os
import requests
import pandas as pd
from dotenv import load_dotenv
import ast
import redis
import logging
from datetime import datetime

# ─── Load Environment Variables ─────────────────────────────
load_dotenv()
CTMS_API_VERSION = os.getenv("CTMS_API_VERSION")
CTMS_URL = os.getenv("CTMS_URL")
REDIS_SESSION_KEY = "ctms:session_id"
SESSION_FILE = "CTMSsession_id.txt"
OUTPUT_CSV = os.getenv("CTMS_STUDY_CSV") or "ctms_study_list.csv"
# PROCESSED_CSV = "processed_studies.csv"
FALLBACK_DATE = os.getenv("CTMS_FALLBACK_DATE", "2000-01-01T00:00:00.000Z")

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

def update_last_modified_date(df):
    if df.empty or 'modified_date__v' not in df.columns:
        return
    try:
        latest_date = df['modified_date__v'].dropna().max()
        if latest_date:
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
    SELECT name__v, protocol_title__clin, global_id__sys, state__v, connect_to_vault_cdms__v, study_type__v, milestone_master_set__v, plat_edl_template__v, critical_path_study__c, cancellation_date__c, european_union_study__c, status__v, study_migration__v, id, sponsoringfunding__c, sponsor_organization__v, external_id__v,
    (SELECT organization__vr.name__v FROM study_organizations__vr) AS organization_names,
    modified_date__v
    FROM study__v
    WHERE (connect_to_vault_cdms__v = false) AND (state__v = 'active_state__v') AND ((milestone_master_set__v = 'OOW000000004010') OR (milestone_master_set__v = 'OOW000000000201') OR (milestone_master_set__v = 'OOW000000004001')) AND (external_id__v = null) AND (modified_date__v > '{modified_date}')
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

# ─── Extract Organization Names ─────────────────────────────
def extract_organization_names(value):
    try:
        if isinstance(value, str):
            value = ast.literal_eval(value)
        if isinstance(value, dict):
            orgs = value.get("data", [])
            names = [org.get("organization__vr.name__v") for org in orgs if "organization__vr.name__v" in org]
            return ", ".join(names) if names else None
        return None
    except Exception as e:
        logger.warning(f"Failed to extract organization names: {e}")
        print(f"⚠️ Failed to extract organization names: {e}")
        return None

# ─── Query Study Records ────────────────────────────────────
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
                    return pd.DataFrame()

            if response.status_code != 200:
                print(f"❌ API error {response.status_code}: {response.text}")
                logger.error(f"API error {response.status_code}: {response.text}")
                break

            json_response = response.json()

            if "errors" in json_response:
                print(f"❌ API returned an error: {json_response['errors']}")
                logger.error(f"API returned an error: {json_response['errors']}")
                if len(studies) == 0:
                    return pd.DataFrame()
                break

            studies.extend(json_response.get("data", []))

            next_page = json_response.get("responseDetails", {}).get("next_page")
            url = f"{CTMS_URL}{next_page}" if next_page else None
            payload = None

    except requests.exceptions.RequestException as e:
        print(f"❌ Network error: {e}")
        logger.error(f"Network error: {e}")
        return pd.DataFrame()
    except Exception as e:
        print(f"❌ Error retrieving studies: {e}")
        logger.error(f"Error retrieving studies: {e}")
        return pd.DataFrame()

    if not studies:
        print("⚠️ No studies returned. Check session or permissions.")
        logger.warning("No studies returned.")
    return pd.DataFrame(studies)

# ─── Save Study List to CSV ────────────────────────────
def save_studies_to_csv(df, output_file):
    df.to_csv(output_file, index=False)
    print(f"✅ Saved {len(df)} studies to {output_file}")
    logger.info(f"Saved {len(df)} studies to {output_file}")

# ─── Main Execution ─────────────────────────────────────────
def main():
    logger.info("🔄 CTMS sync started.")
    print("🔄 CTMS sync started.")

    # Load session ID
    session_id = load_session_id()
    if not session_id:
        print("❌ No valid session ID. Run CTMSAuth.py.")
        logger.error("No valid session ID. Aborting.")
        return

    # Get last modified date from Redis
    modified_date = get_last_modified_date()
    query_str = build_query(modified_date)

    # Retrieve studies from CTMS
    all_studies_df = retrieve_CTMSStudyList(session_id, query_str)
    if all_studies_df.empty:
        print("⚠️ No studies found or unable to retrieve.")
        logger.warning("No studies retrieved.")
        return

    # Extract and classify organization names
    if 'organization_names' in all_studies_df.columns:
        print("🔍 Classifying organization names...")
        all_studies_df['organization_names'] = all_studies_df['organization_names'].apply(extract_organization_names)
        all_studies_df['organization_names'] = all_studies_df['organization_names'].replace(["[]", "null", None], "")

        def classify_study_type(org_name):
            if not org_name or org_name.strip() == "":
                return ""
            org_name_lower = org_name.lower()
            if "almac clinical technologies llc" in org_name_lower or "nanavati" in org_name_lower:
                return "IRT study"
            return "Non IRT study"

        all_studies_df['organization_names'] = all_studies_df['organization_names'].apply(classify_study_type)

    # Save to CSV
    save_studies_to_csv(all_studies_df, OUTPUT_CSV)

    # Update Redis with latest modified_date__v
    update_last_modified_date(all_studies_df)

    print("✅ CTMS sync completed.")
    logger.info("✅ CTMS sync completed.")

if __name__ == "__main__":
    main()