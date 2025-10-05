# import json
# import os
# import requests
# import pandas as pd
# from dotenv import load_dotenv
# import ast
#
# # ─── Load Environment Variables ─────────────────────────────
# load_dotenv()
# CTMS_API_VERSION = os.getenv("CTMS_API_VERSION")
# CTMS_URL = os.getenv("CTMS_URL")
# SESSION_FILE = "CTMSsession_id.txt"
#
# QUERY_STRING = os.getenv("CTMS_STUDY_QUERY") or \
#                "SELECT name__v, external_id__v, global_id__sys FROM study__v WHERE (state__v = 'active_state__v') OR (state__v = 'planning_state__v')"
#
# OUTPUT_CSV = os.getenv("CTMS_STUDY_CSV") or "ctms_study_list.csv"
# PROCESSED_CSV = "processed_studies.csv"
#
# QUERY_STR = """SELECT name__v, protocol_title__clin, global_id__sys, state__v, connect_to_vault_cdms__v, study_type__v, milestone_master_set__v, plat_edl_template__v, critical_path_study__c, cancellation_date__c, european_union_study__c, status__v, study_migration__v, id, sponsoringfunding__c, sponsor_organization__v, external_id__v,(SELECT organization__vr.name__v FROM study_organizations__vr)  AS organization_names
#
# FROM study__v
# WHERE (connect_to_vault_cdms__v = false) AND (state__v = 'active_state__v') AND ((milestone_master_set__v = 'OOW000000004010') OR (milestone_master_set__v = 'OOW000000000201') OR (milestone_master_set__v = 'OOW000000004001')) AND (external_id__v = null)"""
# # ─── Load Session ID ────────────────────────────────────────
# def load_session_id():
#     try:
#         with open(SESSION_FILE) as f:
#             session_id = f.read().strip()
#             if not session_id:
#                 raise ValueError("Session ID file is empty.")
#             print(f" Session ID loaded.")
#             return session_id
#     except Exception as e:
#         print(f" Error loading session ID: {e}")
#         return None
#
#
# # ─── Query Study Records ────────────────────────────────────
# def retrieve_CTMSStudyList(session_id):
#     studies = []
#     base_url = f"{CTMS_URL}/api/{CTMS_API_VERSION}/query"
#     headers = {
#         "Content-Type": "application/x-www-form-urlencoded",
#         "Accept": "application/json",
#         "X-VaultAPI-DescribeQuery": "true",
#         "Authorization": f"Bearer {session_id}"
#     }
#     payload = {"q": QUERY_STR}
#     url = base_url
#
#     print("📡 Fetching studies from ClinOps Vault...")
#     try:
#         while url:
#             response = requests.post(url, data=payload, headers=headers) if url == base_url else requests.get(url,
#                                                                                                               headers=headers)
#
#             # Check for session expiration
#             if response.status_code == 401:
#                 json_resp = response.json()
#                 if "errors" in json_resp and any(
#                         err.get("type") == "INVALID_SESSION_ID" for err in json_resp.get("errors", [])):
#                     print(" ERROR: Session ID has expired. Please run CTMSAuth.py to refresh your session.")
#                     print("   Command: python bulkuser/src/CTMSAuth.py")
#                     return pd.DataFrame()
#
#             # Handle other errors
#             if response.status_code != 200:
#                 print(f" API error {response.status_code}: {response.text}")
#                 break
#
#             json_response = response.json()
#
#             # Debug to see what's actually in the response
#             if "errors" in json_response:
#                 print(f" API returned an error: {json_response['errors']}")
#                 if len(studies) == 0:
#                     return pd.DataFrame()
#                 break
#
#             studies.extend(json_response.get("data", []))
#
#             next_page = json_response.get("responseDetails", {}).get("next_page")
#             url = f"{CTMS_URL}{next_page}" if next_page else None
#             payload = None
#
#     except requests.exceptions.RequestException as e:
#         print(f" Network error: {e}")
#         return pd.DataFrame()
#     except Exception as e:
#         print(f" Error retrieving studies: {e}")
#         return pd.DataFrame()
#
#     if not studies:
#         print(" No studies returned. This could indicate an expired session or insufficient permissions.")
#         print("   If you believe this is incorrect, try refreshing your session with CTMSAuth.py")
#
#     return pd.DataFrame(studies)
#
#
# def extract_organization_names(json_str):
#     try:
#         # Safely parse the stringified dictionary
#         data_dict = ast.literal_eval(json_str)
#         orgs = data_dict.get("data", [])
#         names = [org.get("organization__vr.name__v") for org in orgs if "organization__vr.name__v" in org]
#         return ", ".join(names) if names else None
#     except Exception:
#         return None
#
# # ─── Filter Out Already Processed Studies ───────────────────
# def get_processed_study_ids():
#     if not os.path.exists(PROCESSED_CSV):
#         return set()
#     df = pd.read_csv(PROCESSED_CSV)
#     return set(df['global_id__sys'].dropna().unique())
#
#
# def filter_unprocessed_studies(df, processed_ids):
#     if df.empty:
#         return df
#     return df[~df['global_id__sys'].isin(processed_ids)]
#
#
# # ─── Save Processed Studies ─────────────────────────────────
# def update_processed_studies(new_df):
#     header = not os.path.exists(PROCESSED_CSV)
#     new_df[['global_id__sys']].to_csv(PROCESSED_CSV, mode="a", index=False, header=header)
#     print(f" {len(new_df)} studies marked as processed.")
#
#
# # ─── Save Full Study List to CSV ────────────────────────────
# def save_studies_to_csv(df, output_file):
#     df.to_csv(output_file, index=False)
#     print(f" Saved {len(df)} studies to {output_file}")
#
#
# # ─── Main Execution ─────────────────────────────────────────
# def main():
#     session_id = load_session_id()
#     if not session_id:
#         print(" No valid session ID. Run CTMSAuth.py to generate a new session ID.")
#         return
#
#     all_studies_df = retrieve_CTMSStudyList(session_id)
#     if all_studies_df.empty:
#         print(" No studies found or unable to retrieve studies.")
#         return
#
#     processed_ids = get_processed_study_ids()
#     new_studies_df = filter_unprocessed_studies(all_studies_df, processed_ids)
#     if 'organization_names' in new_studies_df.columns:
#         new_studies_df['organization_names'] = new_studies_df['organization_names'].apply(extract_organization_names)
#
#     if new_studies_df.empty:
#         print(" No new studies to process.")
#     else:
#         save_studies_to_csv(new_studies_df, OUTPUT_CSV)
#         update_processed_studies(new_studies_df)
#
#
# if __name__ == "__main__":
#     main()

import json
import os
import requests
import pandas as pd
from dotenv import load_dotenv
import ast

# ─── Load Environment Variables ─────────────────────────────
load_dotenv()
CTMS_API_VERSION = os.getenv("CTMS_API_VERSION")
CTMS_URL = os.getenv("CTMS_URL")
SESSION_FILE = "CTMSsession_id.txt"

QUERY_STRING = os.getenv("CTMS_STUDY_QUERY") or \
    "SELECT name__v, external_id__v, global_id__sys FROM study__v WHERE (state__v = 'active_state__v') OR (state__v = 'planning_state__v')"

OUTPUT_CSV = os.getenv("CTMS_STUDY_CSV") or "ctms_study_list.csv"
PROCESSED_CSV = "processed_studies.csv"

QUERY_STR = """
SELECT name__v, protocol_title__clin, global_id__sys, state__v, connect_to_vault_cdms__v, study_type__v, milestone_master_set__v, plat_edl_template__v, critical_path_study__c, cancellation_date__c, european_union_study__c, status__v, study_migration__v, id, sponsoringfunding__c, sponsor_organization__v, external_id__v,
(SELECT organization__vr.name__v FROM study_organizations__vr) AS organization_names
FROM study__v
WHERE (connect_to_vault_cdms__v = false) AND (state__v = 'active_state__v') AND ((milestone_master_set__v = 'OOW000000004010') OR (milestone_master_set__v = 'OOW000000000201') OR (milestone_master_set__v = 'OOW000000004001')) AND (external_id__v = null)
"""

# ─── Load Session ID ────────────────────────────────────────
def load_session_id():
    try:
        with open(SESSION_FILE) as f:
            session_id = f.read().strip()
            if not session_id:
                raise ValueError("Session ID file is empty.")
            print("✅ Session ID loaded.")
            return session_id
    except Exception as e:
        print(f"❌ Error loading session ID: {e}")
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
        print(f"⚠️ Failed to extract organization names: {e}")
        return None

# ─── Query Study Records ────────────────────────────────────
def retrieve_CTMSStudyList(session_id):
    studies = []
    base_url = f"{CTMS_URL}/api/{CTMS_API_VERSION}/query"
    headers = {
        "Content-Type": "application/x-www-form-urlencoded",
        "Accept": "application/json",
        "X-VaultAPI-DescribeQuery": "true",
        "Authorization": f"Bearer {session_id}"
    }
    payload = {"q": QUERY_STR}
    url = base_url

    print("📡 Fetching studies from ClinOps Vault...")
    try:
        while url:
            response = requests.post(url, data=payload, headers=headers) if url == base_url else requests.get(url, headers=headers)

            if response.status_code == 401:
                json_resp = response.json()
                if "errors" in json_resp and any(err.get("type") == "INVALID_SESSION_ID" for err in json_resp.get("errors", [])):
                    print("❌ Session ID expired. Run CTMSAuth.py to refresh.")
                    return pd.DataFrame()

            if response.status_code != 200:
                print(f"❌ API error {response.status_code}: {response.text}")
                break

            json_response = response.json()

            if "errors" in json_response:
                print(f"❌ API returned an error: {json_response['errors']}")
                if len(studies) == 0:
                    return pd.DataFrame()
                break

            studies.extend(json_response.get("data", []))

            next_page = json_response.get("responseDetails", {}).get("next_page")
            url = f"{CTMS_URL}{next_page}" if next_page else None
            payload = None

    except requests.exceptions.RequestException as e:
        print(f"❌ Network error: {e}")
        return pd.DataFrame()
    except Exception as e:
        print(f"❌ Error retrieving studies: {e}")
        return pd.DataFrame()

    if not studies:
        print("⚠️ No studies returned. Check session or permissions.")
    return pd.DataFrame(studies)

# ─── Filter Out Already Processed Studies ───────────────────
def get_processed_study_ids():
    if not os.path.exists(PROCESSED_CSV):
        return set()
    df = pd.read_csv(PROCESSED_CSV)
    if 'global_id__sys' not in df.columns:
        print("⚠️ Column 'global_id__sys' missing in processed CSV.")
        return set()
    return set(df['global_id__sys'].dropna().unique())

def filter_unprocessed_studies(df, processed_ids):
    if df.empty:
        return df
    return df[~df['global_id__sys'].isin(processed_ids)].copy()

# ─── Save Processed Studies ─────────────────────────────────
def update_processed_studies(new_df):
    header = not os.path.exists(PROCESSED_CSV)
    new_df[['global_id__sys']].to_csv(PROCESSED_CSV, mode="a", index=False, header=header)
    print(f"✅ {len(new_df)} studies marked as processed.")

# ─── Save Full Study List to CSV ────────────────────────────
def save_studies_to_csv(df, output_file):
    df.to_csv(output_file, index=False)
    print(f"✅ Saved {len(df)} studies to {output_file}")

# ─── Main Execution ─────────────────────────────────────────
def main():
    session_id = load_session_id()
    if not session_id:
        print("❌ No valid session ID. Run CTMSAuth.py.")
        return

    all_studies_df = retrieve_CTMSStudyList(session_id)
    if all_studies_df.empty:
        print("⚠️ No studies found or unable to retrieve.")
        return

    processed_ids = get_processed_study_ids()
    new_studies_df = filter_unprocessed_studies(all_studies_df, processed_ids)

    if 'organization_names' in new_studies_df.columns:
        print("🔍 Extracting organization names...")
        new_studies_df['organization_names'] = new_studies_df['organization_names'].apply(extract_organization_names)
        new_studies_df['organization_names'] = new_studies_df['organization_names'].replace(["[]", "null", None], "")

    if new_studies_df.empty:
        print("✅ No new studies to process.")
    else:
        save_studies_to_csv(new_studies_df, OUTPUT_CSV)
        update_processed_studies(new_studies_df)

if __name__ == "__main__":
    main()