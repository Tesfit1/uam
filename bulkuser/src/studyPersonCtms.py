import os
import json
import requests
import pandas as pd
import redis
from dotenv import load_dotenv

# ─── Load Environment Variables ─────────────────────────────
load_dotenv()
CTMS_API_VERSION = os.getenv("CTMS_API_VERSION")
CTMS_URL = os.getenv("CTMS_URL")
CLIENT_ID = os.getenv("CLIENT_ID")

REDIS_HOST = os.getenv("REDIS_HOST", "localhost")
REDIS_PORT = int(os.getenv("REDIS_PORT", 6379))
REDIS_DB = int(os.getenv("REDIS_DB", 0))
REDIS_SESSION_KEY = "ctms:session_id"
REDIS_MODIFIED_KEY = "ctms:last_modified_person"
SESSION_FILE = "CTMSsession_id.txt"
FALLBACK_DATE = os.getenv("CTMS_FALLBACK_DATE", "2000-01-01T00:00:00.000Z")


# ─── Redis + File Fallback Utilities ────────────────────────
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


def get_last_modified_date():
    try:
        r = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, db=REDIS_DB)
        value = r.get(REDIS_MODIFIED_KEY)
        if value:
            print(f"🕒 Last modified date from Redis: {value.decode()}")
            return value.decode()
        else:
            print("⚠️ No last modified date found in Redis. Using fallback.")
            return FALLBACK_DATE
    except Exception as e:
        print(f"❌ Redis error: {e}")
        return FALLBACK_DATE


def update_last_modified_date(df):
    if df.empty or 'modified_date__v' not in df.columns:
        return
    try:
        latest_date = df['modified_date__v'].dropna().max()
        if latest_date:
            r = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, db=REDIS_DB)
            r.set(REDIS_MODIFIED_KEY, latest_date)
            print(f"✅ Updated Redis with latest modified_date__v: {latest_date}")
    except Exception as e:
        print(f"❌ Failed to update Redis: {e}")


# ─── CTMS Query ─────────────────────────────────────────────
def retrieve_Study_Person_details(session_id, modified_date):
    users = []
    base_url = f"{CTMS_URL}/api/{CTMS_API_VERSION}/query"
    headers = {
        "Content-Type": "application/x-www-form-urlencoded",
        "Accept": "application/json",
        "X-VaultAPI-DescribeQuery": "true",
        "Authorization": f"Bearer {session_id}"
    }
    query = f"""
    SELECT email__clin, name__v , last_name__v, first_name__v, person_type__cr.name__v, team_role__vr.name__v, site_connect_user__v, study__clinr.name__v, study__clinr.status__v, study_country__clinr.name__v, site__clinr.name__v, start_date__clin, end_date__clin, state__v, modified_date__v
    FROM study_person__clin
    WHERE 
      person_type__cr.name__v != 'Internal' AND 
      previous_study_state__c = 'active__c' AND (
        team_role__vr.name__v = 'Deputy Investigator' OR
        team_role__vr.name__v = 'Laboratory Staff' OR
        team_role__vr.name__v = 'Principal Investigator' OR
        team_role__vr.name__v = 'Regulatory Document Co-ordinator' OR
        team_role__vr.name__v = 'Study Co-ordinator' OR
        team_role__vr.name__v = 'Study Nurse' OR
        team_role__vr.name__v = 'Subinvestigator'
      ) AND modified_date__v > '{modified_date}'
    """
    payload = {"q": query}
    url = base_url

    while url:
        response = requests.post(url, data=payload, headers=headers) if url == base_url else requests.get(url,
                                                                                                          headers=headers)
        response.raise_for_status()
        json_response = response.json()
        users.extend(json_response.get("data", []))
        next_page = json_response.get("responseDetails", {}).get("next_page")
        url = CTMS_URL + next_page if next_page else None
        payload = None

    return pd.DataFrame(users)


# ─── Transformation Utilities ───────────────────────────────
def mapper(user_df):
    role_map = {
        "Deputy Investigator": "CDMS Principal Investigator",
        "Laboratory Staff": "CDMS Clinical Research Coordinator",
        "Principal Investigator": "CDMS Principal Investigator",
        "Regulatory Document Co-ordinator": "CDMS Clinical Research Coordinator",
        "Study Co-ordinator": "CDMS Clinical Research Coordinator",
        "Study Nurse": "CDMS Clinical Research Coordinator",
        "Subinvestigator": "CDMS Principal Investigator",
    }
    user_df['mapped_role'] = user_df['team_role__vr.name__v'].map(role_map).fillna('')
    return user_df


def column_renamer(df):
    column_mapping = {
        "email__clin": "Email",
        "name__v": "Full Name",
        "last_name__v": "Last Name",
        "first_name__v": "First Name",
        "person_type__cr.name__v": "Person Type",
        "team_role__vr.name__v": "Team Role",
        "site_connect_user__v": "Site Connect User",
        "study__clinr.name__v": "Study",
        "study__clinr.status__v": "Study Status",
        "study_country__clinr.name__v": "Country Access",
        "site__clinr.name__v": "Site Access",
        "start_date__clin": "Activation Date",
        "end_date__clin": "End Date",
        "state__v": "State",
        "mapped_role": "Study Role",
    }
    return df.rename(columns=column_mapping)


def column_generate(df):
    df["User Name"] = df["Email"]
    df["User Type"] = "Site"
    df["Title"] = ""
    df["Federated ID"] = ""
    df["Company"] = ""
    df["Language"] = "en"
    df["Locale"] = "en_GB"
    df["Timezone"] = "(GMT+01:00) Central European Time (Europe/Berlin)"
    df["Cross Study Role"] = ""
    df["Send Welcome Email"] = "Yes"
    df["Add as Principal Investigator"] = "No"
    df["Study Environment"] = df["Study"]
    df["Access to All Environments"] = "No"
    df["Access to All Sites"] = "No"
    df["Study Access"] = "Enabled"
    df["Country Access"] = ""
    df["Ignore LMS Status"] = "No"
    df["Domain Administrator"] = ""
    df["Service Availability Notifications"] = "No"
    df["Product Announcement Emails"] = "No"
    df["Status"] = "Active"
    df["Security Policy"] = "VeevaId"

    df = df.drop(columns=[
        "Person Type", "Team Role", "Site Connect User", "Study Status", "End Date", "State"
    ], errors="ignore")

    desired_order = [
        'User Name', 'Email', 'User Type', 'Title', 'Last Name', 'First Name', 'Company',
        'Federated ID', 'Language', 'Locale', 'Timezone', 'Security Policy', 'Cross Study Role',
        'Activation Date', 'Send Welcome Email', 'Add as Principal Investigator', 'Study',
        'Study Environment', 'Access to All Environments', 'Study Role', 'Access to All Sites',
        'Study Access', 'Country Access', 'Site Access', 'Ignore LMS Status', 'Domain Administrator',
        'Service Availability Notifications', 'Product Announcement Emails', 'Status'
    ]

    for col in desired_order:
        if col not in df.columns:
            df[col] = None

    return df[desired_order]


# ─── Main Execution ─────────────────────────────────────────
if __name__ == "__main__":
    session_id = load_session_id()
    if not session_id:
        print("❌ No valid session ID. Aborting.")
        exit(1)

    modified_date = get_last_modified_date()
    users_df = retrieve_Study_Person_details(session_id, modified_date)

    if users_df.empty:
        print("✅ No new study person records to process.")
    else:
        mapped_users_df = mapper(users_df)
        mapped_users_df = column_renamer(mapped_users_df)
        mapped_users_df = column_generate(mapped_users_df)
        mapped_users_df.to_csv("study_person_list.csv", index=False)
        update_last_modified_date(mapped_users_df)
        print("✅ Exported study_person_list.csv")
