import json
from dotenv import load_dotenv
import requests
import os
import pandas as pd

load_dotenv()
CTMS_API_VERSION = os.getenv("CTMS_API_VERSION")
CTMS_URL = os.getenv("CTMS_URL")
CLIENT_ID = os.getenv("CLIENT_ID")
SESSION_FILE = "CTMSsession_id.txt"
with open(SESSION_FILE) as f:
    SESSION_ID = f.read().strip()
print(f"Session ID: {SESSION_ID}")


def retrieve_Study_Person_details():
    users = []
    base_url = f"{CTMS_URL}/api/{CTMS_API_VERSION}/query"
    headers = {
        "Content-Type": "application/x-www-form-urlencoded",
        "Accept": "application/json",
        "X-VaultAPI-DescribeQuery": "true",
        "Authorization": f"Bearer {SESSION_ID}"
    }
    payload = {
        "q": """SELECT email__clin, name__v , last_name__v, first_name__v,  person_type__cr.name__v, team_role__vr.name__v, site_connect_user__v, study__clinr.name__v, study__clinr.status__v, study_country__clinr.name__v, site__clinr.name__v, start_date__clin, end_date__clin, state__v
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
  )
"""
    }

    url = base_url
    while url:
        if url == base_url:
            response = requests.post(url, data=payload, headers=headers)
        else:
            response = requests.get(url, headers=headers)
        response.raise_for_status()
        json_response = response.json()
        print(json.dumps(json_response, indent=4))  # Debug

        # Append users from this page
        users.extend(json_response.get("data", []))

        # Get next page URL if present
        next_page = json_response.get("responseDetails", {}).get("next_page")
        if next_page:
            url = CTMS_URL + next_page
            payload = None  # For GET requests, don't send payload
        else:
            url = None

    users_df = pd.DataFrame(users)
    # print(f"Total users retrieved: {len(users_df)}")
    # users_df.to_csv("study_person_list.csv", index=False)
    return users_df


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
    df = df.rename(columns=column_mapping)
    return df


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

    df = df.drop(
        columns=[
            "Person Type",
            "Team Role",
            "Site Connect User",
            "Study Status",
            "End Date",
            "State",
        ],
        errors="ignore",
    )

    desired_order = [
        'User Name', 'Email', 'User Type', 'Title', 'Last Name', 'First Name', 'Company',
        'Federated ID', 'Language', 'Locale', 'Timezone', 'Security Policy', 'Cross Study Role',
        'Activation Date', 'Send Welcome Email', 'Add as Principal Investigator', 'Study',
        'Study Environment', 'Access to All Environments', 'Study Role', 'Access to All Sites',
        'Study Access', 'Country Access', 'Site Access', 'Ignore LMS Status', 'Domain Administrator',
        'Service Availability Notifications', 'Product Announcement Emails', 'Status'
    ]

    # Step 5: Ensure all columns exist, fill missing ones with None
    for col in desired_order:
        if col not in df.columns:
            df[col] = None

    # Step 6: Reorder the DataFrame
    df = df[desired_order]

    return df


if __name__ == "__main__":
    users_df = retrieve_Study_Person_details()
    mapped_users_df = mapper(users_df)
    mapped_users_df = column_renamer(mapped_users_df)
    mapped_users_df = column_generate(mapped_users_df)
    mapped_users_df.to_csv("study_person_list.csv", index=False)
    print("✅ Exported study_person_list.csv")