import os
import json
import pandas as pd
from dotenv import load_dotenv
import requests

# Load environment variables
load_dotenv()
API_VERSION = os.getenv("API_VERSION")
BASE_URL = os.getenv("BASE_URL")
SESSION_FILE = "session_id.txt"

# Read session ID
with open(SESSION_FILE) as f:
    SESSION_ID = f.read().strip()

# Paths
dir = os.path.dirname(os.path.abspath(__file__))
template_path = os.path.join(dir, "user-import-template-24r2.csv")

# Load user data directly from CSV
try:
    df = pd.read_csv(template_path)
    df = df.fillna("")
except Exception as e:
    print(f"Error reading CSV: {e}")
    exit(1)

# Convert DataFrame rows to list of dictionaries
users_to_import = [row.to_dict() for _, row in df.iterrows()]

if not users_to_import:
    print("No users found in the template.")
    exit(0)

payload = {
    "append_site_country_access": True,
    "users": users_to_import
}
print(json.dumps(payload, indent=4))
# Send the POST request to import users
url = f"{BASE_URL}/api/{API_VERSION}/app/cdm/users_json"
headers = {
    "Accept": "application/json",
    "Content-Type": "application/json",
    "Authorization": f"Bearer {SESSION_ID}"
}
response = requests.post(url, headers=headers, data=json.dumps(payload))
response.raise_for_status()
print("Import response:")
print(json.dumps(response.json(), indent=4))

# import os
# import json
# import pandas as pd
# from dotenv import load_dotenv
# import requests
# from validator import validate_import_template
#
# # Load environment variables
# load_dotenv()
# API_VERSION = os.getenv("API_VERSION")
# BASE_URL = os.getenv("BASE_URL")
# SESSION_FILE = "session_id.txt"
# with open(SESSION_FILE) as f:
#     SESSION_ID = f.read().strip()
#
# # Paths
# dir = os.path.dirname(os.path.abspath(__file__))
# template_path = os.path.join(dir, "user-import-template-24r2.csv")
# data_dir = os.path.abspath(os.path.join(dir, os.pardir, "data"))
#
# # Validate before import
# status, errors, valid_df = validate_import_template(template_path, data_dir)
# if not status:
#     print("Validation errors found:")
#     for err in errors:
#         print(err)
#     print("Fix the above issues before importing users.")
#     exit(1)
# else:
#     print("All rows validated successfully. Proceeding with import...")
#
# # Prepare users to import
# users_to_import = [row.to_dict() for _, row in valid_df.iterrows()]
#
# if not users_to_import:
#     print("No new users to import.")
#     exit(0)
#
# # Prepare payload for JSON import
# payload = {
#     "append_site_country_access": True,
#     "users": users_to_import
# }
# print(json.dumps(payload, indent=4))
# # Send the POST request to import users
# url = f"{BASE_URL}/api/{API_VERSION}/app/cdm/users_json"
# headers = {
#     "Accept": "application/json",
#     "Content-Type": "application/json",
#     "Authorization": f"Bearer {SESSION_ID}"
# }
# response = requests.post(url, headers=headers, data=json.dumps(payload))
# response.raise_for_status()
# print("Import response:")
# print(json.dumps(response.json(), indent=4))

# import json
# import os
# import pandas as pd
# from dotenv import load_dotenv
# import requests

# from CDMSStudyList import retrieve_CDMSStudyList
# from CDMSSiteList import retrieve_CDMSsites
# from CDMS_UserList import retrieve_CDMSusers
# from CTMS_UserList import retrieve_CTMS_users
# # from CTMSStudyList import retrieve_CTMSStudyList
# # from CTMSSiteList import retrieve_CTMS_sites

# # Load environment variables
# load_dotenv()
# API_VERSION = os.getenv("API_VERSION")
# BASE_URL = os.getenv("BASE_URL")
# SESSION_FILE = "session_id.txt"
# with open(SESSION_FILE) as f:
#     SESSION_ID = f.read().strip()
# study_name = os.getenv("Study_name")

# # Load user import template
# dir = os.path.dirname(os.path.abspath(__file__))
# template_path = os.path.join(dir, "user-import-template-24r2.csv")
# df = pd.read_csv(template_path, dtype=str).fillna("")

# # Retrieve current studies, sites, and users
# CDMS_studies_df = retrieve_CDMSStudyList()
# CDMS_sites_df = retrieve_CDMSsites()
# CDMS_users_df = retrieve_CDMSusers()
# CTMS_users_df = retrieve_CTMS_users()


# # Prepare lookup sets for fast existence checks
# CDMS_study_set = set(CDMS_studies_df["study"]) if "study" in CDMS_studies_df else set()
# CDMS_site_set = set(CDMS_sites_df["site"]) if "site" in CDMS_sites_df else set()
# CDMS_user_set = set(CDMS_users_df["user_name"]) if "user_name" in CDMS_users_df else set()
# CTMS_user_set = set(CTMS_users_df["user_name"]) if "user_name" in CTMS_users_df else set()

# users_to_import = []
# for idx, row in df.iterrows():
#     user_key = row["User Name"]
#     study = row.get("Study", study_name)
#     site_access = row.get("Site Access", "")
#     # Check study
#     if study and study not in CDMS_study_set:
#         print(f"Skipping user {user_key}: Study '{study}' does not exist.")
#         continue
#     # Check site(s)
#     if site_access:
#         missing_sites = [site for site in site_access.split(",") if site and site not in CDMS_site_set]
#         if missing_sites:
#             print(f"Skipping user {user_key}: Site(s) {missing_sites} do not exist.")
#             continue
#     # Enhanced user existence check
#     exists_in_cdms = user_key in CDMS_user_set
#     exists_in_ctms = user_key in CTMS_user_set
#     if exists_in_cdms and exists_in_ctms:
#         print(f"Skipping user {user_key}: User already exists in BOTH CDMS and CTMS.")
#         continue
#     elif exists_in_cdms:
#         print(f"Skipping user {user_key}: User already exists in CDMS.")
#         continue
#     elif exists_in_ctms:
#         print(f"Skipping user {user_key}: User already exists in CTMS.")
#         continue
#     users_to_import.append(row.to_dict())

# if not users_to_import:
#     print("No new users to import.")
#     exit(0)

# # Prepare payload for JSON import
# payload = {
#     "append_site_country_access": False,
#     "users": users_to_import
# }

# # Send the POST request to import users
# url = f"{BASE_URL}/api/{API_VERSION}/app/cdm/users_json"
# headers = {
#     "Accept": "application/json",
#     "Content-Type": "application/json",
#     "Authorization": f"Bearer {SESSION_ID}"
# }
# response = requests.post(url, headers=headers, data=json.dumps(payload))
# response.raise_for_status()
# print("Import response:")
# print(json.dumps(response.json(), indent=4))