import os
import pandas as pd
import ast

def validate_import_template(template_path, data_dir):
    df = pd.read_csv(template_path, dtype=str).fillna("")

    # Load study-site mapping (study first, sites stringified)
    study_site_df = pd.read_csv(os.path.join(data_dir, "cdms_study_site_list.csv"), dtype=str).fillna("")
    # Build {study: set(sites)} lookup
    study_site_lookup = {}
    for _, row in study_site_df.iterrows():
        study = row["name__v"].strip()
        sites_str = row["sites__vr"]
        try:
            # Convert stringified list/dict to Python object
            sites_dict = ast.literal_eval(sites_str)
            site_names = set()
            for site in sites_dict.get("data", []):
                site_name = site.get("name__v", "").strip()
                if site_name:
                    site_names.add(site_name)
            study_site_lookup[study] = site_names
        except Exception as e:
            study_site_lookup[study] = set()

    # Load user lists for existence check
    cdms_users_df = pd.read_csv(os.path.join(data_dir, "cdms_user_list.csv"), dtype=str).fillna("")
    ctms_users_df = pd.read_csv(os.path.join(data_dir, "ctms_user_list.csv"), dtype=str).fillna("")
    cdms_user_set = set(u.strip() for u in cdms_users_df["user_name__v"]) if "user_name__v" in cdms_users_df else set()
    ctms_user_set = set(u.strip() for u in ctms_users_df["user_name__v"]) if "user_name__v" in ctms_users_df else set()

    errors = []
    valid_rows = []
    for idx, row in df.iterrows():
        user_key = row["User Name"].strip()
        study = row.get("Study", "").strip()
        site_access = row.get("Site Access", "").strip()

        # Check study
        if not study or study not in study_site_lookup:
            errors.append(f"Row {idx+2}: Study '{study}' does not exist for user '{user_key}'.")
            continue

        # Check site(s) for this study
        if site_access:
            allowed_sites = study_site_lookup[study]
            missing_sites = [site for site in site_access.split(",") if site.strip() and site.strip() not in allowed_sites]
            if missing_sites:
                errors.append(f"Row {idx+2}: Site(s) {missing_sites} do not exist for study '{study}' and user '{user_key}'.")
                continue

        # Check user existence
        exists_in_cdms = user_key in cdms_user_set
        exists_in_ctms = user_key in ctms_user_set
        if exists_in_cdms and exists_in_ctms:
            errors.append(f"Row {idx+2}: User '{user_key}' already exists in BOTH CDMS and CTMS.")
            continue
        elif exists_in_cdms:
            errors.append(f"Row {idx+2}: User '{user_key}' already exists in CDMS.")
            continue
        elif exists_in_ctms:
            errors.append(f"Row {idx+2}: User '{user_key}' already exists in CTMS.")
            continue

        valid_rows.append(row)

    if errors:
        return False, errors, None
    else:
        valid_df = pd.DataFrame(valid_rows)
        return True, [], valid_df
#  import os
# import pandas as pd
# import ast

# def validate_import_template(template_path, data_dir):
#     df = pd.read_csv(template_path, dtype=str).fillna("")

#     # Load study-site mapping (sites as keys, study in stringified dict)
#     study_site_df = pd.read_csv(os.path.join(data_dir, "cdms_study_Site_list.csv"), dtype=str).fillna("")
#     # Build {study: set(sites)} lookup
#     study_site_lookup = {}
#     for _, row in study_site_df.iterrows():
#         site = row["name__v"].strip()
#         study_json = row["study__vr"]
#         try:
#             study_dict = ast.literal_eval(study_json)
#             for study_entry in study_dict.get("data", []):
#                 study_name = study_entry.get("name__v", "").strip()
#                 if study_name:
#                     if study_name not in study_site_lookup:
#                         study_site_lookup[study_name] = set()
#                     study_site_lookup[study_name].add(site)
#         except Exception as e:
#             continue

#     # Load user lists for existence check
#     cdms_users_df = pd.read_csv(os.path.join(data_dir, "cdms_user_list.csv"), dtype=str).fillna("")
#     ctms_users_df = pd.read_csv(os.path.join(data_dir, "ctms_user_list.csv"), dtype=str).fillna("")
#     cdms_user_set = set(u.strip() for u in cdms_users_df["user_name__v"]) if "user_name__v" in cdms_users_df else set()
#     ctms_user_set = set(u.strip() for u in ctms_users_df["user_name__v"]) if "user_name__v" in ctms_users_df else set()

#     errors = []
#     valid_rows = []
#     for idx, row in df.iterrows():
#         user_key = row["User Name"].strip()
#         study = row.get("Study", "").strip()
#         site_access = row.get("Site Access", "").strip()

#         # Check study
#         if not study or study not in study_site_lookup:
#             errors.append(f"Row {idx+2}: Study '{study}' does not exist for user '{user_key}'.")
#             continue

#         # Check site(s) for this study
#         if site_access:
#             allowed_sites = study_site_lookup[study]
#             missing_sites = [site for site in site_access.split(",") if site.strip() and site.strip() not in allowed_sites]
#             if missing_sites:
#                 errors.append(f"Row {idx+2}: Site(s) {missing_sites} do not exist for study '{study}' and user '{user_key}'.")
#                 continue

#         # Check user existence
#         exists_in_cdms = user_key in cdms_user_set
#         exists_in_ctms = user_key in ctms_user_set
#         if exists_in_cdms and exists_in_ctms:
#             errors.append(f"Row {idx+2}: User '{user_key}' already exists in BOTH CDMS and CTMS.")
#             continue
#         elif exists_in_cdms:
#             errors.append(f"Row {idx+2}: User '{user_key}' already exists in CDMS.")
#             continue
#         elif exists_in_ctms:
#             errors.append(f"Row {idx+2}: User '{user_key}' already exists in CTMS.")
#             continue

#         valid_rows.append(row)

#     if errors:
#         return False, errors, None
#     else:
#         valid_df = pd.DataFrame(valid_rows)
#         return True, [], valid_df



# 
# 
# import os
# import pandas as pd

# def validate_import_template(template_path, data_dir):
#     df = pd.read_csv(template_path, dtype=str).fillna("")

#     cdms_studies_df = pd.read_csv(os.path.join(data_dir, "cdms_study_list.csv"), dtype=str).fillna("")
#     cdms_sites_df = pd.read_csv(os.path.join(data_dir, "cdms_site_list.csv"), dtype=str).fillna("")
#     cdms_users_df = pd.read_csv(os.path.join(data_dir, "cdms_user_list.csv"), dtype=str).fillna("")
#     ctms_users_df = pd.read_csv(os.path.join(data_dir, "ctms_user_list.csv"), dtype=str).fillna("")

#     cdms_study_set = set(s.strip() for s in cdms_studies_df["name__v"]) if "name__v" in cdms_studies_df else set()
#     cdms_site_set = set(s.strip() for s in cdms_sites_df["name__v"]) if "name__v" in cdms_sites_df else set()
#     cdms_user_set = set(u.strip() for u in cdms_users_df["user_name__v"]) if "user_name__v" in cdms_users_df else set()
#     ctms_user_set = set(u.strip() for u in ctms_users_df["user_name__v"]) if "user_name__v" in ctms_users_df else set()

#     errors = []
#     valid_rows = []
#     for idx, row in df.iterrows():
#         user_key = row["User Name"].strip()
#         study = row.get("Study", "").strip()
#         site_access = row.get("Site Access", "").strip()
#         # Check study
#         if study and study not in cdms_study_set:
#             errors.append(f"Row {idx+2}: Study '{study}' does not exist for user '{user_key}'.")
#             continue
#         # Check site(s)
#         if site_access:
#             missing_sites = [site for site in site_access.split(",") if site.strip() and site.strip() not in cdms_site_set]
#             if missing_sites:
#                 errors.append(f"Row {idx+2}: Site(s) {missing_sites} do not exist for user '{user_key}'.")
#                 continue
#         # Check user existence
#         exists_in_cdms = user_key in cdms_user_set
#         exists_in_ctms = user_key in ctms_user_set
#         if exists_in_cdms and exists_in_ctms:
#             errors.append(f"Row {idx+2}: User '{user_key}' already exists in BOTH CDMS and CTMS.")
#             continue
#         elif exists_in_cdms:
#             errors.append(f"Row {idx+2}: User '{user_key}' already exists in CDMS.")
#             continue
#         elif exists_in_ctms:
#             errors.append(f"Row {idx+2}: User '{user_key}' already exists in CTMS.")
#             continue
#         valid_rows.append(row)

#     if errors:
#         return False, errors, None
#     else:
#         valid_df = pd.DataFrame(valid_rows)
#         return True, [], valid_df

# ------------------------------------------------------------------------------------------------

# import os
# import pandas as pd

# # Load user import template
# dir = os.path.dirname(os.path.abspath(__file__))
# template_path = os.path.join(dir, "user-import-template-24r2.csv")
# df = pd.read_csv(template_path, dtype=str).fillna("")

# # Load lookup CSVs
# data_dir = os.path.abspath(os.path.join(dir, os.pardir, "data"))
# cdms_studies_df = pd.read_csv(os.path.join(data_dir, "cdms_study_list.csv"), dtype=str).fillna("")
# cdms_sites_df = pd.read_csv(os.path.join(data_dir, "cdms_site_list.csv"), dtype=str).fillna("")
# cdms_users_df = pd.read_csv(os.path.join(data_dir, "cdms_user_list.csv"), dtype=str).fillna("")
# ctms_users_df = pd.read_csv(os.path.join(data_dir, "ctms_user_list.csv"), dtype=str).fillna("")

# cdms_study_set = set(s.strip() for s in cdms_studies_df["name__v"]) if "name__v" in cdms_studies_df else set()
# cdms_site_set = set(s.strip() for s in cdms_sites_df["name__v"]) if "name__v" in cdms_sites_df else set()
# cdms_user_set = set(u.strip() for u in cdms_users_df["user_name__v"]) if "user_name__v" in cdms_users_df else set()
# ctms_user_set = set(u.strip() for u in ctms_users_df["user_name__v"]) if "user_name__v" in ctms_users_df else set()

# errors = []
# for idx, row in df.iterrows():
#     user_key = row["User Name"].strip()
#     study = row.get("Study", "").strip()
#     site_access = row.get("Site Access", "").strip()
#     # Check study
#     if study and study not in cdms_study_set:
#         errors.append(f"Row {idx+2}: Study '{study}' does not exist for user '{user_key}'.")
#     # Check site(s)
#     if site_access:
#         missing_sites = [site for site in site_access.split(",") if site.strip() and site.strip() not in cdms_site_set]
#         if missing_sites:
#             errors.append(f"Row {idx+2}: Site(s) {missing_sites} do not exist for user '{user_key}'.")
#     # Check user existence
#     exists_in_cdms = user_key in cdms_user_set
#     exists_in_ctms = user_key in ctms_user_set
#     if exists_in_cdms and exists_in_ctms:
#         errors.append(f"Row {idx+2}: User '{user_key}' already exists in BOTH CDMS and CTMS.")
#     elif exists_in_cdms:
#         errors.append(f"Row {idx+2}: User '{user_key}' already exists in CDMS.")
#     elif exists_in_ctms:
#         errors.append(f"Row {idx+2}: User '{user_key}' already exists in CTMS.")

# if errors:
#     print("Validation errors found:")
#     for err in errors:
#         print(err)
#     print("Fix the above issues before importing users.")
#     exit(1)
# else:
#     print("All rows validated successfully. Proceeding with import...")
#     # ... continue with your API import logic here ...