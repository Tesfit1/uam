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
        user_key = row["user_last_name__v"].strip()
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