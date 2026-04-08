import os
import re
from pathlib import Path
from collections import defaultdict
from typing import Dict, List, Any, Optional

import pandas as pd


# ============================================================
# CONFIGURATION
# ============================================================
# Update these paths/file names to match your local folder structure.
BASE_FOLDER = r"C:/Users/x01540905/Desktop/dash-app-perm-sharedrive/March 2026"
HR_FILE = "Headcount.xlsx"
ACCESS_REPORTS_FOLDER = "USCB - Access Reports 030426"
APP_ACCESS_FILE = "01 app_access_export.txt"
SHARE_DRIVE_FILE = "05 share_drive_access_export.txt"
SHOULD_HAVE_FOLDER = "shd_have_files"
OUTPUT_FILE = "app_perm_sharedrive_combined.xlsx"


# ============================================================
# GENERIC HELPERS
# ============================================================
def clean_text(value: Any) -> Any:
    """Trim strings and collapse repeated whitespace/newlines."""
    if pd.isna(value):
        return value
    if not isinstance(value, str):
        return value
    value = value.replace("\r", " ").replace("\n", " ")
    value = re.sub(r"\s+", " ", value).strip()
    return value


def normalize_key(value: Any) -> str:
    """Create a normalized comparison key for apps, permissions, paths, etc."""
    if pd.isna(value):
        return ""
    value = str(value).upper().strip()
    value = value.replace("\\", "/")
    value = re.sub(r"\s+", " ", value)
    return value


def normalize_path(value: Any) -> str:
    """Normalize share drive paths for reliable comparison."""
    if pd.isna(value):
        return ""
    value = str(value).strip()
    value = value.replace("\\", "/")
    value = re.sub(r"/+", "/", value)
    value = re.sub(r"\s+", " ", value)
    return value.upper().rstrip("/")


def clean_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Upper-case and trim all column names, and clean all string cell values."""
    df = df.copy()
    df.columns = [clean_text(str(col)).upper() for col in df.columns]
    for col in df.columns:
        df[col] = df[col].apply(clean_text)
    return df


def capitalize_strings(df: pd.DataFrame) -> pd.DataFrame:
    """Preserve your earlier step name, but make it safe and generic.

    This title-cases normal text values while leaving non-string values unchanged.
    """
    df = df.copy()
    for col in df.columns:
        df[col] = df[col].apply(lambda x: x.title() if isinstance(x, str) else x)
    return df


def find_first_existing_column(df: pd.DataFrame, candidates: List[str]) -> Optional[str]:
    """Return first matching column from a list of candidate names."""
    for col in candidates:
        if col in df.columns:
            return col
    return None


def safe_list(value: Any) -> List[Any]:
    """Return value as list, handling NaN / None safely."""
    if isinstance(value, list):
        return value
    if pd.isna(value):
        return []
    return [value]


# ============================================================
# FILE LOADING
# ============================================================
def read_txt_export(file_path: str, sep: str = "|", encoding: str = "cp1252") -> pd.DataFrame:
    """Read access export text file."""
    return pd.read_csv(file_path, sep=sep, encoding=encoding, dtype=str)


def read_should_have_files(folder_path: str) -> pd.DataFrame:
    """Read and append all Excel files from should-have folder into one DataFrame.

    Also prints the column structure of each file for easier debugging.
    """
    all_frames = []
    folder = Path(folder_path)

    print("\n====== Column structure of each should-have file ======")
    for file in sorted(folder.glob("*.xlsx")):
        try:
            temp = pd.read_excel(file, dtype=str)
            print(f"\nFile: {file.name}")
            print(f"Columns: {list(temp.columns)}")
            all_frames.append(temp)
        except Exception as exc:
            print(f"Skipping {file.name} due to error: {exc}")

    if not all_frames:
        raise FileNotFoundError(f"No .xlsx files found in should-have folder: {folder_path}")

    combined = pd.concat(all_frames, ignore_index=True)
    return combined


# ============================================================
# SHOULD-HAVE DATA PREPARATION
# ============================================================
def standardize_should_have(df_should: pd.DataFrame) -> pd.DataFrame:
    """Standardize should-have file structure across multiple persona files."""
    df_should = clean_columns(df_should)

    rename_map = {}
    if "APPLICATION NAME" in df_should.columns and "APPLICATION" not in df_should.columns:
        rename_map["APPLICATION NAME"] = "APPLICATION"
    if "ERSONA" in df_should.columns and "PERSONA" not in df_should.columns:
        rename_map["ERSONA"] = "PERSONA"

    df_should = df_should.rename(columns=rename_map)

    # Ensure expected columns exist
    required_cols = [
        "SOURCE",
        "PERSONA",
        "APPLICATION",
        "REQUEST NAME",
        "ASSET TYPE",
        "PERMISSION",
        r"NEW COLUMN\NR-REQUIRED\N0-OPTIONAL",
    ]
    for col in required_cols:
        if col not in df_should.columns:
            df_should[col] = None

    # Keep only required columns in consistent order
    df_should = df_should[required_cols].copy()
    df_should = capitalize_strings(df_should)
    df_should = clean_columns(df_should)
    return df_should


# ============================================================
# HR / DOES-HAVE PREPARATION
# ============================================================
def standardize_hr(df_hr: pd.DataFrame) -> pd.DataFrame:
    df_hr = clean_columns(df_hr)
    df_hr = capitalize_strings(df_hr)
    df_hr = clean_columns(df_hr)

    if "COLLEAGUE ID" in df_hr.columns and "BRID_HR" not in df_hr.columns:
        df_hr = df_hr.rename(columns={"COLLEAGUE ID": "BRID_HR"})

    return df_hr


def standardize_does_have(df: pd.DataFrame) -> pd.DataFrame:
    df = clean_columns(df)
    df = capitalize_strings(df)
    df = clean_columns(df)

    if "BRID" in df.columns and "BRID_DOES_HAVE" not in df.columns:
        df = df.rename(columns={"BRID": "BRID_DOES_HAVE"})

    return df


# ============================================================
# APP PERMISSION LOGIC
# ============================================================
def build_app_perm_map(
    df: pd.DataFrame,
    key_col: str,
    app_col: str,
    perm_col: str,
) -> Dict[str, Dict[str, List[str]]]:
    """Build mapping like:
    key -> {app1: [perm1, perm2], app2: [perm3]}
    """
    mapping: Dict[str, Dict[str, set]] = defaultdict(lambda: defaultdict(set))

    use_cols = [key_col, app_col, perm_col]
    temp = df[use_cols].copy()
    temp = temp.dropna(subset=[key_col, app_col])

    for _, row in temp.iterrows():
        key = clean_text(row[key_col])
        app = clean_text(row[app_col])
        perm = clean_text(row[perm_col]) if perm_col in row else None

        if not key or not app:
            continue

        key = str(key)
        app = str(app)
        if perm and str(perm).strip():
            mapping[key][app].add(str(perm))
        else:
            mapping[key][app]  # create app key even if permission missing

    final_mapping: Dict[str, Dict[str, List[str]]] = {}
    for key, app_dict in mapping.items():
        final_mapping[key] = {app: sorted(list(perms)) for app, perms in app_dict.items()}

    return final_mapping


def compare_application_lists(row: pd.Series, should_col: str, does_col: str) -> pd.Series:
    """Compare app lists and return matched / missing / excess lists."""
    should_apps = sorted(set(safe_list(row.get(should_col, []))))
    does_apps = sorted(set(safe_list(row.get(does_col, []))))

    should_set = set(should_apps)
    does_set = set(does_apps)

    matched = sorted(list(should_set & does_set))
    missing = sorted(list(should_set - does_set))
    excess = sorted(list(does_set - should_set))

    return pd.Series([matched, missing, excess])


def compare_and_flatten_permissions(
    df: pd.DataFrame,
    does_col: str,
    should_col: str,
    brid_col: str = "BRID_HR",
) -> pd.DataFrame:
    """Flatten app + permission comparison into a vertical output.

    Output columns include:
    BRID, APPLICATION, APPLICATION_STATUS, PERMISSION, PERMISSION_STATUS
    """
    output_rows = []

    for _, row in df.iterrows():
        brid = row.get(brid_col)
        does_map = row.get(does_col, {}) if isinstance(row.get(does_col), dict) else {}
        should_map = row.get(should_col, {}) if isinstance(row.get(should_col), dict) else {}

        all_apps = sorted(set(does_map.keys()) | set(should_map.keys()))

        for app in all_apps:
            does_perms = sorted(set(does_map.get(app, [])))
            should_perms = sorted(set(should_map.get(app, [])))

            if app in does_map and app in should_map:
                app_status = "MATCHED_APPLICATION"
            elif app in should_map:
                app_status = "MISSING_APPLICATION"
            else:
                app_status = "EXCESS_APPLICATION"

            all_perms = sorted(set(does_perms) | set(should_perms))

            # If no permission exists, still keep one row per app
            if not all_perms:
                output_rows.append(
                    {
                        "BRID": brid,
                        "APPLICATION": app,
                        "APPLICATION_STATUS": app_status,
                        "PERMISSION": None,
                        "PERMISSION_STATUS": None,
                    }
                )
                continue

            for perm in all_perms:
                if perm in does_perms and perm in should_perms:
                    perm_status = "MATCHED_PERMISSION"
                elif perm in should_perms:
                    perm_status = "MISSING_PERMISSION"
                else:
                    perm_status = "EXCESS_PERMISSION"

                output_rows.append(
                    {
                        "BRID": brid,
                        "APPLICATION": app,
                        "APPLICATION_STATUS": app_status,
                        "PERMISSION": perm,
                        "PERMISSION_STATUS": perm_status,
                    }
                )

    return pd.DataFrame(output_rows)


def process_app_permissions(
    df_hr: pd.DataFrame,
    df_does_have_app: pd.DataFrame,
    df_should_have: pd.DataFrame,
) -> (pd.DataFrame, pd.DataFrame):
    """Run full app permission reconciliation."""
    df_should_app = df_should_have[df_should_have["ASSET TYPE"].eq("APP_ACCESS")].copy()

    should_map = build_app_perm_map(
        df=df_should_app,
        key_col="PERSONA",
        app_col="REQUEST NAME",
        perm_col="PERMISSION",
    )

    does_app_col = find_first_existing_column(
        df_does_have_app,
        ["REQUEST_WORKFLOW", "REQUEST NAME", "APPLICATION", "APPLICATION NAME"],
    )
    does_perm_col = find_first_existing_column(
        df_does_have_app,
        ["PERMISSION", "ENTITLEMENT", "ACCESS", "ACCESS NAME"],
    )

    if does_app_col is None:
        raise KeyError("Could not identify application column in app access export.")
    if does_perm_col is None:
        df_does_have_app["PERMISSION"] = None
        does_perm_col = "PERMISSION"

    does_map = build_app_perm_map(
        df=df_does_have_app,
        key_col="BRID_DOES_HAVE",
        app_col=does_app_col,
        perm_col=does_perm_col,
    )

    df_app = df_hr.copy()
    df_app["DOES_HAVE_APP_AND_PERM"] = df_app["BRID_HR"].map(does_map)
    df_app["SHOULD_HAVE_APP_AND_PERM"] = df_app["PERSONA"].map(should_map)

    df_app["DOES_HAVE_APPS"] = df_app["DOES_HAVE_APP_AND_PERM"].apply(
        lambda d: list(d.keys()) if isinstance(d, dict) else []
    )
    df_app["SHOULD_HAVE_APPS"] = df_app["SHOULD_HAVE_APP_AND_PERM"].apply(
        lambda d: list(d.keys()) if isinstance(d, dict) else []
    )

    df_app[["MATCHED_APPLICATIONS", "MISSING_APPLICATIONS", "EXCESS_APPLICATIONS"]] = df_app.apply(
        lambda row: compare_application_lists(row, "SHOULD_HAVE_APPS", "DOES_HAVE_APPS"),
        axis=1,
    )

    df_app["CNT_SHOULD_HAVE_APPS"] = df_app["SHOULD_HAVE_APPS"].apply(len)
    df_app["CNT_DOES_HAVE_APPS"] = df_app["DOES_HAVE_APPS"].apply(len)
    df_app["CNT_MATCHED_APPLICATIONS"] = df_app["MATCHED_APPLICATIONS"].apply(len)
    df_app["CNT_MISSING_APPLICATIONS"] = df_app["MISSING_APPLICATIONS"].apply(len)
    df_app["CNT_EXCESS_APPLICATIONS"] = df_app["EXCESS_APPLICATIONS"].apply(len)

    flat_app_perm = compare_and_flatten_permissions(
        df=df_app,
        does_col="DOES_HAVE_APP_AND_PERM",
        should_col="SHOULD_HAVE_APP_AND_PERM",
        brid_col="BRID_HR",
    )

    # Keep only first copy per full row combination if repeated
    flat_app_perm = flat_app_perm.drop_duplicates(
        subset=["BRID", "APPLICATION", "APPLICATION_STATUS", "PERMISSION", "PERMISSION_STATUS"],
        keep="first",
    ).reset_index(drop=True)

    return df_app, flat_app_perm


# ============================================================
# SHARE DRIVE LOGIC
# ============================================================
def extract_path_and_access(df: pd.DataFrame, column_name: str) -> pd.DataFrame:
    """Extract PATH and ACCESS_LEVEL from a permission/free-text field.

    Supports strings such as:
    PATH IS: Y:/USCB/ABC ACCESS LEVEL: READ
    """
    df = df.copy()
    df[column_name] = df[column_name].astype(str).str.replace("\n", " ", regex=False)

    df["PATH"] = df[column_name].str.extract(
        r"PATH\s*IS\s*:\s*(.*?)(?=\s*ACCESS\s*LEVEL\s*:|$)",
        expand=False,
    )
    df["ACCESS_LEVEL"] = df[column_name].str.extract(
        r"ACCESS\s*LEVEL\s*:\s*([A-Z_ ]+)",
        expand=False,
    )

    df["PATH"] = df["PATH"].apply(clean_text)
    df["ACCESS_LEVEL"] = df["ACCESS_LEVEL"].apply(clean_text)
    return df


def build_path_access_map(
    df: pd.DataFrame,
    key_col: str,
    path_col: str,
    access_col: str,
) -> Dict[str, List[Dict[str, str]]]:
    """Build mapping:
    key -> [{PATH: ..., ACCESS_LEVEL: ...}, ...]
    """
    mapping: Dict[str, List[Dict[str, str]]] = defaultdict(list)

    use_cols = [key_col, path_col, access_col]
    temp = df[use_cols].copy()
    temp = temp.dropna(subset=[key_col, path_col])

    for _, row in temp.iterrows():
        key = clean_text(row[key_col])
        path = clean_text(row[path_col])
        access = clean_text(row[access_col])

        if not key or not path:
            continue

        record = {
            "PATH": path,
            "ACCESS_LEVEL": access,
            "PATH_NORM": normalize_path(path),
            "ACCESS_NORM": normalize_key(access),
        }
        mapping[str(key)].append(record)

    # Remove duplicates inside each key
    deduped = {}
    for key, items in mapping.items():
        seen = set()
        final_items = []
        for item in items:
            marker = (item["PATH_NORM"], item["ACCESS_NORM"])
            if marker not in seen:
                seen.add(marker)
                final_items.append(item)
        deduped[key] = final_items

    return deduped


def reconcile_share_drive_paths(df_hr: pd.DataFrame) -> pd.DataFrame:
    """Create row-level share drive reconciliation output."""
    rows = []

    for _, row in df_hr.iterrows():
        brid = row.get("BRID_HR")
        persona = row.get("PERSONA")
        should_items = row.get("SHR_DRIVE_SHD_HAVE", []) if isinstance(row.get("SHR_DRIVE_SHD_HAVE"), list) else []
        does_items = row.get("SHR_DRIVE_DOES_HAVE", []) if isinstance(row.get("SHR_DRIVE_DOES_HAVE"), list) else []

        should_by_path = {item["PATH_NORM"]: item for item in should_items}
        does_by_path = {item["PATH_NORM"]: item for item in does_items}

        all_paths = sorted(set(should_by_path.keys()) | set(does_by_path.keys()))

        for path_norm in all_paths:
            should_item = should_by_path.get(path_norm)
            does_item = does_by_path.get(path_norm)

            path_display = None
            if should_item:
                path_display = should_item["PATH"]
            elif does_item:
                path_display = does_item["PATH"]

            if should_item and does_item:
                path_status = "MATCHED_PATH"
                if should_item.get("ACCESS_NORM") == does_item.get("ACCESS_NORM"):
                    access_status = "MATCHED_ACCESS"
                else:
                    access_status = "ACCESS_MISMATCH"
            elif should_item:
                path_status = "MISSING_PATH"
                access_status = "MISSING_ACCESS"
            else:
                path_status = "EXCESS_PATH"
                access_status = "EXCESS_ACCESS"

            rows.append(
                {
                    "BRID_HR": brid,
                    "PERSONA": persona,
                    "PATH": path_display,
                    "PATH_STATUS": path_status,
                    "SHOULD_HAVE_ACCESS_LEVEL": should_item.get("ACCESS_LEVEL") if should_item else None,
                    "DOES_HAVE_ACCESS_LEVEL": does_item.get("ACCESS_LEVEL") if does_item else None,
                    "ACCESS_STATUS": access_status,
                }
            )

    return pd.DataFrame(rows)


def process_share_drive(
    df_hr: pd.DataFrame,
    df_does_have_share: pd.DataFrame,
    df_should_have: pd.DataFrame,
) -> pd.DataFrame:
    """Run full share drive reconciliation."""
    sharedrive_values = {"SHARED_DRIVE", "SHARE_DRIVE", "SHARED DRIVE"}
    df_should_sd = df_should_have[df_should_have["ASSET TYPE"].isin(sharedrive_values)].copy()

    # In should-have files, permission field contains the path/access details in your notebook.
    df_should_sd = extract_path_and_access(df_should_sd, "PERMISSION")

    # In does-have share drive file, path/access may already be present or embedded in one field.
    path_col = find_first_existing_column(
        df_does_have_share,
        ["PATH", "SHARE_DRIVE_DISPLAY_NAME", "SHARE DRIVE DISPLAY NAME", "PERMISSION", "ACCESS DETAIL"],
    )
    access_col = find_first_existing_column(
        df_does_have_share,
        ["ACCESS_LEVEL", "ACCESS LEVEL", "PERMISSION", "ACCESS", "ENTITLEMENT"],
    )

    if path_col is None:
        raise KeyError("Could not identify share drive path column in share drive export.")

    df_does_have_share = df_does_have_share.copy()

    # If path/access are embedded together, extract them.
    if path_col == access_col or access_col is None:
        temp = extract_path_and_access(df_does_have_share, path_col)
        if temp["PATH"].notna().any():
            df_does_have_share["PATH"] = temp["PATH"]
            df_does_have_share["ACCESS_LEVEL"] = temp["ACCESS_LEVEL"]
            path_col = "PATH"
            access_col = "ACCESS_LEVEL"
        else:
            # If only path exists and access level is not available, keep path and set access blank.
            if path_col != "PATH":
                df_does_have_share["PATH"] = df_does_have_share[path_col]
                path_col = "PATH"
            df_does_have_share["ACCESS_LEVEL"] = None
            access_col = "ACCESS_LEVEL"

    should_map = build_path_access_map(
        df=df_should_sd,
        key_col="PERSONA",
        path_col="PATH",
        access_col="ACCESS_LEVEL",
    )
    does_map = build_path_access_map(
        df=df_does_have_share,
        key_col="BRID_DOES_HAVE",
        path_col=path_col,
        access_col=access_col,
    )

    df_sd = df_hr.copy()
    df_sd["SHR_DRIVE_SHD_HAVE"] = df_sd["PERSONA"].map(should_map)
    df_sd["SHR_DRIVE_DOES_HAVE"] = df_sd["BRID_HR"].map(does_map)

    share_drive_df = reconcile_share_drive_paths(df_sd)
    return share_drive_df


# ============================================================
# MAIN PIPELINE
# ============================================================
def main() -> None:
    # --------------------------------------------------------
    # 1) Build file paths
    # --------------------------------------------------------
    hr_path = os.path.join(BASE_FOLDER, HR_FILE)
    app_access_path = os.path.join(BASE_FOLDER, ACCESS_REPORTS_FOLDER, APP_ACCESS_FILE)
    share_drive_path = os.path.join(BASE_FOLDER, ACCESS_REPORTS_FOLDER, SHARE_DRIVE_FILE)
    should_have_folder_path = os.path.join(BASE_FOLDER, SHOULD_HAVE_FOLDER)
    output_path = os.path.join(BASE_FOLDER, OUTPUT_FILE)

    # --------------------------------------------------------
    # 2) Read input files
    # --------------------------------------------------------
    df_hr = pd.read_excel(hr_path, dtype=str)
    df_app_does_have = read_txt_export(app_access_path)
    df_share_does_have = read_txt_export(share_drive_path)
    df_should_have = read_should_have_files(should_have_folder_path)

    # --------------------------------------------------------
    # 3) Standardize all source files
    # --------------------------------------------------------
    df_hr = standardize_hr(df_hr)
    df_app_does_have = standardize_does_have(df_app_does_have)
    df_share_does_have = standardize_does_have(df_share_does_have)
    df_should_have = standardize_should_have(df_should_have)

    # Optional: keep separate invalid HR rows like your original notebook
    if "COMMENTS" in df_hr.columns:
        df_hr_other = df_hr[
            df_hr["COMMENTS"].isin([
                "MULTIPLE PERSONA MAPPED TO SAME CC",
                "UNMATCHED PERSONA",
                "NO PERSONA FOUND",
            ])
        ].copy()
        df_hr_clean = df_hr[df_hr["COMMENTS"] == "CLEAN FOR DASHBOARD"].copy()
    else:
        df_hr_other = pd.DataFrame(columns=df_hr.columns)
        df_hr_clean = df_hr.copy()

    # --------------------------------------------------------
    # 4) Process app permission reconciliation
    # --------------------------------------------------------
    df_app_summary, flat_app_perm = process_app_permissions(
        df_hr=df_hr_clean,
        df_does_have_app=df_app_does_have,
        df_should_have=df_should_have,
    )

    # --------------------------------------------------------
    # 5) Process share drive reconciliation
    # --------------------------------------------------------
    share_drive_df = process_share_drive(
        df_hr=df_hr_clean,
        df_does_have_share=df_share_does_have,
        df_should_have=df_should_have,
    )

    # --------------------------------------------------------
    # 6) Merge app permission output + share drive output
    # --------------------------------------------------------
    merged_df = pd.merge(
        flat_app_perm,
        share_drive_df,
        left_on="BRID",
        right_on="BRID_HR",
        how="left",
    )

    # Add HR summary columns back into final output
    summary_cols = [
        "BRID_HR",
        "SOURCE",
        "FULL NAME",
        "LINE MANAGER",
        "MANAGEMENT LEVEL",
        "COST CENTER - ID",
        "COMBO",
        "PERSONA",
        "COMMENTS",
        "DOES_HAVE_APPS",
        "SHOULD_HAVE_APPS",
        "MATCHED_APPLICATIONS",
        "MISSING_APPLICATIONS",
        "EXCESS_APPLICATIONS",
        "CNT_SHOULD_HAVE_APPS",
        "CNT_DOES_HAVE_APPS",
        "CNT_MATCHED_APPLICATIONS",
        "CNT_MISSING_APPLICATIONS",
        "CNT_EXCESS_APPLICATIONS",
    ]
    summary_cols = [col for col in summary_cols if col in df_app_summary.columns]

    merged_df = pd.merge(
        df_app_summary[summary_cols].drop_duplicates(subset=["BRID_HR"]),
        merged_df,
        left_on="BRID_HR",
        right_on="BRID",
        how="left",
    )

    # Append non-clean HR rows, if you want them present in final workbook
    if not df_hr_other.empty:
        for col in merged_df.columns:
            if col not in df_hr_other.columns:
                df_hr_other[col] = None
        df_hr_other = df_hr_other[merged_df.columns]
        final_df = pd.concat([merged_df, df_hr_other], ignore_index=True)
    else:
        final_df = merged_df.copy()

    # --------------------------------------------------------
    # 7) Write Excel output with multiple tabs
    # --------------------------------------------------------
    with pd.ExcelWriter(output_path, engine="openpyxl") as writer:
        df_app_summary.to_excel(writer, sheet_name="app_summary", index=False)
        flat_app_perm.to_excel(writer, sheet_name="app_permission_flat", index=False)
        share_drive_df.to_excel(writer, sheet_name="share_drive_flat", index=False)
        final_df.to_excel(writer, sheet_name="combined_output", index=False)

    print(f"\nDone. Output file saved to: {output_path}")


if __name__ == "__main__":
    main()
