import pandas as pd


def get_app_perm_flat_file(
    folder_path,
    hr_file='Headcount.xlsx',
    does_have_file='01 app_access_export.txt',
    access_report_folder='USCB - Access Reports 030426',
    should_have_folder='shd_have_files',
    should_have_asset_type='APP_ACCESS'
):
    """
    Returns flattened app-permission reconciliation dataframe.

    Expected existing helper functions:
    - read_print_and_append(path)
    - capitalize_strings(df)
    - built_brid_or_persona_app_perm_map(df, col1, col2, col3)
    - common_applications(row)
    - only_in_should_have_applications(row)
    - only_in_does_have_applications(row)
    - comapre_and_flatten_permissions(df, does_col, should_col)
    """

    # Read input files
    df_hr = pd.read_excel(f"{folder_path}/{hr_file}")
    df_does_have = pd.read_csv(
        f"{folder_path}/{access_report_folder}/{does_have_file}",
        sep='|',
        encoding='cp1252'
    )
    shd_have = read_print_and_append(f"{folder_path}/{should_have_folder}")

    # Keep only required columns from should-have
    shd_have = shd_have[
        ['Source', 'Persona', 'Application', 'Request Name', 'Asset Type',
         'Permission', 'NEW COLUMN\nR-Required\nO-Optional']
    ]

    # Standardize should-have file
    df_shd_have = capitalize_strings(shd_have.copy())
    df_shd_have.columns = df_shd_have.columns.str.upper()
    df_shd_have.columns = [col.strip() for col in df_shd_have.columns]

    # Standardize HR file
    df_hr.columns = df_hr.columns.str.upper()
    df_hr.columns = [col.strip() for col in df_hr.columns]
    df_hr = capitalize_strings(df_hr)
    df_hr = df_hr.rename(columns={'COLLEAGUE ID': 'BRID_HR'})

    # Standardize does-have file
    df_does_have.columns = df_does_have.columns.str.upper()
    df_does_have.columns = [col.strip() for col in df_does_have.columns]
    df_does_have = capitalize_strings(df_does_have)
    df_does_have = df_does_have.rename(columns={'BRID': 'BRID_DOES_HAVE'})

    # Separate HR exceptions
    df_hr_other = df_hr[
        df_hr['COMMENTS'].isin([
            'MULTIPLE PERSONA MAPPED TO SAME CC',
            'UNMATCHED PERSONA',
            'NO PERSONA FOUND'
        ])
    ].copy()

    df_hr = df_hr[df_hr['COMMENTS'] == 'CLEAN FOR DASHBOARD'].copy()

    # Keep only app access in should-have
    df_shd_have = df_shd_have[df_shd_have['ASSET TYPE'] == should_have_asset_type].copy()

    # Build mappings
    shd_have_persona_app_permission_mapping = built_brid_or_persona_app_perm_map(
        df_shd_have,
        col1='PERSONA',
        col2='REQUEST NAME',
        col3='PERMISSION'
    )

    does_have_brid_app_permission_mapping = built_brid_or_persona_app_perm_map(
        df_does_have,
        col1='BRID_DOES_HAVE',
        col2='REQUEST_WORKFLOW_NAME',
        col3='ACCESS_PROFILE'
    )

    # Map should-have and does-have
    df_hr['DOES_HAVE_APP_AND_PERM'] = df_hr['BRID_HR'].map(does_have_brid_app_permission_mapping)
    df_hr['SHOULD_HAVE_APP_AND_PERM'] = df_hr['PERSONA'].map(shd_have_persona_app_permission_mapping)

    # Extract application lists
    df_hr['DOES_HAVE_APPS'] = df_hr['DOES_HAVE_APP_AND_PERM'].apply(
        lambda d: list(d.keys()) if isinstance(d, dict) else []
    )
    df_hr['SHOULD_HAVE_APPS'] = df_hr['SHOULD_HAVE_APP_AND_PERM'].apply(
        lambda d: list(d.keys()) if isinstance(d, dict) else []
    )

    df_hr['SHOULD_HAVE_APPS'] = df_hr['SHOULD_HAVE_APPS'].apply(
        lambda x: [] if isinstance(x, float) and pd.isna(x) else x
    )
    df_hr['DOES_HAVE_APPS'] = df_hr['DOES_HAVE_APPS'].apply(
        lambda x: [] if isinstance(x, float) and pd.isna(x) else x
    )

    df_hr['SHOULD_HAVE_APPS'] = df_hr['SHOULD_HAVE_APPS'].apply(lambda x: list(set(x)))
    df_hr['DOES_HAVE_APPS'] = df_hr['DOES_HAVE_APPS'].apply(lambda x: list(set(x)))

    # App reconciliation
    df_hr['MATCHED_APPLICATIONS'] = df_hr.apply(common_applications, axis=1)
    df_hr['MISSING_APPLICATIONS'] = df_hr.apply(only_in_should_have_applications, axis=1)
    df_hr['EXCESS_APPLICATIONS'] = df_hr.apply(only_in_does_have_applications, axis=1)

    df_hr['CNT_SHOULD_HAVE_APPS'] = df_hr['SHOULD_HAVE_APPS'].apply(len)
    df_hr['CNT_DOES_HAVE_APPS'] = df_hr['DOES_HAVE_APPS'].apply(len)
    df_hr['CNT_MATCHED_APPLICATIONS'] = df_hr['MATCHED_APPLICATIONS'].apply(len)
    df_hr['CNT_MISSING_APPLICATIONS'] = df_hr['MISSING_APPLICATIONS'].apply(len)
    df_hr['CNT_EXCESS_APPLICATIONS'] = df_hr['EXCESS_APPLICATIONS'].apply(len)

    # Select required columns
    df_alt = df_hr[
        ['SOURCE', 'BRID_HR', 'FULL NAME', 'LINE MANAGER', 'MANAGEMENT LEVEL',
         'COST CENTER - ID', 'COMBO', 'PERSONA', 'COMMENTS',
         'DOES_HAVE_APP_AND_PERM', 'SHOULD_HAVE_APP_AND_PERM',
         'DOES_HAVE_APPS', 'SHOULD_HAVE_APPS',
         'MATCHED_APPLICATIONS', 'MISSING_APPLICATIONS', 'EXCESS_APPLICATIONS',
         'CNT_SHOULD_HAVE_APPS', 'CNT_DOES_HAVE_APPS',
         'CNT_MATCHED_APPLICATIONS', 'CNT_MISSING_APPLICATIONS',
         'CNT_EXCESS_APPLICATIONS']
    ].copy()

    # Bring optional/required flag from should-have
    df_alt = pd.merge(
        df_alt,
        df_shd_have[['PERSONA', 'NEW COLUMN\nR-REQUIRED\nO-OPTIONAL']].drop_duplicates(),
        left_on='PERSONA',
        right_on='PERSONA',
        how='left'
    )

    # Flatten permission comparison
    flat_app_perm = comapre_and_flatten_permissions(
        df_alt,
        does_col='DOES_HAVE_APP_AND_PERM',
        should_col='SHOULD_HAVE_APP_AND_PERM'
    )

    flat_app_perm = pd.merge(
        df_alt,
        flat_app_perm,
        left_on='BRID_HR',
        right_on='BRID',
        how='left'
    )

    flat_app_perm = flat_app_perm.drop_duplicates(
        keep='first',
        subset=['BRID_HR', 'APPLICATION', 'APPLICATION_STATUS', 'PERMISSION', 'PERMISSION_STATUS']
    ).reset_index(drop=True)

    # Add HR exception rows if needed
    flat_df = pd.concat([flat_app_perm, df_hr_other], ignore_index=True, sort=False)

    return flat_df



def get_share_drive_file(
    folder_path,
    hr_file='Headcount.xlsx',
    does_have_file='05 share_drive_access_export.txt',
    access_report_folder='USCB - Access Reports 030426',
    should_have_folder='shd_have_files',
    share_drive_asset_types=('SHARED_DRIVE', 'SHARE_DRIVE', 'SHARED DRIVE')
):
    """
    Returns share-drive reconciliation dataframe.

    Expected existing helper functions:
    - read_print_and_append(path)
    - capitalize_strings(df)
    - extract_path_and_access(df, column_name='PERMISSION')
    - built_brid_or_persona_app_perm_map(df, col1, col2, col3)
    - build_path_perm_recon(df)
    """

    # Read input files
    df_hr = pd.read_excel(f"{folder_path}/{hr_file}")
    df_does_have = pd.read_csv(
        f"{folder_path}/{access_report_folder}/{does_have_file}",
        sep='|',
        encoding='cp1252'
    )
    shd_have = read_print_and_append(f"{folder_path}/{should_have_folder}")

    shd_have = shd_have[
        ['Source', 'Persona', 'Application', 'Request Name', 'Asset Type',
         'Permission', 'NEW COLUMN\nR-Required\nO-Optional']
    ]

    # Standardize should-have file
    df_shd_have = capitalize_strings(shd_have.copy())
    df_shd_have.columns = df_shd_have.columns.str.upper()
    df_shd_have.columns = [col.strip() for col in df_shd_have.columns]

    # Standardize HR file
    df_hr.columns = df_hr.columns.str.upper()
    df_hr.columns = [col.strip() for col in df_hr.columns]
    df_hr = capitalize_strings(df_hr)
    df_hr = df_hr.rename(columns={'COLLEAGUE ID': 'BRID_HR'})

    # Standardize does-have file
    df_does_have.columns = df_does_have.columns.str.upper()
    df_does_have.columns = [col.strip() for col in df_does_have.columns]
    df_does_have = capitalize_strings(df_does_have)
    df_does_have = df_does_have.rename(columns={'BRID': 'BRID_DOES_HAVE'})

    # Keep only share drive rows from should-have
    df_shd_have = df_shd_have[
        df_shd_have['ASSET TYPE'].isin(share_drive_asset_types)
    ].copy()

    # Extract PATH and ACCESS_LEVEL from should-have permission text
    df_shd_have = extract_path_and_access(df_shd_have, column_name='PERMISSION')

    # Build mappings
    # PERSONA -> {PATH: ACCESS_LEVEL}
    # BRID   -> {PATH: ACCESS_LEVEL}
    shd_have_persona_path_access_mapping = built_brid_or_persona_app_perm_map(
        df_shd_have,
        col1='PERSONA',
        col2='PATH',
        col3='ACCESS_LEVEL'
    )

    does_have_brid_path_access_mapping = built_brid_or_persona_app_perm_map(
        df_does_have,
        col1='BRID_DOES_HAVE',
        col2='SHARE_DRIVE_DISPLAY_NAME',
        col3='ACCESS_LEVEL'
    )

    # Map to HR file
    df_hr['SHR_DRIVE_SHD_HAVE'] = df_hr['PERSONA'].map(shd_have_persona_path_access_mapping)
    df_hr['SHR_DRIVE_DOES_HAVE'] = df_hr['BRID_HR'].map(does_have_brid_path_access_mapping)

    # Build reconciliation
    share_drive_df = build_path_perm_recon(df_hr)

    share_drive_df = share_drive_df[
        ['BRID_HR', 'SHR_DRIVE_SHD_HAVE', 'SHR_DRIVE_DOES_HAVE',
         'PATH', 'PATH_STATUS', 'ACCESS_LEVEL', 'ACCESS_STATUS']
    ].copy()

    return share_drive_df



def get_app_perm_and_share_drive(folder_path):
    flat_app_perm_df = get_app_perm_flat_file(folder_path=folder_path)
    share_drive_df = get_share_drive_file(folder_path=folder_path)
    return flat_app_perm_df, share_drive_df
