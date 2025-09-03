import pandas as pd

# ----------------------------
# 0) Column mapping & cleaning
# ----------------------------
def _norm(s):
    return (s.astype(str)
              .str.strip()
              .str.replace(r"\s+", " ", regex=True)
              .str.upper())

# Map ACL -> standard columns
_acl_app_col  = "USCB Owned Applications"
_acl_brid_col = "User_Unique_ID"
_acl_perm_col = "Application_Profile_Name" if "Application_Profile_Name" in ACL.columns else "Application_Profile_name"

acl_std = ACL[[_acl_app_col, _acl_brid_col, _acl_perm_col]].copy()
acl_std.columns = ["application_raw", "brid_raw", "permission_raw"]
acl_std["application"] = _norm(acl_std["application_raw"])
acl_std["brid"]        = _norm(acl_std["brid_raw"])
acl_std["permission"]  = acl_std["permission_raw"].astype(str).str.strip()

# Map OWNED/IAM -> standard columns
_own_app_col  = "REQUEST_WORKFLOW_NAME"
_own_brid_col = "BRID"
_own_perm_col = "PERMISSION_OR_PROFILE_NAME"

own_std = owned_does_have[[_own_app_col, _own_brid_col, _own_perm_col]].copy()
own_std.columns = ["application_raw", "brid_raw", "permission_raw"]
own_std["application"] = _norm(own_std["application_raw"])
own_std["brid"]        = _norm(own_std["brid_raw"])
own_std["permission"]  = own_std["permission_raw"].astype(str).str.strip()

# ----------------------------------------------------
# 1) Deduplicate & aggregate permissions per (app,BRID)
# ----------------------------------------------------
acl_agg = (acl_std
           .groupby(["application", "brid"], as_index=False)["permission"]
           .agg(lambda s: "; ".join(sorted(pd.unique([x for x in s if x])))))
acl_agg.rename(columns={"permission": "ACL_permission"}, inplace=True)
acl_agg["in_acl"] = True

own_agg = (own_std
           .groupby(["application", "brid"], as_index=False)["permission"]
           .agg(lambda s: "; ".join(sorted(pd.unique([x for x in s if x])))))
own_agg.rename(columns={"permission": "IAM_permission"}, inplace=True)
own_agg["in_iam"] = True

# -----------------------------------------
# 2) Full outer merge on (application,BRID)
# -----------------------------------------
view = pd.merge(acl_agg, own_agg, on=["application", "brid"], how="outer")
view["in_acl"] = view["in_acl"].fillna(False)
view["in_iam"] = view["in_iam"].fillna(False)
for c in ["ACL_permission", "IAM_permission"]:
    if c not in view:
        view[c] = pd.NA

# Access flags & diffs
view["access_in_both"] = view["in_acl"] & view["in_iam"]
view["access_diff"] = view.apply(
    lambda r: "BOTH" if r["access_in_both"] else ("ACL_ONLY" if r["in_acl"] else ("IAM_ONLY" if r["in_iam"] else "NONE")),
    axis=1
)

# Permission match (case-insensitive; switch to exact case if needed)
view["matched"] = (
    view["access_in_both"] &
    (view["ACL_permission"].fillna("").str.strip().str.lower()
     == view["IAM_permission"].fillna("").str.strip().str.lower())
)

# Pretty ordering
view = (view[[
    "application", "brid",
    "in_acl", "in_iam", "access_in_both", "access_diff",
    "ACL_permission", "IAM_permission", "matched"
]]
    .sort_values(["application", "brid"], kind="stable")
    .reset_index(drop=True)
)

# -------------------------------------------------------
# 3) Application-level summary (sets / counts / matches)
# -------------------------------------------------------
def _set_from(frame, flag_col):
    return (frame.loc[frame[flag_col], ["application", "brid"]]
                 .groupby("application")["brid"]
                 .agg(lambda s: set(s.tolist()))
                 .rename(flag_col))

acl_sets = _set_from(view, "in_acl")
iam_sets = _set_from(view, "in_iam")

summary = pd.DataFrame(index=sorted(set(view["application"])))
summary.index.name = "application"
summary = summary.join(acl_sets, how="left").join(iam_sets, how="left")
summary["in_acl"] = summary["in_acl"].apply(lambda x: x if isinstance(x, set) else set())
summary["in_iam"] = summary["in_iam"].apply(lambda x: x if isinstance(x, set) else set())

summary["both_brids"]    = summary.apply(lambda r: r["in_acl"] & r["in_iam"], axis=1)
summary["acl_only"]      = summary.apply(lambda r: r["in_acl"] - r["in_iam"], axis=1)
summary["iam_only"]      = summary.apply(lambda r: r["in_iam"] - r["in_acl"], axis=1)

summary["n_acl_brids"]   = summary["in_acl"].str.len()
summary["n_iam_brids"]   = summary["in_iam"].str.len()
summary["n_both_brids"]  = summary["both_brids"].str.len()
summary["n_acl_only"]    = summary["acl_only"].str.len()
summary["n_iam_only"]    = summary["iam_only"].str.len()

# permission match counts per application (only where both sides have the BRID)
perm_stats = (view[view["access_in_both"]]
              .groupby("application")["matched"]
              .agg(total_both="size", n_matched="sum"))
perm_stats["n_mismatch"] = perm_stats["total_both"] - perm_stats["n_matched"]

summary = summary.join(perm_stats, how="left").fillna(
    {"total_both": 0, "n_matched": 0, "n_mismatch": 0}
).reset_index()

# Optional: order columns nicely
summary = summary[[
    "application",
    "n_acl_brids", "n_iam_brids", "n_both_brids", "n_acl_only", "n_iam_only",
    "total_both", "n_matched", "n_mismatch",
    "in_acl", "in_iam", "both_brids", "acl_only", "iam_only"
]]

# ---- Results ----
# Row-level detail per (application, BRID):
#   -> view
# Application-level picture (sets + counts + match stats):
#   -> summary

# Example saves:
# view.to_csv("acl_vs_iam_view_by_brid.csv", index=False)
# summary.to_csv("acl_vs_iam_app_summary.csv", index=False)
