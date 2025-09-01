import re
import unicodedata
from io import BytesIO
from datetime import datetime
from pathlib import Path

import numpy as np
import pandas as pd
import streamlit as st

# =============================
# UI
# =============================
st.set_page_config(page_title="Pilot Report Builder", layout="wide")
st.title("🛫 Pilot Report Builder")
st.caption(
    "Upload the 3 Salesforce exports (Block Time, Duty Days, PTO & Off). "
    "We clean headers/names, merge metrics, lock the roster/order, and export a pretty Excel with grouped headers."
)

# =============================
# Hard-locked pilot roster & order
# =============================
PILOT_WHITELIST: list[str] = [
    "Barry Wolfe","Bradley Jordan","Debra Voit","Dustin Anderson","Eric Tange",
    "Grant Fitzer","Ian Hank","James Duffey","Jeffrey Tyson","Joshua Otzen",
    "Nicholas Hoffmann","Randy Ripp","Richard Olson","Robert Myers","Ron Jenson","Sean Sinette",
]

# =============================
# Utilities
# =============================
NOISE_PATTERNS = (
    "filtered by","as of","report","custom object","rows:","columns:","page","dashboard",
    "record count","grand total","subtotal","grouped by","show all","click to","run report"
)
NOISE_NAME_HINTS = ("crew name","sum of","total","grand total","filtered","↑","→",":","|")

def _to_num(s: pd.Series) -> pd.Series:
    return pd.to_numeric(s, errors="coerce")

def clean_pilot_name(s: str) -> str:
    if s is None: return ""
    s = str(s).replace("\xa0"," ").strip()
    s = re.sub(r"\[(.*?)\]|\((.*?)\)","",s)
    s = re.sub(r"\s+"," ",s).strip()
    s = s.strip(" ,;-_/\\|")
    return s

def looks_like_noise(s: str) -> bool:
    if s is None: return True
    t = str(s).strip().lower()
    if t in ("","nan"): return True
    if any(p in t for p in NOISE_PATTERNS): return True
    if any(h in t for h in NOISE_NAME_HINTS): return True
    if not re.search(r"[a-zA-Z]", t): return True
    return False

def drop_empty_metric_rows(df: pd.DataFrame, name_col: str, metric_cols: list[str]) -> pd.DataFrame:
    out = df.copy()
    out[name_col] = out[name_col].map(clean_pilot_name)
    out = out[~out[name_col].map(looks_like_noise)]
    existing = [c for c in metric_cols if c in out.columns]
    if existing:
        nums = out[existing].apply(pd.to_numeric, errors="coerce")
        keep = (nums.notna().sum(axis=1) > 0) & (nums.fillna(0).sum(axis=1) > 0)
        out = out[keep]
    return out.reset_index(drop=True)

def collapse_duplicate_columns(df: pd.DataFrame) -> pd.DataFrame:
    dup_names = df.columns[df.columns.duplicated()].unique()
    for name in dup_names:
        same = [c for c in df.columns if c == name]
        base = same[0]
        for extra in same[1:]:
            df[base] = df[base].where(df[base].notna() & (df[base] != ""), df[extra])
        df = df.loc[:, ~df.columns.duplicated()]
    return df

def _norm(s: str) -> str:
    if s is None: return ""
    s = unicodedata.normalize("NFKD", str(s)).encode("ascii","ignore").decode("ascii")
    s = re.sub(r"[^0-9a-zA-Z]+","",s)
    return s.lower()

# =============================
# Parsers
# =============================
def parse_block_time(xl) -> pd.DataFrame:
    xls = pd.ExcelFile(xl)
    df = pd.read_excel(xl, sheet_name=xls.sheet_names[0], header=35)

    drop_mask = (df.columns.astype(str).str.startswith("Unnamed")) & (df.isna().all())
    df = df.loc[:, ~drop_mask]
    if df.shape[1] > 0:
        df = df[df.iloc[:,0].notna()].reset_index(drop=True)

    cols = list(df.columns)
    name_col = next((c for c in cols if "crew" in str(c).lower()), cols[0])

    out = pd.DataFrame()
    out["Pilot"] = df[name_col].astype(str).map(clean_pilot_name)

    blk_cols = [c for c in cols if "Sum of Block Time" in str(c)]
    if len(blk_cols) > 0: out["Block Hours 30 Day"] = _to_num(df[blk_cols[0]])
    if len(blk_cols) > 1: out["Block Hours 6 Month"] = _to_num(df[blk_cols[1]])
    if len(blk_cols) > 2: out["Block Hours YTD"] = _to_num(df[blk_cols[2]])

    if "Sum of Day Takeoff" in cols:   out["Day Takeoff"] = _to_num(df["Sum of Day Takeoff"]).fillna(0)
    if "Sum of Night Takeoff" in cols: out["Night Takeoff"] = _to_num(df["Sum of Night Takeoff"]).fillna(0)
    if "Sum of Day Landing" in cols:   out["Day Landing"] = _to_num(df["Sum of Day Landing"]).fillna(0)
    if "Sum of Night Landing" in cols: out["Night Landing"] = _to_num(df["Sum of Night Landing"]).fillna(0)

    if "Sum of Flight Log: Holds" in cols:
        out["Holds 6 Month"] = _to_num(df["Sum of Flight Log: Holds"])

    return drop_empty_metric_rows(out, "Pilot", [])

def parse_duty_days(xl) -> pd.DataFrame:
    raw = pd.read_excel(xl, header=None)
    idx_periods = None
    for i in range(10, min(len(raw), 60)):
        row_vals = raw.iloc[i].astype(str).tolist()
        if ("30 Days" in row_vals) and ("90 Days" in row_vals) and ("YTD" in row_vals):
            idx_periods = i; break
    if idx_periods is None:
        raise ValueError("Duty Days: Couldn't locate the periods row (30/90/YTD).")

    idx_metrics = idx_periods + 1
    data = raw.iloc[idx_metrics + 1:].reset_index(drop=True)

    crew_col = 1
    names = data.iloc[:, crew_col].astype(str).str.strip()
    mask = names.notna() & (names != "") & (~names.str.contains("Total", case=False, na=False))
    data, names = data[mask], names[mask]

    duty_df = pd.DataFrame({
        "PilotFirst": names.map(clean_pilot_name),
        # Triplets per period = [RONs, Weekend Duty, Duty Day]
        "Duty Days 30 Day": _to_num(data.iloc[:, 3]),
        "Duty Days 90 Day": _to_num(data.iloc[:, 6]),
        "Duty Days YTD": _to_num(data.iloc[:, 9]),
        "Weekend Duty 30 Day": _to_num(data.iloc[:, 2]),
        "Weekend Duty 90 Day": _to_num(data.iloc[:, 5]),
        "Weekend Duty YTD": _to_num(data.iloc[:, 8]),
        "RONs 30 Day": _to_num(data.iloc[:, 1]),
        "RONs 90 Day": _to_num(data.iloc[:, 4]),
        "RONs YTD": _to_num(data.iloc[:, 7]),
    })
    return drop_empty_metric_rows(duty_df, "PilotFirst", duty_df.columns[1:].tolist())

def parse_pto_off(xl) -> pd.DataFrame:
    raw = pd.read_excel(xl, header=None)
    metrics_idx = None
    for i in range(10, min(len(raw), 50)):
        row_vals = raw.iloc[i].astype(str).tolist()
        if any("Sum of PTO Days" in v for v in row_vals) and any("Sum of Day Off" in v for v in row_vals):
            metrics_idx = i; break
    if metrics_idx is None:
        raise ValueError("PTO/Off: Couldn't find the metrics header row.")

    data = raw.iloc[metrics_idx + 1:].reset_index(drop=True)
    names = data.iloc[:, 1].astype(str).str.strip()
    mask = names.notna() & (names != "") & (~names.str.contains("Total", case=False, na=False))
    data, names = data[mask], names[mask]

    out = pd.DataFrame({
        "PilotFirst": names.map(clean_pilot_name),
        "PTO 30 Day": _to_num(data.iloc[:, 2]),
        "OFF 30 Day": _to_num(data.iloc[:, 3]),
        "PTO 90 Day": _to_num(data.iloc[:, 4]),
        "OFF 90 Day": _to_num(data.iloc[:, 5]),
        "PTO YTD": _to_num(data.iloc[:, 6]),
        "OFF YTD": _to_num(data.iloc[:, 7]),
    })
    return drop_empty_metric_rows(out, "PilotFirst", out.columns[1:].tolist())

# =============================
# Export helper (headers, logo, widths, freeze panes)
# =============================
def round_and_export(rep_out: pd.DataFrame) -> tuple[BytesIO, str]:
    # Round values before export
    block_cols = [c for c in rep_out.columns if "Block Hours" in c]
    other_num_cols = [c for c in rep_out.columns if c != "Pilot" and c not in block_cols and pd.api.types.is_numeric_dtype(rep_out[c])]

    for c in block_cols:
        rep_out[c] = pd.to_numeric(rep_out[c], errors="coerce").round(1)
    for c in other_num_cols:
        rep_out[c] = pd.to_numeric(rep_out[c], errors="coerce").round(0)

    # AVERAGE row (ceil) for non-Block cols
    avg_mask = rep_out["Pilot"].astype(str).str.upper() == "AVERAGE"
    for c in other_num_cols:
        rep_out.loc[avg_mask, c] = np.ceil(pd.to_numeric(rep_out.loc[avg_mask, c], errors="coerce")).astype(int)

    # Excel export
    ts = datetime.now().strftime("%Y%m%d")
    fname = f"Pilot_Report_{ts}.xlsx"
    bio = BytesIO()

    with pd.ExcelWriter(bio, engine="xlsxwriter") as writer:
        # Data at row 2; custom headers above
        rep_out.to_excel(writer, sheet_name="Pilot Report", index=False, header=False, startrow=2)
        wb = writer.book
        ws = writer.sheets["Pilot Report"]

        # Freeze top 2 rows + first column
        ws.freeze_panes(2, 1)

        # ===== Formats =====
        TARGET_RED = "#E4002B"
        WHITE = "#FFFFFF"

        group_red   = wb.add_format({"bold": True,"align":"center","valign":"vcenter","bg_color":TARGET_RED,"font_color":WHITE,"border":1})
        group_white = wb.add_format({"bold": True,"align":"center","valign":"vcenter","bg_color":WHITE,"font_color":TARGET_RED,"border":1})

        sub_red     = wb.add_format({"bold": True,"align":"center","valign":"vcenter","bg_color":TARGET_RED,"font_color":WHITE,"border":1})
        sub_white   = wb.add_format({"bold": True,"align":"center","valign":"vcenter","bg_color":WHITE,"font_color":TARGET_RED,"border":1})

        pilot_sub   = wb.add_format({"bold": True,"align":"left","valign":"vcenter","bg_color":"#F2F2F2","border":1})

        text_left   = wb.add_format({"num_format":"@",  "align":"left",   "valign":"vcenter"})
        int_center  = wb.add_format({"num_format":"0",  "align":"center", "valign":"vcenter"})
        hour_center = wb.add_format({"num_format":"0.0","align":"center", "valign":"vcenter"})

        text_total  = wb.add_format({"num_format":"@",  "align":"left",   "valign":"vcenter","bg_color":"#FFF2CC","bold":True})
        int_total   = wb.add_format({"num_format":"0",  "align":"center", "valign":"vcenter","bg_color":"#FFF2CC","bold":True})
        hour_total  = wb.add_format({"num_format":"0.0","align":"center", "valign":"vcenter","bg_color":"#FFF2CC","bold":True})
        text_avg    = wb.add_format({"num_format":"@",  "align":"left",   "valign":"vcenter","bg_color":"#E2EFDA","italic":True})
        int_avg     = wb.add_format({"num_format":"0",  "align":"center", "valign":"vcenter","bg_color":"#E2EFDA","italic":True})
        hour_avg    = wb.add_format({"num_format":"0.0","align":"center", "valign":"vcenter","bg_color":"#E2EFDA","italic":True})

        cols = list(rep_out.columns)

        # --- Column widths FIRST (keep Pilot at 18) ---
        for j, col in enumerate(cols):
            if col == "Pilot":
                ws.set_column(j, j, 18, text_left)
            elif "Block Hours" in col:
                ws.set_column(j, j, 8, hour_center)
            else:
                ws.set_column(j, j, 8, int_center)

        # ---- Groups (alternating red/white, starting red for Duty Days) ----
        group_defs = [
            ("DUTY DAYS",   ["Duty Days 30 Day","Duty Days 90 Day","Duty Days YTD"]),
            ("BLOCK HOURS", ["Block Hours 30 Day","Block Hours 6 Month","Block Hours YTD"]),
            ("RONs",        ["RONs 30 Day","RONs 90 Day","RONs YTD"]),
            ("WEEKENDS",    ["Weekend Duty 30 Day","Weekend Duty 90 Day","Weekend Duty YTD"]),
            ("PTO",         ["PTO 30 Day","PTO 90 Day","PTO YTD"]),
            ("OFF",         ["OFF 30 Day","OFF 90 Day","OFF YTD"]),
            ("TAKEOFFS 90", ["Day Takeoff 90 Day","Night Takeoff 90 Day"]),
            ("LANDINGS 90", ["Day Landing 90 Day","Night Landing 90 Day"]),
            ("HOLDS",       ["Holds 6 Month"]),
        ]

        col_to_group_idx = {}
        for i, (label, names) in enumerate(group_defs):
            idxs = [k for k, c in enumerate(cols) if c in names]
            if not idxs:
                continue
            left, right = min(idxs), max(idxs)
            fmt = group_red if (i % 2 == 0) else group_white
            if left == right:
                ws.write(0, left, label, fmt)
            else:
                ws.merge_range(0, left, 0, right, label, fmt)
            for k in idxs:
                col_to_group_idx[k] = i

        # Top group header row height
        ws.set_row(0, 50)

        # ---- Row 1: period subheaders (same color alternation) ----
        pilot_col_idx = cols.index("Pilot")
        ws.write(1, pilot_col_idx, "Pilot", pilot_sub)

        def period_label(c: str) -> str:
            if c == "Pilot": return "Pilot"
            if c in ("Day Takeoff 90 Day","Day Landing 90 Day"): return "Day"
            if c in ("Night Takeoff 90 Day","Night Landing 90 Day"): return "Night"
            if "30 Day" in c: return "30 Days"
            if "90 Day" in c: return "90 Days"
            if "6 Month" in c: return "6 Mos"
            if "YTD" in c: return "YTD"
            return c

        for j, col in enumerate(cols):
            if col == "Pilot":
                continue
            fmt = sub_red if (col_to_group_idx.get(j, 0) % 2 == 0) else sub_white
            ws.write(1, j, period_label(col), fmt)

        # ---- Insert logo above Pilot (row 0, col 0) ----
        try:
            candidates = [
                Path(__file__).with_name("logo.png"),
                Path.cwd() / "logo.png",
            ]
            for p in candidates:
                if p.exists():
                    with open(p, "rb") as lf:
                        img_bytes = BytesIO(lf.read())
                    ws.insert_image(
                        0, pilot_col_idx, str(p),
                        {
                            "image_data": img_bytes,
                            "x_offset": 2, "y_offset": 4,
                            "x_scale": 0.8, "y_scale": 0.8,
                            "object_position": 1,  # move/size with cells
                        }
                    )
                    break
        except Exception:
            pass  # ignore logo errors

        # --- Shade TOTAL and AVERAGE only across data columns ---
        first_data_row = 2
        df_idx_total = rep_out.index[rep_out["Pilot"].astype(str).str.upper() == "TOTAL"]
        df_idx_avg   = rep_out.index[rep_out["Pilot"].astype(str).str.upper() == "AVERAGE"]

        def rewrite_row(excel_row: int, df_row: int, total: bool):
            for j, col in enumerate(cols):
                val = rep_out.iat[df_row, j]
                if col == "Pilot":
                    fmt = text_total if total else text_avg
                elif "Block Hours" in col:
                    fmt = hour_total if total else hour_avg
                else:
                    fmt = int_total if total else int_avg
                if pd.isna(val):
                    ws.write_blank(excel_row, j, None, fmt)
                else:
                    ws.write(excel_row, j, val, fmt)

        if len(df_idx_total) == 1:
            excel_row_total = first_data_row + int(df_idx_total[0])
            rewrite_row(excel_row_total, int(df_idx_total[0]), total=True)
        if len(df_idx_avg) == 1:
            excel_row_avg = first_data_row + int(df_idx_avg[0])
            rewrite_row(excel_row_avg, int(df_idx_avg[0]), total=False)

    bio.seek(0)
    return bio, fname

# =============================
# UI: file uploads
# =============================
col1, col2 = st.columns(2)
with col1:
    block_file = st.file_uploader("Block Time export (.xlsx)", type=["xlsx"], key="blk")
    duty_file  = st.file_uploader("Duty Days export (.xlsx)", type=["xlsx"], key="duty")
with col2:
    pto_file   = st.file_uploader("PTO & Off export (.xlsx)", type=["xlsx"], key="pto")

build = st.button("Build Pilot Report ✅", use_container_width=True)

# =============================
# Processing (with diagnostics)
# =============================
if build:
    if not (block_file and duty_file and pto_file):
        st.error("Please upload all three Salesforce reports.")
        st.stop()

    with st.spinner("Parsing files…"):
        st.write("**Files received:**",
                 {"Block": getattr(block_file, "name", "?"),
                  "Duty": getattr(duty_file, "name", "?"),
                  "PTO/OFF": getattr(pto_file, "name", "?")})
        try:
            blk = parse_block_time(block_file)
        except Exception as e:
            st.exception(e); st.stop()
        try:
            dut = parse_duty_days(duty_file)
        except Exception as e:
            st.exception(e); st.stop()
        try:
            pto = parse_pto_off(pto_file)
        except Exception as e:
            st.exception(e); st.stop()

    with st.spinner("Merging & formatting…"):
        # Merge by first token of name
        blk = blk.rename(columns={"Pilot": "Pilot_blk"})
        blk_key = blk.assign(PilotKey=blk["Pilot_blk"].str.split().str[0].str.lower())
        dut_key = dut.assign(PilotKey=dut["PilotFirst"].str.lower())
        pto_key = pto.assign(PilotKey=pto["PilotFirst"].str.split().str[0].str.lower())

        rep = blk_key.merge(dut_key, on="PilotKey", how="outer", suffixes=("", "_dut"))
        rep = rep.merge(pto_key, on="PilotKey", how="outer", suffixes=("", "_pto"))

        # Display name
        def _pick(row):
            if pd.notna(row.get("Pilot_blk")) and str(row["Pilot_blk"]).strip(): return row["Pilot_blk"]
            if pd.notna(row.get("PilotFirst_pto")) and str(row["PilotFirst_pto"]).strip(): return row["PilotFirst_pto"]
