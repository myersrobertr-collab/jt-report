import re
import unicodedata
from io import BytesIO
from datetime import datetime
from pathlib import Path
from typing import Optional, List, Tuple

import numpy as np
import pandas as pd
import streamlit as st

# =============================
# UI
# =============================
st.set_page_config(page_title="Pilot Report Builder — Local", layout="wide")
st.title("🛫 TFS Pilot Report Builder Ver.0925")
st.caption(
    "Upload the 3 .Biz Reports (Block Time, Duty Days, PTO & Off). "
    "Filthy Animals.  "
    "...Go With Trim"
)

# =============================
# Hard-locked pilot roster & order
# =============================
PILOT_WHITELIST: List[str] = [
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

def drop_empty_metric_rows(df: pd.DataFrame, name_col: str, metric_cols: List[str]) -> pd.DataFrame:
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
# Header helpers pinned to your 2025-08-31 export structure
# =============================

def _find_row(df: pd.DataFrame, tokens: List[str], max_rows: int = 120) -> Optional[int]:
    toks = [t.lower() for t in tokens]
    for i in range(min(max_rows, len(df))):
        row = " | ".join(str(x).lower() for x in df.iloc[i].tolist())
        if all(t in row for t in toks):
            return i
    return None

# =============================
# Parsers (stable mapping matching your latest exports)
# =============================

def parse_block_time(xl) -> pd.DataFrame:
    """Maps Block 30/6mo/YTD, Day/Night TO & LDG, Holds 6mo.
    Uses detected header row; falls back to expected positions if needed.
    """
    raw = pd.read_excel(xl, header=None)
    idx_periods = _find_row(raw, ["block", "30"]) or _find_row(raw, ["30", "ytd"]) or 34
    idx_metrics = idx_periods + 1
    header = raw.iloc[idx_metrics].astype(str).tolist()

    def idx_of(pred, default=None):
        for j, h in enumerate(header):
            t = str(h).lower()
            if pred(t):
                return j
        return default

    name_col = idx_of(lambda t: ("crew" in t and "name" in t), 1)
    blk_idxs = [j for j, h in enumerate(header) if str(h).lower().startswith("sum of block time")]
    blk_30   = blk_idxs[0] if len(blk_idxs) >= 1 else 2
    blk_6mo  = blk_idxs[1] if len(blk_idxs) >= 2 else 3
    blk_ytd  = blk_idxs[2] if len(blk_idxs) >= 3 else 4

    day_to   = idx_of(lambda t: "sum of day takeoff" in t, 5)
    night_to = idx_of(lambda t: "sum of night takeoff" in t, 6)
    day_ldg  = idx_of(lambda t: "sum of day landing" in t, 7)
    night_ldg= idx_of(lambda t: "sum of night landing" in t, 8)
    holds    = idx_of(lambda t: ("sum of flight log: holds" in t) or (t.strip()=="holds"), 9)

    data = raw.iloc[idx_metrics + 1:].reset_index(drop=True)
    names = data.iloc[:, name_col].astype(str)

    out = pd.DataFrame({
        "Pilot": names.map(clean_pilot_name),
        "Block Hours 30 Day": _to_num(data.iloc[:, blk_30]),
        "Block Hours 6 Month": _to_num(data.iloc[:, blk_6mo]),
        "Block Hours YTD": _to_num(data.iloc[:, blk_ytd]),
        "Day Takeoff": _to_num(data.iloc[:, day_to]).fillna(0),
        "Night Takeoff": _to_num(data.iloc[:, night_to]).fillna(0),
        "Day Landing": _to_num(data.iloc[:, day_ldg]).fillna(0),
        "Night Landing": _to_num(data.iloc[:, night_ldg]).fillna(0),
        "Holds 6 Month": _to_num(data.iloc[:, holds]),
    })
    return drop_empty_metric_rows(out, "Pilot", [c for c in out.columns if c != "Pilot"])


def parse_duty_days(xl) -> pd.DataFrame:
    """Duty Days export pinned to triplets per period.
    30d: [2] RONs, [3] Weekend, [4] Duty
    90d: [5] RONs, [6] Weekend, [7] Duty
    YTD: [8] RONs, [9] Weekend, [10] Duty
    Names in col 1.
    """
    raw = pd.read_excel(xl, header=None)
    idx_periods = _find_row(raw, ["30", "90", "ytd"]) or 27
    idx_metrics = idx_periods + 1

    data = raw.iloc[idx_metrics + 1:].reset_index(drop=True)
    names = data.iloc[:, 1].astype(str).str.strip()
    mask = names.notna() & (names != "") & (~names.str.contains("Total", case=False, na=False))
    data, names = data[mask], names[mask]

    duty_df = pd.DataFrame({
        "PilotFirst": names.map(clean_pilot_name),
        "RONs 30 Day": _to_num(data.iloc[:, 2]),
        "Weekend Duty 30 Day": _to_num(data.iloc[:, 3]),
        "Duty Days 30 Day": _to_num(data.iloc[:, 4]),
        "RONs 90 Day": _to_num(data.iloc[:, 5]),
        "Weekend Duty 90 Day": _to_num(data.iloc[:, 6]),
        "Duty Days 90 Day": _to_num(data.iloc[:, 7]),
        "RONs YTD": _to_num(data.iloc[:, 8]),
        "Weekend Duty YTD": _to_num(data.iloc[:, 9]),
        "Duty Days YTD": _to_num(data.iloc[:, 10]),
    })
    return drop_empty_metric_rows(duty_df, "PilotFirst", duty_df.columns[1:].tolist())


def parse_pto_off(xl) -> pd.DataFrame:
    """PTO & Off export pinned to pairs per period.
    30d: [2] PTO, [3] OFF
    90d: [4] PTO, [5] OFF
    YTD: [6] PTO, [7] OFF
    Names in col 1.
    """
    raw = pd.read_excel(xl, header=None)
    idx_periods = _find_row(raw, ["pto", "off"]) or 24
    idx_metrics = idx_periods + 1

    data = raw.iloc[idx_metrics + 1:].reset_index(drop=True)
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

def round_and_export(rep_out: pd.DataFrame) -> Tuple[BytesIO, str]:
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
                            "object_position": 1,
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
        st.error("Please upload all three .Biz Reports.")
        st.stop()

    with st.spinner("Parsing files…"):
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

        # Display name (prefer Block, then PTO, then Duty, then key)
        def _pick(row):
            if pd.notna(row.get("Pilot_blk")) and str(row["Pilot_blk"]).strip(): return row["Pilot_blk"]
            if pd.notna(row.get("PilotFirst_pto")) and str(row["PilotFirst_pto"]).strip(): return row["PilotFirst_pto"]
            if pd.notna(row.get("PilotFirst")) and str(row["PilotFirst"]).strip(): return str(row["PilotFirst"]).title()
            return str(row.get("PilotKey", "")).title()
        rep["Pilot"] = rep.apply(_pick, axis=1)

        # Cleanup
        rep = rep.drop(columns=["Pilot_blk","PilotFirst","PilotFirst_pto","PilotKey"], errors="ignore")
        rep = rep.loc[:, ~rep.columns.duplicated()]

        # Lock roster & order using full names
        order = [clean_pilot_name(n).title() for n in PILOT_WHITELIST]
        rep["Pilot"] = rep["Pilot"].map(lambda x: clean_pilot_name(x).title())
        rep = rep[rep["Pilot"].isin(order)].copy()
        rep["Pilot"] = pd.Categorical(rep["Pilot"], categories=order, ordered=True)
        rep = rep.sort_values("Pilot").reset_index(drop=True)

        # Rename takeoff/landing to explicit 90-day labels for grouping
        rep = rep.rename(columns={
            "Day Takeoff": "Day Takeoff 90 Day",
            "Night Takeoff": "Night Takeoff 90 Day",
            "Day Landing": "Day Landing 90 Day",
            "Night Landing": "Night Landing 90 Day",
        })

        # Exact column order requested:
        desired_order = [
            "Pilot",
            "Duty Days 30 Day","Duty Days 90 Day","Duty Days YTD",
            "Block Hours 30 Day","Block Hours 6 Month","Block Hours YTD",
            "RONs 30 Day","RONs 90 Day","RONs YTD",
            "Weekend Duty 30 Day","Weekend Duty 90 Day","Weekend Duty YTD",
            "PTO 30 Day","PTO 90 Day","PTO YTD",
            "OFF 30 Day","OFF 90 Day","OFF YTD",
            "Day Takeoff 90 Day","Night Takeoff 90 Day",
            "Day Landing 90 Day","Night Landing 90 Day",
            "Holds 6 Month",
        ]
        cols_order = [c for c in desired_order if c in rep.columns] + [
            c for c in rep.columns if c not in desired_order and c != "Pilot"
        ]
        rep = rep[cols_order]

        # Fill numerics and final clean
        for c in rep.columns:
            if c != "Pilot" and pd.api.types.is_numeric_dtype(rep[c]):
                rep[c] = rep[c].fillna(0)
        rep = collapse_duplicate_columns(rep)

        # Totals/Averages
        numeric_cols = [c for c in rep.columns if c != "Pilot" and pd.api.types.is_numeric_dtype(rep[c])]
        if not numeric_cols:
            st.error("No numeric columns were detected after merging. Check that exports match your current Salesforce formats.")
            st.stop()

        total_row = {c: rep[c].sum() for c in numeric_cols}; total_row["Pilot"] = "TOTAL"
        avg_row   = {c: rep[c].mean() for c in numeric_cols};   avg_row["Pilot"] = "AVERAGE"
        rep_out = pd.concat([rep, pd.DataFrame([total_row, avg_row])], ignore_index=True)

        try:
            bio, fname = round_and_export(rep_out)
        except Exception as e:
            st.exception(e); st.stop()

    st.success("✅ Report built. Use the download button below.")
    st.download_button(
        "⬇️ Download Pilot Report (Excel)",
        bio,
        fname,
        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        use_container_width=True,
    )
else:
    st.info("Upload your three .Biz Reports and click **Build Pilot Report**.")
