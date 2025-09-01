import re
import unicodedata
import numpy as np
import pandas as pd
import streamlit as st
from io import BytesIO
from datetime import datetime

st.set_page_config(page_title="Pilot Report Builder", layout="wide")
st.title("🛫 Pilot Report Builder (Web)")
st.caption(
    "Upload the 3 Salesforce exports (Block Time, Duty Days, PTO & Off). "
    "We auto-detect headers, clean names, combine metrics, hard-lock the pilot roster & order, "
    "and return a ready-to-download Excel file."
)

PILOT_WHITELIST: list[str] = [
    "Barry Wolfe","Bradley Jordan","Debra Voit","Dustin Anderson","Eric Tange",
    "Grant Fitzer","Ian Hank","James Duffey","Jeffrey Tyson","Joshua Otzen",
    "Nicholas Hoffmann","Randy Ripp","Richard Olson","Robert Myers",
    "Ron Jenson","Sean Sinette",
]

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

# ---------- Parsers ----------
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

    # Triplets: [RONs, Weekend Duty, Duty Day]
    duty_df = pd.DataFrame({
        "PilotFirst": names.map(clean_pilot_name),
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

def round_and_export(rep_out: pd.DataFrame) -> tuple[BytesIO, str]:
    block_cols = [c for c in rep_out.columns if "Block Hours" in c]
    other_num_cols = [c for c in rep_out.columns if c != "Pilot" and c not in block_cols and pd.api.types.is_numeric_dtype(rep_out[c])]
    for c in block_cols:
        rep_out[c] = pd.to_numeric(rep_out[c], errors="coerce").round(1)
    for c in other_num_cols:
        rep_out[c] = pd.to_numeric(rep_out[c], errors="coerce").round(0)
    avg_mask = rep_out["Pilot"].astype(str).str.upper() == "AVERAGE"
    for c in other_num_cols:
        rep_out.loc[avg_mask, c] = np.ceil(pd.to_numeric(rep_out.loc[avg_mask, c], errors="coerce")).astype(int)

    ts = datetime.now().strftime("%Y%m%d")
    fname = f"Pilot_Report_{ts}.xlsx"
    bio = BytesIO()
    with pd.ExcelWriter(bio, engine="xlsxwriter") as writer:
        rep_out.to_excel(writer, sheet_name="Pilot Report", index=False)
        wb = writer.book; ws = writer.sheets["Pilot Report"]
        ws.freeze_panes(1, 0)
        header_fmt = wb.add_format({"bold": True,"font_color":"white","bg_color":"#4F81BD","align":"center","valign":"vcenter","border":1})
        int_fmt  = wb.add_format({"num_format": "0"})
        hour_fmt = wb.add_format({"num_format": "0.0"})
        text_fmt = wb.add_format({"num_format": "@"})
        highlight_total = wb.add_format({"bold": True, "bg_color": "#FFF2CC"})
        highlight_avg   = wb.add_format({"italic": True, "bg_color": "#E2EFDA"})
        for col_idx, col_name in enumerate(rep_out.columns):
            ws.write(0, col_idx, col_name, header_fmt)
        for j, col in enumerate(rep_out.columns):
            if col == "Pilot": ws.set_column(j, j, 16, text_fmt)
            elif "Block Hours" in col: ws.set_column(j, j, 16, hour_fmt)
            else: ws.set_column(j, j, 16, int_fmt)
        for row_idx, pilot_name in enumerate(rep_out["Pilot"], start=1):
            u = str(pilot_name).upper()
            if u == "TOTAL": ws.set_row(row_idx, None, highlight_total)
            elif u == "AVERAGE": ws.set_row(row_idx, None, highlight_avg)
    bio.seek(0)
    return bio, fname

col1, col2 = st.columns(2)
with col1:
    block_file = st.file_uploader("Block Time export (.xlsx)", type=["xlsx"], key="blk")
    duty_file  = st.file_uploader("Duty Days export (.xlsx)", type=["xlsx"], key="duty")
with col2:
    pto_file   = st.file_uploader("PTO & Off export (.xlsx)", type=["xlsx"], key="pto")

if "report_bytes" not in st.session_state:
    st.session_state.report_bytes = None
    st.session_state.report_name = None

build = st.button("Build Pilot Report ✅", use_container_width=True)

if build:
    if not (block_file and duty_file and pto_file):
        st.error("Please upload all three Salesforce reports.")
        st.stop()

    try:
        blk = parse_block_time(block_file)
    except Exception as e:
        st.error(f"Block Time file wasn’t recognized: {e}")
        st.stop()
    try:
        dut = parse_duty_days(duty_file)
    except Exception as e:
        st.error(f"Duty Days file wasn’t recognized: {e}")
        st.stop()
    try:
        pto = parse_pto_off(pto_file)
    except Exception as e:
        st.error(f"PTO & Off file wasn’t recognized: {e}")
        st.stop()

    blk = blk.rename(columns={"Pilot": "Pilot_blk"})
    blk_key = blk.assign(PilotKey=blk["Pilot_blk"].str.split().str[0].str.lower())
    dut_key = dut.assign(PilotKey=dut["PilotFirst"].str.lower())
    pto_key = pto.assign(PilotKey=pto["PilotFirst"].str.split().str[0].str.lower())

    rep = blk_key.merge(dut_key, on="PilotKey", how="outer", suffixes=("", "_dut"))
    rep = rep.merge(pto_key, on="PilotKey", how="outer", suffixes=("", "_pto"))

    def _pick(row):
        if pd.notna(row.get("Pilot_blk")) and str(row["Pilot_blk"]).strip(): return row["Pilot_blk"]
        if pd.notna(row.get("PilotFirst_pto")) and str(row["PilotFirst_pto"]).strip(): return row["PilotFirst_pto"]
        if pd.notna(row.get("PilotFirst")) and str(row["PilotFirst"]).strip(): return str(row["PilotFirst"]).title()
        return str(row.get("PilotKey", "")).title()

    rep["Pilot"] = rep.apply(_pick, axis=1)
    rep = rep.drop(columns=["Pilot_blk","PilotFirst","PilotFirst_pto","PilotKey"], errors="ignore")
    rep = rep.loc[:, ~rep.columns.duplicated()]

    order = [clean_pilot_name(n).title() for n in PILOT_WHITELIST]
    rep["Pilot"] = rep["Pilot"].map(lambda x: clean_pilot_name(x).title())
    rep = rep[rep["Pilot"].isin(order)].copy()
    rep["Pilot"] = pd.Categorical(rep["Pilot"], categories=order, ordered=True)
    rep = rep.sort_values("Pilot").reset_index(drop=True)

    desired_order = [
        "Pilot",
        "Duty Days 30 Day","Duty Days 90 Day","Duty Days YTD",
        "Weekend Duty 30 Day","Weekend Duty 90 Day","Weekend Duty YTD",
        "Block Hours 30 Day","Block Hours 6 Month","Block Hours YTD",
        "RONs 30 Day","RONs 90 Day","RONs YTD",
        "OFF 30 Day","OFF 90 Day","OFF YTD",
        "PTO 30 Day","PTO 90 Day","PTO YTD",
        "Day Takeoff","Night Takeoff","Day Landing","Night Landing",
        "Holds 6 Month",
    ]
    cols_order = [c for c in desired_order if c in rep.columns] + [c for c in rep.columns if c not in desired_order and c != "Pilot"]
    rep = rep[cols_order]

    for c in rep.columns:
        if c != "Pilot" and pd.api.types.is_numeric_dtype(rep[c]):
            rep[c] = rep[c].fillna(0)

    rep = collapse_duplicate_columns(rep)

    numeric_cols = [c for c in rep.columns if c != "Pilot" and pd.api.types.is_numeric_dtype(rep[c])]
    if not numeric_cols:
        st.error("No numeric columns were detected after merging. Check the three exports’ formats.")
        st.stop()

    total_row = {c: rep[c].sum() for c in numeric_cols}; total_row["Pilot"] = "TOTAL"
    avg_row   = {c: rep[c].mean() for c in numeric_cols}; avg_row["Pilot"] = "AVERAGE"
    rep_out = pd.concat([rep, pd.DataFrame([total_row, avg_row])], ignore_index=True)

    bio, fname = round_and_export(rep_out)
    st.session_state.report_bytes = bio.getvalue()
    st.session_state.report_name = fname

    st.success("✅ Report built. Use the download button below.")

if st.session_state.report_bytes and st.session_state.report_name:
    st.download_button(
        "⬇️ Download Pilot Report (Excel)",
        data=st.session_state.report_bytes,
        file_name=st.session_state.report_name,
        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        use_container_width=True,
    )
else:
    st.info("Upload your three reports and click **Build Pilot Report**.")
