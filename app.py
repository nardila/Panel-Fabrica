import io
import re
import requests
import numpy as np
import pandas as pd
import streamlit as st
from datetime import datetime, date
from dateutil.relativedelta import relativedelta
from pytz import timezone

# ============================
# Utilidades de fechas (Buenos Aires)
# ============================
TZ = timezone("America/Argentina/Buenos_Aires")

def today_ba() -> date:
    return datetime.now(TZ).date()

def month_bounds(dt: date):
    start = dt.replace(day=1)
    end = (start + relativedelta(months=1)) - relativedelta(days=1)
    return start, end

def business_days_count(start: date, end: date) -> int:
    return int(np.busday_count(start, end + relativedelta(days=1)))

# ============================
# Lectura de archivos (Google Drive/Sheet por export)
# ============================
DRIVE_ID_REGEX = re.compile(r"(?:/d/|id=)([A-Za-z0-9_-]{10,})")
SHEETS_HOST_RE = re.compile(r"docs\.google\.com/spreadsheets/")

@st.cache_data(show_spinner=False, ttl=3600)
def fetch_excel_bytes(drive_url: str) -> bytes:
    if not drive_url:
        raise ValueError("Falta el enlace de Drive/Sheet.")
    m = DRIVE_ID_REGEX.search(drive_url)
    if not m:
        url = drive_url
    else:
        file_id = m.group(1)
        if SHEETS_HOST_RE.search(drive_url):
            url = f"https://docs.google.com/spreadsheets/d/{file_id}/export?format=xlsx"
        else:
            url = f"https://drive.google.com/uc?export=download&id={file_id}"
    resp = requests.get(url, timeout=90)
    resp.raise_for_status()
    return resp.content

@st.cache_data(show_spinner=False, ttl=3600)
def load_data_from_excel_bytes(xlsx_bytes: bytes):
    xls = pd.ExcelFile(io.BytesIO(xlsx_bytes))
    return {
        "mov": pd.read_excel(xls, sheet_name="MOVIMIENTO_STOCK-3934-1426"),
        "mat": pd.read_excel(xls, sheet_name="MATERIAL-4199-1426"),
        "rep": pd.read_excel(xls, sheet_name="REPORTE_DE_PEDIDOS-4166-1426"),
        "bom": pd.read_excel(xls, sheet_name="DETALLE_BOM-4200-1426"),
    }

# ============================
# Cálculos
# ============================

def compute_unit_labor_cost(df_material: pd.DataFrame, df_bom: pd.DataFrame) -> pd.DataFrame:
    mat = df_material.rename(columns={"MATE_CODIGO": "OPERACION", "MATE_CRM": "COSTO_OPERACION"})
    bom = df_bom.rename(columns={"MBOM_CODIGO": "SKU", "MATE_CODIGO": "OPERACION", "DEBO_CANTIDAD": "CANTIDAD_OP"})
    merged = bom.merge(mat[["OPERACION", "COSTO_OPERACION"]], on="OPERACION", how="left")
    merged["COSTO_PARCIAL"] = merged["CANTIDAD_OP"].fillna(0) * merged["COSTO_OPERACION"].fillna(0)
    return merged.groupby("SKU", as_index=False)["COSTO_PARCIAL"].sum().rename(columns={"COSTO_PARCIAL": "COSTO_MO_UNIT"})

def normalize_date_col(df: pd.DataFrame, col: str) -> pd.Series:
    s = pd.to_datetime(df[col], errors="coerce").dt.tz_localize(None)
    return s.dt.date

# ============================
# Configuración de la página
# ============================

st.set_page_config(page_title="DX Fábrica – KPI", page_icon="📊", layout="wide")

# ============================
# HEADER Y CONTROLES
# ============================

hoy = today_ba()
st.markdown(f"""
<div class="dx-header">
  <div>
    <h1 class="dx-title">DX Fábrica – Panel de KPI</h1>
    <div class="dx-sub">Datos hasta: <b>{hoy.strftime('%-d %b %Y')}</b></div>
  </div>
</div>
""", unsafe_allow_html=True)

ctrl1, ctrl2 = st.columns([0.75, 0.25])

with ctrl1:
    drive_url = st.text_input(
        "Enlace de Google Drive o Google Sheets",
        value=st.session_state.get("_drive_url", st.secrets.get("DRIVE_FILE_URL", "")),
        key="drive_url",
        help="Pegá un link de Google Sheet (se exporta a .xlsx) o un archivo .xlsx en Drive.",
    )

with ctrl2:
    if st.button("🔁 Actualizar", use_container_width=True):
        fetch_excel_bytes.clear()
        load_data_from_excel_bytes.clear()
        st.rerun()

st.session_state["_drive_url"] = drive_url

# ============================
# CARGA DE DATOS
# ============================

drive_url = st.session_state.get("_drive_url", st.secrets.get("DRIVE_FILE_URL", ""))
if drive_url:
    try:
        data = load_data_from_excel_bytes(fetch_excel_bytes(drive_url))
    except Exception as e:
        st.error(f"No se pudo leer el archivo/Sheet. {e}")
        st.stop()
else:
    st.info("Cargá un enlace de Google Drive/Sheet para ver KPIs.")
    st.stop()

st.success("Campo de enlace de Drive funcionando correctamente ✅")
