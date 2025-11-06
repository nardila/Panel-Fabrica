# app_estilo_moderno.py – DX Fábrica – Panel de KPI (UI estilo mock‑up)
# ----------------------------------------------------------------------
# Streamlit app con soporte Google Sheets (export y API) + estética moderna.
# Requisitos: ver requirements.txt (incluye openpyxl, gspread, google-auth).

import io
import re
import requests
import numpy as np
import pandas as pd
import streamlit as st
from datetime import datetime, date
from dateutil.relativedelta import relativedelta
from pytz import timezone

# ---------------------------------
# Utilidades de fechas (zona horaria Buenos Aires)
# ---------------------------------
TZ = timezone("America/Argentina/Buenos_Aires")

def today_ba():
    return datetime.now(TZ).date()

def month_bounds(dt: date):
    start = dt.replace(day=1)
    end = (start + relativedelta(months=1)) - relativedelta(days=1)
    return start, end


def business_days_count(start: date, end: date):
    return int(np.busday_count(start, end + relativedelta(days=1)))

# ---------------------------------
# Descarga/lectura desde Google Drive / Google Sheets
# ---------------------------------
DRIVE_ID_REGEX = re.compile(r"(?:/d/|id=)([A-Za-z0-9_-]{10,})")
SHEETS_HOST_RE = re.compile(r"docs\.google\.com/spreadsheets/")

@st.cache_data(show_spinner=False, ttl=3600)
def fetch_excel_bytes(drive_url: str) -> bytes:
    """Descarga un Excel desde Drive o exporta Google Sheet a .xlsx (sin API)."""
    if not drive_url:
        raise ValueError("Falta el enlace de Drive.")
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
    try:
        resp.raise_for_status()
    except requests.HTTPError as e:
        raise RuntimeError(f"HTTP {resp.status_code} al descargar: {url}") from e
    return resp.content

@st.cache_data(show_spinner=False, ttl=3600)
def load_data_from_excel_bytes(xlsx_bytes: bytes):
    xls = pd.ExcelFile(io.BytesIO(xlsx_bytes))
    h_mov = "MOVIMIENTO_STOCK-3934-1426"
    h_mat = "MATERIAL-4199-1426"
    h_rep = "REPORTE_DE_PEDIDOS-4166-1426"
    h_bom = "DETALLE_BOM-4200-1426"
    return {
        "mov": pd.read_excel(xls, sheet_name=h_mov),
        "mat": pd.read_excel(xls, sheet_name=h_mat),
        "rep": pd.read_excel(xls, sheet_name=h_rep),
        "bom": pd.read_excel(xls, sheet_name=h_bom),
    }

# ---------------------------------
# Lectura vía API de Google Sheets (opcional)
# ---------------------------------
try:
    import gspread
    from google.oauth2.service_account import Credentials
    GSHEETS_AVAILABLE = True
except Exception:
    GSHEETS_AVAILABLE = False

@st.cache_data(show_spinner=False, ttl=3600)
def load_data_from_gsheets_api(drive_url: str):
    if not GSHEETS_AVAILABLE:
        raise RuntimeError("gspread/google-auth no están instalados.")
    m = DRIVE_ID_REGEX.search(drive_url)
    if not m:
        raise ValueError("No se pudo extraer el ID de la Sheet del enlace.")
    spreadsheet_id = m.group(1)
    scopes = [
        "https://www.googleapis.com/auth/spreadsheets.readonly",
        "https://www.googleapis.com/auth/drive.readonly",
    ]
    sa_info = st.secrets.get("gcp_service_account", None)
    if not sa_info:
        raise RuntimeError("Falta st.secrets['gcp_service_account'] con el JSON de la service account.")
    creds = Credentials.from_service_account_info(sa_info, scopes=scopes)
    client = gspread.authorize(creds)
    sh = client.open_by_key(spreadsheet_id)

    def ws_to_df(name: str) -> pd.DataFrame:
        ws = sh.worksheet(name)
        values = ws.get_all_values()
        if not values:
            return pd.DataFrame()
        header, rows = values[0], values[1:]
        df = pd.DataFrame(rows, columns=header)
        for c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="ignore")
            if df[c].dtype == object:
                df[c] = pd.to_datetime(df[c], errors="ignore")
        return df

    return {
        "mov": ws_to_df("MOVIMIENTO_STOCK-3934-1426"),
        "mat": ws_to_df("MATERIAL-4199-1426"),
        "rep": ws_to_df("REPORTE_DE_PEDIDOS-4166-1426"),
        "bom": ws_to_df("DETALLE_BOM-4200-1426"),
    }

# ---------------------------------
# Cálculos
# ---------------------------------
@st.cache_data(show_spinner=False, ttl=3600)
def compute_unit_labor_cost(df_material: pd.DataFrame, df_bom: pd.DataFrame) -> pd.DataFrame:
    mat = df_material.rename(columns={"MATE_CODIGO": "OPERACION", "MATE_CRM": "COSTO_OPERACION"})
    bom = df_bom.rename(columns={"MBOM_CODIGO": "SKU", "MATE_CODIGO": "OPERACION", "DEBO_CANTIDAD": "CANTIDAD_OP"})
    merged = bom.merge(mat[["OPERACION", "COSTO_OPERACION"]], on="OPERACION", how="left")
    merged["COSTO_OPERACION"].fillna(0.0, inplace=True)
    merged["CANTIDAD_OP"].fillna(0.0, inplace=True)
    merged["COSTO_PARCIAL"] = merged["CANTIDAD_OP"] * merged["COSTO_OPERACION"]
    return merged.groupby("SKU", as_index=False)["COSTO_PARCIAL"].sum().rename(columns={"COSTO_PARCIAL": "COSTO_MO_UNIT"})


def normalize_date_col(df: pd.DataFrame, col: str) -> pd.Series:
    s = pd.to_datetime(df[col], errors="coerce").dt.tz_localize(None)
    return s.dt.date

@st.cache_data(show_spinner=False, ttl=1800)
def aggregate_current_month(df_mov: pd.DataFrame, df_rep: pd.DataFrame, unit_cost: pd.DataFrame, today: date):
    month_start, _ = month_bounds(today)
    # Producción
    mov = df_mov.rename(columns={
        "AUDI_FECHA_ALTA": "FECHA",
        "MATE_CODIGO": "SKU",
        "MOST_CANTIDAD": "CANTIDAD"
    }).copy()
    mov["FECHA"] = normalize_date_col(mov, "FECHA")
    mov_month = mov[(mov["FECHA"] >= month_start) & (mov["FECHA"] <= today)]
    prod_by_sku = mov_month.groupby("SKU", as_index=False)["CANTIDAD"].sum()
    prod_with_cost = prod_by_sku.merge(unit_cost, on="SKU", how="left").fillna({"COSTO_MO_UNIT": 0.0})
    prod_with_cost["COSTO_MO_TOTAL"] = prod_with_cost["CANTIDAD"] * prod_with_cost["COSTO_MO_UNIT"]
    total_fabricados = int(prod_with_cost["CANTIDAD"].sum()) if not prod_with_cost.empty else 0
    costo_mo_fabricado = float(prod_with_cost["COSTO_MO_TOTAL"].sum()) if not prod_with_cost.empty else 0.0
    # Ventas
    rep = df_rep.rename(columns={
        "AUDI_FECHA_ALTA": "FECHA",
        "SKU": "SKU",
        "CANTIDAD": "CANTIDAD",
        "PRECIO_FINAL_SIN_DTO": "PRECIO",
        "CRM": "COSTO_TOTAL",
        "MARGEN_3": "MARGEN"
    }).copy()
    rep["FECHA"] = normalize_date_col(rep, "FECHA")
    rep_month = rep[(rep["FECHA"] >= month_start) & (rep["FECHA"] <= today)]
    ventas_by_sku = rep_month.groupby("SKU", as_index=False)["CANTIDAD"].sum()
    ventas_with_cost = ventas_by_sku.merge(unit_cost, on="SKU", how="left").fillna({"COSTO_MO_UNIT": 0.0})
    ventas_with_cost["COSTO_MO_RECUP"] = ventas_with_cost["CANTIDAD"] * ventas_with_cost["COSTO_MO_UNIT"]
    total_vendidos = int(ventas_with_cost["CANTIDAD"].sum()) if not ventas_with_cost.empty else 0
    costo_mo_recuperado = float(ventas_with_cost["COSTO_MO_RECUP"].sum()) if not ventas_with_cost.empty else 0.0
    margen_bruto_actual = float(rep_month["MARGEN"].sum()) if not rep_month.empty else 0.0
    return {
        "prod_by_sku": prod_with_cost,
        "ventas_by_sku": ventas_with_cost,
        "total_fabricados": total_fabricados,
        "costo_mo_fabricado": costo_mo_fabricado,
        "total_vendidos": total_vendidos,
        "costo_mo_recuperado": costo_mo_recuperado,
        "margen_bruto_actual": margen_bruto_actual,
    }

# ---------------------------------
# UI – estilo mock‑up
# ---------------------------------
st.set_page_config(page_title="DX Fábrica – KPI", page_icon="📊", layout="wide")

# CSS para look & feel
st.markdown(
    """
    <style>
    html, body, [class*=css]{ font-family: 'Inter', system-ui, -apple-system, Segoe UI, Roboto, sans-serif; }
    .block-container{ padding-top: 1rem; max-width: 1320px; }
    h1,h2,h3{ font-weight: 700; }
    /* Cards / métricas */
    [data-testid="stMetric"]{ background:#f8fafc; border:1px solid #e5e7eb; border-radius:16px; padding:18px 20px; box-shadow:0 1px 3px rgba(0,0,0,.06); }
    [data-testid="stMetricValue"]{ font-size:2.2rem; color:#111827; }
    [data-testid="stMetricLabel"]{ color:#4b5563; font-size:.95rem; }
    /* Dataframes */
    [data-testid="stDataFrame"]{ border-radius:12px; overflow:hidden; box-shadow:0 2px 6px rgba(0,0,0,.04); }
    /* Progress custom */
    .dx-progress {height:12px; background:#e5e7eb; border-radius:8px; overflow:hidden;}
    .dx-progress > span{display:block; height:100%;}
    .dx-green{background:#22c55e;} .dx-amber{background:#f59e0b;} .dx-red{background:#ef4444;}
    </style>
    """,
    unsafe_allow_html=True,
)

# Header
st.title("📊 DX Fábrica – Panel de KPI")

# Config / inputs
c0, c1 = st.columns([2,1])
with c0:
    default_url = st.secrets.get("DRIVE_FILE_URL", "") if hasattr(st, "secrets") else ""
    drive_url = st.text_input(
        "Enlace de Google Drive al Excel/Sheet (se actualiza cada medianoche)",
        value=default_url,
        help="Pegá el enlace de Google Sheets o de un .xlsx en Drive"
    )
with c1:
    st.caption(":gray[Tip: guarda el enlace en **Secrets** como `DRIVE_FILE_URL`.]")

st.divider()

# Parámetros de objetivo
hoy = today_ba()
mes_inicio, mes_fin = month_bounds(hoy)
colA, colB, colC, colD = st.columns(4)
with colA:
    costo_mensual = st.number_input("Costo mensual total de la fábrica ($)", min_value=0.0, value=50_000_000.0, step=100_000.0, format="%0.2f")
with colB:
    dias_habiles_mes = st.number_input("Días hábiles del mes", min_value=1, value=int(business_days_count(mes_inicio, mes_fin)))
with colC:
    dias_habiles_transc = st.number_input("Días hábiles transcurridos (hasta hoy)", min_value=0, max_value=int(dias_habiles_mes), value=int(business_days_count(mes_inicio, hoy)))
with colD:
    st.metric("Fecha (BA)", hoy.strftime("%Y-%m-%d"))

objetivo_diario = (costo_mensual / dias_habiles_mes) if dias_habiles_mes else 0.0
objetivo_a_hoy = objetivo_diario * dias_habiles_transc

st.divider()

# Carga de datos
use_api = st.toggle("Usar API de Google Sheets (service account)", value=False, help="Requiere st.secrets['gcp_service_account'] y compartir la Sheet con ese mail.")

data = None
try:
    if drive_url:
        if use_api and SHEETS_HOST_RE.search(drive_url):
            data = load_data_from_gsheets_api(drive_url)
        else:
            data = load_data_from_excel_bytes(fetch_excel_bytes(drive_url))
except Exception as e:
    st.warning(f"No se pudo obtener el archivo/Sheet. Error: {e}")

if data is None:
    st.stop()

# Procesamiento
unit_cost = compute_unit_labor_cost(data["mat"], data["bom"])  # costo MO unitario por SKU
agg = aggregate_current_month(data["mov"], data["rep"], unit_cost, hoy)

# Helper progreso HTML con color
def render_progress(label: str, pct: float):
    pct = 0.0 if np.isnan(pct) else max(0.0, min(1.0, pct))
    color = 'dx-green' if pct >= 1 else ('dx-amber' if pct >= 0.8 else 'dx-red')
    st.write(label)
    st.markdown(f"""
    <div class='dx-progress'><span class='{color}' style='width:{pct*100:.1f}%' ></span></div>
    <div style='font-size:12px;color:#6b7280;margin-top:4px'>{pct*100:.1f}% del objetivo</div>
    """, unsafe_allow_html=True)

# ===== KPI Top =====
st.subheader("📈 KPI principales (mes a hoy)")
c1, c2, c3, c4 = st.columns(4)
with c1:
    st.metric("Muebles fabricados", f"{agg['total_fabricados']:,}".replace(",","."))
with c2:
    st.metric("Costo MO fabricado", f"$ {agg['costo_mo_fabricado']:,.2f}".replace(",","."))
with c3:
    st.metric("Muebles vendidos", f"{agg['total_vendidos']:,}".replace(",","."))
with c4:
    st.metric("Costo MO recuperado", f"$ {agg['costo_mo_recuperado']:,.2f}".replace(",","."))

st.divider()

# ===== Objetivo y balanzas =====
st.subheader("🎯 Objetivo y balanzas")

bal_fabricado = agg["costo_mo_fabricado"] - objetivo_a_hoy
bal_recuperado = agg["costo_mo_recuperado"] - objetivo_a_hoy
pct_fabricado = 0.0 if objetivo_a_hoy == 0 else agg["costo_mo_fabricado"] / objetivo_a_hoy
pct_recuperado = 0.0 if objetivo_a_hoy == 0 else agg["costo_mo_recuperado"] / objetivo_a_hoy

b1, b2, b3, b4 = st.columns(4)
with b1:
    st.metric("Costo mensual de fábrica", f"$ {costo_mensual:,.0f}".replace(",","."))
with b2:
    st.metric("Objetivo diario", f"$ {objetivo_diario:,.2f}".replace(",","."))
with b3:
    st.metric("Objetivo acumulado a hoy", f"$ {objetivo_a_hoy:,.2f}".replace(",","."))
with b4:
    st.metric("Margen bruto actual (mes)", f"$ {agg['margen_bruto_actual']:,.2f}".replace(",","."))

c5, c6 = st.columns(2)
with c5:
    st.metric("Balanza: Fabricado vs objetivo", f"$ {bal_fabricado:,.2f}".replace(",","."))
    render_progress("Avance vs objetivo (fabricado)", pct_fabricado)
with c6:
    st.metric("Balanza: Recuperado vs objetivo", f"$ {bal_recuperado:,.2f}".replace(",","."))
    render_progress("Avance vs objetivo (recuperado)", pct_recuperado)

st.divider()

# ===== Detalle por SKU =====
st.subheader("📦 Detalle por SKU (mes a hoy)")
left, right = st.columns(2)
with left:
    st.write(":blue[Producción]")
    dfp = agg["prod_by_sku"].copy()
    if not dfp.empty:
        dfp = dfp.sort_values("COSTO_MO_TOTAL", ascending=False)
        st.dataframe(
            dfp.rename(columns={
                "SKU": "SKU",
                "CANTIDAD": "Cantidad",
                "COSTO_MO_UNIT": "Costo MO unit.",
                "COSTO_MO_TOTAL": "Costo MO total"
            }),
            use_container_width=True
        )
        try:
            st.bar_chart(dfp.set_index("SKU")["COSTO_MO_TOTAL"].head(10))
        except Exception:
            pass
    else:
        st.caption("Sin producción registrada en el mes.")
with right:
    st.write(":green[Ventas]")
    dfv = agg["ventas_by_sku"].copy()
    if not dfv.empty:
        dfv = dfv.sort_values("COSTO_MO_RECUP", ascending=False)
        st.dataframe(
            dfv.rename(columns={
                "SKU": "SKU",
                "CANTIDAD": "Cantidad",
                "COSTO_MO_UNIT": "Costo MO unit.",
                "COSTO_MO_RECUP": "Costo MO recuperado"
            }),
            use_container_width=True
        )
        try:
            st.bar_chart(dfv.set_index("SKU")["COSTO_MO_RECUP"].head(10))
        except Exception:
            pass
    else:
        st.caption("Sin ventas registradas en el mes.")

st.divider()

with st.expander("🔧 Notas y supuestos"):
    st.markdown(
        """
        - **Dos modos de lectura**: exportación a `.xlsx` desde Google Sheets (sin API) o lectura directa por API (toggle).
        - **MO unitario** por SKU = `DETALLE_BOM` × `MATERIAL` (sumatoria por operación).
        - **MO fabricado**/ **recuperado** = Σ(cantidad × MO unitario) en el mes a hoy.
        - **Objetivo** = costo mensual / días hábiles × días hábiles transcurridos.
        - Barras de progreso: verde ≥100%, ámbar 80–99%, rojo <80% del objetivo.
        """
    )

st.success("Estilo moderno aplicado. Si querés, agrego filtros de mes/año y exportación CSV.")
