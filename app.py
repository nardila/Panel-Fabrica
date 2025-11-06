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

# CSS para look & feel (versión mock‑up)
st.markdown(
    """
    <style>
    :root{
      --bg:#0b1020;          /* fondo de header */
      --card:#ffffff;        /* fondo de tarjetas */
      --muted:#6b7280;       /* gris texto secundario */
      --ink:#111827;         /* gris oscuro texto */
      --blue:#2563eb;        /* acento */
      --green:#16a34a;       /* ok */
      --amber:#f59e0b;       /* warn */
      --red:#ef4444;         /* alert */
      --border:#e5e7eb;
    }

    html, body, [class*="css"]{ font-family:'Inter', system-ui, -apple-system, Segoe UI, Roboto, sans-serif; }
    .block-container{ padding-top:0; max-width:1200px; }

    /* ===== Top banner ===== */
    .dx-header{
      background:linear-gradient(90deg, var(--bg), #11193a);
      color:white; padding:18px 22px; border-radius:0 0 18px 18px;
      box-shadow:0 4px 16px rgba(0,0,0,.25); margin-bottom:20px;
    }
    .dx-header h1{margin:0;font-weight:800;letter-spacing:.2px}
    .dx-sub{opacity:.85;font-size:13px;margin-top:6px}

    /* ===== Card (KPI) ===== */
    .dx-grid{display:grid;grid-template-columns:repeat(4,1fr);gap:16px;margin-top:8px}
    @media (max-width:1200px){ .dx-grid{grid-template-columns:repeat(2,1fr);} }
    @media (max-width:680px){ .dx-grid{grid-template-columns:1fr;} }

    .dx-card{
      background:var(--card); border:1px solid var(--border); border-radius:16px;
      padding:18px; box-shadow:0 2px 10px rgba(0,0,0,.06);
    }
    .dx-label{color:var(--muted);font-size:13px;margin-bottom:6px;display:flex;gap:6px;align-items:center}
    .dx-val{color:var(--ink);font-size:32px;font-weight:700;line-height:1.15;margin:0}
    .dx-delta{display:inline-block;padding:3px 8px;border-radius:999px;font-size:12px;margin-top:6px}
    .dx-delta.pos{background:rgba(22,163,74,.12); color:var(--green); border:1px solid rgba(22,163,74,.35)}
    .dx-delta.neg{background:rgba(239,68,68,.12); color:var(--red); border:1px solid rgba(239,68,68,.35)}

    /* ===== Section card ===== */
    .dx-section{background:#fff;border:1px solid var(--border);border-radius:16px;padding:18px;margin:10px 0 18px 0;box-shadow:0 1px 6px rgba(0,0,0,.05)}
    .dx-title{font-weight:800;font-size:18px;display:flex;gap:8px;align-items:center;margin:0 0 12px 0}

    /* ===== Progress pill ===== */
    .dx-progress{height:12px;background:#eef2ff;border-radius:10px;overflow:hidden}
    .dx-progress > span{display:block;height:100%}
    .dx-ok{background:#22c55e;} .dx-warn{background:#f59e0b;} .dx-bad{background:#ef4444;}

    /* DataFrame contenedor */
    [data-testid="stDataFrame"]{border-radius:14px;overflow:hidden;box-shadow:0 2px 8px rgba(0,0,0,.06)}
    </style>
    """,
    unsafe_allow_html=True,
)

# Header visual
st.markdown(f"""
<div class="dx-header">
  <h1>DX Fábrica — Panel de KPI</h1>
  <div class="dx-sub">Datos del mes en curso · Última actualización: {today_ba().strftime('%Y-%m-%d')}</div>
</div>
""", unsafe_allow_html=True)

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

# Helpers UI (cards y progreso estilo mock‑up)

def kpi_card(label, value, icon="📦", delta=None, good=True):
    delta_html = ""
    if delta is not None:
        cls = "pos" if good else "neg"
        sign = "+" if (good and delta >= 0) or (not good and delta < 0) else ""
        delta_html = f'<div class="dx-delta {cls}">{sign}{delta:,.1f}%</div>'.replace(",", ".")
    return f"""
    <div class="dx-card">
      <div class="dx-label">{icon} {label}</div>
      <div class="dx-val">{value}</div>
      {delta_html}
    </div>
    """

def progress_block(title, pct):
    try:
        pct = 0 if pct is None else max(0, min(1, float(pct)))
    except Exception:
        pct = 0
    cls = 'dx-ok' if pct >= 1 else ('dx-warn' if pct >= .8 else 'dx-bad')
    return f"""
    <div style='margin-top:4px'>
      <div class='dx-label' style='margin-bottom:6px;'>{title}</div>
      <div class='dx-progress'><span class='{cls}' style='width:{pct*100:.1f}%'></span></div>
      <div style='color:#6b7280;font-size:12px;margin-top:4px'>{pct*100:.1f}% del objetivo</div>
    </div>
    """ 
def render_progress(label: str, pct: float):
    pct = 0.0 if np.isnan(pct) else max(0.0, min(1.0, pct))
    color = 'dx-green' if pct >= 1 else ('dx-amber' if pct >= 0.8 else 'dx-red')
    st.write(label)
    st.markdown(f"""
    <div class='dx-progress'><span class='{color}' style='width:{pct*100:.1f}%' ></span></div>
    <div style='font-size:12px;color:#6b7280;margin-top:4px'>{pct*100:.1f}% del objetivo</div>
    """, unsafe_allow_html=True)

# ===== KPI Top =====
# ——— KPIs (similares al mock‑up)
kpi_html = """
<div class="dx-grid">
  {c1}
  {c2}
  {c3}
  {c4}
</div>
""".format(
  c1 = kpi_card("Muebles fabricados", f"{agg['total_fabricados']:,}".replace(",","."), icon="🪑"),
  c2 = kpi_card("Costo MO fabricado", f"$ {agg['costo_mo_fabricado']:,.2f}".replace(",","."), icon="🛠️"),
  c3 = kpi_card("Muebles vendidos", f"{agg['total_vendidos']:,}".replace(",","."), icon="🧾"),
  c4 = kpi_card("Costo MO recuperado", f"$ {agg['costo_mo_recuperado']:,.2f}".replace(",","."), icon="💵")
)
st.markdown(kpi_html, unsafe_allow_html=True)

st.divider()

# ===== Objetivo y balanzas =====
# ——— Objetivo y balanzas
st.markdown('<div class="dx-section"><div class="dx-title">🎯 Objetivo y balanzas</div>', unsafe_allow_html=True)

b1, b2, b3, b4 = st.columns(4)
with b1: st.markdown(kpi_card("Costo mensual de fábrica", f"$ {costo_mensual:,.0f}".replace(",","."), icon="🏭"), unsafe_allow_html=True)
with b2: st.markdown(kpi_card("Objetivo diario", f"$ {objetivo_diario:,.2f}".replace(",","."), icon="📅"), unsafe_allow_html=True)
with b3: st.markdown(kpi_card("Objetivo acumulado a hoy", f"$ {objetivo_a_hoy:,.2f}".replace(",","."), icon="📈"), unsafe_allow_html=True)
with b4: st.markdown(kpi_card("Margen bruto (mes)", f"$ {agg['margen_bruto_actual']:,.2f}".replace(",","."), icon="💹"), unsafe_allow_html=True)

bal_fabricado  = agg["costo_mo_fabricado"]  - objetivo_a_hoy
bal_recuperado = agg["costo_mo_recuperado"] - objetivo_a_hoy
pct_fabricado  = (agg["costo_mo_fabricado"]  / objetivo_a_hoy) if objetivo_a_hoy else 0
pct_recuperado = (agg["costo_mo_recuperado"] / objetivo_a_hoy) if objetivo_a_hoy else 0

c5, c6 = st.columns(2)
with c5:
    st.markdown(kpi_card("Balanza: Fabricado vs objetivo", f"$ {bal_fabricado:,.2f}".replace(",","."), icon="⚙️", good=(bal_fabricado>=0)), unsafe_allow_html=True)
    st.markdown(progress_block("Avance vs objetivo (fabricado)", pct_fabricado), unsafe_allow_html=True)
with c6:
    st.markdown(kpi_card("Balanza: Recuperado vs objetivo", f"$ {bal_recuperado:,.2f}".replace(",","."), icon="💵", good=(bal_recuperado>=0)), unsafe_allow_html=True)
    st.markdown(progress_block("Avance vs objetivo (recuperado)", pct_recuperado), unsafe_allow_html=True)

st.markdown('</div>', unsafe_allow_html=True)

st.divider()

# ===== Detalle por SKU =====
# ——— Detalle por SKU (encapsulado en sección)
st.markdown('<div class="dx-section"><div class="dx-title">📦 Detalle por SKU (mes a hoy)</div>', unsafe_allow_html=True)
left, right = st.columns(2)
with left:
    st.caption("Producción")
    dfp = agg["prod_by_sku"].copy()
    if not dfp.empty:
        dfp = dfp.sort_values("COSTO_MO_TOTAL", ascending=False)
        st.dataframe(
            dfp.rename(columns={"SKU":"SKU","CANTIDAD":"Cantidad","COSTO_MO_UNIT":"Costo MO unit.","COSTO_MO_TOTAL":"Costo MO total"}),
            use_container_width=True
        )
        try: st.bar_chart(dfp.set_index("SKU")["COSTO_MO_TOTAL"].head(10))
        except: pass
    else:
        st.caption("Sin producción registrada en el mes.")
with right:
    st.caption("Ventas")
    dfv = agg["ventas_by_sku"].copy()
    if not dfv.empty:
        dfv = dfv.sort_values("COSTO_MO_RECUP", ascending=False)
        st.dataframe(
            dfv.rename(columns={"SKU":"SKU","CANTIDAD":"Cantidad","COSTO_MO_UNIT":"Costo MO unit.","COSTO_MO_RECUP":"Costo MO recuperado"}),
            use_container_width=True
        )
        try: st.bar_chart(dfv.set_index("SKU")["COSTO_MO_RECUP"].head(10))
        except: pass
    else:
        st.caption("Sin ventas registradas en el mes.")

st.markdown('</div>', unsafe_allow_html=True)

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
