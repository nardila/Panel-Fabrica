# DX Fábrica – Panel de KPI (compacto y completo)
# ------------------------------------------------
# Streamlit app para monitorear producción, mano de obra y margen.
# Soporta datos desde Google Drive (Excel) o Google Sheets (export o API).
#
# Requisitos (requirements.txt):
#   streamlit
#   pandas
#   numpy
#   requests
#   python-dateutil
#   pytz
#   gspread
#   google-auth
#   openpyxl

import io
import re
import requests
import numpy as np
import pandas as pd
import streamlit as st
from datetime import datetime, date
from dateutil.relativedelta import relativedelta
from pytz import timezone
from textwrap import dedent

# ============================
# Utilidades de fechas (BA)
# ============================
TZ = timezone("America/Argentina/Buenos_Aires")

def today_ba() -> date:
    return datetime.now(TZ).date()

def month_bounds(dt: date):
    start = dt.replace(day=1)
    end = (start + relativedelta(months=1)) - relativedelta(days=1)
    return start, end


def business_days_count(start: date, end: date) -> int:
    # Lunes(0)–Viernes(4) como días hábiles
    return int(np.busday_count(start, end + relativedelta(days=1)))

# ============================
# Descarga/lectura de archivo
# ============================
DRIVE_ID_REGEX = re.compile(r"(?:/d/|id=)([A-Za-z0-9_-]{10,})")
SHEETS_HOST_RE = re.compile(r"docs\.google\.com/spreadsheets/")

@st.cache_data(show_spinner=False, ttl=3600)
def fetch_excel_bytes(drive_url: str) -> bytes:
    """Descarga un Excel desde Drive o exporta una Google Sheet a .xlsx (sin API)."""
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

# ---- Lectura vía API (opcional) ----
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
        # Tipificación leve
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

# ============================
# Cálculos
# ============================
@st.cache_data(show_spinner=False, ttl=3600)
def compute_unit_labor_cost(df_material: pd.DataFrame, df_bom: pd.DataFrame) -> pd.DataFrame:
    """Costo de mano de obra unitario por SKU = Σ(cant_op × costo_op)."""
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

    # Producción (MOVIMIENTO)
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

    # Ventas (REPORTE)
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

# ============================
# UI – compacto (sin scroll)
# ============================
st.set_page_config(page_title="DX Fábrica – KPI", page_icon="📊", layout="wide")

# CSS base del rediseño + compactado
st.markdown(
    """
    <style>
    :root{
      --bg:#0b1020; --card:#ffffff; --muted:#6b7280; --ink:#111827; --border:#e5e7eb;
      --green:#22c55e; --amber:#f59e0b; --red:#ef4444;
    }
    .block-container { padding-top: 0.5rem; padding-bottom: 0; max-width: 1280px; }
    .dx-header{ background:linear-gradient(90deg, var(--bg), #11193a); color:#fff; padding:16px 20px; border-radius:0 0 16px 16px; margin-bottom:12px; }
    .dx-header h1{ margin:0; font-weight:800; }
    .dx-sub{ opacity:.85; font-size:13px; margin-top:6px }

    .dx-grid{ display:grid; grid-template-columns: repeat(4,1fr); gap:12px; margin-top:4px }
    .dx-card{ background:var(--card); border:1px solid var(--border); border-radius:14px; padding:12px 14px; box-shadow:0 2px 8px rgba(0,0,0,.05); }
    .dx-label{ color:var(--muted); font-size:13px; margin-bottom:6px; display:flex; gap:6px; align-items:center }
    .dx-val{ color:var(--ink); font-size:26px; font-weight:700; line-height:1.15; margin:0 }
    .dx-delta{ display:inline-block; padding:2px 8px; border-radius:999px; font-size:12px; margin-top:6px }
    .dx-delta.pos{ background:rgba(34,197,94,.12); color:var(--green); border:1px solid rgba(34,197,94,.35) }
    .dx-delta.neg{ background:rgba(239,68,68,.12); color:var(--red); border:1px solid rgba(239,68,68,.35) }

    .dx-section{ background:#fff; border:1px solid var(--border); border-radius:14px; padding:12px; box-shadow:0 1px 6px rgba(0,0,0,.05); margin:6px 0 10px 0 }
    .dx-title{ font-weight:800; font-size:16px; display:flex; gap:8px; align-items:center; margin:0 0 8px 0 }

    .dx-progress{ height:10px; background:#eef2ff; border-radius:10px; overflow:hidden }
    .dx-progress > span{ display:block; height:100% }
    .dx-ok{ background: var(--green) } .dx-warn{ background: var(--amber) } .dx-bad{ background: var(--red) }

    [data-testid="stDataFrame"]{ border-radius:12px; overflow:hidden; box-shadow:0 2px 6px rgba(0,0,0,.04) }
    </style>
    """,
    unsafe_allow_html=True,
)


# ===== Scroll Snap Toggle =====
enable_snap = st.toggle("Modo páginas al hacer scroll (scroll-snap)", value=True, help="Activa scroll por secciones: Configuración · Indicadores · Detalle SKU")
if enable_snap:
    # Nuevo enfoque: el contenedor de scroll es el body, no el block-container.
    st.markdown("""
    <style>
      html, body { height: 100%; scroll-snap-type: y mandatory; scroll-behavior: smooth; }
      /* Asegurar que el contenedor interno no intercepte el scroll */
      main > div.block-container { height: auto !important; overflow: visible !important; }
      /* Secciones a pantalla completa sin márgenes extra */
      .snap-section { min-height: 100vh; scroll-snap-align: start; margin: 0; padding: 8px 0 0; display: flex; flex-direction: column; }
      /* Elimina espacios verticales extra de nuestras tarjetas de sección */
      .dx-section { margin: 0 !important; }
    </style>
    """, unsafe_allow_html=True)
# Header visual
hoy = today_ba()
header_html = f"""
<div class=\"dx-header\">
  <h1>DX Fábrica — Panel de KPI</h1>
  <div class=\"dx-sub\">Datos del mes en curso · Última actualización: {hoy.strftime('%Y-%m-%d')}</div>
</div>
"""
if enable_snap: st.markdown('<section class="snap-section" id="config">', unsafe_allow_html=True)
st.markdown(header_html, unsafe_allow_html=True)

# ===== Configuración e inputs superiores =====
col0, colActions = st.columns([1.8, .2])
with col0:
    default_url = st.secrets.get("DRIVE_FILE_URL", "") if hasattr(st, "secrets") else ""
    drive_url = st.text_input(
        "Enlace de Google Drive al Excel/Sheet (se actualiza cada medianoche)",
        value=default_url,
        help="Pegá el enlace de Google Sheets o del archivo .xlsx en Drive."
    )
with colActions:
    pass

mes_inicio, mes_fin = month_bounds(hoy)
colA, colB, colC, colD = st.columns(4)
with colA:
    costo_mensual = st.number_input("Costo mensual total de la fábrica ($)", min_value=0.0, value=50_000_000.0, step=100_000.0, format="%0.2f")
with colB:
    dias_habiles_mes = st.number_input("Días hábiles del mes", min_value=1, value=int(business_days_count(mes_inicio, mes_fin)))
with colC:
    dias_habiles_transc = st.number_input("Días hábiles transcurridos (hasta hoy)", min_value=0, max_value=int(dias_habiles_mes), value=int(business_days_count(mes_inicio, hoy)))
with colD:
    use_api = st.toggle("Usar API de Google Sheets (service account)", value=False, help="Requiere st.secrets['gcp_service_account'] y compartir la Sheet con ese mail.")

objetivo_diario = (costo_mensual / dias_habiles_mes) if dias_habiles_mes else 0.0
objetivo_a_hoy = objetivo_diario * dias_habiles_transc


# Cierre sección 1 (configuración)
if enable_snap: st.markdown('</section>', unsafe_allow_html=True)
# ===== Carga de datos =====
data = None
if drive_url:
    try:
        if use_api and SHEETS_HOST_RE.search(drive_url):
            data = load_data_from_gsheets_api(drive_url)
        else:
            data = load_data_from_excel_bytes(fetch_excel_bytes(drive_url))
    except Exception as e:
        st.warning(f"No se pudo obtener el archivo/Sheet. Error: {e}")

if data is None:
    st.stop()

# ===== Procesamiento =====
unit_cost = compute_unit_labor_cost(data["mat"], data["bom"])  # costo MO unitario por SKU
agg = aggregate_current_month(data["mov"], data["rep"], unit_cost, hoy)

# Helpers UI (cards y progreso estilo mock‑up)

def kpi_card(label, value, icon="📦", delta=None, good=True):
    delta_html = ""
    if delta is not None:
        cls = "pos" if good else "neg"
        sign = "+" if (good and delta >= 0) or (not good and delta < 0) else ""
        delta_html = f'<div class="dx-delta {cls}">{sign}{delta:,.1f}%</div>'.replace(",", ".")
    return dedent(f"""
    <div class=\"dx-card\">
      <div class=\"dx-label\">{icon} {label}</div>
      <div class=\"dx-val\">{value}</div>
      {delta_html}
    </div>
    """).strip()


def progress_block(title, pct):
    try:
        pct = 0 if pct is None else max(0, min(1, float(pct)))
    except Exception:
        pct = 0
    cls = 'dx-ok' if pct >= 1 else ('dx-warn' if pct >= .8 else 'dx-bad')
    return dedent(f"""
    <div>
      <div class='dx-label' style='margin-bottom:6px;'>{title}</div>
      <div class='dx-progress'><span class='{cls}' style='width:{pct*100:.1f}%'></span></div>
      <div style='color:#6b7280;font-size:12px;margin-top:4px'>{pct*100:.1f}% del objetivo</div>
    </div>
    """).strip()

# ====== SECCIÓN 2: INDICADORES ======
if enable_snap: st.markdown('<section class="snap-section" id="indicadores">', unsafe_allow_html=True)
# ===== KPIs principales (fila 1) =====
kpi_html = (
    '<div class="dx-grid">'
    + kpi_card("Muebles fabricados", f"{agg['total_fabricados']:,}".replace(",","."), icon="🪑")
    + kpi_card("Costo MO fabricado", f"$ {agg['costo_mo_fabricado']:,.2f}".replace(",","."), icon="🛠️")
    + kpi_card("Muebles vendidos", f"{agg['total_vendidos']:,}".replace(",","."), icon="🧾")
    + kpi_card("Costo MO recuperado", f"$ {agg['costo_mo_recuperado']:,.2f}".replace(",","."), icon="💵")
    + '</div>'
)
st.markdown(kpi_html, unsafe_allow_html=True)

# ===== Objetivo y balanzas (fila 2, compacto) =====
st.markdown('<div class="dx-section"><div class="dx-title">🎯 Objetivo y balanzas</div>', unsafe_allow_html=True)

bal_fabricado  = agg["costo_mo_fabricado"]  - objetivo_a_hoy
bal_recuperado = agg["costo_mo_recuperado"] - objetivo_a_hoy
pct_fabricado  = (agg["costo_mo_fabricado"]  / objetivo_a_hoy) if objetivo_a_hoy else 0
pct_recuperado = (agg["costo_mo_recuperado"] / objetivo_a_hoy) if objetivo_a_hoy else 0

b1, b2, b3, b4, b5 = st.columns([1,1,1,1,1])
with b1: st.markdown(kpi_card("Costo mensual", f"$ {costo_mensual:,.0f}".replace(",","."), icon="🏭"), unsafe_allow_html=True)
with b2: st.markdown(kpi_card("Objetivo diario", f"$ {objetivo_diario:,.2f}".replace(",","."), icon="📅"), unsafe_allow_html=True)
with b3: st.markdown(kpi_card("Objetivo a hoy", f"$ {objetivo_a_hoy:,.2f}".replace(",","."), icon="📈"), unsafe_allow_html=True)
with b4: st.markdown(kpi_card("Balanza Fabr.", f"$ {bal_fabricado:,.0f}".replace(",","."), icon="⚙️", good=(bal_fabricado>=0)), unsafe_allow_html=True)
with b5: st.markdown(kpi_card("Balanza Recup.", f"$ {bal_recuperado:,.0f}".replace(",","."), icon="💵", good=(bal_recuperado>=0)), unsafe_allow_html=True)

c5, c6, c7 = st.columns([1.6,1.6,1])
with c5:
    st.markdown(progress_block("Avance vs objetivo (fabricado)", pct_fabricado), unsafe_allow_html=True)
with c6:
    st.markdown(progress_block("Avance vs objetivo (recuperado)", pct_recuperado), unsafe_allow_html=True)
with c7:
    st.markdown(kpi_card("Margen bruto", f"$ {agg['margen_bruto_actual']:,.0f}".replace(",","."), icon="💹"), unsafe_allow_html=True)

st.markdown('</div>', unsafe_allow_html=True)

# Cierre sección 2 (indicadores)
if enable_snap: st.markdown('</section>', unsafe_allow_html=True)

# ====== SECCIÓN 3: DETALLE SKU ======
if enable_snap: st.markdown('<section class="snap-section" id="detalle">', unsafe_allow_html=True)
# ===== Detalle por SKU (fila 3) =====
st.markdown('<div class="dx-section"><div class="dx-title">📦 Detalle por SKU (mes a hoy)</div>', unsafe_allow_html=True)
left, right = st.columns(2)
with left:
    st.caption("Producción por SKU")
    dfp = agg["prod_by_sku"].copy()
    if not dfp.empty:
        dfp = dfp.sort_values("COSTO_MO_TOTAL", ascending=False)
        st.dataframe(
            dfp.rename(columns={"SKU":"SKU","CANTIDAD":"Cantidad","COSTO_MO_UNIT":"Costo MO unit.","COSTO_MO_TOTAL":"Costo MO total"}),
            use_container_width=True,
            height=260
        )
        try: st.bar_chart(dfp.set_index("SKU")["COSTO_MO_TOTAL"].head(8), use_container_width=True, height=120)
        except Exception:
            pass
    else:
        st.caption("Sin producción registrada en el mes.")
with right:
    st.caption("Ventas por SKU")
    dfv = agg["ventas_by_sku"].copy()
    if not dfv.empty:
        dfv = dfv.sort_values("COSTO_MO_RECUP", ascending=False)
        st.dataframe(
            dfv.rename(columns={"SKU":"SKU","CANTIDAD":"Cantidad","COSTO_MO_UNIT":"Costo MO unit.","COSTO_MO_RECUP":"Costo MO recuperado"}),
            use_container_width=True,
            height=260
        )
        try: st.bar_chart(dfv.set_index("SKU")["COSTO_MO_RECUP"].head(8), use_container_width=True, height=120)
        except Exception:
            pass
    else:
        st.caption("Sin ventas registradas en el mes.")

st.markdown('</div>', unsafe_allow_html=True)

# Cierre sección 3 (detalle)
if enable_snap: st.markdown('</section>', unsafe_allow_html=True)

# ===== Notas =====
with st.expander("🔧 Notas y supuestos"):
    st.markdown(
        """
        - **Dos modos de lectura**: exportación a `.xlsx` desde Google Sheets (sin API) o lectura directa por API (toggle).
        - **MO unitario** por SKU = `DETALLE_BOM` × `MATERIAL` (sumatoria por operación).
        - **MO fabricado/recuperado** = Σ(cantidad × MO unitario) en el mes a hoy.
        - **Objetivo** = costo mensual / días hábiles × días hábiles transcurridos.
        - Barras de progreso: verde ≥100%, ámbar 80–99%, rojo <80% del objetivo.
        """
    )
