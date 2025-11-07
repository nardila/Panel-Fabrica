# DX Fábrica – Panel de KPI (réplica visual del mock‑up)
# -----------------------------------------------------
# • Layout en 3 bloques dentro de un contenedor con borde
# • Header fino con fecha a la izquierda y control de datos a la derecha
# • KPIs con barras de progreso sutiles
# • Objetivo y balanzas (incluye tarjeta grande de margen)
# • Detalle por SKU (tablas + mini gráficas)
# • Botón de refresco 🔁 y manejo de caché

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
    """Costo MO unitario por SKU = Σ(cant_op × costo_op)."""
    mat = df_material.rename(columns={"MATE_CODIGO": "OPERACION", "MATE_CRM": "COSTO_OPERACION"})
    bom = df_bom.rename(columns={"MBOM_CODIGO": "SKU", "MATE_CODIGO": "OPERACION", "DEBO_CANTIDAD": "CANTIDAD_OP"})
    merged = bom.merge(mat[["OPERACION", "COSTO_OPERACION"]], on="OPERACION", how="left")
    merged["COSTO_PARCIAL"] = merged["CANTIDAD_OP"].fillna(0) * merged["COSTO_OPERACION"].fillna(0)
    return merged.groupby("SKU", as_index=False)["COSTO_PARCIAL"].sum().rename(columns={"COSTO_PARCIAL": "COSTO_MO_UNIT"})


def normalize_date_col(df: pd.DataFrame, col: str) -> pd.Series:
    s = pd.to_datetime(df[col], errors="coerce").dt.tz_localize(None)
    return s.dt.date

@st.cache_data(show_spinner=False, ttl=1800)
def aggregate_current_month(df_mov: pd.DataFrame, df_rep: pd.DataFrame, unit_cost: pd.DataFrame, today: date):
    month_start, _ = month_bounds(today)

    # Producción (mov)
    mov = df_mov.rename(columns={"AUDI_FECHA_ALTA": "FECHA", "MATE_CODIGO": "SKU", "MOST_CANTIDAD": "CANTIDAD"}).copy()
    mov["FECHA"] = normalize_date_col(mov, "FECHA")
    mov_month = mov[(mov["FECHA"] >= month_start) & (mov["FECHA"] <= today)]
    prod = mov_month.groupby("SKU", as_index=False)["CANTIDAD"].sum()
    prod = prod.merge(unit_cost, on="SKU", how="left").fillna({"COSTO_MO_UNIT": 0.0})
    prod["COSTO_MO_TOTAL"] = prod["CANTIDAD"] * prod["COSTO_MO_UNIT"]

    total_fabricados = int(prod["CANTIDAD"].sum()) if not prod.empty else 0
    costo_mo_fabricado = float(prod["COSTO_MO_TOTAL"].sum()) if not prod.empty else 0.0

    # Ventas (rep)
    rep = df_rep.rename(columns={"AUDI_FECHA_ALTA": "FECHA", "SKU": "SKU", "CANTIDAD": "CANTIDAD", "MARGEN_3": "MARGEN"}).copy()
    rep["FECHA"] = normalize_date_col(rep, "FECHA")
    rep_month = rep[(rep["FECHA"] >= month_start) & (rep["FECHA"] <= today)]

    ventas = rep_month.groupby("SKU", as_index=False)["CANTIDAD"].sum()
    ventas = ventas.merge(unit_cost, on="SKU", how="left").fillna({"COSTO_MO_UNIT": 0.0})
    ventas["COSTO_MO_RECUP"] = ventas["CANTIDAD"] * ventas["COSTO_MO_UNIT"]

    total_vendidos = int(ventas["CANTIDAD"].sum()) if not ventas.empty else 0
    costo_mo_recuperado = float(ventas["COSTO_MO_RECUP"].sum()) if not ventas.empty else 0.0

    margen_bruto_actual = float(rep_month["MARGEN"].sum()) if not rep_month.empty else 0.0

    return {
        "prod": prod,
        "ventas": ventas,
        "total_fabricados": total_fabricados,
        "costo_mo_fabricado": costo_mo_fabricado,
        "total_vendidos": total_vendidos,
        "costo_mo_recuperado": costo_mo_recuperado,
        "margen_bruto_actual": margen_bruto_actual,
    }

# ============================
# UI y estilos
# ============================
st.set_page_config(page_title="DX Fábrica – KPI", page_icon="📊", layout="wide")

# --- Estilos globales (réplica mock‑up) ---
st.markdown(
    """
    <style>
      html, body { background:#f7f8fb; }
      .block-container{ max-width: 1200px; padding-top: 0.5rem; }

      /* Contenedor principal */
      .dx-wrap{ background:#fff; border:1px solid #e7e9ef; border-radius:12px; padding:14px 16px 18px; box-shadow: 0 2px 10px rgba(0,0,0,.05); }

      /* Header */
      .dx-header{ display:flex; align-items:center; justify-content:space-between; margin-bottom:10px; }
      .dx-title{ font-size:28px; font-weight:800; margin:0; color:#0f172a; }
      .dx-sub{ font-size:13px; color:#6b7280; margin-top:4px; }

      /* Botón superior derecho */
      .dx-btn{ font-size:12px; padding:8px 10px; border:1px solid #e5e7eb; border-radius:10px; background:#fff; }

      /* Grillas y tarjetas */
      .dx-grid{ display:grid; grid-template-columns: repeat(4, 1fr); gap:12px; }
      .dx-card{ background:#fff; border:1px solid #e7e9ef; border-radius:12px; padding:12px 14px; box-shadow: 0 2px 6px rgba(0,0,0,.05); }
      .dx-label{ color:#6b7280; font-size:13px; margin-bottom:6px; display:flex; gap:6px; align-items:center }
      .dx-val{ color:#0f172a; font-weight:800; font-size:26px; line-height:1.1; }
      .dx-muted{ color:#6b7280; font-size:12px; }

      /* Progreso sutil */
      .dx-progress{ height:9px; background:#eef2ff; border-radius:10px; overflow:hidden; margin-top:8px }
      .dx-progress span{ display:block; height:100%; background:#22c55e }
      .dx-progress.bad span{ background:#ef4444 }

      /* Secciones con título */
      .dx-section{ margin-top:12px; }
      .dx-sec-title{ font-size:16px; font-weight:800; color:#0f172a; margin:0 0 8px 0 }

      /* Tarjeta margen grande */
      .dx-card-green{ background:#22c55e; color:#fff; border:0; }
      .dx-card-green .dx-label{ color:#e0ffe9 }

      /* Tablas redondeadas */
      [data-testid="stDataFrame"]{ border-radius:12px; overflow:hidden; box-shadow:0 2px 6px rgba(0,0,0,.04); }
    </style>
    """,
    unsafe_allow_html=True,
)

# ===== HEADER =====
hoy = today_ba()
col_left, col_right = st.columns([1,0.38])
with col_left:
    st.markdown(
        f"""
        <div class='dx-header'>
          <div>
            <h1 class='dx-title'>DX Fábrica – Panel de KPI</h1>
            <div class='dx-sub'>Datos hasta: <b>{hoy.strftime('%-d %b %Y')}</b></div>
          </div>
        </div>
        """,
        unsafe_allow_html=True,
    )
with col_right:
    # Bloque de configuración accesible rápidamente
    with st.expander("Enlace de Drive / modo API", expanded=False):
        default_url = st.secrets.get("DRIVE_FILE_URL", "")
        drive_url = st.text_input("URL de Google Drive o Google Sheets", value=default_url, key="drive_url_input")
        st.session_state["_drive_url"] = drive_url
        st.caption("Pegá un link de Google Sheet (se exporta a .xlsx automáticamente) o un archivo .xlsx en Drive.")
    st.write("")

# Contenedor principal
st.markdown('<div class="dx-wrap">', unsafe_allow_html=True)

# ===== PARÁMETROS DEL MES =====
mes_inicio, mes_fin = month_bounds(hoy)
colA, colB, colC, colD = st.columns([1.2,1,1,1])
with colA:
    costo_mensual = st.number_input("Costo mensual total de fábrica ($)", value=50_000_000.0, step=100_000.0, format="%0.2f")
with colB:
    dias_habiles_mes = st.number_input("Días hábiles del mes", value=int(business_days_count(mes_inicio, mes_fin)))
with colC:
    dias_trans = st.number_input("Días hábiles transcurridos (hoy)", value=int(business_days_count(mes_inicio, hoy)))
with colD:
    if st.button("🔁 Actualizar", use_container_width=True):
        fetch_excel_bytes.clear()
        load_data_from_excel_bytes.clear()
        aggregate_current_month.clear()
        st.rerun()

objetivo_diario = (costo_mensual / dias_habiles_mes) if dias_habiles_mes else 0.0
objetivo_a_hoy = objetivo_diario * dias_trans

# ===== CARGA DE DATOS =====
drive_url = st.session_state.get("_drive_url", st.secrets.get("DRIVE_FILE_URL", ""))
data = None
if drive_url:
    try:
        data = load_data_from_excel_bytes(fetch_excel_bytes(drive_url))
    except Exception as e:
        st.error(f"No se pudo leer el archivo/Sheet. {e}")
        st.stop()
else:
    st.info("Carga un enlace de Google Drive/Sheet para ver KPIs.")
    st.stop()

unit_cost = compute_unit_labor_cost(data["mat"], data["bom"])  # costo MO unitario por SKU
agg = aggregate_current_month(data["mov"], data["rep"], unit_cost, hoy)

# Helpers
fmt_mon = lambda x: (f"$ {x:,.0f}".replace(",", "."))
fmt_int = lambda x: (f"{int(x):,}".replace(",", "."))

# ===== FILA 1: KPI PRINCIPALES =====
st.markdown('<div class="dx-grid">', unsafe_allow_html=True)
# KPI 1
st.markdown(
    f"""
    <div class='dx-card'>
      <div class='dx-label'>🪑 Muebles fabricados <span class='dx-muted'>(mes a hoy)</span></div>
      <div class='dx-val'>{fmt_int(agg['total_fabricados'])}</div>
      <div class='dx-progress'><span style='width:{min(100, (agg['total_fabricados'] or 0)/max(1, agg['total_vendidos'] or 1)*80):.1f}%'></span></div>
    </div>
    """,
    unsafe_allow_html=True,
)
# KPI 2
st.markdown(
    f"""
    <div class='dx-card'>
      <div class='dx-label'>🛠️ Costo MO fabricado <span class='dx-muted'>(mes a hoy)</span></div>
      <div class='dx-val'>{fmt_mon(agg['costo_mo_fabricado'])}</div>
      <div class='dx-progress'><span style='width:{min(100, (agg['costo_mo_fabricado']/max(1, objetivo_a_hoy))*100):.1f}%'></span></div>
    </div>
    """,
    unsafe_allow_html=True,
)
# KPI 3
st.markdown(
    f"""
    <div class='dx-card'>
      <div class='dx-label'>🧾 Muebles vendidos <span class='dx-muted'>(mes a hoy)</span></div>
      <div class='dx-val'>{fmt_int(agg['total_vendidos'])}</div>
      <div class='dx-progress'><span class='{'bad' if agg['total_vendidos']<agg['total_fabricados'] else ''}' style='width:{min(100, (agg['total_vendidos']/max(1, agg['total_fabricados']))*100):.1f}%'></span></div>
    </div>
    """,
    unsafe_allow_html=True,
)
# KPI 4
st.markdown(
    f"""
    <div class='dx-card'>
      <div class='dx-label'>💵 Costo MO recuperado <span class='dx-muted'>(mes a hoy)</span></div>
      <div class='dx-val'>{fmt_mon(agg['costo_mo_recuperado'])}</div>
      <div class='dx-progress'><span style='width:{min(100, (agg['costo_mo_recuperado']/max(1, objetivo_a_hoy))*100):.1f}%'></span></div>
    </div>
    """,
    unsafe_allow_html=True,
)
st.markdown('</div>', unsafe_allow_html=True)

# ===== FILA 2: OBJETIVOS Y BALANZAS =====
st.markdown('<div class="dx-section">', unsafe_allow_html=True)

c1, c2, c3, c4, c5 = st.columns([1,1,1,1,1.2])
with c1:
    st.markdown(
        f"""
        <div class='dx-card'>
          <div class='dx-label'>🏭 Costo mensual de fábrica</div>
          <div class='dx-val'>{fmt_mon(costo_mensual)}</div>
        </div>
        """,
        unsafe_allow_html=True,
    )
with c2:
    st.markdown(
        f"""
        <div class='dx-card'>
          <div class='dx-label'>📅 Objetivo diario</div>
          <div class='dx-val'>{fmt_mon(objetivo_diario)}</div>
        </div>
        """,
        unsafe_allow_html=True,
    )
with c3:
    bal_fab = agg["costo_mo_fabricado"] - objetivo_a_hoy
    pct_fab = (agg["costo_mo_fabricado"]/max(1, objetivo_a_hoy))
    st.markdown(
        f"""
        <div class='dx-card'>
          <div class='dx-label'>⚙️ Balanza: Fabricado vs objetivo</div>
          <div class='dx-val' style='color:{'#16a34a' if bal_fab>=0 else '#ef4444'}'>{fmt_mon(bal_fab)}</div>
          <div class='dx-progress { 'bad' if pct_fab<1 else '' }'><span style='width:{min(100, pct_fab*100):.1f}%'></span></div>
        </div>
        """,
        unsafe_allow_html=True,
    )
with c4:
    bal_rec = agg["costo_mo_recuperado"] - objetivo_a_hoy
    pct_rec = (agg["costo_mo_recuperado"]/max(1, objetivo_a_hoy))
    st.markdown(
        f"""
        <div class='dx-card'>
          <div class='dx-label'>💵 Balanza: Recuperado vs objetivo</div>
          <div class='dx-val' style='color:{'#16a34a' if bal_rec>=0 else '#ef4444'}'>{fmt_mon(bal_rec)}</div>
          <div class='dx-progress { 'bad' if pct_rec<1 else '' }'><span style='width:{min(100, pct_rec*100):.1f}%'></span></div>
        </div>
        """,
        unsafe_allow_html=True,
    )
with c5:
    st.markdown(
        f"""
        <div class='dx-card dx-card-green'>
          <div class='dx-label'>💹 Margen bruto actual (mes)</div>
          <div class='dx-val' style='font-size:30px'>{fmt_mon(agg['margen_bruto_actual'])}</div>
          <div class='dx-muted' style='color:#e7ffe8'>Producción por debajo</div>
        </div>
        """,
        unsafe_allow_html=True,
    )

st.markdown('</div>', unsafe_allow_html=True)

# ===== FILA 3: DETALLE POR SKU =====
st.markdown('<div class="dx-section">', unsafe_allow_html=True)
left, right = st.columns([1,1])
with left:
    st.markdown("<div class='dx-sec-title'>📦 Producción por SKU</div>", unsafe_allow_html=True)
    dfp = agg["prod"].copy()
    if not dfp.empty:
        dfp = dfp.sort_values("COSTO_MO_TOTAL", ascending=False)
        st.dataframe(
            dfp.rename(columns={"SKU":"SKU","CANTIDAD":"Cantidad","COSTO_MO_UNIT":"Costo MO unit.","COSTO_MO_TOTAL":"Costo MO total"}),
            use_container_width=True,
            height=260,
        )
    else:
        st.caption("Sin producción registrada en el mes.")
with right:
    st.markdown("<div class='dx-sec-title'>🧾 Ventas por SKU</div>", unsafe_allow_html=True)
    dfv = agg["ventas"].copy()
    if not dfv.empty:
        dfv = dfv.sort_values("COSTO_MO_RECUP", ascending=False)
        st.dataframe(
            dfv.rename(columns={"SKU":"SKU","CANTIDAD":"Cantidad","COSTO_MO_UNIT":"Costo MO unit.","COSTO_MO_RECUP":"Costo MO recuperado"}),
            use_container_width=True,
            height=260,
        )
    else:
        st.caption("Sin ventas registradas en el mes.")

st.markdown('</div>', unsafe_allow_html=True)

# Cierre contenedor principal
st.markdown('</div>', unsafe_allow_html=True)
