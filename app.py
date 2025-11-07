# DX Fábrica – Panel de KPI (versión completa con header compacto y contenedor con borde)

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

def load_data_from_excel_bytes(xlsx_bytes: bytes):
    xls = pd.ExcelFile(io.BytesIO(xlsx_bytes))
    return {
        "mov": pd.read_excel(xls, sheet_name="MOVIMIENTO_STOCK-3934-1426"),
        "mat": pd.read_excel(xls, sheet_name="MATERIAL-4199-1426"),
        "rep": pd.read_excel(xls, sheet_name="REPORTE_DE_PEDIDOS-4166-1426"),
        "bom": pd.read_excel(xls, sheet_name="DETALLE_BOM-4200-1426"),
    }

# ============================
# Cálculos base
# ============================

def compute_unit_labor_cost(df_material: pd.DataFrame, df_bom: pd.DataFrame) -> pd.DataFrame:
    """Costo de mano de obra unitario por SKU = Σ(cant_op × costo_op)."""
    mat = df_material.rename(columns={"MATE_CODIGO": "OPERACION", "MATE_CRM": "COSTO_OPERACION"})
    bom = df_bom.rename(columns={"MBOM_CODIGO": "SKU", "MATE_CODIGO": "OPERACION", "DEBO_CANTIDAD": "CANTIDAD_OP"})
    merged = bom.merge(mat[["OPERACION", "COSTO_OPERACION"]], on="OPERACION", how="left")
    merged["COSTO_PARCIAL"] = merged["CANTIDAD_OP"].fillna(0) * merged["COSTO_OPERACION"].fillna(0)
    return merged.groupby("SKU", as_index=False)["COSTO_PARCIAL"].sum().rename(columns={"COSTO_PARCIAL": "COSTO_MO_UNIT"})


def normalize_date_col(df: pd.DataFrame, col: str) -> pd.Series:
    return pd.to_datetime(df[col], errors="coerce").dt.tz_localize(None).dt.date


def aggregate_current_month(df_mov: pd.DataFrame, df_rep: pd.DataFrame, unit_cost: pd.DataFrame, today: date):
    month_start, _ = month_bounds(today)

    # Producción
    mov = df_mov.rename(columns={"AUDI_FECHA_ALTA": "FECHA", "MATE_CODIGO": "SKU", "MOST_CANTIDAD": "CANTIDAD"}).copy()
    mov["FECHA"] = normalize_date_col(mov, "FECHA")
    mov_month = mov[(mov["FECHA"] >= month_start) & (mov["FECHA"] <= today)]
    prod = mov_month.groupby("SKU", as_index=False)["CANTIDAD"].sum().merge(unit_cost, on="SKU", how="left").fillna(0)
    prod["COSTO_MO_TOTAL"] = prod["CANTIDAD"] * prod["COSTO_MO_UNIT"]

    # Ventas
    rep = df_rep.rename(columns={"AUDI_FECHA_ALTA": "FECHA", "SKU": "SKU", "CANTIDAD": "CANTIDAD", "MARGEN_3": "MARGEN"}).copy()
    rep["FECHA"] = normalize_date_col(rep, "FECHA")
    rep_month = rep[(rep["FECHA"] >= month_start) & (rep["FECHA"] <= today)]
    ventas = rep_month.groupby("SKU", as_index=False)["CANTIDAD"].sum().merge(unit_cost, on="SKU", how="left").fillna(0)
    ventas["COSTO_MO_RECUP"] = ventas["CANTIDAD"] * ventas["COSTO_MO_UNIT"]

    return {
        "prod": prod,
        "ventas": ventas,
        "fabricados": int(prod["CANTIDAD"].sum()),
        "costo_fabricado": float(prod["COSTO_MO_TOTAL"].sum()),
        "vendidos": int(ventas["CANTIDAD"].sum()),
        "costo_recuperado": float(ventas["COSTO_MO_RECUP"].sum()),
        "margen": float(rep_month["MARGEN"].sum()) if not rep_month.empty else 0.0,
    }

# ============================
# Interfaz Streamlit con estilo
# ============================
st.set_page_config(page_title="DX Fábrica – KPI", layout="wide")

# --- Estilos visuales (header 20% y shell con borde) ---
st.markdown("""
<style>
:root{
  --bg:#0b1020; --card:#ffffff; --muted:#6b7280; --ink:#111827; --border:#e5e7eb;
  --green:#22c55e; --amber:#f59e0b; --red:#ef4444;
}
/* ancho y respiración general */
.block-container{ padding-top:.5rem; padding-bottom:0; max-width:1280px; }

/* ===== HEADER (compacto ~20% del anterior) ===== */
.dx-header{ background:linear-gradient(90deg, var(--bg), #11193a); color:#fff; border-radius:10px; padding:4px 14px; margin:0 0 8px 0; }
.dx-head-row{ display:flex; align-items:flex-end; justify-content:space-between; gap:16px; }
.dx-title{ margin:0; line-height:1; font-weight:800; font-size:18px; }
.dx-upd{ margin:0; font-size:10px; opacity:.9; white-space:nowrap; }

/* ===== CONTENEDOR GENERAL CON BORDE ===== */
.dx-shell{ background:#fff; border:1px solid var(--border); border-radius:16px; padding:14px 16px; box-shadow:0 2px 10px rgba(0,0,0,.04); }

/* Tarjetas / grilla */
.dx-grid{ display:grid; grid-template-columns:repeat(4,1fr); gap:12px; margin-top:8px }
.dx-card{ background:var(--card); border:1px solid var(--border); border-radius:14px; padding:12px 14px; box-shadow:0 2px 8px rgba(0,0,0,.05); }
.dx-label{ color:var(--muted); font-size:13px; margin-bottom:6px; display:flex; gap:6px; align-items:center }
.dx-val{ color:var(--ink); font-size:26px; font-weight:700; line-height:1.15; margin:0 }

/* DataFrames redondeados */
[data-testid="stDataFrame"]{ border-radius:12px; overflow:hidden; box-shadow:0 2px 6px rgba(0,0,0,.04) }
</style>
""", unsafe_allow_html=True)

# --- Header ---
hoy = today_ba()
st.markdown(f"""
<div class="dx-header">
  <div class="dx-head-row">
    <h1 class="dx-title">DX Fábrica — Panel de KPI</h1>
    <p class="dx-upd">Datos del mes en curso · <b>Última actualización:</b> {hoy}</p>
  </div>
</div>
""", unsafe_allow_html=True)

# --- Shell con borde (envolvemos todo) ---
st.markdown('<div class="dx-shell">', unsafe_allow_html=True)

# --- Tabs ---
tab_config, tab_kpi, tab_detalle = st.tabs(["⚙️ Configuración", "📊 Indicadores", "📦 Detalle SKU"])

with tab_config:
    default_url = st.secrets.get("DRIVE_FILE_URL", "")
    drive_url = st.text_input("Enlace de Google Drive o Google Sheet", value=default_url)
    mes_ini, mes_fin = month_bounds(hoy)

    c1, c2, c3 = st.columns(3)
    with c1:
        costo_mensual = st.number_input("Costo mensual total ($)", value=50_000_000.0, step=100_000.0)
    with c2:
        dias_mes = st.number_input("Días hábiles del mes", value=int(business_days_count(mes_ini, mes_fin)))
    with c3:
        dias_trans = st.number_input("Días hábiles transcurridos", value=int(business_days_count(mes_ini, hoy)))

    objetivo_diario = (costo_mensual / dias_mes) if dias_mes else 0.0
    objetivo_a_hoy = objetivo_diario * dias_trans

    # Intentamos cargar datos aquí para cachearlos
    data = None
    if drive_url:
        try:
            data = load_data_from_excel_bytes(fetch_excel_bytes(drive_url))
            st.success("Datos cargados correctamente ✅")
        except Exception as e:
            st.error(f"No se pudo obtener el archivo/Sheet. Error: {e}")

    st.session_state["cfg"] = dict(url=drive_url, costo=costo_mensual, dias_mes=dias_mes, dias_trans=dias_trans, obj_d=objetivo_diario, obj_h=objetivo_a_hoy, data=data, today=hoy)

with tab_kpi:
    cfg = st.session_state.get("cfg", {})
    data = cfg.get("data")
    if not data:
        st.info("Cargá primero los datos en la pestaña **Configuración**.")
        st.stop()

    unit_cost = compute_unit_labor_cost(data["mat"], data["bom"])  # costo MO unitario por SKU
    agg = aggregate_current_month(data["mov"], data["rep"], unit_cost, cfg["today"])  # métricas del mes

    # Render de KPIs en grilla
    kpi_html = f"""
    <div class='dx-grid'>
      <div class='dx-card'><div class='dx-label'>🪑 Muebles fabricados</div><div class='dx-val'>{agg['fabricados']:,}</div></div>
      <div class='dx-card'><div class='dx-label'>🛠️ Costo MO fabricado</div><div class='dx-val'>$ {agg['costo_fabricado']:,.0f}</div></div>
      <div class='dx-card'><div class='dx-label'>🧾 Muebles vendidos</div><div class='dx-val'>{agg['vendidos']:,}</div></div>
      <div class='dx-card'><div class='dx-label'>💵 Costo MO recuperado</div><div class='dx-val'>$ {agg['costo_recuperado']:,.0f}</div></div>
    </div>
    <br>
    <div class='dx-card'><div class='dx-label'>💹 Margen bruto actual</div><div class='dx-val'>$ {agg['margen']:,.0f}</div></div>
    """
    st.markdown(kpi_html.replace(",", "."), unsafe_allow_html=True)

with tab_detalle:
    cfg = st.session_state.get("cfg", {})
    data = cfg.get("data")
    if not data:
        st.info("Cargá primero los datos en la pestaña **Configuración**.")
        st.stop()

    unit_cost = compute_unit_labor_cost(data["mat"], data["bom"])  # costo MO unitario por SKU
    agg = aggregate_current_month(data["mov"], data["rep"], unit_cost, cfg["today"])  # métricas del mes

    st.subheader("📦 Producción por SKU")
    st.dataframe(agg["prod"].rename(columns={"SKU":"SKU","CANTIDAD":"Cantidad","COSTO_MO_UNIT":"Costo MO unit.","COSTO_MO_TOTAL":"Costo MO total"}))

    st.subheader("🧾 Ventas por SKU")
    st.dataframe(agg["ventas"].rename(columns={"SKU":"SKU","CANTIDAD":"Cantidad","COSTO_MO_UNIT":"Costo MO unit.","COSTO_MO_RECUP":"Costo MO recuperado"}))

# --- Cerrar shell ---
st.markdown('</div>', unsafe_allow_html=True)
