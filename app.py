# DX Fábrica – Panel de KPI (versión estable con pestañas)

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

def today_ba():
    return datetime.now(TZ).date()

def month_bounds(dt: date):
    start = dt.replace(day=1)
    end = (start + relativedelta(months=1)) - relativedelta(days=1)
    return start, end

def business_days_count(start: date, end: date):
    return int(np.busday_count(start, end + relativedelta(days=1)))

# ============================
# Lectura de archivos
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
def compute_unit_labor_cost(df_material, df_bom):
    mat = df_material.rename(columns={"MATE_CODIGO": "OPERACION", "MATE_CRM": "COSTO_OPERACION"})
    bom = df_bom.rename(columns={"MBOM_CODIGO": "SKU", "MATE_CODIGO": "OPERACION", "DEBO_CANTIDAD": "CANTIDAD_OP"})
    merged = bom.merge(mat[["OPERACION", "COSTO_OPERACION"]], on="OPERACION", how="left")
    merged["COSTO_PARCIAL"] = merged["CANTIDAD_OP"].fillna(0) * merged["COSTO_OPERACION"].fillna(0)
    return merged.groupby("SKU", as_index=False)["COSTO_PARCIAL"].sum().rename(columns={"COSTO_PARCIAL": "COSTO_MO_UNIT"})

def normalize_date_col(df, col):
    return pd.to_datetime(df[col], errors="coerce").dt.tz_localize(None).dt.date

def aggregate_current_month(df_mov, df_rep, unit_cost, today):
    month_start, _ = month_bounds(today)
    mov = df_mov.rename(columns={"AUDI_FECHA_ALTA": "FECHA", "MATE_CODIGO": "SKU", "MOST_CANTIDAD": "CANTIDAD"}).copy()
    mov["FECHA"] = normalize_date_col(mov, "FECHA")
    mov_month = mov[(mov["FECHA"] >= month_start) & (mov["FECHA"] <= today)]
    prod = mov_month.groupby("SKU", as_index=False)["CANTIDAD"].sum().merge(unit_cost, on="SKU", how="left").fillna(0)
    prod["COSTO_MO_TOTAL"] = prod["CANTIDAD"] * prod["COSTO_MO_UNIT"]

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
# Interfaz Streamlit
# ============================
st.set_page_config(page_title="DX Fábrica – KPI", layout="wide")

hoy = today_ba()
header = f"<h1 style='font-weight:800;margin-bottom:0'>📊 DX Fábrica – Panel de KPI</h1><p style='color:gray'>Actualizado: {hoy}</p>"

st.markdown(header, unsafe_allow_html=True)

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

    objetivo_diario = costo_mensual / dias_mes
    objetivo_a_hoy = objetivo_diario * dias_trans

    st.session_state["cfg"] = dict(url=drive_url, costo=costo_mensual, dias_mes=dias_mes, dias_trans=dias_trans, obj_d=objetivo_diario, obj_h=objetivo_a_hoy)

with tab_kpi:
    cfg = st.session_state.get("cfg")
    if not cfg or not cfg["url"]:
        st.warning("Cargá primero los parámetros en Configuración.")
        st.stop()

    try:
        data = load_data_from_excel_bytes(fetch_excel_bytes(cfg["url"]))
        unit_cost = compute_unit_labor_cost(data["mat"], data["bom"])
        agg = aggregate_current_month(data["mov"], data["rep"], unit_cost, hoy)
    except Exception as e:
        st.error(f"Error cargando datos: {e}")
        st.stop()

    st.metric("Muebles fabricados", f"{agg['fabricados']:,}".replace(",","."))
    st.metric("Costo MO fabricado", f"$ {agg['costo_fabricado']:,.2f}".replace(",","."))
    st.metric("Muebles vendidos", f"{agg['vendidos']:,}".replace(",","."))
    st.metric("Costo MO recuperado", f"$ {agg['costo_recuperado']:,.2f}".replace(",","."))
    st.metric("Margen bruto actual", f"$ {agg['margen']:,.2f}".replace(",","."))

with tab_detalle:
    cfg = st.session_state.get("cfg")
    if not cfg:
        st.warning("Primero completá la configuración.")
        st.stop()

    data = load_data_from_excel_bytes(fetch_excel_bytes(cfg["url"]))
    unit_cost = compute_unit_labor_cost(data["mat"], data["bom"])
    agg = aggregate_current_month(data["mov"], data["rep"], unit_cost, hoy)

    st.subheader("Producción por SKU")
    st.dataframe(agg["prod"].head(20))

    st.subheader("Ventas por SKU")
    st.dataframe(agg["ventas"].head(20))
