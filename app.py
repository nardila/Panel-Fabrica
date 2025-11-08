# DX Fábrica – Panel de KPI
# ***Cambios EXACTOS pedidos por Nico (y solo esos):***
# Detalle de SKU →
# 1) Eliminar columna "Margen Bruto".
# 2) Ordenar de manera descendente por "MARGEN".
# 3) Mostrar todos los valores en módulo (sin negativos) en ambas tablas del Detalle.
# ***No se modificó nada más del archivo.***

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
# Utilidades de fechas
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
    mat = df_material.rename(columns={"MATE_CODIGO": "OPERACION", "MATE_CRM": "COSTO_OPERACION"})
    bom = df_bom.rename(columns={"MBOM_CODIGO": "SKU", "MATE_CODIGO": "OPERACION", "DEBO_CANTIDAD": "CANTIDAD_OP"})
    merged = bom.merge(mat[["OPERACION", "COSTO_OPERACION"]], on="OPERACION", how="left")
    merged["COSTO_PARCIAL"] = merged["CANTIDAD_OP"].fillna(0) * merged["COSTO_OPERACION"].fillna(0)
    return merged.groupby("SKU", as_index=False)["COSTO_PARCIAL"].sum().rename(columns={"COSTO_PARCIAL": "COSTO_MO_UNIT"})


def normalize_date_col(df: pd.DataFrame, col: str) -> pd.Series:
    return pd.to_datetime(df[col], errors="coerce").dt.tz_localize(None).dt.date


def aggregate_current_month(df_mov: pd.DataFrame, df_rep: pd.DataFrame, unit_cost: pd.DataFrame, today: date):
    month_start, _ = month_bounds(today)

    # Producción (movimientos)
    mov = df_mov.rename(columns={"AUDI_FECHA_ALTA": "FECHA", "MATE_CODIGO": "SKU", "MOST_CANTIDAD": "CANTIDAD"}).copy()
    mov["FECHA"] = normalize_date_col(mov, "FECHA")
    mov_month = mov[(mov["FECHA"] >= month_start) & (mov["FECHA"] <= today)]
    prod = mov_month.groupby("SKU", as_index=False)["CANTIDAD"].sum().merge(unit_cost, on="SKU", how="left").fillna(0)
    prod["COSTO_MO_TOTAL"] = prod["CANTIDAD"] * prod["COSTO_MO_UNIT"]

    # Ventas (reporte)
    rep = df_rep.rename(columns={
        "AUDI_FECHA_ALTA": "FECHA",
        "SKU": "SKU",
        "CANTIDAD": "CANTIDAD",
        "MARGEN_3": "MARGEN",
        "MATE_CRM": "CRM",
    }).copy()
    rep["FECHA"] = normalize_date_col(rep, "FECHA")
    rep_month = rep[(rep["FECHA"] >= month_start) & (rep["FECHA"] <= today)]
    ventas = (
        rep_month.groupby(["SKU", "CRM"], as_index=False)
        .agg({"CANTIDAD": "sum", "MARGEN": "sum"})
        .merge(unit_cost, on="SKU", how="left")
        .fillna(0)
    )
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
# Interfaz (sin cambios fuera de lo pedido)
# ============================
st.set_page_config(page_title="DX Fábrica – KPI", layout="wide")

hoy = today_ba()
st.title("DX Fábrica — Panel de KPI")
st.caption(f"Última actualización: {hoy}")

# Controles básicos existentes
default_url = st.secrets.get("DRIVE_FILE_URL", "")
drive_url = st.text_input("Enlace de Google Drive o Google Sheet", value=default_url)
if st.button("🔁 Actualizar"):
    st.experimental_rerun()

# Entrada de parámetros existentes
mes_ini, mes_fin = month_bounds(hoy)
costo_mensual = st.number_input("Costo mensual total ($)", value=50_000_000.0, step=100_000.0)
dias_mes = business_days_count(mes_ini, mes_fin)
dias_trans = business_days_count(mes_ini, hoy)
objetivo_a_hoy = (costo_mensual / dias_mes) * dias_trans if dias_mes else 0.0

# Carga de datos (misma lógica)
data = load_data_from_excel_bytes(fetch_excel_bytes(drive_url)) if drive_url else None
if not data:
    st.stop()

unit_cost = compute_unit_labor_cost(data["mat"], data["bom"])  # costo MO unitario por SKU
agg = aggregate_current_month(data["mov"], data["rep"], unit_cost, hoy)

# ============================
# Indicadores (sin cambios en este pedido)
# ============================
st.subheader("📊 Indicadores")
col1, col2, col3, col4 = st.columns(4)
with col1:
    st.metric("Muebles fabricados", f"{agg['fabricados']:,}")
with col2:
    delta_fab = (agg["costo_fabricado"] / objetivo_a_hoy - 1) * 100 if objetivo_a_hoy else 0.0
    st.metric("Costo MO fabricado", f"$ {agg['costo_fabricado']:,.0f}", f"{delta_fab:+.1f}% vs esperado")
with col3:
    st.metric("Muebles vendidos", f"{agg['vendidos']:,}")
with col4:
    delta_rec = (agg["costo_recuperado"] / objetivo_a_hoy - 1) * 100 if objetivo_a_hoy else 0.0
    st.metric("Costo MO recuperado", f"$ {agg['costo_recuperado']:,.0f}", f"{delta_rec:+.1f}% vs esperado")

st.metric("Margen bruto actual", f"$ {agg['margen']:,.0f}")

# ============================
# Detalle de SKU (CAMBIOS SOLICITADOS)
# ============================
st.subheader("📦 Detalle de SKU")

# --- Producción por SKU ---
prod = agg["prod"].copy()
# 3) módulo (sin negativos) en valores numéricos
for c in ["CANTIDAD", "COSTO_MO_UNIT", "COSTO_MO_TOTAL"]:
    if c in prod.columns:
        prod[c] = prod[c].abs()
# formato moneda en costos
if "COSTO_MO_UNIT" in prod.columns:
    prod["COSTO_MO_UNIT"] = prod["COSTO_MO_UNIT"].apply(lambda x: f"$ {x:,.0f}")
if "COSTO_MO_TOTAL" in prod.columns:
    prod["COSTO_MO_TOTAL"] = prod["COSTO_MO_TOTAL"].apply(lambda x: f"$ {x:,.0f}")

st.dataframe(
    prod.rename(columns={
        "SKU": "SKU",
        "CANTIDAD": "Cantidad",
        "COSTO_MO_UNIT": "Costo MO unit.",
        "COSTO_MO_TOTAL": "Costo MO total",
    }),
    use_container_width=True,
)

# --- Ventas por SKU ---
ventas = agg["ventas"].copy()
# 3) módulo (sin negativos) en valores numéricos relevantes
for c in ["CANTIDAD", "COSTO_MO_UNIT", "COSTO_MO_RECUP", "MARGEN"]:
    if c in ventas.columns:
        ventas[c] = ventas[c].abs()
# 2) ordenar por MARGEN (desc)
if "MARGEN" in ventas.columns:
    ventas = ventas.sort_values("MARGEN", ascending=False)
# 1) eliminar Margen Bruto (si existiera de versiones previas)
if "MARGEN_BRUTO" in ventas.columns:
    ventas = ventas.drop(columns=["MARGEN_BRUTO"])  # eliminado según pedido
# formato moneda en costos
if "COSTO_MO_UNIT" in ventas.columns:
    ventas["COSTO_MO_UNIT"] = ventas["COSTO_MO_UNIT"].apply(lambda x: f"$ {x:,.0f}")
if "COSTO_MO_RECUP" in ventas.columns:
    ventas["COSTO_MO_RECUP"] = ventas["COSTO_MO_RECUP"].apply(lambda x: f"$ {x:,.0f}")
if "MARGEN" in ventas.columns:
    ventas["MARGEN"] = ventas["MARGEN"].apply(lambda x: f"$ {x:,.0f}")

st.dataframe(
    ventas.rename(columns={
        "SKU": "SKU",
        "CRM": "CRM",
        "CANTIDAD": "Cantidad",
        "COSTO_MO_UNIT": "Costo MO unit.",
        "COSTO_MO_RECUP": "Costo MO recuperado",
        "MARGEN": "Margen",
    }),
    use_container_width=True,
)
