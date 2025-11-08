# Versión con mejoras solicitadas (indicadores y detalle SKU)
import io
import re
import requests
import numpy as np
import pandas as pd
import streamlit as st
from datetime import datetime, date
from dateutil.relativedelta import relativedelta
from pytz import timezone

TZ = timezone("America/Argentina/Buenos_Aires")

def today_ba():
    return datetime.now(TZ).date()

def month_bounds(dt: date):
    start = dt.replace(day=1)
    end = (start + relativedelta(months=1)) - relativedelta(days=1)
    return start, end

def business_days_count(start: date, end: date):
    return int(np.busday_count(start, end + relativedelta(days=1)))

DRIVE_ID_REGEX = re.compile(r"(?:/d/|id=)([A-Za-z0-9_-]{10,})")
SHEETS_HOST_RE = re.compile(r"docs\.google\.com/spreadsheets/")

@st.cache_data(ttl=3600)
def fetch_excel_bytes(drive_url: str) -> bytes:
    m = DRIVE_ID_REGEX.search(drive_url)
    if not m:
        url = drive_url
    else:
        file_id = m.group(1)
        if SHEETS_HOST_RE.search(drive_url):
            url = f"https://docs.google.com/spreadsheets/d/{file_id}/export?format=xlsx"
        else:
            url = f"https://drive.google.com/uc?export=download&id={file_id}"
    resp = requests.get(url, timeout=60)
    resp.raise_for_status()
    return resp.content

@st.cache_data(ttl=3600)
def load_data_from_excel_bytes(xlsx_bytes: bytes):
    xls = pd.ExcelFile(io.BytesIO(xlsx_bytes))
    return {
        "mov": pd.read_excel(xls, sheet_name="MOVIMIENTO_STOCK-3934-1426"),
        "mat": pd.read_excel(xls, sheet_name="MATERIAL-4199-1426"),
        "rep": pd.read_excel(xls, sheet_name="REPORTE_DE_PEDIDOS-4166-1426"),
        "bom": pd.read_excel(xls, sheet_name="DETALLE_BOM-4200-1426"),
    }

def compute_unit_labor_cost(df_material, df_bom):
    mat = df_material.rename(columns={"MATE_CODIGO": "OPERACION", "MATE_CRM": "COSTO_OPERACION"})
    bom = df_bom.rename(columns={"MBOM_CODIGO": "SKU", "MATE_CODIGO": "OPERACION", "DEBO_CANTIDAD": "CANTIDAD_OP"})
    merged = bom.merge(mat[["OPERACION", "COSTO_OPERACION"]], on="OPERACION", how="left")
    merged["COSTO_PARCIAL"] = merged["CANTIDAD_OP"].fillna(0) * merged["COSTO_OPERACION"].fillna(0)
    return merged.groupby("SKU", as_index=False)["COSTO_PARCIAL"].sum().rename(columns={"COSTO_PARCIAL": "COSTO_MO_UNIT"})

def normalize_date_col(df, col):
    return pd.to_datetime(df[col], errors="coerce").dt.tz_localize(None).dt.date

@st.cache_data(ttl=1800)
def aggregate_current_month(df_mov, df_rep, unit_cost, today):
    month_start, _ = month_bounds(today)
    mov = df_mov.rename(columns={"AUDI_FECHA_ALTA": "FECHA", "MATE_CODIGO": "SKU", "MOST_CANTIDAD": "CANTIDAD"}).copy()
    mov["FECHA"] = normalize_date_col(mov, "FECHA")
    mov_month = mov[(mov["FECHA"] >= month_start) & (mov["FECHA"] <= today)]
    prod = mov_month.groupby("SKU", as_index=False)["CANTIDAD"].sum().merge(unit_cost, on="SKU", how="left").fillna(0)
    prod["COSTO_MO_TOTAL"] = prod["CANTIDAD"] * prod["COSTO_MO_UNIT"]
    rep = df_rep.rename(columns={"AUDI_FECHA_ALTA": "FECHA", "SKU": "SKU", "CANTIDAD": "CANTIDAD", "MARGEN_3": "MARGEN", "MATE_CRM": "CRM"}).copy()
    rep["FECHA"] = normalize_date_col(rep, "FECHA")
    rep_month = rep[(rep["FECHA"] >= month_start) & (rep["FECHA"] <= today)]
    ventas = rep_month.groupby(["SKU", "CRM"], as_index=False).agg({"CANTIDAD": "sum", "MARGEN": "sum"}).merge(unit_cost, on="SKU", how="left").fillna(0)
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

st.set_page_config(page_title="DX Fábrica – KPI", layout="wide")

hoy = today_ba()
st.title("DX Fábrica — Panel de KPI")
st.caption(f"Última actualización: {hoy}")

drive_url = st.text_input("Enlace de Google Drive o Sheet", st.secrets.get("DRIVE_FILE_URL", ""))
if st.button("🔁 Actualizar"):
    fetch_excel_bytes.clear()
    load_data_from_excel_bytes.clear()
    aggregate_current_month.clear()
    st.rerun()

data = load_data_from_excel_bytes(fetch_excel_bytes(drive_url))
unit_cost = compute_unit_labor_cost(data["mat"], data["bom"])
agg = aggregate_current_month(data["mov"], data["rep"], unit_cost, hoy)

mes_ini, mes_fin = month_bounds(hoy)
costo_mensual = st.number_input("Costo mensual total ($)", value=50_000_000.0, step=100_000.0)
dias_mes = business_days_count(mes_ini, mes_fin)
dias_trans = business_days_count(mes_ini, hoy)
objetivo_a_hoy = (costo_mensual / dias_mes) * dias_trans

porc_fabricado = (agg['costo_fabricado'] / objetivo_a_hoy - 1) * 100 if objetivo_a_hoy else 0
porc_recuperado = (agg['costo_recuperado'] / objetivo_a_hoy - 1) * 100 if objetivo_a_hoy else 0

st.subheader("📊 Indicadores")
c1, c2, c3 = st.columns(3)
with c1:
    st.metric("Costo MO Fabricado", f"$ {agg['costo_fabricado']:,.0f}", f"{porc_fabricado:+.1f}% vs esperado")
with c2:
    st.metric("Costo MO Recuperado", f"$ {agg['costo_recuperado']:,.0f}", f"{porc_recuperado:+.1f}% vs esperado")
with c3:
    st.metric("Costo mensual total", f"$ {costo_mensual:,.0f}")

st.subheader("📦 Detalle de SKU")
prod = agg["prod"].copy()
ventas = agg["ventas"].copy()

prod["COSTO_MO_UNIT"] = prod["COSTO_MO_UNIT"].apply(lambda x: f"$ {x:,.0f}")
prod["COSTO_MO_TOTAL"] = prod["COSTO_MO_TOTAL"].apply(lambda x: f"$ {x:,.0f}")
st.dataframe(prod.rename(columns={"SKU":"SKU","CANTIDAD":"Cantidad","COSTO_MO_UNIT":"Costo MO unit.","COSTO_MO_TOTAL":"Costo MO total"}))

ventas["COSTO_MO_UNIT"] = ventas["COSTO_MO_UNIT"].apply(lambda x: f"$ {x:,.0f}")
ventas["COSTO_MO_RECUP"] = ventas["COSTO_MO_RECUP"].apply(lambda x: f"$ {x:,.0f}")
ventas = ventas.assign(MARGEN_BRUTO=ventas["MARGEN"] - ventas["COSTO_MO_RECUP"])
ventas = ventas.sort_values("MARGEN_BRUTO", ascending=False)

st.dataframe(ventas.rename(columns={"SKU":"SKU","CRM":"CRM","CANTIDAD":"Cantidad","COSTO_MO_UNIT":"Costo MO unit.","COSTO_MO_RECUP":"Costo MO recuperado","MARGEN_BRUTO":"Margen Bruto"}))
