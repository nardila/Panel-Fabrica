# app.py – DX Fábrica – Panel KPI
# -------------------------------
# Streamlit app para monitorear KPI de producción, costos de mano de obra y márgenes.
# Orígenes de datos: archivo Excel en Google Drive (se actualiza cada medianoche).
# Despliegue sugerido: GitHub + Streamlit Cloud.
#
# Requisitos (requirements.txt):
#   streamlit
#   pandas
#   numpy
#   requests
#   python-dateutil
#   pytz
#
# Configuración: definir la variable DRIVE_FILE_URL (en st.secrets o input) con el enlace de Drive al Excel.
#   - Se aceptan enlaces compartidos de Drive y enlaces directos de descarga.
#   - Alternativamente, se puede subir el archivo manualmente con el uploader del panel.

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
    # Lunes(0)–Viernes(4) como días hábiles; permite festivos opcionales (lista vacía por defecto)
    bdays = np.busday_count(start, end + relativedelta(days=1))
    return int(bdays)

# ---------------------------------
# Descarga de archivo desde Google Drive
# ---------------------------------
DRIVE_ID_REGEX = re.compile(r"(?:/d/|id=)([A-Za-z0-9_-]{10,})")

@st.cache_data(show_spinner=False, ttl=3600)
def fetch_excel_bytes(drive_url: str) -> bytes:
    if not drive_url:
        raise ValueError("Falta el enlace de Drive.")
    m = DRIVE_ID_REGEX.search(drive_url)
    if m:
        file_id = m.group(1)
        url = f"https://drive.google.com/uc?export=download&id={file_id}"
    else:
        # Intento directo (por si ya es un enlace de descarga)
        url = drive_url
    resp = requests.get(url, timeout=60)
    resp.raise_for_status()
    return resp.content

@st.cache_data(show_spinner=False, ttl=3600)
def load_data_from_excel_bytes(xlsx_bytes: bytes):
    xls = pd.ExcelFile(io.BytesIO(xlsx_bytes))
    # Nombres esperados de hojas (según el archivo entregado)
    h_mov = "MOVIMIENTO_STOCK-3934-1426"
    h_mat = "MATERIAL-4199-1426"
    h_rep = "REPORTE_DE_PEDIDOS-4166-1426"
    h_bom = "DETALLE_BOM-4200-1426"

    df_mov = pd.read_excel(xls, sheet_name=h_mov)
    df_mat = pd.read_excel(xls, sheet_name=h_mat)
    df_rep = pd.read_excel(xls, sheet_name=h_rep)
    df_bom = pd.read_excel(xls, sheet_name=h_bom)

    return {"mov": df_mov, "mat": df_mat, "rep": df_rep, "bom": df_bom}

# ---------------------------------
# Cálculo de costos unitarios de mano de obra por SKU
# ---------------------------------
@st.cache_data(show_spinner=False, ttl=3600)
def compute_unit_labor_cost(df_material: pd.DataFrame, df_bom: pd.DataFrame) -> pd.DataFrame:
    # df_material: columnas MATE_CODIGO (operación), MATE_CRM (costo)
    # df_bom: MBOM_CODIGO (SKU), MATE_CODIGO (operación), DEBO_CANTIDAD (cantidad operación por unidad)
    mat = df_material.rename(columns={"MATE_CODIGO": "OPERACION", "MATE_CRM": "COSTO_OPERACION"})
    bom = df_bom.rename(columns={"MBOM_CODIGO": "SKU", "MATE_CODIGO": "OPERACION", "DEBO_CANTIDAD": "CANTIDAD_OP"})

    merged = bom.merge(mat[["OPERACION", "COSTO_OPERACION"]], on="OPERACION", how="left")
    merged["COSTO_OPERACION"].fillna(0.0, inplace=True)
    merged["CANTIDAD_OP"].fillna(0.0, inplace=True)

    merged["COSTO_PARCIAL"] = merged["CANTIDAD_OP"] * merged["COSTO_OPERACION"]
    unit_cost = merged.groupby("SKU", as_index=False)["COSTO_PARCIAL"].sum().rename(columns={"COSTO_PARCIAL": "COSTO_MO_UNIT"})
    return unit_cost

# ---------------------------------
# Agregados de producción y ventas (mes en curso, a día de hoy)
# ---------------------------------

def normalize_date_col(df: pd.DataFrame, col: str) -> pd.Series:
    s = pd.to_datetime(df[col], errors="coerce").dt.tz_localize(None)
    return s.dt.date

@st.cache_data(show_spinner=False, ttl=1800)
def aggregate_current_month(df_mov: pd.DataFrame, df_rep: pd.DataFrame, unit_cost: pd.DataFrame, today: date):
    month_start, month_end = month_bounds(today)

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

    # Margen bruto actual (suma de MARGEN en el mes)
    # Nota: En los datos de ejemplo CRM aparece negativo; aquí no se usa CRM para margen, solo columna MARGEN.
    margen_bruto_actual = float(rep_month["MARGEN"].sum()) if not rep_month.empty else 0.0

    return {
        "prod_by_sku": prod_with_cost,
        "ventas_by_sku": ventas_with_cost,
        "total_fabricados": total_fabricados,
        "costo_mo_fabricado": costo_mo_fabricado,
        "total_vendidos": total_vendidos,
        "costo_mo_recuperado": costo_mo_recuperado,
        "margen_bruto_actual": margen_bruto_actual,
        "rep_month": rep_month,
        "mov_month": mov_month,
    }

# ---------------------------------
# UI
# ---------------------------------
st.set_page_config(page_title="DX Fábrica – KPI", page_icon="📊", layout="wide")
st.title("📊 DX Fábrica – Panel de KPI")

# Entrada de configuración: URL de Drive y costos fijos del mes
col0, col1 = st.columns([2, 1])
with col0:
    default_url = st.secrets.get("DRIVE_FILE_URL", "") if hasattr(st, "secrets") else ""
    drive_url = st.text_input("Enlace de Google Drive al Excel (se actualiza cada medianoche)", value=default_url, help="Pegá el enlace compartido del archivo .xlsx. Ej.: https://drive.google.com/file/d/FILE_ID/view?usp=sharing")
with col1:
    st.caption(":gray[Tip: podés guardar el enlace en **Secrets** de Streamlit Cloud como `DRIVE_FILE_URL`]")

st.divider()

# Parámetros del mes
hoy = today_ba()
mes_inicio, mes_fin = month_bounds(hoy)

colA, colB, colC, colD = st.columns(4)
with colA:
    costo_mensual = st.number_input("Costo mensual total de la fábrica ($)", min_value=0.0, value=50_000_000.0, step=100_000.0, format="%0.2f")
with colB:
    dias_habiles_mes_auto = business_days_count(mes_inicio, mes_fin)
    dias_habiles_mes = st.number_input("Días hábiles del mes", min_value=1, value=int(dias_habiles_mes_auto), step=1)
with colC:
    dias_habiles_transc_auto = business_days_count(mes_inicio, hoy)
    dias_habiles_transc = st.number_input("Días hábiles transcurridos (hasta hoy)", min_value=0, max_value=int(dias_habiles_mes), value=int(dias_habiles_transc_auto), step=1)
with colD:
    st.metric("Fecha (BA)", hoy.strftime("%Y-%m-%d"))

objetivo_diario = (costo_mensual / dias_habiles_mes) if dias_habiles_mes else 0.0
objetivo_a_hoy = objetivo_diario * dias_habiles_transc

st.divider()

# Carga del archivo
data = None
err = None
if drive_url:
    try:
        xbytes = fetch_excel_bytes(drive_url)
        data = load_data_from_excel_bytes(xbytes)
    except Exception as e:
        err = str(e)
        st.warning(f"No se pudo descargar el archivo desde Drive. Error: {err}")

if data is None:
    st.info("Como alternativa, subí el archivo .xlsx manualmente.")
    upl = st.file_uploader("Subir Excel", type=["xlsx"])
    if upl:
        try:
            data = load_data_from_excel_bytes(upl.read())
        except Exception as e:
            st.error(f"Error al leer el Excel subido: {e}")

if data is None:
    st.stop()

# Procesamiento
unit_cost = compute_unit_labor_cost(data["mat"], data["bom"])  # Costo MO unitario por SKU
agg = aggregate_current_month(data["mov"], data["rep"], unit_cost, hoy)

# KPI principales
k1, k2, k3, k4 = st.columns(4)
with k1:
    st.metric("Muebles fabricados (mes a hoy)", f"{agg['total_fabricados']:,}".replace(",","."))
with k2:
    st.metric("Costo MO fabricado (mes a hoy)", f"$ {agg['costo_mo_fabricado']:,.2f}".replace(",","."))
with k3:
    st.metric("Muebles vendidos (mes a hoy)", f"{agg['total_vendidos']:,}".replace(",","."))
with k4:
    st.metric("Costo MO recuperado por ventas (mes a hoy)", f"$ {agg['costo_mo_recuperado']:,.2f}".replace(",","."))

k5, k6, k7 = st.columns(3)
with k5:
    st.metric("Objetivo diario MO", f"$ {objetivo_diario:,.2f}".replace(",","."))
with k6:
    st.metric("Objetivo acumulado a hoy", f"$ {objetivo_a_hoy:,.2f}".replace(",","."))
with k7:
    st.metric("Margen bruto actual (mes)", f"$ {agg['margen_bruto_actual']:,.2f}".replace(",","."))

st.divider()

# Balanzas
bal_fabricado = agg["costo_mo_fabricado"] - objetivo_a_hoy
bal_recuperado = agg["costo_mo_recuperado"] - objetivo_a_hoy

b1, b2 = st.columns(2)
with b1:
    st.subheader("Balanza costo teórico vs fabricado")
    st.metric("Diferencia", f"$ {bal_fabricado:,.2f}".replace(",","."), help="Positivo: por encima del objetivo. Negativo: por debajo.")
with b2:
    st.subheader("Balanza costo teórico vs recuperado por venta")
    st.metric("Diferencia", f"$ {bal_recuperado:,.2f}".replace(",","."), help="Positivo: por encima del objetivo. Negativo: por debajo.")

st.divider()

# Detalle por SKU – Producción y Ventas del mes
st.subheader("Detalle por SKU (mes a hoy)")
left, right = st.columns(2)
with left:
    st.write(":blue[Producción]")
    dfp = agg["prod_by_sku"].copy()
    if not dfp.empty:
        dfp = dfp.sort_values("COSTO_MO_TOTAL", ascending=False)
        st.dataframe(dfp.rename(columns={"CANTIDAD": "Cantidad", "COSTO_MO_UNIT": "Costo MO Unit", "COSTO_MO_TOTAL": "Costo MO Total"}), use_container_width=True)
    else:
        st.caption("Sin producción registrada en el mes.")
with right:
    st.write(":green[Ventas]")
    dfv = agg["ventas_by_sku"].copy()
    if not dfv.empty:
        dfv = dfv.sort_values("COSTO_MO_RECUP", ascending=False)
        st.dataframe(dfv.rename(columns={"CANTIDAD": "Cantidad", "COSTO_MO_UNIT": "Costo MO Unit", "COSTO_MO_RECUP": "Costo MO Recuperado"}), use_container_width=True)
    else:
        st.caption("Sin ventas registradas en el mes.")

st.divider()

with st.expander("🔧 Notas y supuestos"):
    st.markdown(
        """
        - **Costo de mano de obra (MO) unitario**: se calcula combinando `DETALLE_BOM` (cantidades por operación) y `MATERIAL` (costo por operación).
        - **MO fabricado** = Σ(cantidad fabricada por SKU × costo MO unitario del SKU) en el mes hasta hoy.
        - **MO recuperado por ventas** = Σ(cantidad vendida por SKU × costo MO unitario del SKU) en el mes hasta hoy.
        - **Objetivo teórico** = costo mensual / días hábiles del mes × días hábiles transcurridos.
        - Los **días hábiles** se computan Lunes–Viernes (sin feriados); podés ajustar manualmente.
        - `MARGEN_3` se suma tal cual para el **margen bruto actual** del mes.
        - Si en tus datos el campo `CRM` (costo) viene negativo, no afecta los KPI de MO (se usa solo `MARGEN_3` para margen).
        - El enlace de Drive puede guardarse como `DRIVE_FILE_URL` en *Secrets* de Streamlit Cloud.
        """
    )

st.success("Listo. El panel se recalcula cada vez que el Excel de Drive se actualiza.")
