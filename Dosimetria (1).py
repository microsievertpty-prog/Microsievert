import io, re, requests, pandas as pd, streamlit as st
from datetime import datetime
from io import BytesIO
from dateutil.parser import parse as dtparse
from typing import List, Any, Optional, Set

from openpyxl import Workbook
from openpyxl.styles import Font, Alignment, Border, Side, PatternFill
from openpyxl.utils import get_column_letter, column_index_from_string
from openpyxl.drawing.image import Image as XLImage

# ===================== NINOX CONFIG =====================
API_TOKEN   = "edf312a0-98b8-11f0-883e-db77626d62e5"
TEAM_ID     = "YrsYfTegptdZcHJEj"
DATABASE_ID = "ow1geqnkz00e"
BASE_URL    = "https://api.ninox.com/v1"

DEFAULT_BASE_TABLE_ID   = "J"  # LISTA DE CÓDIGO
DEFAULT_REPORT_TABLE_ID = "C"  # REPORTE

# ===================== STREAMLIT CONFIG =====================
st.set_page_config(page_title="Microsievert - Dosimetría", page_icon="🧪", layout="wide")
st.title("🧪 Sistema de Gestión de Dosimetría — Microsievert")
st.caption("Ninox + Procesamiento VALOR − CONTROL + Reporte Actual/Anual/Vida + Exportación")

if "df_final" not in st.session_state:
    st.session_state.df_final = None

# ===================== Ninox helpers =====================
def ninox_headers():
    return {"Authorization": f"Bearer {API_TOKEN}", "Content-Type": "application/json"}

def fetch_ninox_records(table_id: str, timeout: int = 10) -> pd.DataFrame:
    url = f"{BASE_URL}/teams/{TEAM_ID}/databases/{DATABASE_ID}/tables/{table_id}/records"
    out, offset = [], 0
    while True:
        r = requests.get(url, headers=ninox_headers(),
                         params={"perPage": 1000, "offset": offset},
                         timeout=timeout)
        r.raise_for_status()
        batch = r.json()
        if not batch: break
        out.extend(batch)
        if len(batch) < 1000: break
        offset += 1000
    rows = [x.get("fields", {}) for x in out]
    return pd.DataFrame(rows) if rows else pd.DataFrame()

# ===================== Aux =====================
def _find_codigo_field(cols) -> Optional[str]:
    targets = {"CÓDIGO DE DOSÍMETRO","CÓDIGO_DOSÍMETRO","CODIGO DE DOSIMETRO","CODIGO_DOSIMETRO"}
    for c in cols:
        if str(c).strip().upper() in targets:
            return c
    return None

# ===================== Construcción registros =====================
def construir_registros(dfp, dfd, periodo_filtro="— TODOS —"):
    registros = []
    pf = (periodo_filtro or "").strip().upper()
    codigo_field = _find_codigo_field(dfp.columns)
    usa_nuevo = (codigo_field is not None) and ("PERIODO DE LECTURA" in dfp.columns)

    if usa_nuevo:
        for need in ["NOMBRE","APELLIDO","CÉDULA","COMPAÑÍA","TIPO DE DOSÍMETRO"]:
            if need not in dfp.columns: dfp[need] = ""
        for _, fila in dfp.iterrows():
            cod = str(fila.get(codigo_field,"")).strip().upper()
            per = str(fila.get("PERIODO DE LECTURA","")).strip().upper()
            if not cod or cod == "NAN": continue
            if pf not in ("", "— TODOS —") and per != pf and not per.startswith("CONTROL"):
                continue
            row = dfd.loc[dfd["dosimeter"].astype(str).str.upper() == cod]
            if row.empty: continue
            r0 = row.iloc[0]
            fecha = r0.get("timestamp", pd.NaT)
            fecha_str = ""
            if pd.notna(fecha):
                try: fecha_str = pd.to_datetime(fecha).strftime("%d/%m/%Y %H:%M")
                except: pass
            nombre_raw = f"{str(fila.get('NOMBRE','')).strip()} {str(fila.get('APELLIDO','')).strip()}".strip()
            registros.append({
                "PERIODO DE LECTURA": per or "CONTROL",
                "COMPAÑÍA": fila.get("COMPAÑÍA",""),
                "CÓDIGO_DOSÍMETRO": cod,
                "NOMBRE": nombre_raw or str(fila.get("CÓDIGO USUARIO","") or ""),
                "CÉDULA": fila.get("CÉDULA",""),
                "FECHA DE LECTURA": fecha_str,
                "TIPO DE DOSÍMETRO": fila.get("TIPO DE DOSÍMETRO","CE") or "CE",
                "Hp(10)": float(r0.get("hp10dose",0.0)),
                "Hp(0.07)": float(r0.get("hp0.07dose",0.0)),
                "Hp(3)": float(r0.get("hp3dose",0.0)),
            })
    else:
        # aquí iría la rama de esquema viejo si aún la usas
        pass
    return registros

# ===================== TABS =====================
tab1, tab2 = st.tabs(["📥 Carga y Subida a Ninox","📊 Reporte desde Ninox"])

with tab1:
    st.subheader("📤 Cargar archivo de Dosis y cruzar con LISTA DE CÓDIGO (Ninox)")
    with st.sidebar:
        st.markdown("### ⚙️ Configuración (TAB 1)")
        base_table_id   = st.text_input("Table ID LISTA DE CÓDIGO", value=DEFAULT_BASE_TABLE_ID, key="tab1_base")
        report_table_id = st.text_input("Table ID REPORTE", value=DEFAULT_REPORT_TABLE_ID, key="tab1_report")
        periodo_filtro  = st.text_input("Filtro PERIODO (opcional)", value="— TODOS —", key="tab1_per")
        subir_pm_como_texto = st.checkbox("Subir 'PM' como TEXTO", value=True, key="tab1_pm_texto")
        debug_uno = st.checkbox("Enviar 1 registro (debug)", value=False, key="tab1_debug")

    # --- Conexión manual a Ninox ---
    st.markdown("#### Conexión a Ninox (LISTA DE CÓDIGO)")
    col_a,col_b = st.columns([1,1])
    with col_a: do_connect = st.button("🔌 Conectar a Ninox ahora", use_container_width=True)
    with col_b: short_timeout = st.number_input("Timeout (seg)", 3,30,5,1)

    df_participantes = None
    if do_connect:
        with st.spinner("Conectando…"):
            try:
                df_participantes = fetch_ninox_records(base_table_id, timeout=short_timeout)
                if df_participantes.empty: st.warning("Conectado, pero la tabla está vacía.")
                else: 
                    st.success(f"Conectado a Ninox. Filas: {len(df_participantes)}")
                    st.dataframe(df_participantes.head(10), use_container_width=True)
            except requests.Timeout:
                st.error("⏱️ Ninox tardó demasiado en responder. Intenta de nuevo o sube el archivo sin conexión.")
            except Exception as e:
                st.error(f"❌ Error: {e}")
    st.session_state.df_participantes = df_participantes

    # Archivo de dosis
    st.markdown("#### Archivo de Dosis")
    upload = st.file_uploader("Selecciona CSV/XLS/XLSX", type=["csv","xls","xlsx"])
    if upload:
        try:
            df_dosis = pd.read_csv(upload, sep=";", engine="python")
        except: 
            upload.seek(0)
            df_dosis = pd.read_csv(upload)
        st.dataframe(df_dosis.head(), use_container_width=True)


