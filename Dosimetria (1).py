import io
import re
import json
import requests
import pandas as pd
import streamlit as st
from datetime import datetime
from typing import List, Dict, Any, Optional

# ============== CONFIG NINOX ==============
API_TOKEN = "edf312a0-98b8-11f0-883e-db77626d62e5"   # <- tu token
TEAM_ID   = "YrsYfTegptdZcHJEj"                      # <- tu team
DB_ID     = "ow1geqnkz00e"                           # <- tu database
TABLE_NAME = "LISTA DE CODIGO"                       # <- tabla con CÓDIGO_DOSÍMETRO y PERIODO DE LECTURA

API_BASE = "https://api.ninoxdb.de/v1"

# ============== UTILIDADES ==============
SPANISH_MONTHS = {
    "ENERO":1,"ENE":1,
    "FEBRERO":2,"FEB":2,
    "MARZO":3,"MAR":3,
    "ABRIL":4,"ABR":4,
    "MAYO":5,"MAY":5,
    "JUNIO":6,"JUN":6,
    "JULIO":7,"JUL":7,
    "AGOSTO":8,"AGO":8,
    "SEPTIEMBRE":9,"SEP":9,"SEPT":9,
    "OCTUBRE":10,"OCT":10,
    "NOVIEMBRE":11,"NOV":11,
    "DICIEMBRE":12,"DIC":12
}

def normalize_period_str(period_text: str) -> Optional[tuple]:
    """
    Acepta 'AGOSTO 2025', 'AGO 2025', '08/2025', '2025-08', 'Agosto-2025', etc.
    Retorna (year, month) o None si no se puede parsear.
    """
    if not isinstance(period_text, str):
        return None
    t = period_text.strip().upper().replace(".", " ").replace("-", " ").replace("/", " ")
    t = re.sub(r"\s+", " ", t)

    # Formato YYYY MM
    m = re.match(r"^(20\d{2})\s+(\d{1,2})$", t)
    if m:
        y = int(m.group(1)); mm = int(m.group(2))
        if 1 <= mm <= 12: return (y, mm)

    # Formato MM YYYY
    m = re.match(r"^(\d{1,2})\s+(20\d{2})$", t)
    if m:
        mm = int(m.group(1)); y = int(m.group(2))
        if 1 <= mm <= 12: return (y, mm)

    # Formato MES YYYY (en español)
    parts = t.split(" ")
    if len(parts) == 2 and parts[0] in SPANISH_MONTHS and parts[1].isdigit():
        y = int(parts[1]); mm = SPANISH_MONTHS[parts[0]]
        return (y, mm)

    return None

def period_from_date(dt: datetime) -> tuple:
    return (dt.year, dt.month)

def fmt_period_es(year: int, month: int) -> str:
    # Devuelve algo como "AGO 2025"
    inv = {v:k for k,v in SPANISH_MONTHS.items() if len(k)==3}  # abreviaturas
    name = inv.get(month)
    return f"{name} {year}" if name else f"{month:02d}/{year}"

# ============== NINOX CLIENT ==============
def ninox_headers() -> Dict[str, str]:
    return {"Authorization": f"Bearer {API_TOKEN}", "Content-Type": "application/json"}

def fetch_table_records(table_name: str, limit: int = 10000) -> pd.DataFrame:
    """
    Lee todos los registros de una tabla de Ninox.
    NOTA: Ninox puede paginar en 10k; si necesitas más, implementa paginación adicional.
    """
    url = f"{API_BASE}/teams/{TEAM_ID}/databases/{DB_ID}/tables/{requests.utils.quote(table_name, safe='')}/records"
    params = {"limit": limit}
    r = requests.get(url, headers=ninox_headers(), params=params)
    if r.status_code != 200:
        raise RuntimeError(f"Error Ninox GET /tables/{table_name}/records: {r.status_code} — {r.text}")

    rows = r.json()  # lista de dicts con campos en 'fields'
    # Aplana 'fields'
    flat = []
    for rec in rows:
        fields = rec.get("fields", {})
        fields["_id"] = rec.get("id")
        flat.append(fields)
    df = pd.DataFrame(flat)
    return df

# ============== CSV NORMALIZACIÓN ==============
CSV_RENAMES = {
    "timestamp": "FECHA DE LECTURA",
    "dosimeter": "CÓDIGO_DOSÍMETRO",
    "Hp (10) dose corr.": "Hp (10)",
    "Hp (0.07) dose corr.": "Hp (0.07)",
    "Hp (3) dose corr.": "Hp (3)",
}

def normalize_csv(df: pd.DataFrame) -> pd.DataFrame:
    # Renombrar columnas clave si existen
    cols = {c: CSV_RENAMES.get(c, c) for c in df.columns}
    df = df.rename(columns=cols)

    # Parsear fecha
    if "FECHA DE LECTURA" in df.columns:
        def _parse(x):
            if pd.isna(x): return pd.NaT
            try:
                return pd.to_datetime(x, errors="coerce", utc=False)
            except Exception:
                return pd.NaT
        df["FECHA DE LECTURA"] = df["FECHA DE LECTURA"].apply(_parse)

        # Periodo derivado del timestamp
        df["PERIODO (CSV)"] = df["FECHA DE LECTURA"].apply(
            lambda d: fmt_period_es(d.year, d.month) if pd.notna(d) else None
        )

    # Asegurar dosímetro string limpio
    if "CÓDIGO_DOSÍMETRO" in df.columns:
        df["CÓDIGO_DOSÍMETRO"] = df["CÓDIGO_DOSÍMETRO"].astype(str).str.strip()

    return df

# ============== APP ==============
st.set_page_config(page_title="Cruce Ninox ↔ Dosis", layout="wide")
st.title("Cruce de dosis con Ninox")

with st.sidebar:
    st.header("Conexión Ninox")
    token_in = st.text_input("API Token", value=API_TOKEN, type="password")
    team_in  = st.text_input("Team ID", value=TEAM_ID)
    db_in    = st.text_input("Database ID", value=DB_ID)
    table_in = st.text_input("Tabla (Ninox)", value=TABLE_NAME)

    st.markdown("---")
    period_text = st.text_input("Periodo a filtrar (p. ej. 'AGOSTO 2025' o 'AGO 2025')", value="AGOSTO 2025")
    periodo = normalize_period_str(period_text)
    if not periodo:
        st.warning("Escribe un periodo válido (ej: 'AGOSTO 2025', 'AGO 2025', '08 2025', '2025 08').")
    else:
        st.caption(f"Periodo entendido como: {fmt_period_es(periodo[0], periodo[1])}")

    st.markdown("---")
    st.caption("Sube uno o varios CSV de dosis:")
    files = st.file_uploader("CSV(s) con columnas: timestamp, dosimeter, Hp (10) dose corr., Hp (0.07) dose corr., Hp (3) dose corr.", type=["csv"], accept_multiple_files=True)

# Actualiza credenciales en tiempo de ejecución
API_TOKEN = token_in or API_TOKEN
TEAM_ID   = team_in  or TEAM_ID
DB_ID     = db_in    or DB_ID
TABLE_NAME = table_in or TABLE_NAME

# 1) Conexión y preview de Ninox
st.subheader("1) Lectura de Ninox")
ninox_ok = False
ninox_df = pd.DataFrame()
try:
    ninox_df = fetch_table_records(TABLE_NAME)
    ninox_ok = True
    st.success(f"Conectado a Ninox: {TABLE_NAME} — {len(ninox_df)} registros")
    st.dataframe(ninox_df.head(20), use_container_width=True)
except Exception as e:
    st.error(str(e))

# 2) Subir y normalizar CSVs
st.subheader("2) CSV(s) de dosis — normalización")
csv_df = pd.DataFrame()
if files:
    dfs = []
    for f in files:
        try:
            df = pd.read_csv(f)
        except UnicodeDecodeError:
            f.seek(0)
            df = pd.read_csv(f, encoding="latin1")
        df = normalize_csv(df)
        df["_archivo_origen"] = f.name
        dfs.append(df)
        st.caption(f"Archivo: **{f.name}** — filas: {len(df)}")
        st.dataframe(df.head(10), use_container_width=True)
    if dfs:
        csv_df = pd.concat(dfs, ignore_index=True)
else:
    st.info("Sube al menos un CSV para continuar.")

# 3) Cruce por CÓDIGO_DOSÍMETRO y filtrado por PERIODO
st.subheader("3) Cruce y filtrado por periodo")
if ninox_ok and not csv_df.empty and periodo:
    # Limpieza nombres Ninox
    if "CÓDIGO_DOSÍMETRO" not in ninox_df.columns:
        # Buscar columna candidata
        cand = [c for c in ninox_df.columns if "DOSÍMETRO" in c.upper()]
        if cand:
            ninox_df = ninox_df.rename(columns={cand[0]: "CÓDIGO_DOSÍMETRO"})
    if "PERIODO DE LECTURA" not in ninox_df.columns:
        cand = [c for c in ninox_df.columns if "PERIODO" in c.upper() and "LECT" in c.upper()]
        if cand:
            ninox_df = ninox_df.rename(columns={cand[0]: "PERIODO DE LECTURA"})

    # Normalizar PERIODO DE LECTURA de Ninox a (year, month)
    def parse_periodo_ninox(x):
        y_m = normalize_period_str(str(x)) if pd.notna(x) else None
        return y_m
    ninox_df["__PERIODO_YM__"] = ninox_df["PERIODO DE LECTURA"].apply(parse_periodo_ninox)

    # Periodo objetivo
    tgt_y, tgt_m = periodo

    ninox_f = ninox_df[ninox_df["__PERIODO_YM__"] == (tgt_y, tgt_m)].copy()

    # Join por dosímetro
    left = csv_df.copy()
    right = ninox_f.copy()

    # Asegurar tipos
    left["CÓDIGO_DOSÍMETRO"] = left["CÓDIGO_DOSÍMETRO"].astype(str).str.strip()
    right["CÓDIGO_DOSÍMETRO"] = right["CÓDIGO_DOSÍMETRO"].astype(str).str.strip()

    merged = pd.merge(
        left,
        right,
        on="CÓDIGO_DOSÍMETRO",
        how="left",
        suffixes=("_CSV", "_NINOX")
    )

    # Filtrar filas cuyo timestamp cae en el mismo periodo (seguridad doble)
    merged["__TS_YM__"] = merged["FECHA DE LECTURA"].apply(lambda d: (d.year, d.month) if pd.notna(d) else None)
    merged = merged[(merged["__TS_YM__"] == (tgt_y, tgt_m))]

    # Reordenar columnas principales
    cols_front = [
        "CÓDIGO_DOSÍMETRO",
        "FECHA DE LECTURA",
        "Hp (10)", "Hp (0.07)", "Hp (3)",
        "PERIODO (CSV)",
        "PERIODO DE LECTURA",
    ]
    front = [c for c in cols_front if c in merged.columns]
    merged = merged[front + [c for c in merged.columns if c not in front]]

    st.success(f"Coincidencias en {fmt_period_es(tgt_y, tgt_m)}: {len(merged)} filas")
    st.dataframe(merged.head(30), use_container_width=True)

    # No coincidencias por dosímetro
    not_matched = left[~left["CÓDIGO_DOSÍMETRO"].isin(right["CÓDIGO_DOSÍMETRO"])]
    if not not_matched.empty:
        st.warning(f"⚠️ {len(not_matched)} fila(s) sin coincidencia por CÓDIGO_DOSÍMETRO en Ninox (para el mismo periodo).")
        st.dataframe(not_matched.head(20), use_container_width=True)

else:
    st.info("A la espera de conexión a Ninox, CSVs y un periodo válido.")

st.markdown("---")
st.caption("Tip: puedes cambiar el periodo en la barra lateral (por ejemplo, 'AGO 2025') para rehacer el cruce.")
