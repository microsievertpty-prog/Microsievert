# -*- coding: utf-8 -*-
import io, re, requests
import pandas as pd
import streamlit as st
from datetime import datetime
from typing import Any, Dict, List, Optional

# ===================== NINOX CONFIG =====================
API_TOKEN   = "edf312a0-98b8-11f0-883e-db77626d62e5"
TEAM_ID     = "YrsYfTegptdZcHJEj"
DATABASE_ID = "ow1geqnkz00e"
BASE_URL    = "https://api.ninox.com/v1"

TABLE_LISTA = "LISTA DE CODIGO"
TABLE_BASE  = "BASE DE DATOS"

# ===================== STREAMLIT BASE =====================
st.set_page_config("Microsievert — Dosimetría", "🧪", layout="wide")
st.title("🧪 Carga y Cruce de Dosis → Ninox (BASE DE DATOS)")

with st.sidebar:
    st.header("Configuración")
    origen_lista = st.radio("Origen de LISTA DE CÓDIGO", ["Ninox", "Archivo"], horizontal=True)
    tabla_lectura  = st.text_input("Tabla de lectura (si es Ninox)", value=TABLE_LISTA)
    tabla_salida   = st.text_input("Tabla de escritura (Ninox)", value=TABLE_BASE)
    subir_pm_texto = st.checkbox("Subir 'PM' como texto (si Hp son Texto en Ninox)", True)
    debug_uno      = st.checkbox("Enviar 1 registro (debug)", False)

# ===================== Helpers =====================
def _ninox_headers():
    return {"Authorization": f"Bearer {API_TOKEN}", "Content-Type": "application/json"}

def ninox_list_tables(team_id: str, db_id: str):
    r = requests.get(f"{BASE_URL}/teams/{team_id}/databases/{db_id}/tables", headers=_ninox_headers(), timeout=30)
    r.raise_for_status()
    return r.json()

def resolve_table_id(table_hint: str) -> str:
    hint = (table_hint or "").strip()
    if hint and " " not in hint and len(hint) <= 8:
        return hint
    for t in ninox_list_tables(TEAM_ID, DATABASE_ID):
        if str(t.get("name","")).strip().lower() == hint.lower():
            return str(t.get("id"))
        if str(t.get("id","")) == hint:
            return hint
    return hint

def ninox_fetch_all(table_hint: str, pagesize: int = 1000) -> List[Dict[str,Any]]:
    table_id = resolve_table_id(table_hint)
    url = f"{BASE_URL}/teams/{TEAM_ID}/databases/{DATABASE_ID}/tables/{table_id}/records"
    out, skip = [], 0
    while True:
        r = requests.get(url, headers=_ninox_headers(), params={"limit": pagesize, "skip": skip}, timeout=60)
        if r.status_code == 404:
            raise RuntimeError(f"No encuentro la tabla '{table_hint}' (ID resuelto: {table_id})")
        r.raise_for_status()
        chunk = r.json()
        if not chunk: break
        out.extend(chunk)
        if len(chunk) < pagesize: break
        skip += pagesize
    return out

def ninox_get_fields(table_hint: str) -> List[str]:
    table_id = resolve_table_id(table_hint)
    info = ninox_list_tables(TEAM_ID, DATABASE_ID)
    for t in info:
        if str(t.get("id")) == str(table_id):
            cols = t.get("fields") or t.get("columns") or []
            return [c.get("name") for c in cols if isinstance(c, dict)]
    return []

def ninox_insert_rows(table_hint: str, rows: List[Dict[str,Any]], batch: int = 400) -> Dict[str,Any]:
    table_id = resolve_table_id(table_hint)
    url = f"{BASE_URL}/teams/{TEAM_ID}/databases/{DATABASE_ID}/tables/{table_id}/records"
    if not rows: return {"ok": True, "inserted": 0}
    inserted = 0
    for i in range(0, len(rows), batch):
        chunk = rows[i:i+batch]
        r = requests.post(url, headers=_ninox_headers(), json=chunk, timeout=60)
        if r.status_code != 200:
            return {"ok": False, "inserted": inserted, "error": f"{r.status_code} {r.text}"}
        inserted += len(chunk)
    return {"ok": True, "inserted": inserted}

# ----------------- Normalizadores -----------------
def norm_code(x: Any) -> str:
    if x is None: return ""
    s = str(x).strip().upper().replace("\u00A0"," ")
    s = re.sub(r"[^A-Z0-9]", "", s)
    if re.fullmatch(r"WB\d{6}", s): return s
    m = re.fullmatch(r"WB(\d+)", s)
    if m: return f"WB{int(m.group(1)):06d}"
    m2 = re.fullmatch(r"(\d+)", s)
    if m2: return f"WB{int(m2.group(1)):06d}"
    return s

# ===================== LISTA DE CÓDIGO =====================
def load_lista_from_ninox() -> pd.DataFrame:
    recs = ninox_fetch_all(tabla_lectura)
    rows = []
    for r in recs:
        f = r.get("fields", {}) or {}
        rows.append({
            "PERIODO_NORM": str(f.get("PERIODO DE LECTURA") or "").strip().upper().rstrip("."),
            "COMPAÑÍA": f.get("CLIENTE") or f.get("COMPAÑÍA") or "",
            "CÓDIGO_DOSÍMETRO": f.get("CÓDIGO_DOSÍMETRO") or f.get("CÓDIGO DE DOSÍMETRO") or "",
            "CÓDIGO_USUARIO": f.get("CÓDIGO USUARIO") or "",
            "NOMBRE": f.get("NOMBRE") or "",
            "APELLIDO": f.get("APELLIDO") or "",
            "CÉDULA": f.get("CÉDULA") or "",
            "TIPO DE DOSÍMETRO": f.get("TIPO DE DOSÍMETRO") or "CE",
            "ETIQUETA": f.get("ETIQUETA") or "",
        })
    df = pd.DataFrame(rows)
    if df.empty: return df
    df["CODIGO"] = df["CÓDIGO_DOSÍMETRO"].map(norm_code)
    df["NOMBRE_COMPLETO"] = (df["NOMBRE"].fillna("").astype(str).str.strip() + " " +
                             df["APELLIDO"].fillna("").astype(str).str.strip()).str.strip()
    df["CONTROL_FLAG"] = df.apply(lambda r: any(str(r.get(k,"")).strip().upper() == "CONTROL"
                                                for k in ["ETIQUETA","NOMBRE","CÉDULA"]), axis=1)
    return df

# ===================== Dosis =====================
def leer_dosis(upload) -> Optional[pd.DataFrame]:
    if not upload: return None
    df = pd.read_excel(upload) if upload.name.lower().endswith(("xlsx","xls")) else pd.read_csv(upload)
    df.columns = df.columns.str.strip().str.lower()
    if "dosimeter" not in df.columns:
        for alt in ("dosimetro","codigo","codigodosimetro","codigo_dosimetro","wb","cod"):
            if alt in df.columns: df.rename(columns={alt:"dosimeter"}, inplace=True); break
    for k in ("hp10dose","hp0.07dose","hp3dose"):
        if k not in df.columns: df[k] = 0.0
    df["dosimeter"] = df["dosimeter"].map(norm_code)
    return df

# ===================== Procesamiento =====================
st.subheader("1) Cargar LISTA DE CÓDIGO")
df_lista = load_lista_from_ninox() if origen_lista=="Ninox" else pd.DataFrame()
if df_lista.empty:
    st.warning("LISTA vacía o sin datos")
    st.stop()

st.dataframe(df_lista.head(10))

# Filtros
periodos = sorted(df_lista["PERIODO_NORM"].dropna().unique().tolist())
per_sel = st.multiselect("Filtrar por PERIODO", periodos, default=[])
df_lista_f = df_lista[df_lista["PERIODO_NORM"].isin(per_sel)] if per_sel else df_lista

# Subir dosis
st.subheader("2) Subir Dosis")
upl_dosis = st.file_uploader("CSV/XLSX dosis", type=["csv","xls","xlsx"])
df_dosis = leer_dosis(upl_dosis) if upl_dosis else None
if df_dosis is not None and not df_dosis.empty:
    st.dataframe(df_dosis.head(10))

# Procesar
if st.button("✅ Procesar"):
    if df_dosis is None or df_dosis.empty:
        st.error("No hay datos de dosis.")
    else:
        idx_dosis = df_dosis.set_index("dosimeter")
        regs = []
        for _, r in df_lista_f.iterrows():
            code = r["CODIGO"]
            if code in idx_dosis.index:
                d = idx_dosis.loc[code]
                regs.append({
                    "PERIODO DE LECTURA": r["PERIODO_NORM"],
                    "COMPAÑÍA": r["COMPAÑÍA"],
                    "CÓDIGO DE DOSÍMETRO": code,
                    "CÓDIGO USUARIO": r["CÓDIGO_USUARIO"],
                    "NOMBRE": r["NOMBRE_COMPLETO"],
                    "CÉDULA": r["CÉDULA"],
                    "TIPO DE DOSÍMETRO": r["TIPO DE DOSÍMETRO"],
                    "Hp(10)": d.get("hp10dose",0.0),
                    "Hp(0.07)": d.get("hp0.07dose",0.0),
                    "Hp(3)": d.get("hp3dose",0.0),
                })
        if regs:
            df_final = pd.DataFrame(regs)
            st.success(f"{len(df_final)} registros generados")
            st.dataframe(df_final)
            st.session_state["df_final"] = df_final
        else:
            st.warning("⚠️ No hubo coincidencias")

# Subir a Ninox
if st.button("⬆️ Subir a Ninox"):
    df_final = st.session_state.get("df_final")
    if df_final is None or df_final.empty:
        st.error("Primero Procesa los datos.")
    else:
        fields_exist = set(ninox_get_fields(tabla_salida))
        rows = []
        for _, row in df_final.iterrows():
            payload = {k: str(v) for k,v in row.items() if k in fields_exist}
            rows.append({"fields": payload})
        res = ninox_insert_rows(tabla_salida, rows)
        if res.get("ok"):
            st.success(f"✅ {res['inserted']} registros subidos a Ninox")
        else:
            st.error(f"❌ Error: {res['error']}")
