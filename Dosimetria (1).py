# -*- coding: utf-8 -*-
import re, io, requests
import pandas as pd
import streamlit as st
from datetime import datetime
from typing import Any, Dict, List, Optional

# ----------------- Config Ninox -----------------
API_TOKEN   = "edf312a0-98b8-11f0-883e-db77626d62e5"
TEAM_ID     = "YrsYfTegptdZcHJEj"
DATABASE_ID = "ow1geqnkz00e"
BASE_URL    = "https://api.ninox.com/v1"
TABLE_LISTA = "LISTA DE CODIGO"
TABLE_BASE  = "BASE DE DATOS"

# ----------------- UI -----------------
st.set_page_config("Microsievert — Dosimetría", "🧪", layout="wide")
st.title("🧪 Carga y Cruce de Dosis → Ninox (BASE DE DATOS)")

with st.sidebar:
    st.header("Configuración")
    origen_lista = st.radio("Origen de LISTA DE CÓDIGO", ["Ninox", "Archivo"], horizontal=True)
    tabla_lectura  = st.text_input("Tabla de lectura (si es Ninox)", value=TABLE_LISTA)
    tabla_salida   = st.text_input("Tabla de escritura (Ninox)", value=TABLE_BASE)
    subir_pm_texto = st.checkbox("Subir 'PM' como texto (si Hp son Texto en Ninox)", True)
    debug_uno      = st.checkbox("Enviar 1 registro (debug)", False)

# ----------------- Utilidades -----------------
def _ninox_headers():
    return {"Authorization": f"Bearer {API_TOKEN}", "Content-Type": "application/json"}

def ninox_list_tables():
    r = requests.get(f"{BASE_URL}/teams/{TEAM_ID}/databases/{DATABASE_ID}/tables",
                     headers=_ninox_headers(), timeout=30)
    r.raise_for_status()
    return r.json()

def resolve_table_id(table_hint: str) -> str:
    hint = (table_hint or "").strip()
    if hint and " " not in hint and len(hint) <= 8:
        return hint
    for t in ninox_list_tables():
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
    for t in ninox_list_tables():
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

# Normalizador de código WB
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

# ----------------- LISTA DE CÓDIGO: desde Ninox -----------------
def load_lista_from_ninox() -> pd.DataFrame:
    recs = ninox_fetch_all(tabla_lectura)
    rows = []
    for r in recs:
        f = r.get("fields", {}) or {}
        rows.append({
            "PERIODO_NORM": str(f.get("PERIODO DE LECTURA") or "").strip().upper().rstrip("."),
            "CLIENTE": f.get("CLIENTE") or f.get("COMPAÑÍA") or "",
            "CÓDIGO_DOSÍMETRO": f.get("CÓDIGO_DOSÍMETRO") or f.get("CÓDIGO DE DOSÍMETRO") or "",
            "CÓDIGO USUARIO": f.get("CÓDIGO USUARIO") or "",
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

# ----------------- LISTA DE CÓDIGO: desde Archivo -----------------
def detect_hoja_asignar(xls: pd.ExcelFile) -> str:
    for s in xls.sheet_names:
        sl = s.lower()
        if "asignar" in sl and ("dosim" in sl or "dosí" in sl or "dosimétro" in sl or "dosímetro" in sl):
            return s
    # fallback: primera hoja
    return xls.sheet_names[0]

def load_lista_from_file(upload) -> pd.DataFrame:
    name = upload.name.lower()
    if name.endswith((".xlsx", ".xls")):
        xls = pd.ExcelFile(upload)
        sheet = detect_hoja_asignar(xls)
        df = pd.read_excel(xls, sheet_name=sheet)
    else:
        # CSV: intentar auto-delimitador y codificación
        raw = upload.read(); upload.seek(0)
        for enc in ("utf-8-sig","latin-1","utf-16"):
            try:
                df = pd.read_csv(io.BytesIO(raw), sep=None, engine="python", encoding=enc)
                break
            except Exception:
                df = None
        if df is None: df = pd.read_csv(io.BytesIO(raw))

    # normalizar nombres
    norm = (df.columns.astype(str).str.strip().str.lower()
            .str.replace(r"\s+", " ", regex=True))
    df.columns = norm

    def pick(*alts, default=""):
        for a in alts:
            if a.lower() in df.columns: return a.lower()
        return None

    c_periodo = pick("periodo de lectura","periodo","período de lectura","período")
    c_cliente = pick("cliente","compañía","compania")
    c_codigo  = pick("código_dosímetro","codigo_dosímetro","código de dosímetro","codigo de dosímetro",
                     "código","codigo","dosímetro","dosimetro","wb")
    c_codusr  = pick("código usuario","codigo usuario","cod_usuario","código de usuario","codigo de usuario")
    c_nombre  = pick("nombre")
    c_apell   = pick("apellido","apellidos")
    c_ced     = pick("cédula","cedula")
    c_tipo    = pick("tipo de dosímetro","tipo de dosimetro","tipo de dosímetro ")
    c_etq     = pick("etiqueta","tag")

    out = pd.DataFrame()
    out["PERIODO_NORM"]   = df[c_periodo] if c_periodo else ""
    out["CLIENTE"]        = df[c_cliente] if c_cliente else ""
    out["CÓDIGO_DOSÍMETRO"]= df[c_codigo] if c_codigo else ""
    out["CÓDIGO USUARIO"] = df[c_codusr] if c_codusr else ""
    out["NOMBRE"]         = df[c_nombre] if c_nombre else ""
    out["APELLIDO"]       = df[c_apell] if c_apell else ""
    out["CÉDULA"]         = df[c_ced] if c_ced else ""
    out["TIPO DE DOSÍMETRO"]= df[c_tipo] if c_tipo else "CE"
    out["ETIQUETA"]       = df[c_etq] if c_etq else ""

    out["PERIODO_NORM"] = out["PERIODO_NORM"].fillna("").astype(str).str.upper().str.strip().str.rstrip(".")
    out["CODIGO"]       = out["CÓDIGO_DOSÍMETRO"].map(norm_code)
    out["NOMBRE_COMPLETO"] = (out["NOMBRE"].fillna("").astype(str).str.strip() + " " +
                              out["APELLIDO"].fillna("").astype(str).str.strip()).str.strip()
    out["CONTROL_FLAG"] = out.apply(lambda r: any(str(r.get(k,"")).strip().upper() == "CONTROL"
                                                  for k in ["ETIQUETA","NOMBRE","CÉDULA"]), axis=1)
    return out

# ----------------- Dosis -----------------
def leer_dosis(upload) -> Optional[pd.DataFrame]:
    if not upload: return None
    if upload.name.lower().endswith((".xlsx",".xls")):
        df = pd.read_excel(upload)
    else:
        raw = upload.read(); upload.seek(0)
        df = None
        for enc in ("utf-8-sig","latin-1","utf-16"):
            try:
                df = pd.read_csv(io.BytesIO(raw), sep=None, engine="python", encoding=enc); break
            except Exception:
                continue
        if df is None: df = pd.read_csv(io.BytesIO(raw))
    df.columns = df.columns.astype(str).str.strip().str.lower()
    if "dosimeter" not in df.columns:
        for alt in ("dosimetro","codigo","codigodosimetro","codigo_dosimetro","wb","cod"):
            if alt in df.columns: df.rename(columns={alt:"dosimeter"}, inplace=True); break
    for k in ("hp10dose","hp0.07dose","hp3dose"):
        if k not in df.columns: df[k] = 0.0
    df["dosimeter"] = df["dosimeter"].map(norm_code)
    return df

# ----------------- 1) Cargar LISTA -----------------
st.subheader("1) Cargar LISTA DE CÓDIGO")

df_lista = pd.DataFrame()
if origen_lista == "Ninox":
    try:
        df_lista = load_lista_from_ninox()
    except Exception as e:
        st.error(f"No pude leer Ninox: {e}")
else:
    upl_lista = st.file_uploader("Subir LISTA DE CÓDIGO (Excel/CSV). Se buscará hoja 'asignar_DOSÍMETRO…'",
                                 type=["xlsx","xls","csv"], key="upl_lista_codigo")
    if upl_lista is not None:
        try:
            df_lista = load_lista_from_file(upl_lista)
        except Exception as e:
            st.error(f"No pude leer el archivo: {e}")

if df_lista.empty:
    st.warning("LISTA vacía o sin datos")
    st.stop()

st.dataframe(df_lista.head(20), use_container_width=True)

# Filtro de periodos (vacío = todos)
periodos = sorted(df_lista["PERIODO_NORM"].dropna().unique().tolist())
per_sel = st.multiselect("Filtrar por PERIODO DE LECTURA (elige uno o varios, vacío=todos)", periodos, default=[])
df_lista_f = df_lista[df_lista["PERIODO_NORM"].isin(per_sel)] if per_sel else df_lista

# ----------------- 2) Subir Dosis -----------------
st.subheader("2) Subir Archivo de Dosis")
upl_dosis = st.file_uploader("Selecciona CSV/XLS/XLSX (dosis)", type=["csv","xls","xlsx"], key="upl_dosis")
df_dosis = leer_dosis(upl_dosis) if upl_dosis else None
if df_dosis is not None and not df_dosis.empty:
    st.success(f"Dosis cargadas: {len(df_dosis)} fila(s)")
    st.dataframe(df_dosis.head(15), use_container_width=True)

# ----------------- 3) Procesar -----------------
st.subheader("3) Procesar y generar registros")
if st.button("✅ Procesar", type="primary"):
    if df_dosis is None or df_dosis.empty:
        st.error("No hay datos de dosis.")
    else:
        idx_dosis = df_dosis.set_index("dosimeter")
        regs = []
        for _, r in df_lista_f.iterrows():
            code = r["CODIGO"]
            if not code or code not in idx_dosis.index:
                continue
            d = idx_dosis.loc[code]
            if isinstance(d, pd.DataFrame):  # por si hay varias lecturas, usa la última
                d = d.sort_values(by=df_dosis.columns[0]).iloc[-1]
            regs.append({
                "PERIODO DE LECTURA": r["PERIODO_NORM"],
                "COMPAÑÍA": r["CLIENTE"],
                "CÓDIGO DE DOSÍMETRO": code,
                "CÓDIGO USUARIO": r.get("CÓDIGO USUARIO",""),
                "NOMBRE": r["NOMBRE_COMPLETO"],
                "CÉDULA": r["CÉDULA"],
                "FECHA DE LECTURA": "",   # si quieres propagar timestamp, mapea aquí
                "TIPO DE DOSÍMETRO": r["TIPO DE DOSÍMETRO"] or "CE",
                "Hp (10)": float(d.get("hp10dose", 0.0) or 0.0),
                "Hp (0.07)": float(d.get("hp0.07dose", 0.0) or 0.0),
                "Hp (3)": float(d.get("hp3dose", 0.0) or 0.0),
            })
        if regs:
            df_final = pd.DataFrame(regs)
            st.success(f"¡Listo! Registros generados: {len(df_final)}")
            st.dataframe(df_final, use_container_width=True)
            st.session_state["df_final"] = df_final
        else:
            st.warning("⚠️ No hay coincidencias CÓDIGO_DOSÍMETRO ↔ dosimeter (revisa periodos/códigos).")

# ----------------- 4) Subir a Ninox -----------------
st.subheader("4) Subir TODO a Ninox (tabla BASE DE DATOS)")
if st.button("⬆️ Subir a Ninox"):
    df_final = st.session_state.get("df_final")
    if df_final is None or df_final.empty:
        st.error("Primero pulsa 'Procesar'.")
    else:
        fields_exist = set(ninox_get_fields(TABLE_BASE))
        rows, skipped = [], set()
        for _, row in df_final.iterrows():
            payload = {}
            for k, v in row.items():
                if k in fields_exist:
                    payload[k] = str(v)
                else:
                    skipped.add(k)
            rows.append({"fields": payload})
        res = ninox_insert_rows(TABLE_BASE, rows, batch=300)
        if res.get("ok"):
            st.success(f"✅ Subido a Ninox: {res.get('inserted', 0)} registro(s).")
            if skipped:
                st.info("Campos ignorados (no existen en Ninox): " + ", ".join(sorted(skipped)))
        else:
            st.error(f"❌ Error: {res.get('error')}")
