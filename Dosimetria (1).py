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

TABLE_LISTA = "LISTA DE CODIGO"   # lectura (si eliges Ninox)
TABLE_BASE  = "BASE DE DATOS"     # escritura (salida)

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

@st.cache_data(ttl=300, show_spinner=False)
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

@st.cache_data(ttl=300, show_spinner=False)
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
            out = []
            for c in cols:
                nm = c.get("name") if isinstance(c, dict) else None
                if nm: out.append(nm)
            return out
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

def as_value(v: Any):
    if v is None: return ""
    s = str(v).strip().replace(",", ".")
    if s.upper() == "PM": return "PM"
    try: return float(s)
    except: return s

def as_num(v: Any) -> float:
    if v is None: return 0.0
    s = str(v).strip().replace(",", ".")
    if s == "" or s.upper() == "PM": return 0.0
    try: return float(s)
    except: return 0.0

# ===================== LISTA DE CÓDIGO: desde Ninox =====================
def load_lista_from_ninox() -> pd.DataFrame:
    recs = ninox_fetch_all(tabla_lectura)
    rows = []
    for r in recs:
        f = r.get("fields", {}) or {}
        rows.append({
            "PERIODO_NORM": str(f.get("PERIODO DE LECTURA") or "").strip().upper().rstrip("."),
            "COMPAÑÍA": f.get("CLIENTE") or f.get("COMPAÑÍA") or "",
            "CÓDIGO_DOSÍMETRO": f.get("CÓDIGO_DOSÍMETRO") or f.get("CÓDIGO DE DOSÍMETRO") or "",
            "NOMBRE": f.get("NOMBRE") or "",
            "APELLIDO": f.get("APELLIDO") or "",
            "CÉDULA": f.get("CÉDULA") or "",
            "TIPO DE DOSÍMETRO": f.get("TIPO DE DOSÍMETRO") or "CE",
            "ETIQUETA": f.get("ETIQUETA") or "",
        })
    df = pd.DataFrame(rows)
    if df.empty:
        return pd.DataFrame(columns=["PERIODO_NORM","COMPAÑÍA","CÓDIGO_DOSÍMETRO","NOMBRE","APELLIDO","CÉDULA","TIPO DE DOSÍMETRO","ETIQUETA","CODIGO","NOMBRE_COMPLETO","CONTROL_FLAG"])
    df["CODIGO"] = df["CÓDIGO_DOSÍMETRO"].map(norm_code)
    df["NOMBRE_COMPLETO"] = (df["NOMBRE"].fillna("").astype(str).str.strip() + " " +
                             df["APELLIDO"].fillna("").astype(str).str.strip()).str.strip()
    df["CONTROL_FLAG"] = df.apply(lambda r: any(str(r.get(k,"")).strip().upper() == "CONTROL"
                                                for k in ["ETIQUETA","NOMBRE","CÉDULA"]), axis=1)
    return df

# ===================== LISTA DE CÓDIGO: desde Archivo =====================
def load_lista_from_file(upload, sheet_name: Optional[str]) -> pd.DataFrame:
    if upload is None:
        return pd.DataFrame()
    name = upload.name.lower()
    # leer
    if name.endswith((".xlsx",".xls")):
        df_raw = pd.read_excel(upload, sheet_name=sheet_name) if sheet_name else pd.read_excel(upload)
    else:
        raw = upload.read(); upload.seek(0)
        for enc in ("utf-8-sig","latin-1","cp1252"):
            try:
                df_raw = pd.read_csv(io.BytesIO(raw), sep=None, engine="python", encoding=enc)
                break
            except Exception:
                df_raw = None
        if df_raw is None:
            df_raw = pd.read_csv(io.BytesIO(raw))
    if df_raw is None or df_raw.empty:
        return pd.DataFrame()

    # normalizar cabeceras
    def norm_cols(s: str) -> str:
        s = s.strip().lower()
        s = s.replace("á","a").replace("é","e").replace("í","i").replace("ó","o").replace("ú","u").replace("ñ","n")
        s = s.replace(" ", "").replace(".", "").replace("(", "").replace(")", "")
        return s
    cols_norm = {c: norm_cols(str(c)) for c in df_raw.columns}
    df_raw.rename(columns=cols_norm, inplace=True)

    # mapear campos esperados
    def pick(*alts):
        for a in alts:
            if a in df_raw.columns: return a
        return None

    col_periodo = pick("periododelectura","periodolectura","periodo","periodo_de_lectura","periodo_delectura")
    col_cliente = pick("cliente","compania","compania_","companiaempresa")
    col_codigo  = pick("codigodosimetro","codigo_dosimetro","codigodosimetro_","codigodosimetroasignado","codigodosimetroasignar","wb","codigo")
    col_nombre  = pick("nombre","nombres")
    col_apell   = pick("apellido","apellidos")
    col_cedula  = pick("cedula","cédula","id","identificacion")
    col_tipo    = pick("tipodedosimetro","tipo","tipodosimetro")
    col_etiqueta= pick("etiqueta","tag")

    df = pd.DataFrame({
        "PERIODO_NORM": df_raw[col_periodo] if col_periodo else "",
        "COMPAÑÍA": df_raw[col_cliente] if col_cliente else "",
        "CÓDIGO_DOSÍMETRO": df_raw[col_codigo] if col_codigo else "",
        "NOMBRE": df_raw[col_nombre] if col_nombre else "",
        "APELLIDO": df_raw[col_apell] if col_apell else "",
        "CÉDULA": df_raw[col_cedula] if col_cedula else "",
        "TIPO DE DOSÍMETRO": df_raw[col_tipo] if col_tipo else "CE",
        "ETIQUETA": df_raw[col_etiqueta] if col_etiqueta else "",
    })
    df["PERIODO_NORM"] = df["PERIODO_NORM"].fillna("").astype(str).str.strip().str.upper().str.rstrip(".")
    df["COMPAÑÍA"] = df["COMPAÑÍA"].fillna("").astype(str).str.strip()
    df["CODIGO"] = df["CÓDIGO_DOSÍMETRO"].map(norm_code)
    df["NOMBRE_COMPLETO"] = (df["NOMBRE"].fillna("").astype(str).str.strip() + " " +
                             df["APELLIDO"].fillna("").astype(str).str.strip()).str.strip()
    df["CÉDULA"] = df["CÉDULA"].fillna("").astype(str).str.strip()
    df["TIPO DE DOSÍMETRO"] = df["TIPO DE DOSÍMETRO"].fillna("").astype(str).str.strip()
    df["ETIQUETA"] = df["ETIQUETA"].fillna("").astype(str).str.strip()
    df["CONTROL_FLAG"] = df.apply(lambda r: any(str(r.get(k,"")).strip().upper() == "CONTROL"
                                                for k in ["ETIQUETA","NOMBRE","CÉDULA"]), axis=1)
    return df

# ===================== Dosis (archivo) =====================
def leer_dosis(upload) -> Optional[pd.DataFrame]:
    if not upload: return None
    name = upload.name.lower()
    if name.endswith((".xlsx",".xls")):
        df = pd.read_excel(upload)
    else:
        raw = upload.read(); upload.seek(0)
        for enc in ("utf-8-sig","latin-1","cp1252"):
            try:
                df = pd.read_csv(io.BytesIO(raw), sep=None, engine="python", encoding=enc); break
            except Exception: df = None
        if df is None:
            df = pd.read_csv(io.BytesIO(raw))
    norm = (df.columns.astype(str).str.strip().str.lower()
            .str.replace(" ", "", regex=False)
            .str.replace("(", "").str.replace(")", "").str.replace(".", "", regex=False))
    df.columns = norm
    if "dosimeter" not in df.columns:
        for alt in ("dosimetro","codigo","codigodosimetro","codigo_dosimetro","wb","cod"):
            if alt in df.columns: df.rename(columns={alt:"dosimeter"}, inplace=True); break
    for src, dst in [("hp10dosecorr","hp10dose"),("hp10","hp10dose"),
                     ("hp007dosecorr","hp0.07dose"),("hp007","hp0.07dose"),
                     ("hp3dosecorr","hp3dose"),("hp3","hp3dose")]:
        if src in df.columns and dst not in df.columns:
            df.rename(columns={src:dst}, inplace=True)
    for k in ("hp10dose","hp0.07dose","hp3dose"):
        if k in df.columns: df[k] = pd.to_numeric(df[k], errors="coerce").fillna(0.0)
        else: df[k] = 0.0
    if "dosimeter" in df.columns:
        df["dosimeter"] = df["dosimeter"].map(norm_code)
    if "timestamp" in df.columns:
        df["timestamp"] = pd.to_datetime(df["timestamp"], errors="coerce")
    return df

# ===================== Valor - Control =====================
def periodo_desde_fecha(per_str: str, fecha_str: str) -> str:
    per = (per_str or "").strip().upper().rstrip(".")
    if per and per != "CONTROL": return per
    if not fecha_str: return per
    try:
        d = pd.to_datetime(fecha_str, dayfirst=True, errors="coerce")
        if pd.isna(d): return per
        meses = ["ENERO","FEBRERO","MARZO","ABRIL","MAYO","JUNIO","JULIO","AGOSTO","SEPTIEMBRE","OCTUBRE","NOVIEMBRE","DICIEMBRE"]
        return f"{meses[d.month-1]} {d.year}"
    except: return per

def aplicar_valor_menos_control(regs: List[Dict[str,Any]]) -> List[Dict[str,Any]]:
    if not regs: return regs
    base10 = float(regs[0]["Hp(10)"]); base07 = float(regs[0]["Hp(0.07)"]); base3 = float(regs[0]["Hp(3)"])
    for i, r in enumerate(regs):
        r["PERIODO DE LECTURA"] = periodo_desde_fecha(r.get("PERIODO DE LECTURA",""), r.get("FECHA DE LECTURA",""))
        if i == 0:
            r["NOMBRE"] = "CONTROL"
            r["Hp(10)"]  = f"{base10:.2f}"; r["Hp(0.07)"] = f"{base07:.2f}"; r["Hp(3)"] = f"{base3:.2f}"
        else:
            for key, base in (("Hp(10)",base10),("Hp(0.07)",base07),("Hp(3)",base3)):
                diff = float(r[key]) - base
                r[key] = "PM" if diff < 0.005 else f"{diff:.2f}"
    return regs

# ============== Cargar LISTA (según origen) ==============
if origen_lista == "Ninox":
    df_lista = load_lista_from_ninox()
else:
    st.subheader("1) Subir LISTA DE CÓDIGO (Archivo)")
    upl_lista = st.file_uploader("CSV/XLS/XLSX — LISTA", type=["csv","xls","xlsx"], key="upl_lista")
    sheet = st.text_input("Hoja (opcional, ej. 'asignar_DOSÍMETRO')", value="")
    df_lista = load_lista_from_file(upl_lista, sheet_name=sheet or None)

if df_lista.empty:
    st.warning("No hay filas en LISTA DE CÓDIGO para trabajar.")
    st.stop()

st.success(f"LISTA cargada: {len(df_lista)} fila(s)")
st.dataframe(df_lista.head(20), use_container_width=True)

# ============== Filtros ==============
periodos = sorted(df_lista["PERIODO_NORM"].dropna().astype(str).unique().tolist())
per_sel = st.multiselect("Filtrar por PERIODO DE LECTURA (elige uno o varios; vacío = TODOS)", periodos, default=[])
cod_input = st.text_input("Filtrar por CÓDIGO (WBxxxxxx) — opcional", value="")
cod_norm = norm_code(cod_input) if cod_input.strip() else ""

df_lista_f = df_lista.copy()
if per_sel:
    df_lista_f = df_lista_f[df_lista_f["PERIODO_NORM"].isin([p.strip().upper() for p in per_sel])]
if cod_norm:
    df_lista_f = df_lista_f[df_lista_f["CODIGO"] == cod_norm]

if df_lista_f.empty:
    st.warning("Tras los filtros no queda ninguna fila de LISTA.")
    st.stop()

# ============== Dosis ==============
st.subheader("2) Subir Archivo de Dosis")
upl_dosis = st.file_uploader("Selecciona CSV/XLS/XLSX (dosis)", type=["csv","xls","xlsx"], key="upl_dosis")
df_dosis = leer_dosis(upl_dosis) if upl_dosis else None
if df_dosis is not None and not df_dosis.empty:
    st.success(f"Dosis cargadas: {len(df_dosis)} fila(s)")
    st.dataframe(df_dosis.head(15), use_container_width=True)

# ============== Procesar ==============
nombre_arch = st.text_input("Nombre archivo (sin extensión)", f"ReporteDosimetria_{datetime.now():%Y-%m-%d}")

if st.button("✅ Procesar", type="primary", use_container_width=True):
    if df_dosis is None or df_dosis.empty:
        st.error("No hay datos de dosis.")
    elif "dosimeter" not in df_dosis.columns:
        st.error("El archivo de dosis debe tener la columna 'dosimeter'.")
    else:
        idx_dosis = df_dosis.set_index("dosimeter")
        base = pd.concat([df_lista_f[df_lista_f["CONTROL_FLAG"]],
                          df_lista_f[~df_lista_f["CONTROL_FLAG"]]], ignore_index=True)

        regs, no_match = [], []
        for _, r in base.iterrows():
            code = r["CODIGO"]
            if not code: continue
            if code not in idx_dosis.index:
                no_match.append(code); continue
            d = idx_dosis.loc[code]
            if isinstance(d, pd.DataFrame):
                d = d.sort_values(by="timestamp", na_position="first").iloc[-1]
            ts = d.get("timestamp")
            fecha_str = ""
            try:
                fecha_str = pd.to_datetime(ts).strftime("%d/%m/%Y %H:%M") if pd.notna(ts) else ""
            except: pass
            regs.append({
                "PERIODO DE LECTURA": r["PERIODO_NORM"],
                "COMPAÑÍA": r["COMPAÑÍA"],
                "CÓDIGO DE DOSÍMETRO": code,
                "NOMBRE": r["NOMBRE_COMPLETO"] or r["NOMBRE"],
                "CÉDULA": r["CÉDULA"],
                "FECHA DE LECTURA": fecha_str,
                "TIPO DE DOSÍMETRO": r["TIPO DE DOSÍMETRO"] or "CE",
                "Hp(10)": float(d.get("hp10dose", 0.0) or 0.0),
                "Hp(0.07)": float(d.get("hp0.07dose", 0.0) or 0.0),
                "Hp(3)": float(d.get("hp3dose", 0.0) or 0.0),
            })

        if not regs:
            st.warning("⚠️ No hay coincidencias CÓDIGO_DOSÍMETRO ↔ dosimeter (revisa periodos/códigos).")
            if no_match:
                with st.expander("Códigos de LISTA sin dosis"):
                    st.write(sorted(set(no_match)))
        else:
            regs = aplicar_valor_menos_control(regs)
            df_final = pd.DataFrame(regs)
            df_final["PERIODO DE LECTURA"] = df_final["PERIODO DE LECTURA"].astype(str).str.upper().str.rstrip(".")
            df_final.loc[df_final.index.min(), "NOMBRE"] = "CONTROL"
            df_final["NOMBRE"] = df_final["NOMBRE"].astype(str).str.strip().str.rstrip(".")
            st.success(f"¡Listo! Registros generados: {len(df_final)}")
            st.dataframe(df_final, use_container_width=True)
            # Descargar
            def to_xlsx(df: pd.DataFrame) -> bytes:
                bio = io.BytesIO()
                with pd.ExcelWriter(bio, engine="openpyxl") as w:
                    df.to_excel(w, index=False, sheet_name="REPORTE")
                bio.seek(0); return bio.read()
            st.download_button("⬇️ Descargar Excel (VALOR − CONTROL)",
                               data=to_xlsx(df_final),
                               file_name=f"{(nombre_arch or 'ReporteDosimetria')}.xlsx",
                               mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")
            st.session_state["df_final"] = df_final

# ============== Subir a Ninox (BASE DE DATOS) ==============
st.markdown("---")
st.subheader("3) Subir TODO a Ninox → Tabla: BASE DE DATOS")

CUSTOM_MAP = {
    "PERIODO DE LECTURA": "PERIODO DE LECTURA",
    "COMPAÑÍA": "COMPAÑÍA",
    "CÓDIGO DE DOSÍMETRO": "CÓDIGO DE DOSÍMETRO",
    "NOMBRE": "NOMBRE",
    "CÉDULA": "CÉDULA",
    "FECHA DE LECTURA": "FECHA DE LECTURA",
    "TIPO DE DOSÍMETRO": "TIPO DE DOSÍMETRO",
}
SPECIAL_MAP = {"Hp(10)": "Hp (10)", "Hp(0.07)": "Hp (0.07)", "Hp(3)": "Hp (3)"}

def resolve_dest(col: str) -> str:
    if col in SPECIAL_MAP: return SPECIAL_MAP[col]
    return CUSTOM_MAP.get(col, col)

def _hp_payload(v: Any, as_text_pm=True):
    if isinstance(v, str) and v.strip().upper() == "PM":
        return "PM" if as_text_pm else None
    try: return float(v)
    except: return (v if v is not None else None)

def _to_str(v: Any) -> str:
    if pd.isna(v): return ""
    return str(v)

if st.button("⬆️ Subir a Ninox (BASE DE DATOS)", use_container_width=True):
    df_final = st.session_state.get("df_final")
    if df_final is None or df_final.empty:
        st.error("Primero pulsa ‘Procesar’.")
    else:
        try:
            fields_exist = set(ninox_get_fields(tabla_salida))
        except Exception as e:
            st.error(f"No pude leer los campos de Ninox: {e}")
            fields_exist = set()
        rows, skipped = [], set()
        iterator = df_final.head(1).iterrows() if debug_uno else df_final.iterrows()
        for _, row in iterator:
            payload = {}
            for col in df_final.columns:
                dest = resolve_dest(col)
                if fields_exist and dest not in fields_exist:
                    skipped.add(dest); continue
                val = row[col]
                if dest in {"Hp (10)","Hp (0.07)","Hp (3)"}:
                    payload[dest] = _hp_payload(val, as_text_pm=subir_pm_texto)
                else:
                    payload[dest] = _to_str(val)
            rows.append({"fields": payload})
        res = ninox_insert_rows(tabla_salida, rows, batch=300)
        if res.get("ok"):
            st.success(f"✅ Subido a Ninox: {res.get('inserted',0)} registro(s).")
            if skipped:
                st.info("Columnas omitidas por no existir en Ninox:\n- " + "\n- ".join(sorted(skipped)))
        else:
            st.error(f"❌ Error al subir: {res.get('error')}")
            if skipped:
                st.info("Campos a crear/verificar:\n- " + "\n- ".join(sorted(skipped)))
