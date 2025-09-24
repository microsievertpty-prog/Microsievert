# -*- coding: utf-8 -*-
import io, re, json, requests
import pandas as pd
import streamlit as st
from datetime import datetime
from typing import Any, Dict, List, Optional

# ===================== NINOX CONFIG =====================
API_TOKEN   = "edf312a0-98b8-11f0-883e-db77626d62e5"
TEAM_ID     = "YrsYfTegptdZcHJEj"
DATABASE_ID = "ow1geqnkz00e"
BASE_URL    = "https://api.ninox.com/v1"

# Nombres de tabla (pueden ser nombre visible o ID corto)
TABLE_LISTA = "LISTA DE CODIGO"   # lee personas/códigos
TABLE_BASE  = "BASE DE DATOS"     # escribe resultados

# ===================== STREAMLIT UI =====================
st.set_page_config("Microsievert — Dosimetría", "🧪", layout="wide")
st.title("🧪 Carga y Cruce de Dosis → Ninox (BASE DE DATOS)")

with st.sidebar:
    st.header("Configuración (TAB 1)")
    tabla_lectura  = st.text_input("Tabla de lectura (LISTA)", value=TABLE_LISTA)
    tabla_salida   = st.text_input("Tabla de escritura (BASE)", value=TABLE_BASE)
    subir_pm_texto = st.checkbox("Subir 'PM' como texto (si Hp son Texto en Ninox)", True)
    debug_uno      = st.checkbox("Enviar 1 registro (debug)", False)
    ver_tablas     = st.checkbox("Mostrar tablas Ninox (debug)", False)

# ===================== Helpers Ninox =====================
def _ninox_headers():
    return {"Authorization": f"Bearer {API_TOKEN}", "Content-Type": "application/json"}

@st.cache_data(ttl=300, show_spinner=False)
def ninox_list_tables(team_id: str, db_id: str):
    r = requests.get(f"{BASE_URL}/teams/{team_id}/databases/{db_id}/tables", headers=_ninox_headers(), timeout=30)
    r.raise_for_status()
    return r.json()

def resolve_table_id(table_hint: str) -> str:
    """Acepta nombre visible o ID de Ninox y devuelve el ID real."""
    hint = (table_hint or "").strip()
    if hint and " " not in hint and len(hint) <= 8:
        return hint
    for t in ninox_list_tables(TEAM_ID, DATABASE_ID):
        if str(t.get("name","")).strip().lower() == hint.lower():
            return str(t.get("id"))
        if str(t.get("id","")) == hint:
            return hint
    return hint  # dejar que la API falle si no existe

@st.cache_data(ttl=300, show_spinner=False)
def ninox_fetch_all(table_hint: str, pagesize: int = 1000) -> List[Dict[str,Any]]:
    """Lee TODAS las filas (sin límite) de una tabla de Ninox."""
    table_id = resolve_table_id(table_hint)
    url = f"{BASE_URL}/teams/{TEAM_ID}/databases/{DATABASE_ID}/tables/{table_id}/records"
    out, skip = [], 0
    while True:
        r = requests.get(url, headers=_ninox_headers(), params={"limit": pagesize, "skip": skip}, timeout=60)
        if r.status_code == 404:
            raise RuntimeError(f"No encuentro la tabla '{table_hint}' (ID resuelto: {table_id})")
        r.raise_for_status()
        chunk = r.json()
        if not chunk:
            break
        out.extend(chunk)
        if len(chunk) < pagesize:
            break
        skip += pagesize
    return out

@st.cache_data(ttl=180, show_spinner=False)
def ninox_get_fields(table_hint: str) -> List[str]:
    table_id = resolve_table_id(table_hint)
    info = ninox_list_tables(TEAM_ID, DATABASE_ID)
    for t in info:
        if str(t.get("id")) == str(table_id):
            cols = t.get("fields") or t.get("columns") or []
            names = []
            for c in cols:
                nm = c.get("name") if isinstance(c, dict) else None
                if nm: names.append(nm)
            return names
    return []

def ninox_insert_rows(table_hint: str, rows: List[Dict[str,Any]], batch: int = 400) -> Dict[str,Any]:
    table_id = resolve_table_id(table_hint)
    url = f"{BASE_URL}/teams/{TEAM_ID}/databases/{DATABASE_ID}/tables/{table_id}/records"
    inserted = 0
    if not rows:
        return {"ok": True, "inserted": 0}
    for i in range(0, len(rows), batch):
        chunk = rows[i:i+batch]
        r = requests.post(url, headers=_ninox_headers(), json=chunk, timeout=60)
        if r.status_code != 200:
            return {"ok": False, "inserted": inserted, "error": f"{r.status_code} {r.text}"}
        inserted += len(chunk)
    return {"ok": True, "inserted": inserted}

# ===================== Normalizadores =====================
def norm_code(x: Any) -> str:
    """Normaliza códigos a formato WB000123 (admite '123', 'WB123', 'WB000123', etc.)."""
    if x is None: return ""
    s = str(x).strip().upper().replace("\u00A0", " ")
    s = re.sub(r"[^A-Z0-9]", "", s)
    if re.fullmatch(r"WB\d{6}", s):  # ya ok
        return s
    m = re.fullmatch(r"WB(\d+)", s)
    if m:
        return f"WB{int(m.group(1)):06d}"
    m2 = re.fullmatch(r"(\d+)", s)
    if m2:
        return f"WB{int(m2.group(1)):06d}"
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

# ===================== Lectura LISTA DE CÓDIGO =====================
def load_lista_df() -> pd.DataFrame:
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
    df = pd.DataFrame(rows) if rows else pd.DataFrame(columns=[
        "PERIODO_NORM","COMPAÑÍA","CÓDIGO_DOSÍMETRO","NOMBRE","APELLIDO","CÉDULA","TIPO DE DOSÍMETRO","ETIQUETA"
    ])
    df["CODIGO"] = df["CÓDIGO_DOSÍMETRO"].map(norm_code)
    df["NOMBRE_COMPLETO"] = (df["NOMBRE"].fillna("").astype(str).str.strip() + " " +
                             df["APELLIDO"].fillna("").astype(str).str.strip()).str.strip()
    df["CONTROL_FLAG"] = df.apply(lambda r: any(str(r.get(k,"")).strip().upper() == "CONTROL"
                                                for k in ["ETIQUETA","NOMBRE","CÉDULA"]), axis=1)
    return df

# ===================== Lectura Dosis (archivo) =====================
def leer_dosis(upload) -> Optional[pd.DataFrame]:
    if not upload: return None
    name = upload.name.lower()
    # Excel
    if name.endswith((".xlsx",".xls")):
        df = pd.read_excel(upload)
    else:
        # CSV con _auto sniff_ y fallback de encoding/delimitador
        raw = upload.read(); upload.seek(0)
        for enc in ("utf-8-sig","latin-1","cp1252"):
            try:
                df = pd.read_csv(io.BytesIO(raw), sep=None, engine="python", encoding=enc)
                break
            except Exception:
                df = None
        if df is None:
            df = pd.read_csv(io.BytesIO(raw))  # último intento

    # Normalizar encabezados
    norm = (df.columns.astype(str).str.strip().str.lower()
            .str.replace(" ", "", regex=False)
            .str.replace("(", "").str.replace(")", "").str.replace(".", "", regex=False))
    df.columns = norm

    # Encajar nombres claves
    if "dosimeter" not in df.columns:
        for alt in ("dosimetro","codigo","codigodosimetro","codigo_dosimetro","wb","cod"):
            if alt in df.columns:
                df.rename(columns={alt:"dosimeter"}, inplace=True)
                break

    if "hp10dose" not in df.columns:
        for alt in ("hp10dosecorr","hp10","hp10dos","hpdose10"):
            if alt in df.columns:
                df.rename(columns={alt:"hp10dose"}, inplace=True); break
    if "hp0.07dose" not in df.columns:
        for alt in ("hp007dosecorr","hp007","hp007dose","hp0,07dose","hp007dos"):
            if alt in df.columns:
                df.rename(columns={alt:"hp0.07dose"}, inplace=True); break
    if "hp3dose" not in df.columns:
        for alt in ("hp3dosecorr","hp3","hp3dos"):
            if alt in df.columns:
                df.rename(columns={alt:"hp3dose"}, inplace=True); break

    # Tipos y limpieza
    for k in ("hp10dose","hp0.07dose","hp3dose"):
        if k in df.columns:
            df[k] = pd.to_numeric(df[k], errors="coerce").fillna(0.0)
        else:
            df[k] = 0.0
    if "dosimeter" in df.columns:
        df["dosimeter"] = df["dosimeter"].map(norm_code)
    if "timestamp" in df.columns:
        df["timestamp"] = pd.to_datetime(df["timestamp"], errors="coerce")
    return df

# ===================== Valor - Control =====================
def periodo_desde_fecha(per_str: str, fecha_str: str) -> str:
    per = (per_str or "").strip().upper().rstrip(".")
    if per and per != "CONTROL":
        return per
    if not fecha_str:
        return per
    try:
        d = pd.to_datetime(fecha_str, dayfirst=True, errors="coerce")
        if pd.isna(d): return per
        meses = ["ENERO","FEBRERO","MARZO","ABRIL","MAYO","JUNIO","JULIO","AGOSTO","SEPTIEMBRE","OCTUBRE","NOVIEMBRE","DICIEMBRE"]
        return f"{meses[d.month-1]} {d.year}"
    except Exception:
        return per

def aplicar_valor_menos_control(regs: List[Dict[str,Any]]) -> List[Dict[str,Any]]:
    if not regs: return regs
    base10 = float(regs[0]["Hp(10)"]); base07 = float(regs[0]["Hp(0.07)"]); base3 = float(regs[0]["Hp(3)"])
    for i, r in enumerate(regs):
        r["PERIODO DE LECTURA"] = periodo_desde_fecha(r.get("PERIODO DE LECTURA",""), r.get("FECHA DE LECTURA",""))
        if i == 0:
            r["NOMBRE"] = "CONTROL"
            r["Hp(10)"]  = f"{base10:.2f}"
            r["Hp(0.07)"] = f"{base07:.2f}"
            r["Hp(3)"]   = f"{base3:.2f}"
        else:
            for key, base in (("Hp(10)",base10),("Hp(0.07)",base07),("Hp(3)",base3)):
                diff = float(r[key]) - base
                r[key] = "PM" if diff < 0.005 else f"{diff:.2f}"
    return regs

# ===================== UI principal =====================
# 1) Cargar LISTA desde Ninox
try:
    if ver_tablas:
        st.expander("Tablas Ninox (debug)").json(ninox_list_tables(TEAM_ID, DATABASE_ID))
    df_lista = load_lista_df()
    if df_lista.empty:
        st.warning("LISTA DE CÓDIGO está vacía.")
    else:
        st.success(f"Conectado a Ninox. Tabla: {tabla_lectura}")
        st.dataframe(df_lista.head(20), use_container_width=True)
except Exception as e:
    st.error(f"Error leyendo LISTA DE CÓDIGO: {e}")
    st.stop()

# 2) Filtros de periodo y código
periodos = sorted(df_lista["PERIODO_NORM"].dropna().astype(str).unique().tolist())
per_sel = st.multiselect("Filtrar por PERIODO DE LECTURA (elige uno o varios; vacío = TODOS)", periodos, default=[])

cod_input = st.text_input("Filtrar por CÓDIGO (WBxxxxxx) — opcional", value="")
cod_norm = norm_code(cod_input) if cod_input.strip() else ""

df_lista_f = df_lista.copy()
if per_sel:
    df_lista_f = df_lista_f[df_lista_f["PERIODO_NORM"].isin([p.strip().upper() for p in per_sel])]
if cod_norm:
    antes = len(df_lista_f)
    df_lista_f = df_lista_f[df_lista_f["CODIGO"] == cod_norm]
    st.info(f"Código filtrado: {cod_norm}. Filas en LISTA tras filtros: {len(df_lista_f)} (antes: {antes})")
    if df_lista_f.empty:
        disp = (df_lista[df_lista["CODIGO"] == cod_norm]["PERIODO_NORM"]
                .dropna().astype(str).unique().tolist())
        st.warning("No hay filas para ese código con los periodos seleccionados." +
                   (f" Periodos disponibles para {cod_norm}: " + ", ".join(sorted(disp)) if disp else ""))

# 3) Subir archivo de Dosis
st.subheader("2) Subir Archivo de Dosis")
upl = st.file_uploader("Selecciona CSV/XLS/XLSX (dosis)", type=["csv","xls","xlsx"])
df_dosis = leer_dosis(upl) if upl else None
if df_dosis is not None and not df_dosis.empty:
    st.success(f"Dosis cargadas: {len(df_dosis)} fila(s)")
    st.dataframe(df_dosis.head(20), use_container_width=True)

# 4) Procesar
nombre_arch = st.text_input("Nombre archivo (sin extensión)", f"ReporteDosimetria_{datetime.now():%Y-%m-%d}")
if st.button("✅ Procesar", type="primary", use_container_width=True):
    if df_lista_f.empty:
        st.error("No hay filas en LISTA para los filtros elegidos.")
    elif df_dosis is None or df_dosis.empty:
        st.error("No hay datos de dosis.")
    elif "dosimeter" not in df_dosis.columns:
        st.error("El archivo de dosis debe tener la columna 'dosimeter'.")
    else:
        # índice por dosímetro de dosis
        idx_dosis = df_dosis.set_index("dosimeter")
        # Ordenar: CONTROL primero
        base = pd.concat([df_lista_f[df_lista_f["CONTROL_FLAG"]],
                          df_lista_f[~df_lista_f["CONTROL_FLAG"]]], ignore_index=True)

        regs = []
        not_found = []
        for _, r in base.iterrows():
            code = r["CODIGO"]
            if not code: 
                continue
            if code not in idx_dosis.index:
                not_found.append(code)
                continue
            d = idx_dosis.loc[code]
            if isinstance(d, pd.DataFrame):
                d = d.sort_values(by="timestamp", na_position="first").iloc[-1]
            ts = d.get("timestamp")
            fecha_str = ""
            try:
                fecha_str = pd.to_datetime(ts).strftime("%d/%m/%Y %H:%M") if pd.notna(ts) else ""
            except Exception:
                fecha_str = ""
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
            st.warning("⚠️ No se encontraron coincidencias entre CÓDIGO_DOSÍMETRO y dosimeter. Revisa periodos/códigos.")
            if not_found:
                with st.expander("Debug de códigos no coincidentes"):
                    st.write(sorted(set(not_found)))
        else:
            regs = aplicar_valor_menos_control(regs)
            df_final = pd.DataFrame(regs)
            df_final["PERIODO DE LECTURA"] = df_final["PERIODO DE LECTURA"].astype(str).str.upper().str.rstrip(".")
            if not df_final.empty:
                df_final.loc[df_final.index.min(), "NOMBRE"] = "CONTROL"
                df_final["NOMBRE"] = df_final["NOMBRE"].astype(str).str.strip().str.rstrip(".")
            st.success(f"¡Listo! Registros generados: {len(df_final)}")
            st.dataframe(df_final, use_container_width=True)

            # Descarga XLS simple (opcional)
            def to_xlsx(df: pd.DataFrame) -> bytes:
                bio = io.BytesIO()
                with pd.ExcelWriter(bio, engine="openpyxl") as w:
                    df.to_excel(w, index=False, sheet_name="REPORTE")
                bio.seek(0); return bio.read()
            st.download_button("⬇️ Descargar Excel (VALOR − CONTROL)",
                               data=to_xlsx(df_final),
                               file_name=f"{(nombre_arch or 'ReporteDosimetria')}.xlsx",
                               mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")

            # Guardar en sesión para subir luego
            st.session_state["df_final"] = df_final

st.markdown("---")
st.subheader("3) Subir TODO a Ninox → Tabla: BASE DE DATOS")

# Mapeo exacto de columnas destino en Ninox
CUSTOM_MAP = {
    "PERIODO DE LECTURA": "PERIODO DE LECTURA",
    "COMPAÑÍA": "COMPAÑÍA",
    "CÓDIGO DE DOSÍMETRO": "CÓDIGO DE DOSÍMETRO",   # tu tabla BASE usa este nombre (con espacios)
    "NOMBRE": "NOMBRE",
    "CÉDULA": "CÉDULA",
    "FECHA DE LECTURA": "FECHA DE LECTURA",
    "TIPO DE DOSÍMETRO": "TIPO DE DOSÍMETRO",
}
SPECIAL_MAP = {"Hp(10)": "Hp (10)", "Hp(0.07)": "Hp (0.07)", "Hp(3)": "Hp (3)"}

def resolve_dest(col: str) -> str:
    if col in SPECIAL_MAP: return SPECIAL_MAP[col]
    if col in CUSTOM_MAP:  return CUSTOM_MAP[col]
    return col

def _hp_payload(v: Any, as_text_pm=True):
    if isinstance(v, str) and v.strip().upper() == "PM":
        return "PM" if as_text_pm else None
    try: return float(v)
    except: return (v if v is not None else None)

def _to_str(v: Any) -> str:
    if pd.isna(v): return ""
    return str(v)

if st.button("⬆️ Subir a Ninox (BASE DE DATOS)", type="secondary", use_container_width=True):
    df_final = st.session_state.get("df_final")
    if df_final is None or df_final.empty:
        st.error("Primero pulsa ‘Procesar’.")
    else:
        try:
            fields_exist = set(ninox_get_fields(tabla_salida))
            if not fields_exist:
                st.warning("No pude leer los campos de la tabla en Ninox. Verifica nombre/ID.")
        except Exception as e:
            st.error(f"No se pudo leer el esquema de Ninox: {e}")
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

        if debug_uno:
            st.caption("Payload (primer registro):")
            st.json(rows[:1])

        with st.spinner("Subiendo a Ninox..."):
            res = ninox_insert_rows(tabla_salida, rows, batch=300)

        if res.get("ok"):
            st.success(f"✅ Subido a Ninox: {res.get('inserted',0)} registro(s).")
            if skipped:
                st.info("Columnas omitidas por no existir en Ninox:\n- " + "\n- ".join(sorted(skipped)))
        else:
            st.error(f"❌ Error al subir: {res.get('error')}")
            if skipped:
                st.info("Campos que deberías crear en Ninox:\n- " + "\n- ".join(sorted(skipped)))
