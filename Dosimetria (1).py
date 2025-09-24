# -*- coding: utf-8 -*-
import io, re
from io import BytesIO
from datetime import datetime
from typing import List, Dict, Any, Optional, Set

import requests
import pandas as pd
import streamlit as st
from dateutil.parser import parse as dtparse

# ===================== NINOX CONFIG =====================
API_TOKEN   = "edf312a0-98b8-11f0-883e-db77626d62e5"
TEAM_ID     = "YrsYfTegptdZcHJEj"
DATABASE_ID = "ow1geqnkz00e"
BASE_URL    = "https://api.ninox.com/v1"

# Tablas
DEFAULT_SOURCE_TABLE  = "LISTA DE CODIGO"   # lectura
DEFAULT_TARGET_TABLE  = "BASE DE DATOS"     # escritura

# ===================== STREAMLIT =====================
st.set_page_config(page_title="Microsievert - Dosimetría", page_icon="🧪", layout="wide")
st.title("🧪 Microsievert — Gestión de Dosimetría (Ninox)")
st.caption("Lee TODO Ninox, filtra por periodo(s), cruza con dosis, VALOR−CONTROL, y sube a BASE DE DATOS.")

# -------- util cache bust --------
col_top1, col_top2 = st.columns([1,1])
with col_top1:
    if st.button("🔄 Refrescar (limpiar caché y volver a leer Ninox)"):
        st.cache_data.clear()
        st.experimental_rerun()
with col_top2:
    st.write("")

# ===================== Ninox helpers =====================
def ninox_headers():
    return {"Authorization": f"Bearer {API_TOKEN}", "Content-Type": "application/json"}

@st.cache_data(ttl=300, show_spinner=False)
def ninox_list_tables(team_id: str, db_id: str):
    url = f"{BASE_URL}/teams/{team_id}/databases/{db_id}/tables"
    r = requests.get(url, headers=ninox_headers(), timeout=30)
    r.raise_for_status()
    return r.json()

def resolve_table_id(team_id: str, db_id: str, hint: str) -> str:
    hint = (hint or "").strip()
    if hint and " " not in hint and len(hint) <= 8:
        return hint
    for t in ninox_list_tables(team_id, db_id):
        if str(t.get("name","")).strip().lower() == hint.lower():
            return str(t.get("id"))
    return hint  # deja que la API dé error si no existe

@st.cache_data(ttl=300, show_spinner=False)
def ninox_fetch_all(team_id: str, db_id: str, table_hint: str, page_size: int = 1000) -> list:
    """Lee TODOS los registros de una tabla Ninox (limit/skip)"""
    table_id = resolve_table_id(team_id, db_id, table_hint)
    url = f"{BASE_URL}/teams/{team_id}/databases/{db_id}/tables/{table_id}/records"
    out, skip = [], 0
    while True:
        r = requests.get(url, headers=ninox_headers(), params={"limit": page_size, "skip": skip}, timeout=60)
        if r.status_code == 404:
            raise FileNotFoundError(f"Tabla '{table_hint}' (ID '{table_id}') no existe.")
        r.raise_for_status()
        chunk = r.json()
        if not chunk:
            break
        out.extend(chunk)
        if len(chunk) < page_size:
            break
        skip += page_size
    return out

def ninox_fetch_records_df(team_id: str, db_id: str, table_hint: str) -> pd.DataFrame:
    recs = ninox_fetch_all(team_id, db_id, table_hint)
    rows = [x.get("fields", {}) for x in recs]
    return pd.DataFrame(rows) if rows else pd.DataFrame()

@st.cache_data(ttl=120, show_spinner=False)
def ninox_get_fieldnames(team_id: str, db_id: str, table_hint: str) -> set:
    table_id = resolve_table_id(team_id, db_id, table_hint)
    url = f"{BASE_URL}/teams/{team_id}/databases/{db_id}/tables"
    r = requests.get(url, headers=ninox_headers(), timeout=30)
    r.raise_for_status()
    info = r.json()
    for t in info:
        if str(t.get("id")) == str(table_id):
            cols = t.get("fields") or t.get("columns") or []
            return {c.get("name") for c in cols if isinstance(c, dict) and c.get("name")}
    return set()

def ninox_insert_rows(team_id: str, db_id: str, table_hint: str, rows: list, batch_size: int = 400):
    table_id = resolve_table_id(team_id, db_id, table_hint)
    url = f"{BASE_URL}/teams/{team_id}/databases/{db_id}/tables/{table_id}/records"
    inserted = 0
    for i in range(0, len(rows), batch_size):
        chunk = rows[i:i+batch_size]
        r = requests.post(url, headers=ninox_headers(), json=chunk, timeout=60)
        if r.status_code != 200:
            return {"ok": False, "inserted": inserted, "error": f"{r.status_code} {r.text}"}
        inserted += len(chunk)
    return {"ok": True, "inserted": inserted}

# ===================== Normalizadores =====================
def normalize_lista_codigo(df: pd.DataFrame) -> pd.DataFrame:
    """Asegura columnas y normaliza PERIODO y CODIGO."""
    need = [
        "CÉDULA","CÓDIGO USUARIO","NOMBRE","APELLIDO","FECHA DE NACIMIENTO",
        "CLIENTE","CÓDIGO_CLIENTE","ETIQUETA","CÓDIGO_DOSÍMETRO",
        "PERIODO DE LECTURA","TIPO DE DOSÍMETRO"
    ]
    for c in need:
        if c not in df.columns:
            df[c] = ""

    ap = df["APELLIDO"].fillna("").astype(str).str.strip()
    df["NOMBRE_COMPLETO"] = (df["NOMBRE"].fillna("").astype(str).str.strip() + " " + ap).str.strip()
    df["CODIGO"] = df["CÓDIGO_DOSÍMETRO"].fillna("").astype(str).str.strip().str.upper()

    df["PERIODO_NORM"] = (
        df["PERIODO DE LECTURA"].fillna("").astype(str).str.strip().str.upper()
        .str.replace(r"\s+", " ", regex=True)
        .str.replace(r"\.+$", "", regex=True)
    )

    def is_control_row(r):
        for k in ["ETIQUETA","NOMBRE","CÉDULA","CÓDIGO USUARIO"]:
            if str(r.get(k,"")).strip().upper() == "CONTROL":
                return True
        return False

    df["CONTROL_FLAG"] = df.apply(is_control_row, axis=1)
    for c in ["CLIENTE","TIPO DE DOSÍMETRO","CÉDULA"]:
        df[c] = df[c].fillna("").astype(str).str.strip()

    return df

def leer_dosis(upload) -> Optional[pd.DataFrame]:
    if not upload:
        return None
    name = upload.name.lower()
    if name.endswith(".csv"):
        try:
            df = pd.read_csv(upload, delimiter=';', engine='python')
        except Exception:
            upload.seek(0)
            df = pd.read_csv(upload)
    else:
        df = pd.read_excel(upload)

    norm = (df.columns.astype(str).str.strip().str.lower()
            .str.replace(' ', '', regex=False)
            .str.replace('(', '').str.replace(')', '')
            .str.replace('.', '', regex=False))
    df.columns = norm

    if 'dosimeter' not in df.columns:
        for alt in ['dosimetro','codigo','codigodosimetro','codigo_dosimetro']:
            if alt in df.columns:
                df.rename(columns={alt: 'dosimeter'}, inplace=True); break

    for cand in ['hp10dosecorr','hp10dose','hp10']:
        if cand in df.columns: df.rename(columns={cand: 'hp10dose'}, inplace=True); break
    for cand in ['hp007dosecorr','hp007dose','hp007']:
        if cand in df.columns: df.rename(columns={cand: 'hp0.07dose'}, inplace=True); break
    for cand in ['hp3dosecorr','hp3dose','hp3']:
        if cand in df.columns: df.rename(columns={cand: 'hp3dose'}, inplace=True); break

    for k in ['hp10dose','hp0.07dose','hp3dose']:
        if k in df.columns: df[k] = pd.to_numeric(df[k], errors='coerce').fillna(0.0)
        else: df[k] = 0.0

    if 'timestamp' in df.columns:
        df['timestamp'] = pd.to_datetime(df['timestamp'], errors='coerce')

    if 'dosimeter' in df.columns:
        df['dosimeter'] = df['dosimeter'].astype(str).str.strip().str.upper()

    return df

def periodo_desde_fecha(periodo_str: str, fecha_str: str) -> str:
    per = (periodo_str or "").strip().upper()
    per = re.sub(r'\.+$', '', per).strip()
    if per and per != "CONTROL": return per
    if not fecha_str: return per or ""
    try:
        fecha = pd.to_datetime(fecha_str, dayfirst=True, errors="coerce")
        if pd.isna(fecha): return per or ""
        meses = ["ENERO","FEBRERO","MARZO","ABRIL","MAYO","JUNIO","JULIO","AGOSTO","SEPTIEMBRE","OCTUBRE","NOVIEMBRE","DICIEMBRE"]
        return f"{meses[fecha.month-1]} {fecha.year}"
    except Exception:
        return per or ""

def aplicar_valor_menos_control(registros: List[Dict[str,Any]]):
    if not registros: return registros
    base10 = float(registros[0]['Hp(10)'])
    base07 = float(registros[0]['Hp(0.07)'])
    base3  = float(registros[0]['Hp(3)'])
    for i, r in enumerate(registros):
        r['PERIODO DE LECTURA'] = periodo_desde_fecha(r.get('PERIODO DE LECTURA',''), r.get('FECHA DE LECTURA',''))
        if i == 0:
            r['NOMBRE'] = "CONTROL"
            r['Hp(10)'] = f"{base10:.2f}"
            r['Hp(0.07)'] = f"{base07:.2f}"
            r['Hp(3)'] = f"{base3:.2f}"
        else:
            for key, base in [('Hp(10)', base10), ('Hp(0.07)', base07), ('Hp(3)', base3)]:
                diff = float(r[key]) - base
                r[key] = "PM" if diff < 0.005 else f"{diff:.2f}"
    return registros

# ===================== UI: configuración =====================
with st.sidebar:
    st.markdown("### ⚙️ Configuración (TAB 1)")
    source_table  = st.text_input("Tabla de lectura (personas/códigos)", value=DEFAULT_SOURCE_TABLE)
    target_table  = st.text_input("Tabla de escritura (salida)", value=DEFAULT_TARGET_TABLE)
    subir_pm_texto = st.checkbox("Subir 'PM' como TEXTO (si Hp son texto en Ninox)", value=True)
    debug_one      = st.checkbox("Enviar 1 registro (debug)", value=False)
    show_tables    = st.checkbox("Mostrar tablas Ninox (debug)", value=False)

# ===================== Leer TODO Ninox (LISTA DE CODIGO) =====================
try:
    df_lista_raw = ninox_fetch_records_df(TEAM_ID, DATABASE_ID, source_table)
    if show_tables:
        st.expander("Tablas Ninox (debug)").json(ninox_list_tables(TEAM_ID, DATABASE_ID))
    if df_lista_raw.empty:
        st.error("No hay filas en LISTA DE CODIGO.")
        st.stop()
    df_lista = normalize_lista_codigo(df_lista_raw)

    st.success(f"Conectado a Ninox. Tabla: {source_table} — Filas: {len(df_lista)}")
    st.dataframe(df_lista.head(25), use_container_width=True)
except Exception as e:
    st.error(f"Error leyendo '{source_table}': {e}")
    st.stop()

# ===================== Selector de periodos (multi) =====================
periodos = sorted([p for p in df_lista["PERIODO_NORM"].dropna().astype(str).unique() if p.strip() != ""],
                  key=lambda s: (int(re.search(r"(19|20)\d{2}", s).group(0)) if re.search(r"(19|20)\d{2}", s) else 0,
                                 "ENERO FEBRERO MARZO ABRIL MAYO JUNIO JULIO AGOSTO SEPTIEMBRE OCTUBRE NOVIEMBRE DICIEMBRE".split().index(s.split()[0]) if s.split() else -1),
                  reverse=True)
st.markdown("#### Filtrar por PERIODO DE LECTURA (elige uno o varios, **vacío = TODOS**)")
periodos_sel = st.multiselect("PERIODO DE LECTURA", options=periodos, default=[])

# Aplica filtro (vacío = todos)
if periodos_sel:
    df_lista_f = df_lista[df_lista["PERIODO_NORM"].isin(periodos_sel)].copy()
else:
    df_lista_f = df_lista.copy()

# Muestra conteos por periodo para confirmar que lee TODO
with st.expander("Resumen de periodos detectados"):
    st.write(df_lista.groupby("PERIODO_NORM").size().sort_values(ascending=False))

# ===================== Archivo de Dosis =====================
st.markdown("### Archivo de Dosis")
upl = st.file_uploader("Selecciona CSV/XLS/XLSX", type=["csv","xls","xlsx"])
df_dosis = leer_dosis(upl) if upl else None
if df_dosis is not None:
    st.caption(f"Vista previa dosis (normalizada) — filas: {len(df_dosis)}")
    st.dataframe(df_dosis.head(20), use_container_width=True)

# ===================== Procesar =====================
colL, colR = st.columns([1,1])
with colL:
    nombre_out = st.text_input("Nombre archivo (sin extensión)", value=f"ReporteDosimetria_{datetime.now().strftime('%Y-%m-%d')}")
with colR:
    btn_proc = st.button("✅ Procesar", type="primary", use_container_width=True)

def construir_registros(df_lista_use: pd.DataFrame, df_dosis_use: pd.DataFrame) -> List[Dict[str,Any]]:
    if df_lista_use.empty or df_dosis_use is None or df_dosis_use.empty:
        return []

    idx = df_dosis_use.set_index("dosimeter")
    registros = []

    # Control primero
    base = pd.concat([df_lista_use[df_lista_use["CONTROL_FLAG"]], df_lista_use[~df_lista_use["CONTROL_FLAG"]]],
                     ignore_index=True)

    miss = []
    for _, r in base.iterrows():
        cod = r["CODIGO"]
        if not cod or cod.lower() == "nan":
            continue
        if cod not in idx.index:
            miss.append(cod)
            continue

        d = idx.loc[cod]
        if isinstance(d, pd.DataFrame):
            d = d.sort_values(by="timestamp").iloc[-1]

        ts = d.get("timestamp", pd.NaT)
        fecha_str = ""
        try:
            if pd.notna(ts): fecha_str = pd.to_datetime(ts).strftime("%d/%m/%Y %H:%M")
        except Exception: pass

        registros.append({
            "PERIODO DE LECTURA": r["PERIODO_NORM"] or "",
            "CLIENTE": r["CLIENTE"],
            "CÓDIGO DE DOSÍMETRO": cod,
            "NOMBRE": r["NOMBRE_COMPLETO"] or r["NOMBRE"],
            "CÉDULA": r["CÉDULA"],
            "FECHA DE LECTURA": fecha_str,
            "TIPO DE DOSÍMETRO": r["TIPO DE DOSÍMETRO"] or "CE",
            "Hp(10)": float(d.get("hp10dose", 0.0) or 0.0),
            "Hp(0.07)": float(d.get("hp0.07dose", 0.0) or 0.0),
            "Hp(3)": float(d.get("hp3dose", 0.0) or 0.0),
        })

    # Debug de no encontrados
    if st.checkbox("📎 Mostrar debug de códigos"):
        st.write("Códigos en dosis no encontrados en LISTA DE CODIGO filtrada:", sorted(set(idx.index) - set(base["CODIGO"])))
        st.write("Códigos en LISTA DE CODIGO filtrada sin dosis:", sorted(set(miss)))

    # Orden: control primero
    registros.sort(key=lambda x: (x.get("NOMBRE","").strip().upper() != "CONTROL", x.get("NOMBRE","")))
    return registros

if btn_proc:
    if df_lista_f.empty:
        st.error("No hay filas en LISTA DE CODIGO (tras aplicar filtro de periodo).")
    elif df_dosis is None or df_dosis.empty:
        st.error("No hay datos de dosis.")
    elif 'dosimeter' not in df_dosis.columns:
        st.error("El archivo de dosis debe tener la columna 'dosimeter'.")
    else:
        with st.spinner("Procesando…"):
            registros = construir_registros(df_lista_f, df_dosis)
            if not registros:
                st.warning("No hay coincidencias CÓDIGO_DOSÍMETRO ↔ dosimeter (revisa periodos/códigos).")
            else:
                registros = aplicar_valor_menos_control(registros)
                df_final = pd.DataFrame(registros)

                # Limpiezas de texto
                df_final['PERIODO DE LECTURA'] = (
                    df_final['PERIODO DE LECTURA'].astype(str).str.replace(r'\.+$', '', regex=True).str.strip()
                )
                if not df_final.empty:
                    df_final.loc[df_final.index.min(), 'NOMBRE'] = 'CONTROL'
                    df_final['NOMBRE'] = df_final['NOMBRE'].astype(str).str.replace(r'\.+$', '', regex=True).str.strip()

                st.success(f"¡Listo! Registros generados: {len(df_final)}")
                st.dataframe(df_final, use_container_width=True)

                # Excel simple (opcional)
                def to_excel_simple(df: pd.DataFrame):
                    bio = BytesIO()
                    with pd.ExcelWriter(bio, engine="openpyxl") as w:
                        df.to_excel(w, index=False, sheet_name="REPORTE")
                    bio.seek(0)
                    return bio.getvalue()

                xlsx = to_excel_simple(df_final)
                st.download_button("⬇️ Descargar Excel (VALOR−CONTROL)",
                                   data=xlsx,
                                   file_name=f"{(nombre_out.strip() or 'ReporteDosimetria')}.xlsx",
                                   mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")

                # Guardar en sesión para subir
                st.session_state["df_final"] = df_final

st.markdown("---")
st.subheader("⬆️ Subir a Ninox (BASE DE DATOS)")

CUSTOM_MAP = {
    "PERIODO DE LECTURA": "PERIODO DE LECTURA",
    "CLIENTE": "CLIENTE",
    "CÓDIGO DE DOSÍMETRO": "CÓDIGO_DOSÍMETRO",   # <- campo con guion bajo
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

def _hp_out(v, as_text_pm=True):
    if isinstance(v, str) and v.strip().upper() == "PM":
        return "PM" if as_text_pm else None
    try:
        return float(v)
    except Exception:
        return v if v is not None else None

def _as_str(v):
    if pd.isna(v): return ""
    if isinstance(v, (pd.Timestamp,)):
        return v.strftime("%Y-%m-%d %H:%M:%S")
    return str(v)

if st.button("Subir TODO a Ninox (tabla BASE DE DATOS)"):
    df_final = st.session_state.get("df_final")
    if df_final is None or df_final.empty:
        st.error("Primero pulsa **Procesar**.")
    else:
        try:
            fields_in_target = ninox_get_fieldnames(TEAM_ID, DATABASE_ID, target_table)
        except Exception as e:
            st.error(f"No pude leer campos de '{target_table}': {e}")
            fields_in_target = set()

        with st.expander("Campos detectados en destino"):
            st.write(sorted(fields_in_target))

        rows, skipped = [], set()
        iterator = df_final.head(1).iterrows() if debug_one else df_final.iterrows()

        for _, row in iterator:
            payload = {}
            for c in df_final.columns:
                dest = resolve_dest(c)
                if fields_in_target and dest not in fields_in_target:
                    skipped.add(dest); continue
                val = row[c]
                if dest in {"Hp (10)", "Hp (0.07)", "Hp (3)"}:
                    val = _hp_out(val, as_text_pm=subir_pm_texto)
                else:
                    val = _as_str(val)
                payload[dest] = val
            rows.append({"fields": payload})

        if rows:
            with st.spinner("Subiendo a Ninox…"):
                res = ninox_insert_rows(TEAM_ID, DATABASE_ID, target_table, rows, batch_size=300)

            if res.get("ok"):
                st.success(f"✅ Subido a Ninox: {res.get('inserted', 0)} registro(s).")
                if skipped:
                    st.info("Columnas omitidas por no existir en Ninox:\n- " + "\n- ".join(sorted(skipped)))
            else:
                st.error(f"❌ Error al subir: {res.get('error')}")
                if skipped:
                    st.info("Revisa/crea en Ninox los campos omitidos:\n- " + "\n- ".join(sorted(skipped)))
        else:
            st.warning("No hay filas para subir.")
