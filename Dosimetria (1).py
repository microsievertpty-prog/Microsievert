# -*- coding: utf-8 -*-
import io
import re
import json
import requests
import pandas as pd
import streamlit as st
from datetime import datetime
from io import BytesIO
from typing import List, Dict, Any, Optional

# ===================== NINOX CONFIG =====================
API_TOKEN   = "edf312a0-98b8-11f0-883e-db77626d62e5"
TEAM_ID     = "YrsYfTegptdZcHJEj"
DATABASE_ID = "ow1geqnkz00e"
BASE_URL    = "https://api.ninox.com/v1"

# Tablas por NOMBRE (se resuelven a ID automáticamente)
TABLE_LISTA = "LISTA DE CODIGO"     # lectura (personas / códigos / periodo)
TABLE_BASE  = "BASE DE DATOS"       # escritura y reporte

# ===================== STREAMLIT APP =====================
st.set_page_config(page_title="Microsievert — Dosimetría", page_icon="🧪", layout="wide")
st.title("🧪 Carga y Cruce de Dosis → Ninox (BASE DE DATOS)")

# --------- Estado
if "df_lista" not in st.session_state:
    st.session_state.df_lista = None
if "df_dosis" not in st.session_state:
    st.session_state.df_dosis = None

# ===================== Utilidades =====================
def ninox_headers():
    return {"Authorization": f"Bearer {API_TOKEN}", "Content-Type": "application/json"}

def ninox_list_tables(team_id: str, db_id: str):
    url = f"{BASE_URL}/teams/{team_id}/databases/{db_id}/tables"
    r = requests.get(url, headers=ninox_headers(), timeout=30)
    r.raise_for_status()
    return r.json()

def ninox_resolve_table_id(team_id: str, db_id: str, table_hint: str) -> str:
    hint = (table_hint or "").strip()
    # Si parece ID (corto y sin espacios), se usa tal cual
    if hint and " " not in hint and len(hint) <= 8:
        return hint
    for t in ninox_list_tables(team_id, db_id):
        name = str(t.get("name","")).strip().lower()
        tid  = str(t.get("id","")).strip()
        if name == hint.lower() or tid == hint:
            return tid
    return hint

def ninox_fetch_all(table_hint: str, page_size: int = 1000) -> List[dict]:
    table_id = ninox_resolve_table_id(TEAM_ID, DATABASE_ID, table_hint)
    url = f"{BASE_URL}/teams/{TEAM_ID}/databases/{DATABASE_ID}/tables/{table_id}/records"
    out, skip = [], 0
    while True:
        r = requests.get(url, headers=ninox_headers(), params={"limit": page_size, "skip": skip}, timeout=60)
        r.raise_for_status()
        chunk = r.json()
        if not chunk: break
        out.extend(chunk)
        if len(chunk) < page_size: break
        skip += page_size
    return out

def ninox_insert_rows(table_hint: str, rows: List[dict], batch_size: int = 300) -> Dict[str, Any]:
    table_id = ninox_resolve_table_id(TEAM_ID, DATABASE_ID, table_hint)
    url = f"{BASE_URL}/teams/{TEAM_ID}/databases/{DATABASE_ID}/tables/{table_id}/records"
    if not rows:
        return {"ok": True, "inserted": 0}
    inserted = 0
    for i in range(0, len(rows), batch_size):
        chunk = rows[i:i+batch_size]
        r = requests.post(url, headers=ninox_headers(), json=chunk, timeout=60)
        if r.status_code != 200:
            return {"ok": False, "inserted": inserted, "error": f"{r.status_code} {r.text}"}
        inserted += len(chunk)
    return {"ok": True, "inserted": inserted}

def ninox_get_fields(table_hint: str) -> set:
    table_id = ninox_resolve_table_id(TEAM_ID, DATABASE_ID, table_hint)
    info = ninox_list_tables(TEAM_ID, DATABASE_ID)
    fields = set()
    for t in info:
        if str(t.get("id")) == str(table_id):
            cols = t.get("fields") or t.get("columns") or []
            for c in cols:
                nm = c.get("name") if isinstance(c, dict) else None
                if nm: fields.add(nm)
            break
    return fields

def as_num(v) -> float:
    if v is None: return 0.0
    s = str(v).strip().replace(",", ".")
    if s == "" or s.upper() == "PM": return 0.0
    try: return float(s)
    except Exception: return 0.0

# ===================== Lectura de archivos =====================
def read_lista(upload) -> Optional[pd.DataFrame]:
    """
    Lee la LISTA DE CODIGO desde CSV/XLS/XLSX.
    - Si es Excel: busca la hoja cuyo nombre contenga 'asignar_DOSÍMETRO' (ignorando may/min).
    - Normaliza columnas clave.
    Campos esperados/útiles:
      CÓDIGO_DOSÍMETRO, PERIODO DE LECTURA, CÓDIGO DE USUARIO, NOMBRE, COMPAÑÍA, TIPO DE DOSÍMETRO, CÉDULA
    """
    if not upload:
        return None

    name = upload.name.lower()
    df = None
    if name.endswith((".xlsx", ".xls")):
        xls = pd.ExcelFile(upload)
        sheet = None
        for s in xls.sheet_names:
            if "asignar" in s.lower() and "dos" in s.lower():
                sheet = s; break
        if sheet is None:
            sheet = xls.sheet_names[0]
        df = pd.read_excel(xls, sheet_name=sheet)
    else:
        # CSV: intenta distintas codificaciones y separadores
        raw = upload.read(); upload.seek(0)
        tried = [
            dict(sep=";", engine="python", encoding="utf-8-sig"),
            dict(sep=None, engine="python", encoding="utf-8-sig"),
            dict(sep=None, engine="python", encoding="latin-1"),
            dict(sep=",", engine="python", encoding="utf-8-sig"),
        ]
        for opts in tried:
            try:
                df = pd.read_csv(BytesIO(raw), **opts); break
            except Exception:
                continue
        if df is None:
            df = pd.read_csv(BytesIO(raw))
    if df is None or df.empty:
        return None

    # Normalización nombres
    df.columns = [str(c).strip() for c in df.columns]

    # Acomoda alias de columnas
    rename_map = {}
    for c in df.columns:
        cl = c.lower()
        if "código" in cl and "dos" in cl:
            rename_map[c] = "CÓDIGO_DOSÍMETRO"
        elif "codigo" in cl and "dos" in cl:
            rename_map[c] = "CÓDIGO_DOSÍMETRO"
        elif cl in ("periodo de lectura", "período de lectura", "periodo", "período"):
            rename_map[c] = "PERIODO DE LECTURA"
        elif "código de usuario" in cl or "codigo de usuario" in cl or cl == "codigo usuario":
            rename_map[c] = "CÓDIGO DE USUARIO"
        elif cl == "compañía" or cl == "compania":
            rename_map[c] = "COMPAÑÍA"
        elif "tipo de dos" in cl:
            rename_map[c] = "TIPO DE DOSÍMETRO"
        elif cl == "cedula" or cl == "cédula":
            rename_map[c] = "CÉDULA"
    if rename_map:
        df = df.rename(columns=rename_map)

    needed = ["CÓDIGO_DOSÍMETRO", "PERIODO DE LECTURA"]
    for c in needed:
        if c not in df.columns:
            df[c] = ""

    # Limpiezas y banderas
    df["CÓDIGO_DOSÍMETRO"] = df["CÓDIGO_DOSÍMETRO"].astype(str).str.strip().str.upper()
    df["PERIODO DE LECTURA"] = df["PERIODO DE LECTURA"].astype(str).str.strip().str.upper().str.replace(r"\.+$", "", regex=True)
    if "CÓDIGO DE USUARIO" in df.columns:
        df["CÓDIGO DE USUARIO"] = df["CÓDIGO DE USUARIO"].astype(str).str.strip()

    # Nombre (si tienes NOMBRE+APELLIDO)
    if "NOMBRE" not in df.columns and "NOMBRE_COMPLETO" in df.columns:
        df["NOMBRE"] = df["NOMBRE_COMPLETO"]
    elif "NOMBRE" not in df.columns:
        df["NOMBRE"] = ""

    # Control flag
    def is_control_row(r):
        for k in ["NOMBRE", "CÉDULA", "CÓDIGO DE USUARIO"]:
            if k in r and str(r[k]).strip().upper() == "CONTROL":
                return True
        return False
    df["IS_CONTROL"] = df.apply(is_control_row, axis=1)

    return df

def read_dosis(upload) -> Optional[pd.DataFrame]:
    if not upload:
        return None
    name = upload.name.lower()

    if name.endswith((".xlsx", ".xls")):
        df = pd.read_excel(upload)
    else:
        raw = upload.read(); upload.seek(0)
        tried = [
            dict(sep=";", engine="python", encoding="utf-8-sig"),
            dict(sep=None, engine="python", encoding="utf-8-sig"),
            dict(sep=None, engine="python", encoding="latin-1"),
            dict(sep=",", engine="python", encoding="utf-8-sig"),
        ]
        df = None
        for opts in tried:
            try:
                df = pd.read_csv(BytesIO(raw), **opts); break
            except Exception:
                continue
        if df is None:
            df = pd.read_csv(BytesIO(raw))

    if df is None or df.empty:
        return None

    # Normaliza columnas
    norm = (
        df.columns.astype(str)
        .str.strip().str.lower()
        .str.replace(" ", "", regex=False)
        .str.replace("(", "").str.replace(")", "")
        .str.replace(".", "", regex=False)
    )
    df.columns = norm

    # dosimeter
    if "dosimeter" not in df.columns:
        for alt in ["dosimetro", "codigo", "codigodosimetro", "codigo_dosimetro"]:
            if alt in df.columns:
                df.rename(columns={alt: "dosimeter"}, inplace=True); break

    # hp fields
    for cand in ["hp10dosecorr", "hp10dose", "hp10"]:
        if cand in df.columns:
            df.rename(columns={cand: "hp10dose"}, inplace=True); break
    for cand in ["hp007dosecorr", "hp007dose", "hp007"]:
        if cand in df.columns:
            df.rename(columns={cand: "hp0.07dose"}, inplace=True); break
    for cand in ["hp3dosecorr", "hp3dose", "hp3"]:
        if cand in df.columns:
            df.rename(columns={cand: "hp3dose"}, inplace=True); break

    # Numéricos
    for k in ["hp10dose", "hp0.07dose", "hp3dose"]:
        if k in df.columns:
            df[k] = pd.to_numeric(df[k], errors="coerce").fillna(0.0)
        else:
            df[k] = 0.0

    if "dosimeter" in df.columns:
        df["dosimeter"] = df["dosimeter"].astype(str).str.strip().str.upper()

    # timestamp opcional
    if "timestamp" in df.columns:
        df["timestamp"] = pd.to_datetime(df["timestamp"], errors="coerce")

    return df

# ===================== UI: Sección 1 — Cargar LISTA DE CÓDIGO =====================
st.subheader("1) Cargar LISTA DE CÓDIGO")
upl_lista = st.file_uploader("Subir LISTA DE CÓDIGO (CSV/XLS/XLSX) — si es Excel, la hoja puede llamarse 'asignar_DOSÍMETRO...'", type=["csv", "xlsx", "xls"])
df_lista = read_lista(upl_lista) if upl_lista else None
if df_lista is not None and not df_lista.empty:
    st.session_state.df_lista = df_lista.copy()
    st.success(f"LISTA cargada: {len(df_lista)} fila(s)")
    st.dataframe(df_lista.head(20), use_container_width=True)
else:
    st.info("LISTA vacía o sin datos")

# ===================== UI: Sección 2 — Cargar dosis =====================
st.subheader("2) Subir Archivo de Dosis")
upl_dosis = st.file_uploader("Selecciona CSV/XLS/XLSX (dosis)", type=["csv", "xls", "xlsx"], key="upl_dosis")
df_dosis = read_dosis(upl_dosis) if upl_dosis else None
if df_dosis is not None and not df_dosis.empty:
    st.session_state.df_dosis = df_dosis.copy()
    st.success(f"Dosis cargadas: {len(df_dosis)} fila(s)")
    st.dataframe(df_dosis.head(20), use_container_width=True)
else:
    st.info("Sube un archivo de dosis para continuar")

# ===================== Filtros y cruce =====================
st.subheader("3) Cruce por PERIODO y CÓDIGO_DOSÍMETRO ↔ dosimeter")
if st.session_state.df_lista is not None and st.session_state.df_dosis is not None:
    dfL = st.session_state.df_lista.copy()
    dfD = st.session_state.df_dosis.copy()

    # Periodos disponibles (de la LISTA)
    periods = sorted(dfL["PERIODO DE LECTURA"].dropna().astype(str).unique().tolist())
    sel_periods = st.multiselect("Filtrar por PERIODO DE LECTURA (elige uno o varios; vacío = TODOS)", periods)
    if sel_periods:
        dfL = dfL[dfL["PERIODO DE LECTURA"].isin([p.upper() for p in sel_periods])]

    # Index para join por código (último timestamp por código en dosis)
    dfD = dfD.sort_values("timestamp")
    dfD_last = dfD.groupby("dosimeter", as_index=False).last()

    # Join por código
    merged = dfL.merge(dfD_last, left_on="CÓDIGO_DOSÍMETRO", right_on="dosimeter", how="left", indicator=True)
    misses = merged[merged["_merge"] == "left_only"][["CÓDIGO_DOSÍMETRO", "PERIODO DE LECTURA"]]
    if not misses.empty:
        with st.expander("⚠️ Códigos de LISTA sin dosis (no encontrados en dosimeter)"):
            st.dataframe(misses, use_container_width=True)

    # Filtra solo coincidencias
    matched = merged[merged["_merge"] == "both"].copy()
    if matched.empty:
        st.warning("No hay coincidencias CÓDIGO_DOSÍMETRO ↔ dosimeter (revisa periodos/códigos).")
    else:
        st.success(f"Coincidencias: {len(matched)}")
        st.dataframe(matched.head(20), use_container_width=True)

        # ===================== Subida a Ninox (BASE DE DATOS) =====================
        st.subheader("4) Subir coincidencias → Ninox (BASE DE DATOS)")

        subir_pm_como_texto = st.checkbox("Subir 'PM' como texto (si tu tabla tiene Hp como texto)", value=False)
        solo_debug_1 = st.checkbox("Solo 1 registro (debug)", value=False)

        # Campos en Ninox
        try:
            ninox_fields = ninox_get_fields(TABLE_BASE)
        except Exception as e:
            ninox_fields = set()
            st.error(f"No pude leer los campos de Ninox: {e}")

        map_fields = {
            "PERIODO DE LECTURA": "PERIODO DE LECTURA",
            "COMPAÑÍA": "COMPAÑÍA",
            "CÓDIGO_DOSÍMETRO": "CÓDIGO DE DOSÍMETRO",
            "CÓDIGO DE USUARIO": "CÓDIGO DE USUARIO",
            "NOMBRE": "NOMBRE",
            "CÉDULA": "CÉDULA",
            "TIPO DE DOSÍMETRO": "TIPO DE DOSÍMETRO",
            # Hp mapeados
            "hp10dose": "Hp (10)",
            "hp0.07dose": "Hp (0.07)",
            "hp3dose": "Hp (3)",
        }

        def cast_hp(v):
            if isinstance(v, str) and v.strip().upper() == "PM":
                return "PM" if subir_pm_como_texto else None
            try:
                return float(v)
            except Exception:
                return None

        rows = []
        it = matched.head(1).iterrows() if solo_debug_1 else matched.iterrows()
        for _, r in it:
            fields = {}
            for src, dst in map_fields.items():
                if ninox_fields and dst not in ninox_fields:
                    continue
                val = r.get(src, "")
                if dst in {"Hp (10)", "Hp (0.07)", "Hp (3)"}:
                    val = cast_hp(val)
                else:
                    val = "" if pd.isna(val) else str(val)
                fields[dst] = val
            rows.append({"fields": fields})

        if st.button("⬆️ Subir a BASE DE DATOS"):
            with st.spinner("Subiendo…"):
                res = ninox_insert_rows(TABLE_BASE, rows, batch_size=300)
            if res.get("ok"):
                st.success(f"✅ Subido a Ninox: {res.get('inserted', 0)} registro(s)")
            else:
                st.error(f"❌ Error al subir: {res.get('error')}")

# ===================== TAB: REPORTE FINAL =====================
st.markdown("---")
st.header("📊 Reporte Final: suma por **CÓDIGO DE USUARIO** (personas) y por **CÓDIGO DE DOSÍMETRO** (CONTROL)")

# Cargar tabla BASE DE DATOS desde Ninox
try:
    base_records = ninox_fetch_all(TABLE_BASE)
    rows = []
    for rec in base_records:
        f = rec.get("fields", {}) or {}
        rows.append({
            "PERIODO DE LECTURA": f.get("PERIODO DE LECTURA",""),
            "COMPAÑÍA": f.get("COMPAÑÍA",""),
            "CÓDIGO DE DOSÍMETRO": f.get("CÓDIGO DE DOSÍMETRO",""),
            "CÓDIGO DE USUARIO": f.get("CÓDIGO DE USUARIO",""),
            "NOMBRE": f.get("NOMBRE",""),
            "CÉDULA": f.get("CÉDULA",""),
            "TIPO DE DOSÍMETRO": f.get("TIPO DE DOSÍMETRO",""),
            "Hp (10)": as_num(f.get("Hp (10)")),
            "Hp (0.07)": as_num(f.get("Hp (0.07)")),
            "Hp (3)": as_num(f.get("Hp (3)")),
        })
    base = pd.DataFrame(rows)
except Exception as e:
    base = pd.DataFrame()
    st.error(f"No pude leer BASE DE DATOS: {e}")

if base.empty:
    st.info("Aún no hay datos en BASE DE DATOS.")
else:
    # Normaliza
    base["PERIODO DE LECTURA"] = base["PERIODO DE LECTURA"].astype(str).str.upper().str.replace(r"\.+$", "", regex=True)
    base["NOMBRE"] = base["NOMBRE"].fillna("").astype(str).str.strip()
    base["CÉDULA"] = base["CÉDULA"].fillna("").astype(str).str.strip()
    base["COMPAÑÍA"] = base["COMPAÑÍA"].fillna("").astype(str).str.strip()
    base["CÓDIGO DE USUARIO"] = base["CÓDIGO DE USUARIO"].fillna("").astype(str).str.strip()
    base["CÓDIGO DE DOSÍMETRO"] = base["CÓDIGO DE DOSÍMETRO"].fillna("").astype(str).str.strip().upper()

    # Filtros
    col1, col2 = st.columns([2,2])
    with col1:
        per_opts = sorted(base["PERIODO DE LECTURA"].dropna().unique().tolist())
        per_sel = st.multiselect("Filtrar por PERIODO(S) (vacío = todos)", per_opts)
    with col2:
        comp_opts = ["(todas)"] + sorted([c for c in base["COMPAÑÍA"].dropna().unique().tolist() if c])
        comp_sel = st.selectbox("Filtrar por COMPAÑÍA", comp_opts, index=0)

    df_rep = base.copy()
    if per_sel:
        df_rep = df_rep[df_rep["PERIODO DE LECTURA"].isin(per_sel)]
    if comp_sel != "(todas)":
        df_rep = df_rep[df_rep["COMPAÑÍA"] == comp_sel]

    if df_rep.empty:
        st.warning("No hay filas con los filtros seleccionados.")
    else:
        # Marca CONTROL
        df_rep["IS_CONTROL"] = df_rep.apply(
            lambda r: str(r["NOMBRE"]).upper()=="CONTROL" or str(r["CÉDULA"]).upper()=="CONTROL", axis=1
        )

        # Personas → agrupar por CÓDIGO DE USUARIO
        grp_user = (df_rep[~df_rep["IS_CONTROL"]]
                    .groupby(["PERIODO DE LECTURA","CÓDIGO DE USUARIO"], as_index=False)
                    .agg({
                        "COMPAÑÍA":"first",
                        "NOMBRE":"first",
                        "CÉDULA":"first",
                        "Hp (10)":"sum",
                        "Hp (0.07)":"sum",
                        "Hp (3)":"sum"
                    }))
        grp_user["AGRUPACIÓN"] = "POR USUARIO"

        # Control → agrupar por CÓDIGO DE DOSÍMETRO
        grp_ctrl = (df_rep[df_rep["IS_CONTROL"]]
                    .groupby(["PERIODO DE LECTURA","CÓDIGO DE DOSÍMETRO"], as_index=False)
                    .agg({
                        "COMPAÑÍA":"first",
                        "NOMBRE":"first",
                        "CÉDULA":"first",
                        "Hp (10)":"sum",
                        "Hp (0.07)":"sum",
                        "Hp (3)":"sum"
                    }))
        grp_ctrl["AGRUPACIÓN"] = "CONTROL (POR DOSÍMETRO)"

        # Unimos y ordenamos un poco
        # Uniformar nombres de columnas para mostrar en una sola tabla
        grp_user = grp_user.rename(columns={"CÓDIGO DE USUARIO":"CLAVE"})
        grp_ctrl = grp_ctrl.rename(columns={"CÓDIGO DE DOSÍMETRO":"CLAVE"})
        reporte = pd.concat([grp_user, grp_ctrl], ignore_index=True)
        reporte = reporte[[
            "AGRUPACIÓN","PERIODO DE LECTURA","CLAVE","COMPAÑÍA","NOMBRE","CÉDULA",
            "Hp (10)","Hp (0.07)","Hp (3)"
        ]].sort_values(["AGRUPACIÓN","PERIODO DE LECTURA","CLAVE"])

        st.success(f"Reporte generado: {len(reporte)} fila(s)")
        st.dataframe(reporte, use_container_width=True)

        # Descargas
        csv_bytes = reporte.to_csv(index=False).encode("utf-8-sig")
        st.download_button("⬇️ Descargar Reporte (CSV)", data=csv_bytes,
                           file_name=f"ReporteFinal_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
                           mime="text/csv")

        def to_excel_bytes(df: pd.DataFrame, sheet_name="Reporte"):
            bio = BytesIO()
            with pd.ExcelWriter(bio, engine="openpyxl") as w:
                df.to_excel(w, index=False, sheet_name=sheet_name)
            bio.seek(0)
            return bio.read()

        xlsx_bytes = to_excel_bytes(reporte)
        st.download_button("⬇️ Descargar Reporte (Excel)",
                           data=xlsx_bytes,
                           file_name=f"ReporteFinal_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx",
                           mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")
