# -*- coding: utf-8 -*-
import io, re, unicodedata
from io import BytesIO
from typing import Any, Dict, List, Optional

import pandas as pd
import requests
import streamlit as st
from datetime import datetime

# ===================== NINOX CONFIG =====================
API_TOKEN   = "edf312a0-98b8-11f0-883e-db77626d62e5"
TEAM_ID     = "YrsYfTegptdZcHJEj"
DATABASE_ID = "ow1geqnkz00e"
BASE_URL    = "https://api.ninox.com/v1"

TABLE_WRITE_NAME = "BASE DE DATOS"  # escritura en Ninox

# ===================== UI =====================
st.set_page_config(page_title="Microsievert — Dosimetría", page_icon="🧪", layout="wide")
st.title("🧪 Carga y Cruce de Dosis → Ninox (**BASE DE DATOS**)")

# ===================== Helpers =====================
def ninox_headers():
    return {"Authorization": f"Bearer {API_TOKEN}", "Content-Type": "application/json"}

@st.cache_data(ttl=300, show_spinner=False)
def ninox_list_tables(team_id: str, db_id: str):
    url = f"{BASE_URL}/teams/{team_id}/databases/{db_id}/tables"
    r = requests.get(url, headers=ninox_headers(), timeout=30)
    r.raise_for_status()
    return r.json()

def resolve_table_id(table_hint: str) -> str:
    """Acepta ID corto o NOMBRE legible y devuelve ID real."""
    hint = (table_hint or "").strip()
    if hint and " " not in hint and len(hint) <= 8:
        return hint
    for t in ninox_list_tables(TEAM_ID, DATABASE_ID):
        if str(t.get("name", "")).strip().lower() == hint.lower():
            return str(t.get("id", "")).strip()
    return hint

def ninox_insert(table_hint: str, rows: List[Dict[str, Any]], batch_size: int = 300) -> Dict[str, Any]:
    table_id = resolve_table_id(table_hint)
    url = f"{BASE_URL}/teams/{TEAM_ID}/databases/{DATABASE_ID}/tables/{table_id}/records"
    n, inserted = len(rows), 0
    if n == 0:
        return {"ok": True, "inserted": 0}
    for i in range(0, n, batch_size):
        chunk = rows[i:i+batch_size]
        r = requests.post(url, headers=ninox_headers(), json=chunk, timeout=60)
        if r.status_code != 200:
            return {"ok": False, "inserted": inserted, "error": f"{r.status_code} {r.text}"}
        inserted += len(chunk)
    return {"ok": True, "inserted": inserted}

def strip_accents(s: str) -> str:
    return "".join(c for c in unicodedata.normalize("NFD", s) if unicodedata.category(c) != "Mn")

def norm_cols(df: pd.DataFrame) -> pd.DataFrame:
    def _n(x: str) -> str:
        x = strip_accents(str(x)).strip()
        x = re.sub(r"\s+", " ", x)
        return x
    out = df.copy()
    out.columns = [_n(c) for c in out.columns]
    return out

def coalesce_col(df: pd.DataFrame, candidates: List[str]) -> Optional[str]:
    got = [c for c in df.columns if c in candidates]
    return got[0] if got else None

def parse_csv_robust(upload) -> pd.DataFrame:
    raw = upload.read(); upload.seek(0)
    for sep in [";", ",", None]:
        for enc in ["utf-8-sig", "latin-1"]:
            try:
                return pd.read_csv(BytesIO(raw), sep=sep, engine="python", encoding=enc)
            except Exception:
                continue
    try:
        return pd.read_excel(BytesIO(raw))
    except Exception:
        raise

def _to_dt_str(ts):
    if pd.isna(ts): return ""
    try:
        return pd.to_datetime(ts).strftime("%d/%m/%Y %H:%M")
    except Exception:
        return str(ts)

# ===================== Lectores =====================
def leer_lista_codigo(upload) -> Optional[pd.DataFrame]:
    """
    Lee LISTA DE CÓDIGO desde CSV/XLS/XLSX.
    Si es Excel, intenta hoja que contenga 'asignar' y 'dosimet' en el nombre; si no, usa la primera.
    Normaliza a columnas estándar:
    - CÉDULA, CÓDIGO DE USUARIO, NOMBRE, CLIENTE, CÓDIGO_DOSÍMETRO, PERIODO DE LECTURA, TIPO DE DOSÍMETRO, ETIQUETA, _IS_CONTROL
    """
    if not upload: return None
    name = upload.name.lower()
    if name.endswith((".xlsx", ".xls")):
        xls = pd.ExcelFile(upload)
        sheet = None
        for s in xls.sheet_names:
            s_norm = strip_accents(s).lower()
            if "asignar" in s_norm and "dosimet" in s_norm:
                sheet = s; break
        if sheet is None:
            sheet = xls.sheet_names[0]
        df = pd.read_excel(xls, sheet_name=sheet)
    else:
        df = parse_csv_robust(upload)

    df = norm_cols(df)
    c_ced   = coalesce_col(df, ["CEDULA"])
    c_user  = coalesce_col(df, ["CODIGO DE USUARIO","CODIGO_USUARIO","CODIGO USUARIO"])
    c_nom   = coalesce_col(df, ["NOMBRE","NOMBRE COMPLETO","NOMBRE_COMPLETO"])
    c_ap    = coalesce_col(df, ["APELLIDO","APELLIDOS"])
    c_cli   = coalesce_col(df, ["CLIENTE","COMPANIA"])
    c_cod   = coalesce_col(df, ["CODIGO_DOSIMETRO","CODIGO DE DOSIMETRO","CODIGO DOSIMETRO","CÓDIGO_DOSÍMETRO","CÓDIGO DE DOSÍMETRO"])
    c_per   = coalesce_col(df, ["PERIODO DE LECTURA","PERIODO_DE_LECTURA","PERIODO"])
    c_tipo  = coalesce_col(df, ["TIPO DE DOSIMETRO","TIPO_DE_DOSIMETRO","TIPO DOSIMETRO","TIPO DE DOSÍMETRO"])
    c_etq   = coalesce_col(df, ["ETIQUETA"])

    out = pd.DataFrame()
    out["CÉDULA"]            = df[c_ced] if c_ced else ""
    out["CÓDIGO DE USUARIO"] = df[c_user] if c_user else ""
    if c_nom and c_ap:
        out["NOMBRE"] = (df[c_nom].astype(str).str.strip() + " " + df[c_ap].astype(str).str.strip()).str.strip()
    elif c_nom:
        out["NOMBRE"] = df[c_nom].astype(str).str.strip()
    else:
        out["NOMBRE"] = ""
    out["CLIENTE"]            = df[c_cli].astype(str).str.strip() if c_cli else ""
    out["CÓDIGO_DOSÍMETRO"]   = (df[c_cod].astype(str).str.strip().str.upper() if c_cod else "")
    out["PERIODO DE LECTURA"] = (df[c_per].astype(str).str.strip().str.upper() if c_per else "")
    out["TIPO DE DOSÍMETRO"]  = df[c_tipo].astype(str).str.strip() if c_tipo else ""
    out["ETIQUETA"]           = df[c_etq].astype(str).str.strip() if c_etq else ""

    # marca de control
    def _is_ctrl(r):
        for k in ["ETIQUETA","NOMBRE","CÉDULA","CÓDIGO DE USUARIO"]:
            v = str(r.get(k, "")).strip().upper()
            if v == "CONTROL":
                return True
        return False
    out["_IS_CONTROL"] = out.apply(_is_ctrl, axis=1)

    # normaliza campos clave
    out["CÓDIGO_DOSÍMETRO"]   = out["CÓDIGO_DOSÍMETRO"].astype(str).str.strip().str.upper()
    out["PERIODO DE LECTURA"] = out["PERIODO DE LECTURA"].astype(str).str.strip().str.upper()

    return out

def leer_dosis(upload) -> Optional[pd.DataFrame]:
    if not upload: return None
    name = upload.name.lower()
    if name.endswith((".xlsx",".xls")):
        df = pd.read_excel(upload)
    else:
        df = parse_csv_robust(upload)

    # normaliza columnas
    cols = (df.columns.astype(str).str.strip().str.lower()
            .str.replace(" ", "", regex=False)
            .str.replace("(", "").str.replace(")", "")
            .str.replace(".", "", regex=False))
    df.columns = cols

    # campos críticos
    if "dosimeter" not in df.columns:
        for alt in ["dosimetro","codigo","codigodosimetro","codigo_dosimetro"]:
            if alt in df.columns:
                df.rename(columns={alt:"dosimeter"}, inplace=True); break

    for cands, dest in [
        (["hp10dosecorr","hp10dose","hp10"], "hp10dose"),
        (["hp007dosecorr","hp007dose","hp007"], "hp0.07dose"),
        (["hp3dosecorr","hp3dose","hp3"], "hp3dose")
    ]:
        for c in cands:
            if c in df.columns:
                df.rename(columns={c:dest}, inplace=True); break
        if dest not in df.columns:
            df[dest] = 0.0
        else:
            df[dest] = pd.to_numeric(df[dest], errors="coerce").fillna(0.0)

    if "dosimeter" in df.columns:
        df["dosimeter"] = df["dosimeter"].astype(str).str.strip().str.upper()
    if "timestamp" in df.columns:
        df["timestamp"] = pd.to_datetime(df["timestamp"], errors="coerce")

    return df

# ===================== Valor−Control (3 decimales + PM) =====================
def aplicar_valor_menos_control_3dec(df: pd.DataFrame) -> pd.DataFrame:
    """
    Resta, por cada grupo (PERIODO DE LECTURA, CLIENTE), el valor de la fila CONTROL
    a todas las demás filas. Si el resultado es < 0.005 => 'PM' (texto).
    Si no, se muestra el valor redondeado a 3 decimales.
    El CONTROL se mantiene con sus valores base (también a 3 decimales).
    """
    if df is None or df.empty:
        return df

    def _fmt3(x: float) -> str:
        try:
            return f"{float(x):.3f}"
        except Exception:
            return "0.000"

    out = df.copy()

    # Asegura tipos numéricos
    for h in ["Hp (10)", "Hp (0.07)", "Hp (3)"]:
        out[h] = pd.to_numeric(out[h], errors="coerce").fillna(0.0)

    # Marca de control
    out["_is_ctrl"] = out["NOMBRE"].fillna("").astype(str).str.strip().str.upper().eq("CONTROL")

    # Resta por grupo
    for (per, cli), g in out.groupby(["PERIODO DE LECTURA", "CLIENTE"], dropna=False):
        g_ctrl = g[g["_is_ctrl"]]
        if g_ctrl.empty:
            # Sin control: solo formato a 3 decimales
            for i in g.index:
                out.at[i, "Hp (10)"]   = _fmt3(out.at[i, "Hp (10)"])
                out.at[i, "Hp (0.07)"] = _fmt3(out.at[i, "Hp (0.07)"])
                out.at[i, "Hp (3)"]    = _fmt3(out.at[i, "Hp (3)"])
            continue

        # Primer CONTROL del grupo como base
        b10 = float(g_ctrl.iloc[0]["Hp (10)"])
        b07 = float(g_ctrl.iloc[0]["Hp (0.07)"])
        b03 = float(g_ctrl.iloc[0]["Hp (3)"])

        for i in g.index:
            if out.at[i, "_is_ctrl"]:
                out.at[i, "Hp (10)"]   = _fmt3(b10)
                out.at[i, "Hp (0.07)"] = _fmt3(b07)
                out.at[i, "Hp (3)"]    = _fmt3(b03)
            else:
                d10 = float(out.at[i, "Hp (10)"])   - b10
                d07 = float(out.at[i, "Hp (0.07)"]) - b07
                d03 = float(out.at[i, "Hp (3)"])    - b03

                out.at[i, "Hp (10)"]   = "PM" if d10 < 0.005 else _fmt3(d10)
                out.at[i, "Hp (0.07)"] = "PM" if d07 < 0.005 else _fmt3(d07)
                out.at[i, "Hp (3)"]    = "PM" if d03 < 0.005 else _fmt3(d03)

    return out.drop(columns=["_is_ctrl"], errors="ignore")

# ===================== Construcción de registros (match + periodo exacto) =====================
def construir_registros(df_lista: pd.DataFrame,
                        df_dosis: pd.DataFrame,
                        periodos: List[str]) -> pd.DataFrame:
    # Filtro EXACTO de período (ej. "AGOSTO 2025")
    df_l = df_lista.copy()
    df_l["PERIODO DE LECTURA"] = df_l["PERIODO DE LECTURA"].astype(str).str.strip().str.upper()
    df_l["CÓDIGO_DOSÍMETRO"]   = df_l["CÓDIGO_DOSÍMETRO"].astype(str).str.strip().str.upper()

    selected = [p.strip().upper() for p in periodos if str(p).strip()]
    if selected:
        df_l = df_l[df_l["PERIODO DE LECTURA"].isin(selected)]

    # índice por dosímetro (para match exacto)
    idx = df_dosis.set_index("dosimeter") if "dosimeter" in df_dosis.columns else pd.DataFrame().set_index(pd.Index([]))

    registros = []
    # Control primero
    df_l = pd.concat([df_l[df_l["_IS_CONTROL"]], df_l[~df_l["_IS_CONTROL"]]], ignore_index=True)

    for _, r in df_l.iterrows():
        cod = str(r["CÓDIGO_DOSÍMETRO"]).strip().str.upper()
        if not cod or cod == "NAN":
            continue
        if cod not in idx.index:
            continue

        d = idx.loc[cod]
        if isinstance(d, pd.DataFrame):
            d = d.sort_values(by="timestamp").iloc[-1]
        ts = d.get("timestamp", pd.NaT)
        fecha_str = _to_dt_str(ts)

        registros.append({
            "PERIODO DE LECTURA": r["PERIODO DE LECTURA"],
            "CLIENTE": r.get("CLIENTE",""),                    # cliente desde LISTA
            "CÓDIGO DE DOSÍMETRO": cod,
            "CÓDIGO DE USUARIO": r.get("CÓDIGO DE USUARIO",""),
            "NOMBRE": "CONTROL" if r["_IS_CONTROL"] else r.get("NOMBRE",""),
            "CÉDULA": r.get("CÉDULA",""),
            "FECHA DE LECTURA": fecha_str,                    # timestamp → FECHA DE LECTURA
            "TIPO DE DOSÍMETRO": r.get("TIPO DE DOSÍMETRO","") or "CE",
            "Hp (10)":  float(d.get("hp10dose", 0.0) or 0.0),
            "Hp (0.07)":float(d.get("hp0.07dose", 0.0) or 0.0),
            "Hp (3)":   float(d.get("hp3dose", 0.0) or 0.0),
            "_IS_CONTROL": bool(r["_IS_CONTROL"])
        })

    df_final = pd.DataFrame(registros)
    if not df_final.empty:
        df_final = df_final.sort_values(["_IS_CONTROL","NOMBRE","CÉDULA"], ascending=[False, True, True]).reset_index(drop=True)
    return df_final

# ===================== TABS =====================
tab1, tab2 = st.tabs(["1) Cargar y Subir a Ninox", "2) Reporte Final (sumas)"])

# ------------------ TAB 1 ------------------
with tab1:
    st.subheader("1) Cargar LISTA DE CÓDIGO")
    upl_lista = st.file_uploader("Sube la LISTA DE CÓDIGO (CSV / XLS / XLSX)", type=["csv","xls","xlsx"], key="upl_lista")
    df_lista = leer_lista_codigo(upl_lista) if upl_lista else None
    if df_lista is not None and not df_lista.empty:
        st.success(f"LISTA cargada: {len(df_lista)} filas")
        st.dataframe(df_lista.head(20), use_container_width=True)
    else:
        st.info("LISTA vacía o sin datos")

    st.subheader("2) Subir Archivo de Dosis")
    upl_dosis = st.file_uploader("Selecciona CSV/XLS/XLSX (dosis)", type=["csv","xls","xlsx"], key="upl_dosis")
    df_dosis = leer_dosis(upl_dosis) if upl_dosis else None
    if df_dosis is not None and not df_dosis.empty:
        st.success(f"Dosis cargadas: {len[df_dosis]} fila(s)")
        st.dataframe(df_dosis.head(15), use_container_width=True)

    # Filtro de períodos (exactos, ej. AGOSTO 2025)
    per_options = sorted(df_lista["PERIODO DE LECTURA"].dropna().astype(str).str.upper().unique().tolist()) if df_lista is not None else []
    periodos_sel = st.multiselect("Filtrar por PERIODO DE LECTURA (elige uno o varios; vacío = TODOS)", per_options, default=[])

    colA, colB = st.columns([1,1])
    with colA:
        nombre_reporte = st.text_input("Nombre archivo (sin extensión)", value=f"ReporteDosimetria_{datetime.now().strftime('%Y-%m-%d')}")
    with colB:
        subir_pm_como_texto = st.checkbox("Guardar 'PM' como texto (si Hp son texto en Ninox)", value=True)

    btn_proc = st.button("✅ Procesar y Previsualizar", type="primary")
    if btn_proc:
        if df_lista is None or df_lista.empty:
            st.error("Primero sube la LISTA DE CÓDIGO.")
        elif df_dosis is None or df_dosis.empty:
            st.error("Sube el archivo de dosis.")
        elif "dosimeter" not in df_dosis.columns:
            st.error("El archivo de dosis debe incluir la columna 'dosimeter'.")
        else:
            df_final = construir_registros(df_lista, df_dosis, periodos_sel)

            if df_final.empty:
                with st.expander("Debug de coincidencias (no se encontraron)"):
                    st.write({
                        "dosimeter únicos en dosis": sorted(df_dosis["dosimeter"].dropna().unique().tolist()) if "dosimeter" in df_dosis.columns else [],
                        "CÓDIGO_DOSÍMETRO únicos en LISTA (según filtro)": sorted(df_lista["CÓDIGO_DOSÍMETRO"].dropna().unique().tolist()) if "CÓDIGO_DOSÍMETRO" in df_lista.columns else []
                    })
                st.warning("⚠️ No hay coincidencias **CÓDIGO_DOSÍMETRO** ↔ **dosimeter** (revisa periodos/códigos).")
            else:
                # Valor−Control (3 decimales, PM<0.005)
                df_final = aplicar_valor_menos_control_3dec(df_final)

                st.session_state.df_final = df_final.drop(columns=["_IS_CONTROL"], errors="ignore")
                st.success(f"¡Listo! Registros generados: {len(st.session_state.df_final)}")
                st.dataframe(st.session_state.df_final, use_container_width=True)

    st.markdown("---")
    st.subheader("3) Subir TODO a Ninox (tabla **BASE DE DATOS**)")

    def _hp_value(v, as_text_pm=True):
        if isinstance(v, str) and v.strip().upper() == "PM":
            return "PM" if as_text_pm else None
        try:
            return float(v)
        except Exception:
            return v if v is not None else None

    def _to_str(v):
        if pd.isna(v): return ""
        if isinstance(v, (pd.Timestamp, )):
            return v.strftime("%Y-%m-%d %H:%M:%S")
        return str(v)

    if st.button("⬆️ Subir a Ninox (BASE DE DATOS)"):
        df_final = st.session_state.get("df_final")
        if df_final is None or df_final.empty:
            st.error("No hay datos procesados. Pulsa 'Procesar y Previsualizar' primero.")
        else:
            rows = []
            for _, row in df_final.iterrows():
                fields = {
                    "PERIODO DE LECTURA": _to_str(row.get("PERIODO DE LECTURA","")),
                    "CLIENTE": _to_str(row.get("CLIENTE","")),
                    "CÓDIGO DE DOSÍMETRO": _to_str(row.get("CÓDIGO DE DOSÍMETRO","")),
                    "CÓDIGO DE USUARIO": _to_str(row.get("CÓDIGO DE USUARIO","")),
                    "NOMBRE": _to_str(row.get("NOMBRE","")),
                    "CÉDULA": _to_str(row.get("CÉDULA","")),
                    "FECHA DE LECTURA": _to_str(row.get("FECHA DE LECTURA","")),  # timestamp formateado
                    "TIPO DE DOSÍMETRO": _to_str(row.get("TIPO DE DOSÍMETRO","")),
                    "Hp (10)":  _hp_value(row.get("Hp (10)"), subir_pm_como_texto),
                    "Hp (0.07)":_hp_value(row.get("Hp (0.07)"), subir_pm_como_texto),
                    "Hp (3)":   _hp_value(row.get("Hp (3)"), subir_pm_como_texto),
                }
                rows.append({"fields": fields})

            with st.spinner("Subiendo a Ninox..."):
                res = ninox_insert(TABLE_WRITE_NAME, rows, batch_size=300)

            if res.get("ok"):
                st.success(f"✅ Subido a Ninox: {res.get('inserted', 0)} registro(s).")
            else:
                st.error(f"❌ Error al subir: {res.get('error')}")

# ------------------ TAB 2 ------------------
with tab2:
    st.subheader("📊 Reporte Final: suma por **CÓDIGO DE USUARIO** (personas) y **CONTROL** por **CÓDIGO DE DOSÍMETRO**")
    df_rep = st.session_state.get("df_final")

    if df_rep is None or df_rep.empty:
        st.info("No hay datos en memoria. Genera el cruce en la pestaña 1 para ver el reporte.")
    else:
        # Normalizaciones defensivas
        for col in ["CÓDIGO DE USUARIO","CÓDIGO DE DOSÍMETRO","NOMBRE"]:
            if col in df_rep.columns:
                df_rep[col] = df_rep[col].fillna("").astype(str).str.strip()

        # Convertir PM→0.0 para sumar
        rep = df_rep.copy()
        for h in ["Hp (10)","Hp (0.07)","Hp (3)"]:
            rep[h] = pd.to_numeric(rep[h].replace("PM", 0.0), errors="coerce").fillna(0.0)

        # Personas: agrupar por CÓDIGO DE USUARIO (excluye CONTROL)
        personas = rep[rep["NOMBRE"].str.strip().str.upper() != "CONTROL"].copy()
        if not personas.empty:
            per_group = personas.groupby("CÓDIGO DE USUARIO", as_index=False).agg({
                "CLIENTE":"last","NOMBRE":"last","CÉDULA":"last",
                "Hp (10)":"sum","Hp (0.07)":"sum","Hp (3)":"sum"
            }).rename(columns={
                "Hp (10)":"Hp10_SUM","Hp (0.07)":"Hp007_SUM","Hp (3)":"Hp3_SUM"
            })
            for c in ["Hp10_SUM","Hp007_SUM","Hp3_SUM"]:
                per_group[c] = per_group[c].map(lambda x: f"{x:.3f}")
            st.markdown("### Personas — Suma por **CÓDIGO DE USUARIO**")
            st.dataframe(per_group, use_container_width=True)
        else:
            st.info("No hay filas de personas (todas serían CONTROL).")

        # Control: agrupar por CÓDIGO DE DOSÍMETRO
        control = rep[rep["NOMBRE"].str.strip().str.upper() == "CONTROL"].copy()
        if not control.empty:
            ctrl_group = control.groupby("CÓDIGO DE DOSÍMETRO", as_index=False).agg({
                "CLIENTE":"last","Hp (10)":"sum","Hp (0.07)":"sum","Hp (3)":"sum"
            }).rename(columns={
                "Hp (10)":"Hp10_SUM","Hp (0.07)":"Hp007_SUM","Hp (3)":"Hp3_SUM"
            })
            for c in ["Hp10_SUM","Hp007_SUM","Hp3_SUM"]:
                ctrl_group[c] = ctrl_group[c].map(lambda x: f"{x:.3f}")
            st.markdown("### CONTROL — Suma por **CÓDIGO DE DOSÍMETRO**")
            st.dataframe(ctrl_group, use_container_width=True)
        else:
            st.info("No hay filas de CONTROL en el cruce actual.")



