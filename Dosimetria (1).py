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

# ===================== Lectores =====================
def leer_lista_codigo(upload) -> Optional[pd.DataFrame]:
    """Lee LISTA DE CÓDIGO desde CSV/XLS/XLSX (si es Excel, intenta hoja 'asignar_DOSÍMETRO...' o la primera)."""
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
    # columnas candidatas
    c_ced   = coalesce_col(df, ["CEDULA"])
    c_user  = coalesce_col(df, ["CODIGO DE USUARIO","CODIGO_USUARIO","CODIGO USUARIO"])
    c_nom   = coalesce_col(df, ["NOMBRE","NOMBRE COMPLETO","NOMBRE_COMPLETO"])
    c_ap    = coalesce_col(df, ["APELLIDO","APELLIDOS"])
    c_cli   = coalesce_col(df, ["CLIENTE","COMPANIA"])
    c_cod   = coalesce_col(df, ["CODIGO_DOSIMETRO","CODIGO DE DOSIMETRO","CODIGO DOSIMETRO"])
    c_per   = coalesce_col(df, ["PERIODO DE LECTURA","PERIODO_DE_LECTURA","PERIODO"])
    c_tipo  = coalesce_col(df, ["TIPO DE DOSIMETRO","TIPO_DE_DOSIMETRO","TIPO DOSIMETRO"])
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
    out["CLIENTE"]           = df[c_cli].astype(str).str.strip() if c_cli else ""
    out["CÓDIGO_DOSÍMETRO"]  = (df[c_cod].astype(str).str.strip().str.upper() if c_cod else "")
    out["PERIODO DE LECTURA"] = (df[c_per].astype(str).str.strip().str.upper() if c_per else "")
    out["TIPO DE DOSÍMETRO"] = df[c_tipo].astype(str).str.strip() if c_tipo else ""
    out["ETIQUETA"]          = df[c_etq].astype(str).str.strip() if c_etq else ""

    # marca de control
    def _is_ctrl(r):
        for k in ["ETIQUETA","NOMBRE","CÉDULA","CÓDIGO DE USUARIO"]:
            v = str(r.get(k, "")).strip().upper()
            if v == "CONTROL":
                return True
        return False
    out["_IS_CONTROL"] = out.apply(_is_ctrl, axis=1)
    return out

def leer_dosis(upload) -> Optional[pd.DataFrame]:
    if not upload: return None
    name = upload.name.lower()
    if name.endswith((".xlsx",".xls")):
        df = pd.read_excel(upload)
    else:
        df = parse_csv_robust(upload)
    # normaliza
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
    for cands, dest in [ (["hp10dosecorr","hp10dose","hp10"], "hp10dose"),
                         (["hp007dosecorr","hp007dose","hp007"], "hp0.07dose"),
                         (["hp3dosecorr","hp3dose","hp3"], "hp3dose") ]:
        for c in cands:
            if c in df.columns: df.rename(columns={c:dest}, inplace=True); break
        if dest not in df.columns: df[dest] = 0.0
        else: df[dest] = pd.to_numeric(df[dest], errors="coerce").fillna(0.0)
    if "dosimeter" in df.columns:
        df["dosimeter"] = df["dosimeter"].astype(str).str.strip().str.upper()
    if "timestamp" in df.columns:
        df["timestamp"] = pd.to_datetime(df["timestamp"], errors="coerce")
    return df

# ===================== Construcción de registros (match + periodo) =====================
def construir_registros(df_lista: pd.DataFrame,
                        df_dosis: pd.DataFrame,
                        periodos: List[str]) -> pd.DataFrame:
    # Filtrado por periodo(s)
    df_l = df_lista.copy()
    df_l["PERIODO DE LECTURA"] = df_l["PERIODO DE LECTURA"].astype(str).str.strip().str.upper()
    df_l["CÓDIGO_DOSÍMETRO"]   = df_l["CÓDIGO_DOSÍMETRO"].astype(str).str.strip().str.upper()

    selected = [p.strip().upper() for p in periodos if str(p).strip()]
    if selected:
        df_l = df_l[df_l["PERIODO DE LECTURA"].isin(selected)]

    # índice por dosímetro
    idx = df_dosis.set_index("dosimeter") if "dosimeter" in df_dosis.columns else pd.DataFrame().set_index(pd.Index([]))

    registros = []
    # Control primero
    df_l = pd.concat([df_l[df_l["_IS_CONTROL"]], df_l[~df_l["_IS_CONTROL"]]], ignore_index=True)

    for _, r in df_l.iterrows():
        cod = str(r["CÓDIGO_DOSÍMETRO"]).strip().upper()
        if not cod or cod == "NAN":
            continue
        if cod not in idx.index:
            continue
        d = idx.loc[cod]
        if isinstance(d, pd.DataFrame):
            d = d.sort_values(by="timestamp").iloc[-1]
        ts = d.get("timestamp", pd.NaT)
        fecha_str = ""
        try:
            fecha_str = pd.to_datetime(ts).strftime("%d/%m/%Y %H:%M") if pd.notna(ts) else ""
        except Exception:
            fecha_str = ""

        registros.append({
            "PERIODO DE LECTURA": r["PERIODO DE LECTURA"],
            "CLIENTE": r.get("CLIENTE",""),
            "CÓDIGO DE DOSÍMETRO": cod,
            "CÓDIGO DE USUARIO": r.get("CÓDIGO DE USUARIO",""),
            "NOMBRE": r.get("NOMBRE",""),
            "CÉDULA": r.get("CÉDULA",""),
            "FECHA DE LECTURA": fecha_str,
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

# ===================== Resta de CONTROL + Formato PM/3 dec =====================
def aplicar_resta_control_y_formato(df_final: pd.DataFrame, umbral_pm: float = 0.005):
    """
    Resta el valor del CONTROL a cada fila de persona usando claves:
    (PERIODO DE LECTURA, CLIENTE, TIPO DE DOSÍMETRO) → (PERIODO, CLIENTE) → (PERIODO).
    Tras la resta, valores < umbral_pm => 'PM'. Devuelve:
      - df_vista: DF listo para mostrar/subir (Hp formateados a 'PM' o 3 decimales)
      - df_num:   DF con columnas numéricas corregidas para agregaciones
    """
    if df_final is None or df_final.empty:
        return df_final, df_final

    df = df_final.copy()

    # Asegurar claves
    for c in ["PERIODO DE LECTURA", "CLIENTE", "TIPO DE DOSÍMETRO", "NOMBRE"]:
        if c not in df.columns:
            df[c] = ""

    # Asegurar Hp numéricos
    for h in ["Hp (10)", "Hp (0.07)", "Hp (3)"]:
        if h not in df.columns:
            df[h] = 0.0
        df[h] = pd.to_numeric(df[h], errors="coerce").fillna(0.0)

    # Separar control y personas
    is_control = df["NOMBRE"].astype(str).str.strip().str.upper().eq("CONTROL")
    df_ctrl = df[is_control].copy()
    df_per  = df[~is_control].copy()

    # Construir tablas de control en varios niveles de especificidad
    def agg_ctrl(g):
        return g.agg({"Hp (10)":"mean","Hp (0.07)":"mean","Hp (3)":"mean"})

    ctrl_lvl3 = df_ctrl.groupby(["PERIODO DE LECTURA","CLIENTE","TIPO DE DOSÍMETRO"], as_index=False).apply(agg_ctrl)
    ctrl_lvl2 = df_ctrl.groupby(["PERIODO DE LECTURA","CLIENTE"], as_index=False).apply(agg_ctrl)
    ctrl_lvl1 = df_ctrl.groupby(["PERIODO DE LECTURA"], as_index=False).apply(agg_ctrl)

    # Merge progresivo: primero nivel 3, después 2, luego 1
    out = df_per.copy()
    for lvl, keys in [
        (ctrl_lvl3, ["PERIODO DE LECTURA","CLIENTE","TIPO DE DOSÍMETRO"]),
        (ctrl_lvl2, ["PERIODO DE LECTURA","CLIENTE"]),
        (ctrl_lvl1, ["PERIODO DE LECTURA"]),
    ]:
        if isinstance(lvl, pd.DataFrame) and not lvl.empty:
            out = out.merge(
                lvl.rename(columns={
                    "Hp (10)":"Hp10_CTRL",
                    "Hp (0.07)":"Hp007_CTRL",
                    "Hp (3)":"Hp3_CTRL"
                }),
                on=keys, how="left"
            )

    # Consolidar columnas de control (maneja posibles sufijos de merge)
    def first_nonnull(series: pd.Series) -> float:
        for v in series:
            if pd.notna(v):
                return float(v)
        return 0.0

    out["Hp10_CTRL"]  = out.filter(regex=r"^Hp10_CTRL").apply(first_nonnull, axis=1) if not out.filter(regex=r"^Hp10_CTRL").empty else 0.0
    out["Hp007_CTRL"] = out.filter(regex=r"^Hp007_CTRL").apply(first_nonnull, axis=1) if not out.filter(regex=r"^Hp007_CTRL").empty else 0.0
    out["Hp3_CTRL"]   = out.filter(regex=r"^Hp3_CTRL").apply(first_nonnull, axis=1) if not out.filter(regex=r"^Hp3_CTRL").empty else 0.0

    # Calcular valores corregidos (no negativos)
    out["_Hp10_NUM"]  = (out["Hp (10)"]   - out["Hp10_CTRL"]).clip(lower=0.0)
    out["_Hp007_NUM"] = (out["Hp (0.07)"] - out["Hp007_CTRL"]).clip(lower=0.0)
    out["_Hp3_NUM"]   = (out["Hp (3)"]    - out["Hp3_CTRL"]).clip(lower=0.0)

    # Formateo visible: PM si < umbral, si no a 3 decimales (siempre string)
    def fmt(v: float) -> str:
        return "PM" if float(v) < umbral_pm else f"{float(v):.3f}"

    out_view = out.copy()
    out_view["Hp (10)"]   = out_view["_Hp10_NUM"].map(fmt)
    out_view["Hp (0.07)"] = out_view["_Hp007_NUM"].map(fmt)
    out_view["Hp (3)"]    = out_view["_Hp3_NUM"].map(fmt)

    # CONTROL: formateo a 3 decimales (string)
    df_ctrl_view = df_ctrl.copy()
    for h in ["Hp (10)","Hp (0.07)","Hp (3)"]:
        df_ctrl_view[h] = df_ctrl_view[h].map(lambda x: f"{float(x):.3f}")

    # Unir de nuevo (CONTROL arriba)
    df_vista = pd.concat([df_ctrl_view, out_view], ignore_index=True)

    # Orden: CONTROL primero
    df_vista = df_vista.sort_values(
        by=["NOMBRE","CÉDULA"], ascending=[True, True]
    ).sort_values(
        by="NOMBRE", key=lambda s: s.str.upper().ne("CONTROL")
    ).reset_index(drop=True)

    # DF numérico para agregaciones (solo personas), conservando metadatos
    df_num = out[["_Hp10_NUM","_Hp007_NUM","_Hp3_NUM","PERIODO DE LECTURA","CLIENTE","CÓDIGO DE USUARIO","CÓDIGO DE DOSÍMETRO","NOMBRE","CÉDULA","TIPO DE DOSÍMETRO","FECHA DE LECTURA"]].copy()

    return df_vista, df_num

# ===================== TABS =====================
tab1, tab2 = st.tabs(["1) Cargar y Subir a Ninox", "2) Reporte Final (sumas)"])

# ------------------ TAB 1 ------------------
with tab1:
    st.subheader("1) Cargar LISTA DE CÓDIGO")
    upl_lista = st.file_uploader("Sube la LISTA DE CÓDIGO (CSV / XLS / XLSX)", type=["csv","xls","xlsx"], key="upl_lista")
    df_lista = leer_lista_codigo(upl_lista) if upl_lista else None
    if df_lista is not None and not df_lista.empty:
        st.success(f"LISTA cargada: {len[df_lista]} filas")
        st.dataframe(df_lista.head(20), use_container_width=True)
    else:
        st.info("LISTA vacía o sin datos")

    st.subheader("2) Subir Archivo de Dosis")
    upl_dosis = st.file_uploader("Selecciona CSV/XLS/XLSX (dosis)", type=["csv","xls","xlsx"], key="upl_dosis")
    df_dosis = leer_dosis(upl_dosis) if upl_dosis else None
    if df_dosis is not None and not df_dosis.empty:
        st.success(f"Dosis cargadas: {len(df_dosis)} fila(s)")
        st.dataframe(df_dosis.head(15), use_container_width=True)

    # Filtro de periodos
    per_options = sorted(df_lista["PERIODO DE LECTURA"].dropna().astype(str).str.upper().unique().tolist()) if df_lista is not None else []
    periodos_sel = st.multiselect("Filtrar por PERIODO DE LECTURA (elige uno o varios; vacío = TODOS)", per_options, default=[])

    colA, colB = st.columns([1,1])
    with colA:
        nombre_reporte = st.text_input("Nombre archivo (sin extensión)", value=f"ReporteDosimetria_{datetime.now().strftime('%Y-%m-%d')}")
    with colB:
        # Siempre queremos subir PM/3 decimales como texto según requerimiento actual
        subir_pm_como_texto = True
        st.caption("Los valores se subirán como texto: 'PM' o número con 3 decimales.")

    btn_proc = st.button("✅ Procesar y Previsualizar", type="primary")
    if btn_proc:
        if df_lista is None or df_lista.empty:
            st.error("Primero sube la LISTA DE CÓDIGO.")
        elif df_dosis is None or df_dosis.empty:
            st.error("Sube el archivo de dosis.")
        elif "dosimeter" not in df_dosis.columns:
            st.error("El archivo de dosis debe incluir la columna 'dosimeter'.")
        else:
            df_final_raw = construir_registros(df_lista, df_dosis, periodos_sel)
            if df_final_raw.empty:
                with st.expander("Debug de coincidencias (no se encontraron)"):
                    st.write({
                        "dosimeter únicos en dosis": sorted(df_dosis["dosimeter"].dropna().unique().tolist()) if "dosimeter" in df_dosis.columns else [],
                        "CÓDIGO_DOSÍMETRO únicos en LISTA (según filtro)": sorted(df_lista["CÓDIGO_DOSÍMETRO"].dropna().unique().tolist()) if "CÓDIGO_DOSÍMETRO" in df_lista.columns else []
                    })
                st.warning("⚠️ No hay coincidencias **CÓDIGO_DOSÍMETRO** ↔ **dosimeter** (revisa periodos/códigos).")
            else:
                # Aplica resta de CONTROL y formato (PM / 3 decimales)
                df_vista, df_num_corr = aplicar_resta_control_y_formato(df_final_raw, umbral_pm=0.005)

                # Guardar en sesión:
                st.session_state.df_final_vista = df_vista.drop(columns=["_IS_CONTROL"], errors="ignore")
                st.session_state.df_final_num   = df_num_corr

                st.success(f"¡Listo! Registros generados (corregidos por CONTROL): {len(st.session_state.df_final_vista)}")
                st.dataframe(st.session_state.df_final_vista, use_container_width=True)

    st.markdown("---")
    st.subheader("3) Subir TODO a Ninox (tabla **BASE DE DATOS**)")

    def _hp_value_as_text(v) -> str:
        """Siempre devuelve texto: 'PM' o número con 3 decimales."""
        if isinstance(v, str) and v.strip().upper() == "PM":
            return "PM"
        try:
            return f"{float(v):.3f}"
        except Exception:
            return ""

    def _to_str(v):
        if pd.isna(v): return ""
        if isinstance(v, (pd.Timestamp, )):
            return v.strftime("%Y-%m-%d %H:%M:%S")
        return str(v)

    if st.button("⬆️ Subir a Ninox (BASE DE DATOS)"):
        df_para_subir = st.session_state.get("df_final_vista")  # ← usa la vista con PM/3 decimales
        if df_para_subir is None or df_para_subir.empty:
            st.error("No hay datos procesados. Pulsa 'Procesar y Previsualizar' primero.")
        else:
            rows = []
            for _, row in df_para_subir.iterrows():
                fields = {
                    "PERIODO DE LECTURA": _to_str(row.get("PERIODO DE LECTURA","")),
                    "CLIENTE": _to_str(row.get("CLIENTE","")),
                    "CÓDIGO DE DOSÍMETRO": _to_str(row.get("CÓDIGO DE DOSÍMETRO","")),
                    "CÓDIGO DE USUARIO": _to_str(row.get("CÓDIGO DE USUARIO","")),
                    "NOMBRE": _to_str(row.get("NOMBRE","")),
                    "CÉDULA": _to_str(row.get("CÉDULA","")),
                    "FECHA DE LECTURA": _to_str(row.get("FECHA DE LECTURA","")),
                    "TIPO DE DOSÍMETRO": _to_str(row.get("TIPO DE DOSÍMETRO","")),
                    # SIEMPRE texto con 3 decimales o 'PM'
                    "Hp (10)": _hp_value_as_text(row.get("Hp (10)")),
                    "Hp (0.07)": _hp_value_as_text(row.get("Hp (0.07)")),
                    "Hp (3)": _hp_value_as_text(row.get("Hp (3)")),
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
    df_vista = st.session_state.get("df_final_vista")
    df_num   = st.session_state.get("df_final_num")  # numérico corregido para agregaciones

    if df_vista is None or df_vista.empty or df_num is None or df_num.empty:
        st.info("No hay datos en memoria. Genera el cruce en la pestaña 1 para ver el reporte.")
    else:
        # Personas: agrupar por CÓDIGO DE USUARIO (excluye CONTROL) usando numérico corregido
        personas = df_num[df_num["NOMBRE"].str.strip().str.upper() != "CONTROL"].copy()
        if not personas.empty:
            per_group = personas.groupby("CÓDIGO DE USUARIO", as_index=False).agg({
                "CLIENTE":"last","NOMBRE":"last","CÉDULA":"last",
                "_Hp10_NUM":"sum","_Hp007_NUM":"sum","_Hp3_NUM":"sum"
            }).rename(columns={
                "_Hp10_NUM":"Hp10_SUM","_Hp007_NUM":"Hp007_SUM","_Hp3_NUM":"Hp3_SUM"
            })
            st.markdown("### Personas — Suma por **CÓDIGO DE USUARIO** (corregido por CONTROL)")
            st.dataframe(per_group, use_container_width=True)
        else:
            st.info("No hay filas de personas (todas serían CONTROL).")

        # CONTROL: sumar valores originales de control (de la vista)
        control_vista = df_vista[df_vista["NOMBRE"].str.strip().str.upper() == "CONTROL"].copy()
        if not control_vista.empty:
            for h in ["Hp (10)","Hp (0.07)","Hp (3)"]:
                control_vista[h] = pd.to_numeric(control_vista[h], errors="coerce").fillna(0.0)
            ctrl_group = control_vista.groupby("CÓDIGO DE DOSÍMETRO", as_index=False).agg({
                "CLIENTE":"last","Hp (10)":"sum","Hp (0.07)":"sum","Hp (3)":"sum"
            }).rename(columns={"Hp (10)":"Hp10_SUM","Hp (0.07)":"Hp007_SUM","Hp (3)":"Hp3_SUM"})
            st.markdown("### CONTROL — Suma por **CÓDIGO DE DOSÍMETRO**")
            st.dataframe(ctrl_group, use_container_width=True)
        else:
            st.info("No hay filas de CONTROL en el cruce actual.")


