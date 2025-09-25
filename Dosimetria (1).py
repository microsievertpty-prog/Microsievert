# -*- coding: utf-8 -*-
import io
import re
import pandas as pd
import streamlit as st
from io import BytesIO
from datetime import datetime
from typing import List, Dict, Any, Optional

# ===================== CONFIG NINOX =====================
API_TOKEN   = "edf312a0-98b8-11f0-883e-db77626d62e5"
TEAM_ID     = "YrsYfTegptdZcHJEj"
DATABASE_ID = "ow1geqnkz00e"
BASE_URL    = "https://api.ninox.com/v1"
TARGET_TABLE = "BASE DE DATOS"  # nombre visible de la tabla

# ===================== UI BASE =====================
st.set_page_config(page_title="Microsievert — Dosimetría", page_icon="🧪", layout="wide")
st.title("🧪 Carga y Cruce de Dosis → Ninox (**BASE DE DATOS**)")

# ======================================================================
# Utilidades mínimas
# ======================================================================
def _ninox_headers():
    return {"Authorization": f"Bearer {API_TOKEN}", "Content-Type": "application/json"}

@st.cache_data(ttl=300, show_spinner=False)
def ninox_list_tables(team_id: str, db_id: str):
    import requests
    url = f"{BASE_URL}/teams/{team_id}/databases/{db_id}/tables"
    r = requests.get(url, headers=_ninox_headers(), timeout=30)
    r.raise_for_status()
    return r.json()

def ninox_resolve_table_id(team_id: str, db_id: str, table_hint: str) -> str:
    hint = (table_hint or "").strip()
    if hint and " " not in hint and len(hint) <= 8:
        return hint
    for t in ninox_list_tables(team_id, db_id):
        if str(t.get("name","")).strip().lower() == hint.lower():
            return str(t.get("id"))
    return hint  # dejar que API falle claro si no existe

@st.cache_data(ttl=120, show_spinner=False)
def ninox_get_table_fields(team_id: str, db_id: str, table_hint: str):
    import requests
    tid = ninox_resolve_table_id(team_id, db_id, table_hint)
    url = f"{BASE_URL}/teams/{team_id}/databases/{db_id}/tables"
    r = requests.get(url, headers=_ninox_headers(), timeout=30)
    r.raise_for_status()
    fields = set()
    for t in r.json():
        if str(t.get("id")) == str(tid):
            cols = t.get("fields") or t.get("columns") or []
            for c in cols:
                nm = c.get("name") if isinstance(c, dict) else None
                if nm: fields.add(nm)
            break
    return fields

def ninox_insert_records(team_id: str, db_id: str, table_hint: str, rows: list, batch_size: int = 300):
    import requests
    tid = ninox_resolve_table_id(team_id, db_id, table_hint)
    url = f"{BASE_URL}/teams/{team_id}/databases/{db_id}/tables/{tid}/records"
    if not rows: return {"ok": True, "inserted": 0}
    inserted = 0
    for i in range(0, len(rows), batch_size):
        chunk = rows[i:i+batch_size]
        r = requests.post(url, headers=_ninox_headers(), json=chunk, timeout=60)
        if r.status_code != 200:
            return {"ok": False, "inserted": inserted, "error": f"{r.status_code} {r.text}"}
        inserted += len(chunk)
    return {"ok": True, "inserted": inserted}

def _to_dt_str(ts):
    if pd.isna(ts): return ""
    try: return pd.to_datetime(ts).strftime("%d/%m/%Y %H:%M")
    except Exception: return str(ts)

# ======================================================================
# Lectura de archivos
# ======================================================================
def leer_lista_codigo(file) -> pd.DataFrame:
    """
    Acepta CSV/XLS/XLSX. Si es Excel y existe hoja que empieza con 'asignar_DOSÍMETRO'
    la usa; de lo contrario usa la 1ª hoja.
    Normaliza columnas clave a:
      PERIODO DE LECTURA, CLIENTE, CÓDIGO DE USUARIO, NOMBRE, APELLIDO, CÉDULA,
      CÓDIGO_DOSÍMETRO, TIPO DE DOSÍMETRO, ETIQUETA (opcional)
    """
    if file is None: return pd.DataFrame()

    name = file.name.lower()
    raw = file.read()
    file.seek(0)

    def _cleanup_cols(cols):
        c = (pd.Index(cols).astype(str)
             .str.strip().str.upper()
             .str.replace(r"\s+", " ", regex=True)
             .str.replace("Á","A").str.replace("É","E").str.replace("Í","I").str.replace("Ó","O").str.replace("Ú","U")
             .str.replace("DOSIMETRO", "DOSÍMETRO")
        )
        return c

    if name.endswith((".xlsx",".xls")):
        xls = pd.ExcelFile(BytesIO(raw))
        sheet = [s for s in xls.sheet_names if str(s).lower().startswith("asignar_dos") ]
        sheet_name = sheet[0] if sheet else xls.sheet_names[0]
        df = pd.read_excel(xls, sheet_name=sheet_name)
    else:
        # CSV – tratar de detectar separador y encoding
        try:
            df = pd.read_csv(BytesIO(raw), sep=None, engine="python", encoding="utf-8-sig")
        except Exception:
            df = pd.read_csv(BytesIO(raw), sep=None, engine="python", encoding="latin-1")

    df.columns = _cleanup_cols(df.columns)

    # Mapeos de nombres
    remap = {
        "PERIODO": "PERIODO DE LECTURA",
        "PERIODO LECTURA": "PERIODO DE LECTURA",
        "CLIENTE": "CLIENTE",
        "CODIGO DE USUARIO": "CÓDIGO DE USUARIO",
        "CODIGO USUARIO": "CÓDIGO DE USUARIO",
        "CODIGO DOSIMETRO": "CÓDIGO_DOSÍMETRO",
        "CODIGO DE DOSIMETRO": "CÓDIGO_DOSÍMETRO",
        "CODIGO_DOSIMETRO": "CÓDIGO_DOSÍMETRO",
        "TIPO DE DOSIMETRO": "TIPO DE DOSÍMETRO",
    }
    df = df.rename(columns={k:v for k,v in remap.items() if k in df.columns})

    # Asegurar columnas
    needed = ["PERIODO DE LECTURA","CLIENTE","CÓDIGO DE USUARIO","NOMBRE","APELLIDO","CÉDULA",
              "CÓDIGO_DOSÍMETRO","TIPO DE DOSÍMETRO","ETIQUETA"]
    for c in needed:
        if c not in df.columns:
            df[c] = ""

    # Derivados
    df["CÓDIGO_DOSÍMETRO"] = df["CÓDIGO_DOSÍMETRO"].astype(str).str.strip().str.upper()
    df["PERIODO_NORM"] = (df["PERIODO DE LECTURA"].astype(str).str.strip().str.upper()
                          .str.replace(r"\.+$", "", regex=True))
    df["NOMBRE_COMPLETO"] = (df["NOMBRE"].fillna("").astype(str).str.strip()+" "+
                              df["APELLIDO"].fillna("").astype(str).str.strip()).str.strip()
    def _is_control(row):
        for k in ["ETIQUETA","NOMBRE","CÉDULA","CÓDIGO DE USUARIO"]:
            v = str(row.get(k,"")).strip().upper()
            if v == "CONTROL": return True
        return False
    df["_IS_CONTROL"] = df.apply(_is_control, axis=1)
    return df

def leer_dosis(file) -> pd.DataFrame:
    """
    Normaliza a:
      dosimeter, timestamp, hp10dose, hp0.07dose, hp3dose
    """
    if file is None: return pd.DataFrame()
    name = file.name.lower()

    if name.endswith(".csv"):
        try:
            df = pd.read_csv(file, sep=None, engine="python", encoding="utf-8-sig")
        except Exception:
            file.seek(0)
            df = pd.read_csv(file, sep=None, engine="python", encoding="latin-1")
    else:
        df = pd.read_excel(file)

    norm = (pd.Index(df.columns).astype(str).str.strip().str.lower()
            .str.replace(" ", "", regex=False)
            .str.replace("(", "").str.replace(")", "")
            .str.replace(".", "", regex=False))
    df.columns = norm

    # Renombrar candidatos
    def _ren(df, cands, dest):
        for c in cands:
            if c in df.columns:
                df.rename(columns={c: dest}, inplace=True)
                return
    _ren(df, ["dosimeter","dosimetro","codigo","codigodosimetro","codigo_dosimetro"], "dosimeter")
    _ren(df, ["timestamp","fecha","fechahora"], "timestamp")
    _ren(df, ["hp10dosecorr","hp10dose","hp10"], "hp10dose")
    _ren(df, ["hp007dosecorr","hp007dose","hp007","hp007dosecorr"], "hp0.07dose")
    _ren(df, ["hp3dosecorr","hp3dose","hp3"], "hp3dose")

    for k in ["hp10dose","hp0.07dose","hp3dose"]:
        if k in df.columns:
            df[k] = pd.to_numeric(df[k], errors="coerce").fillna(0.0)
        else:
            df[k] = 0.0

    if "dosimeter" in df.columns:
        df["dosimeter"] = (df["dosimeter"].astype(str).str.strip().str.upper()
                           .str.replace(r"\s+", "", regex=True))
    if "timestamp" in df.columns:
        df["timestamp"] = pd.to_datetime(df["timestamp"], errors="coerce")
    return df[["timestamp","dosimeter","hp10dose","hp0.07dose","hp3dose"]].copy()

# ======================================================================
# VALOR - CONTROL (3 decimales, PM < 0.005)
# ======================================================================
def aplicar_valor_menos_control_3dec(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty: return df
    def _fmt3(x): 
        try: return f"{float(x):.3f}"
        except Exception: return ""

    out = df.copy()
    out["_is_ctrl"] = out["NOMBRE"].fillna("").astype(str).str.strip().str.upper().eq("CONTROL")

    # Por PERIODO (si deseas por cliente también, usa ["PERIODO DE LECTURA","CLIENTE"])
    for per, g in out.groupby("PERIODO DE LECTURA", dropna=False):
        g_ctrl = g[g["_is_ctrl"]]
        if g_ctrl.empty: 
            continue
        b10 = float(g_ctrl.iloc[0]["Hp (10)"]) if pd.notna(g_ctrl.iloc[0]["Hp (10)"]) else 0.0
        b07 = float(g_ctrl.iloc[0]["Hp (0.07)"]) if pd.notna(g_ctrl.iloc[0]["Hp (0.07)"]) else 0.0
        b03 = float(g_ctrl.iloc[0]["Hp (3)"]) if pd.notna(g_ctrl.iloc[0]["Hp (3)"]) else 0.0
        for i in g.index:
            if out.at[i, "_is_ctrl"]:
                out.at[i, "Hp (10)"]  = _fmt3(b10)
                out.at[i, "Hp (0.07)"] = _fmt3(b07)
                out.at[i, "Hp (3)"]   = _fmt3(b03)
            else:
                d10 = float(out.at[i, "Hp (10)"])  - b10
                d07 = float(out.at[i, "Hp (0.07)"]) - b07
                d03 = float(out.at[i, "Hp (3)"])   - b03
                out.at[i, "Hp (10)"]  = "PM" if d10 < 0.005 else _fmt3(d10)
                out.at[i, "Hp (0.07)"] = "PM" if d07 < 0.005 else _fmt3(d07)
                out.at[i, "Hp (3)"]   = "PM" if d03 < 0.005 else _fmt3(d03)

    return out.drop(columns=["_is_ctrl"], errors="ignore")

# ======================================================================
# Cruce y construcción registros
# ======================================================================
def construir_registros(df_lista: pd.DataFrame, df_dosis: pd.DataFrame, periodos: List[str]) -> pd.DataFrame:
    if df_lista.empty or df_dosis.empty: return pd.DataFrame()

    # Filtro por periodos
    if periodos:
        per_up = [p.strip().upper() for p in periodos]
        base = df_lista[df_lista["PERIODO_NORM"].isin(per_up)].copy()
    else:
        base = df_lista.copy()

    if base.empty: return pd.DataFrame()

    # Índice dosis por código
    d = df_dosis.copy()
    d["dosimeter"] = d["dosimeter"].astype(str).str.strip().str.upper().str.replace(r"\s+","", regex=True)
    idx = d.sort_values("timestamp").set_index("dosimeter")

    rows = []
    # CONTROL primero
    base = pd.concat([base[base["_IS_CONTROL"]], base[~base["_IS_CONTROL"]]], ignore_index=True)

    for _, r in base.iterrows():
        code = str(r["CÓDIGO_DOSÍMETRO"]).strip().upper().replace(" ","")
        if not code or code.lower()=="nan": 
            continue
        if code not in idx.index:
            continue
        rec = idx.loc[code]
        if isinstance(rec, pd.DataFrame):
            rec = rec.iloc[-1]
        ts = _to_dt_str(rec.get("timestamp"))
        rows.append({
            "PERIODO DE LECTURA": r["PERIODO_NORM"],
            "CLIENTE": r["CLIENTE"],
            "CÓDIGO DE USUARIO": str(r["CÓDIGO DE USUARIO"]).strip(),
            "CÓDIGO DE DOSÍMETRO": code,
            "NOMBRE": r["NOMBRE_COMPLETO"] if not r["_IS_CONTROL"] else "CONTROL",
            "CÉDULA": str(r["CÉDULA"]).strip(),
            "FECHA DE LECTURA": ts,
            "TIPO DE DOSÍMETRO": r["TIPO DE DOSÍMETRO"] or "CE",
            "Hp (10)": float(rec.get("hp10dose",0.0) or 0.0),
            "Hp (0.07)": float(rec.get("hp0.07dose",0.0) or 0.0),
            "Hp (3)": float(rec.get("hp3dose",0.0) or 0.0),
        })
    return pd.DataFrame(rows)

# ======================================================================
# UI – Tabs
# ======================================================================
tab1, tab2 = st.tabs(["1) Procesar y Subir a Ninox", "2) 📊 Reporte Final"])

with tab1:
    st.subheader("1) Cargar **LISTA DE CÓDIGO**")
    upl_lista = st.file_uploader("Sube CSV/XLS/XLSX (hoja 'asignar_DOSÍMETRO...' si existe)", type=["csv","xls","xlsx"], key="upl_lista")

    df_lista = leer_lista_codigo(upl_lista) if upl_lista else pd.DataFrame()
    if df_lista.empty:
        st.warning("LISTA vacía o sin datos")
        st.stop()

    st.success(f"LISTA cargada: {len(df_lista)} filas")
    st.dataframe(df_lista.head(25), use_container_width=True)

    st.markdown("---")
    st.subheader("2) Subir Archivo de **Dosis**")
    upl_dosis = st.file_uploader("Selecciona CSV/XLS/XLSX (dosis)", type=["csv","xls","xlsx"], key="upl_dosis")
    df_dosis = leer_dosis(upl_dosis) if upl_dosis else pd.DataFrame()
    if not df_dosis.empty:
        st.success(f"Dosis cargadas: {len(df_dosis)} fila(s)")
        st.dataframe(df_dosis.head(15), use_container_width=True)

    st.markdown("---")
    st.subheader("3) Filtros")
    per_opts = sorted(df_lista["PERIODO_NORM"].dropna().astype(str).unique().tolist())
    periodos_sel = st.multiselect("Filtrar por **PERIODO DE LECTURA** (elige uno o varios; vacío = TODOS)", per_opts, default=[])

    subir_pm_como_texto = st.checkbox("Subir 'PM' como TEXTO (si Hp son texto en Ninox)", value=True)
    debug_uno = st.checkbox("Enviar solo 1 registro (debug)", value=False)

    st.markdown("---")
    colp1, colp2 = st.columns([1,1])
    with colp1:
        nombre_reporte = st.text_input("Nombre de archivo (sin extensión)", f"ReporteDosimetria_{datetime.now().strftime('%Y-%m-%d')}")

    btn_proc = st.button("✅ Procesar", type="primary")
    if btn_proc:
        if df_dosis.empty:
            st.error("Debes subir el archivo de **Dosis**.")
            st.stop()

        with st.spinner("Procesando cruce…"):
            df_final = construir_registros(df_lista, df_dosis, periodos_sel)
            if df_final.empty:
                st.warning("⚠️ No se encontraron coincidencias CÓDIGO_DOSÍMETRO ↔ dosimeter (revisa periodos/códigos).")
                st.stop()

            # VALOR - CONTROL (3 decimales, PM < 0.005)
            df_final = aplicar_valor_menos_control_3dec(df_final)

            st.success(f"¡Listo! Registros generados: {len(df_final)}")
            st.dataframe(df_final, use_container_width=True)

            # Export simple CSV
            csv_bytes = df_final.to_csv(index=False).encode("utf-8-sig")
            st.download_button("⬇️ Descargar CSV", data=csv_bytes,
                               file_name=f"{(nombre_reporte.strip() or 'ReporteDosimetria')}.csv",
                               mime="text/csv")

            # ===== Subida a Ninox =====
            st.markdown("### 4) Subir a Ninox → Tabla **BASE DE DATOS**")
            ninox_fields = ninox_get_table_fields(TEAM_ID, DATABASE_ID, TARGET_TABLE)
            with st.expander("Campos detectados en Ninox"):
                st.write(sorted(ninox_fields))

            # Mapa de columnas (nuestro → Ninox)
            SPECIAL_MAP = {"Hp (10)": "Hp (10)", "Hp (0.07)": "Hp (0.07)", "Hp (3)": "Hp (3)"}
            CUSTOM_MAP = {
                "PERIODO DE LECTURA": "PERIODO DE LECTURA",
                "CLIENTE": "COMPAÑÍA",            # si prefieres "CLIENTE" como campo, cámbialo aquí
                "CÓDIGO DE USUARIO": "CÓDIGO DE USUARIO",
                "CÓDIGO DE DOSÍMETRO": "CÓDIGO DE DOSÍMETRO",
                "NOMBRE": "NOMBRE",
                "CÉDULA": "CÉDULA",
                "FECHA DE LECTURA": "FECHA DE LECTURA",
                "TIPO DE DOSÍMETRO": "TIPO DE DOSÍMETRO",
            }

            def resolve_dest_name(col):
                return SPECIAL_MAP.get(col, CUSTOM_MAP.get(col, col))

            def serialize_hp(v):
                if isinstance(v,str) and v.strip().upper()=="PM":
                    return "PM" if subir_pm_como_texto else None
                try: return float(v)
                except Exception: return v

            rows = []
            iterator = df_final.head(1).iterrows() if debug_uno else df_final.iterrows()
            for _, rr in iterator:
                payload = {}
                for c in df_final.columns:
                    dest = resolve_dest_name(c)
                    if ninox_fields and dest not in ninox_fields:
                        continue
                    val = rr[c]
                    if dest in {"Hp (10)","Hp (0.07)","Hp (3)"}:
                        payload[dest] = serialize_hp(val)
                    else:
                        payload[dest] = "" if pd.isna(val) else str(val)
                rows.append({"fields": payload})

            if st.button("⬆️ Subir a Ninox (BASE DE DATOS)"):
                with st.spinner("Subiendo…"):
                    res = ninox_insert_records(TEAM_ID, DATABASE_ID, TARGET_TABLE, rows, batch_size=300)
                if res.get("ok"):
                    st.success(f"✅ Subido a Ninox: {res.get('inserted',0)} registro(s).")
                else:
                    st.error(f"❌ Error al subir: {res.get('error')}")

# ======================================================================
# TAB 2 – Reporte Final (sumas)
# ======================================================================
with tab2:
    st.subheader("📊 Reporte Final: suma por **CÓDIGO DE USUARIO** (personas) y bloque **CONTROL** (por CÓDIGO DE DOSÍMETRO)")

    if "df_final" in locals() or "df_final" in globals():
        pass  # nada
    # Permitimos recomputar a partir de archivos si el usuario no procesó en esta sesión
    upl_lista2 = st.file_uploader("LISTA DE CÓDIGO (para reporte) — opcional si ya procesaste", type=["csv","xls","xlsx"], key="upl_lista2")
    upl_dosis2 = st.file_uploader("Dosis (para reporte) — opcional si ya procesaste", type=["csv","xls","xlsx"], key="upl_dosis2")

    if upl_lista2 and upl_dosis2:
        df_lista_r = leer_lista_codigo(upl_lista2)
        df_dosis_r = leer_dosis(upl_dosis2)
        per_opts_r = sorted(df_lista_r["PERIODO_NORM"].dropna().astype(str).unique().tolist())
        per_sel_r = st.multiselect("Periodo(s) para el reporte", per_opts_r, default=per_opts_r[:1])
        df_final_r = construir_registros(df_lista_r, df_dosis_r, per_sel_r)
        df_final_r = aplicar_valor_menos_control_3dec(df_final_r)
    else:
        df_final_r = locals().get("df_final", pd.DataFrame())

    if df_final_r.empty:
        st.info("Sube archivos en este tab o procesa primero en el Tab 1 para ver el reporte.")
        st.stop()

    # Suma por CÓDIGO DE USUARIO (ignorando CONTROL)
    dfp = df_final_r.copy()
    dfp = dfp[dfp["NOMBRE"].str.upper() != "CONTROL"].copy()
    # Convertir Hp a num para sumar, ignorando PM
    def _num_or0(x):
        try: return float(x)
        except Exception: return 0.0
    for k in ["Hp (10)","Hp (0.07)","Hp (3)"]:
        dfp[k] = dfp[k].apply(_num_or0)

    grp = (dfp.groupby(["PERIODO DE LECTURA","CÓDIGO DE USUARIO","CLIENTE"], as_index=False)
               .agg({"Hp (10)":"sum","Hp (0.07)":"sum","Hp (3)":"sum"}))

    st.markdown("#### 🧮 Suma por **CÓDIGO DE USUARIO** (se excluye CONTROL)")
    st.dataframe(grp, use_container_width=True)

    # CONTROL por CÓDIGO DE DOSÍMETRO (última línea CONTROL de cada periodo)
    ctrl = df_final_r[df_final_r["NOMBRE"].str.upper()=="CONTROL"].copy()
    ctrl = ctrl[["PERIODO DE LECTURA","CÓDIGO DE DOSÍMETRO","Hp (10)","Hp (0.07)","Hp (3)"]]
    st.markdown("#### 🎛️ CONTROL por **CÓDIGO DE DOSÍMETRO**")
    st.dataframe(ctrl, use_container_width=True)

    # Descarga
    colx1, colx2 = st.columns(2)
    with colx1:
        st.download_button("⬇️ Descargar Sumas por CÓDIGO DE USUARIO (CSV)",
                           data=grp.to_csv(index=False).encode("utf-8-sig"),
                           file_name=f"suma_por_codigo_usuario_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
                           mime="text/csv")
    with colx2:
        st.download_button("⬇️ Descargar CONTROL por CÓDIGO_DE_DOSÍMETRO (CSV)",
                           data=ctrl.to_csv(index=False).encode("utf-8-sig"),
                           file_name=f"control_por_codigo_dosimetro_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
                           mime="text/csv")
