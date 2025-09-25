# -*- coding: utf-8 -*-
import io, re
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
TARGET_TABLE = "BASE DE DATOS"

st.set_page_config(page_title="Microsievert — Dosimetría", page_icon="🧪", layout="wide")
st.title("🧪 Carga y Cruce de Dosis → Ninox (BASE DE DATOS)")

# ---------- Utilidades ----------
def norm_code(x) -> str:
    """Quita todo lo que no sea A-Z o 0-9 y convierte a MAYÚSCULAS."""
    if x is None: return ""
    s = str(x).strip().upper()
    return re.sub(r"[^A-Z0-9]", "", s)

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
    return hint

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
            for c in (t.get("fields") or t.get("columns") or []):
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

# ---------- Lecturas ----------
def leer_lista_codigo(file) -> pd.DataFrame:
    if file is None: return pd.DataFrame()
    name = file.name.lower()
    raw = file.read(); file.seek(0)

    def _cleanup_cols(cols):
        c = (pd.Index(cols).astype(str)
             .str.strip().str.upper()
             .str.replace(r"\s+"," ", regex=True)
             .str.replace("Á","A").str.replace("É","E").str.replace("Í","I").str.replace("Ó","O").str.replace("Ú","U")
             .str.replace("DOSIMETRO","DOSÍMETRO"))
        return c

    if name.endswith((".xlsx",".xls")):
        xls = pd.ExcelFile(BytesIO(raw))
        sheet = [s for s in xls.sheet_names if str(s).lower().startswith("asignar_dos")]
        df = pd.read_excel(xls, sheet_name=(sheet[0] if sheet else xls.sheet_names[0]))
    else:
        try:
            df = pd.read_csv(BytesIO(raw), sep=None, engine="python", encoding="utf-8-sig")
        except Exception:
            df = pd.read_csv(BytesIO(raw), sep=None, engine="python", encoding="latin-1")

    df.columns = _cleanup_cols(df.columns)
    remap = {
        "PERIODO":"PERIODO DE LECTURA",
        "PERIODO LECTURA":"PERIODO DE LECTURA",
        "CODIGO DE USUARIO":"CÓDIGO DE USUARIO",
        "CODIGO USUARIO":"CÓDIGO DE USUARIO",
        "CODIGO DOSIMETRO":"CÓDIGO_DOSÍMETRO",
        "CODIGO DE DOSIMETRO":"CÓDIGO_DOSÍMETRO",
        "CODIGO_DOSIMETRO":"CÓDIGO_DOSÍMETRO",
        "TIPO DE DOSIMETRO":"TIPO DE DOSÍMETRO",
    }
    df = df.rename(columns={k:v for k,v in remap.items() if k in df.columns})

    needed = ["PERIODO DE LECTURA","CLIENTE","CÓDIGO DE USUARIO","NOMBRE","APELLIDO","CÉDULA",
              "CÓDIGO_DOSÍMETRO","TIPO DE DOSÍMETRO","ETIQUETA"]
    for c in needed:
        if c not in df.columns: df[c] = ""

    df["PERIODO_NORM"] = (df["PERIODO DE LECTURA"].astype(str).str.strip().str.upper()
                          .str.replace(r"\.+$","", regex=True))
    df["NOMBRE_COMPLETO"] = (df["NOMBRE"].fillna("").astype(str).str.strip()+" "+
                             df["APELLIDO"].fillna("").astype(str).str.strip()).str.strip()
    df["COD_NORM"] = df["CÓDIGO_DOSÍMETRO"].map(norm_code)

    def _is_control(row):
        for k in ["ETIQUETA","NOMBRE","CÉDULA","CÓDIGO DE USUARIO"]:
            if str(row.get(k,"")).strip().upper() == "CONTROL": return True
        return False
    df["_IS_CONTROL"] = df.apply(_is_control, axis=1)
    return df

def leer_dosis(file) -> pd.DataFrame:
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
        if k in df.columns: df[k] = pd.to_numeric(df[k], errors="coerce").fillna(0.0)
        else: df[k] = 0.0

    if "dosimeter" in df.columns:
        df["dosimeter"] = df["dosimeter"].astype(str)
    if "timestamp" in df.columns:
        df["timestamp"] = pd.to_datetime(df["timestamp"], errors="coerce")

    out = df[["timestamp","dosimeter","hp10dose","hp0.07dose","hp3dose"]].copy()
    out["COD_NORM"] = out["dosimeter"].map(norm_code)
    return out

# ---------- Valor - Control (3 decimales / PM) ----------
def aplicar_valor_menos_control_3dec(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty: return df
    def _fmt3(x):
        try: return f"{float(x):.3f}"
        except Exception: return ""
    out = df.copy()
    out["_is_ctrl"] = out["NOMBRE"].fillna("").astype(str).str.strip().str.upper().eq("CONTROL")
    for (per, cli), g in out.groupby(["PERIODO DE LECTURA","CLIENTE"], dropna=False):
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
                d10 = float(out.at[i,"Hp (10)"])  - b10
                d07 = float(out.at[i,"Hp (0.07)"]) - b07
                d03 = float(out.at[i,"Hp (3)"])   - b03
                out.at[i,"Hp (10)"]  = "PM" if d10 < 0.005 else _fmt3(d10)
                out.at[i,"Hp (0.07)"] = "PM" if d07 < 0.005 else _fmt3(d07)
                out.at[i,"Hp (3)"]   = "PM" if d03 < 0.005 else _fmt3(d03)
    return out.drop(columns=["_is_ctrl"], errors="ignore")

# ---------- Cruce ----------
def construir_registros(df_lista: pd.DataFrame, df_dosis: pd.DataFrame, periodos: List[str]) -> pd.DataFrame:
    if df_lista.empty or df_dosis.empty: return pd.DataFrame()

    # Filtro de periodos: exacto o contiene (más tolerante)
    if periodos:
        per_up = [p.strip().upper() for p in periodos]
        m1 = df_lista["PERIODO_NORM"].isin(per_up)
        m2 = False
        for p in per_up:
            m2 = m2 | df_lista["PERIODO_NORM"].str.contains(p, na=False)
        base = df_lista[m1 | m2].copy()
    else:
        base = df_lista.copy()
    if base.empty: return pd.DataFrame()

    d = df_dosis.copy()
    idx = d.sort_values("timestamp").set_index("COD_NORM")

    rows = []
    base = pd.concat([base[base["_IS_CONTROL"]], base[~base["_IS_CONTROL"]]], ignore_index=True)
    for _, r in base.iterrows():
        cod_norm = r["COD_NORM"]
        if not cod_norm: 
            continue
        if cod_norm not in idx.index:
            continue
        rec = idx.loc[cod_norm]
        if isinstance(rec, pd.DataFrame): rec = rec.iloc[-1]
        rows.append({
            "PERIODO DE LECTURA": r["PERIODO_NORM"],
            "CLIENTE": r["CLIENTE"],
            "CÓDIGO DE USUARIO": str(r["CÓDIGO DE USUARIO"]).strip(),
            "CÓDIGO DE DOSÍMETRO": r["COD_NORM"],
            "NOMBRE": "CONTROL" if r["_IS_CONTROL"] else r["NOMBRE_COMPLETO"],
            "CÉDULA": str(r["CÉDULA"]).strip(),
            "FECHA DE LECTURA": _to_dt_str(rec.get("timestamp")),
            "TIPO DE DOSÍMETRO": r["TIPO DE DOSÍMETRO"] or "CE",
            "Hp (10)": float(rec.get("hp10dose",0.0) or 0.0),
            "Hp (0.07)": float(rec.get("hp0.07dose",0.0) or 0.0),
            "Hp (3)": float(rec.get("hp3dose",0.0) or 0.0),
        })
    return pd.DataFrame(rows)

# ===================== UI =====================
tab1, tab2 = st.tabs(["1) Procesar y Subir", "2) Reporte Final"])

with tab1:
    st.subheader("1) Cargar LISTA DE CÓDIGO")
    upl_lista = st.file_uploader("CSV/XLS/XLSX (hoja 'asignar_DOSÍMETRO...' si está)", type=["csv","xls","xlsx"], key="l1")
    df_lista = leer_lista_codigo(upl_lista) if upl_lista else pd.DataFrame()
    if df_lista.empty:
        st.warning("LISTA vacía o sin datos"); st.stop()
    st.success(f"LISTA cargada: {len(df_lista)} filas")
    st.dataframe(df_lista.head(20), use_container_width=True)

    st.subheader("2) Cargar Archivo de Dosis")
    upl_dosis = st.file_uploader("CSV/XLS/XLSX (dosis)", type=["csv","xls","xlsx"], key="d1")
    df_dosis = leer_dosis(upl_dosis) if upl_dosis else pd.DataFrame()
    if df_dosis.empty:
        st.warning("Sube el archivo de dosis"); st.stop()
    st.success(f"Dosis cargadas: {len(df_dosis)} filas")
    st.dataframe(df_dosis.head(15), use_container_width=True)

    st.subheader("3) Filtros")
    per_opts = sorted(df_lista["PERIODO_NORM"].dropna().astype(str).unique().tolist())
    st.caption(f"Periodos detectados en LISTA: {', '.join(per_opts) if per_opts else '(sin periodo)'}")
    periodos_sel = st.multiselect("PERIODO(S) (vacío = TODOS)", per_opts, default=[])

    # ---- Debug de coincidencias ----
    with st.expander("🔍 Debug de coincidencias"):
        list_codes = sorted(set(df_lista["COD_NORM"]) - {""})
        dose_codes = sorted(set(df_dosis["COD_NORM"]) - {""})
        both = sorted(set(list_codes).intersection(dose_codes))
        only_list = sorted(set(list_codes) - set(dose_codes))[:30]
        only_dose = sorted(set(dose_codes) - set(list_codes))[:30]

        st.write(f"LISTA → códigos únicos: {len(set(list_codes))}")
        st.write(f"DOSIS → códigos únicos: {len(set(dose_codes))}")
        st.write(f"Intersección: {len(both)}")
        st.write("Ejemplos en LISTA y no en dosis (máx 30):", only_list)
        st.write("Ejemplos en dosis y no en LISTA (máx 30):", only_dose)

    subir_pm_como_texto = st.checkbox("Subir 'PM' como TEXTO", value=True)
    debug_uno = st.checkbox("Enviar 1 registro (debug)", value=False)

    if st.button("✅ Procesar"):
        df_final = construir_registros(df_lista, df_dosis, periodos_sel)
        if df_final.empty:
            st.error("⚠️ No se encontraron coincidencias CÓDIGO_DOSÍMETRO ↔ dosimeter (revisa periodos/códigos).")
            st.stop()

        df_final = aplicar_valor_menos_control_3dec(df_final)
        st.success(f"¡Listo! Registros generados: {len(df_final)}")
        st.dataframe(df_final, use_container_width=True)

        # Subida a Ninox
        st.markdown("### 4) Subir a Ninox (BASE DE DATOS)")
        ninox_fields = ninox_get_table_fields(TEAM_ID, DATABASE_ID, TARGET_TABLE)
        st.caption("Campos en Ninox: " + ", ".join(sorted(ninox_fields)))

        SPECIAL_MAP = {"Hp (10)":"Hp (10)","Hp (0.07)":"Hp (0.07)","Hp (3)":"Hp (3)"}
        CUSTOM_MAP = {
            "PERIODO DE LECTURA":"PERIODO DE LECTURA",
            "CLIENTE":"COMPAÑÍA",               # Cambia a "CLIENTE" si así se llama en tu Ninox
            "CÓDIGO DE USUARIO":"CÓDIGO DE USUARIO",
            "CÓDIGO DE DOSÍMETRO":"CÓDIGO DE DOSÍMETRO",
            "NOMBRE":"NOMBRE",
            "CÉDULA":"CÉDULA",
            "FECHA DE LECTURA":"FECHA DE LECTURA",
            "TIPO DE DOSÍMETRO":"TIPO DE DOSÍMETRO",
        }
        def resolve_dest(col): return SPECIAL_MAP.get(col, CUSTOM_MAP.get(col, col))
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
                dest = resolve_dest(c)
                if ninox_fields and dest not in ninox_fields: continue
                val = rr[c]
                if dest in {"Hp (10)","Hp (0.07)","Hp (3)"}:
                    payload[dest] = serialize_hp(val)
                else:
                    payload[dest] = "" if pd.isna(val) else str(val)
            rows.append({"fields": payload})

        if st.button("⬆️ Subir a Ninox"):
            res = ninox_insert_records(TEAM_ID, DATABASE_ID, TARGET_TABLE, rows, batch_size=300)
            if res.get("ok"): st.success(f"✅ Subido: {res.get('inserted',0)} registro(s)")
            else: st.error(f"❌ Error: {res.get('error')}")

with tab2:
    st.subheader("📊 Reporte Final (sumas)")
    st.caption("Procesa primero en el Tab 1 o vuelve a subir archivos aquí para recomputar.")
