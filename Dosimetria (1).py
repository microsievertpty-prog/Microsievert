# -*- coding: utf-8 -*-
import io
import re
import requests
import pandas as pd
import streamlit as st
from datetime import datetime
from io import BytesIO
from dateutil.parser import parse as dtparse
from typing import List, Dict, Any, Optional, Set

from openpyxl import Workbook
from openpyxl.styles import Font, Alignment, Border, Side, PatternFill
from openpyxl.utils import get_column_letter, column_index_from_string

# ===================== NINOX CONFIG =====================
API_TOKEN   = "edf312a0-98b8-11f0-883e-db77626d62e5"
TEAM_ID     = "YrsYfTegptdZcHJEj"
DATABASE_ID = "ow1geqnkz00e"
BASE_URL    = "https://api.ninox.com/v1"

# Tablas: lee de LISTA DE CODIGO, sube y reporta a BASE DE DATOS
DEFAULT_BASE_TABLE_ID   = "LISTA DE CODIGO"   # lectura
DEFAULT_REPORT_TABLE_ID = "BASE DE DATOS"     # escritura / reportes

# ===================== STREAMLIT =====================
st.set_page_config(page_title="Microsievert - Dosimetría", page_icon="🧪", layout="wide")
st.title("🧪 Sistema de Gestión de Dosimetría — Microsievert")
st.caption("Ninox + VALOR−CONTROL + Reporte Actual/Anual/Vida + Exportación")

if "df_final" not in st.session_state:
    st.session_state.df_final = None

# ===================== Ninox helpers =====================
def ninox_headers():
    return {"Authorization": f"Bearer {API_TOKEN}", "Content-Type": "application/json"}

@st.cache_data(ttl=300, show_spinner=False)
def ninox_list_tables(team_id: str, db_id: str):
    url = f"{BASE_URL}/teams/{team_id}/databases/{db_id}/tables"
    r = requests.get(url, headers=ninox_headers(), timeout=30)
    r.raise_for_status()
    return r.json()

def ninox_resolve_table_id(team_id: str, db_id: str, table_hint: str) -> str:
    hint = (table_hint or "").strip()
    if hint and " " not in hint and len(hint) <= 8:
        return hint
    for t in ninox_list_tables(team_id, db_id):
        tname = str(t.get("name", "")).strip().lower()
        tid   = str(t.get("id", "")).strip()
        if tname == hint.lower() or tid == hint:
            return tid
    return hint

@st.cache_data(ttl=300, show_spinner=False)
def ninox_fetch_records(team_id: str, db_id: str, table_hint: str, page_size: int = 1000) -> pd.DataFrame:
    table_id = ninox_resolve_table_id(team_id, db_id, table_hint)
    url = f"{BASE_URL}/teams/{team_id}/databases/{db_id}/tables/{table_id}/records"
    out = []
    try:
        skip = 0
        while True:
            r = requests.get(url, headers=ninox_headers(), params={"limit": page_size, "skip": skip}, timeout=60)
            if r.status_code == 404:
                raise FileNotFoundError(f"Tabla '{table_hint}' (ID '{table_id}') no existe.")
            r.raise_for_status()
            chunk = r.json()
            if not chunk: break
            out.extend(chunk)
            if len(chunk) < page_size: break
            skip += page_size
    except FileNotFoundError:
        raise
    except Exception:
        offset = 0
        while True:
            r = requests.get(url, headers=ninox_headers(), params={"perPage": page_size, "offset": offset}, timeout=60)
            if r.status_code == 404:
                raise FileNotFoundError(f"Tabla '{table_hint}' (ID '{table_id}') no existe.")
            r.raise_for_status()
            batch = r.json()
            if not batch: break
            out.extend(batch)
            if len(batch) < page_size: break
            offset += page_size
    rows = [x.get("fields", {}) for x in out]
    df = pd.DataFrame(rows) if rows else pd.DataFrame()
    df.columns = [str(c) for c in df.columns]
    return df

def ninox_insert_records(team_id: str, db_id: str, table_hint: str, rows: list, batch_size: int = 400):
    table_id = ninox_resolve_table_id(team_id, db_id, table_hint)
    url = f"{BASE_URL}/teams/{team_id}/databases/{db_id}/tables/{table_id}/records"
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

@st.cache_data(ttl=120, show_spinner=False)
def ninox_get_table_fields(team_id: str, db_id: str, table_hint: str):
    table_id = ninox_resolve_table_id(team_id, db_id, table_hint)
    url = f"{BASE_URL}/teams/{team_id}/databases/{db_id}/tables"
    r = requests.get(url, headers=ninox_headers(), timeout=30)
    r.raise_for_status()
    info = r.json()
    fields = set()
    for t in info:
        if str(t.get("id")) == str(table_id):
            cols = t.get("fields") or t.get("columns") or []
            for c in cols:
                name = c.get("name") if isinstance(c, dict) else None
                if name: fields.add(name)
            break
    return fields

# ===================== Utilidades =====================
def round2(x: float) -> float: return float(f"{x:.2f}")

def as_value(v: Any):
    if v is None: return ""
    s = str(v).strip().replace(",", ".")
    if s.upper() == "PM": return "PM"
    try: return float(s)
    except Exception: return s

def as_num(v: Any) -> float:
    if v is None: return 0.0
    s = str(v).strip().replace(",", ".")
    if s == "" or s.upper() == "PM": return 0.0
    try: return float(s)
    except Exception: return 0.0

def pm_or_sum(raws, numeric_sum) -> Any:
    import pandas as _pd
    if isinstance(raws, (list, tuple, set)): arr = list(raws)
    elif isinstance(raws, _pd.Series):       arr = raws.tolist()
    elif raws is None or (isinstance(raws, float) and _pd.isna(raws)) or raws == "": arr = []
    else: arr = [raws]
    vals = [str(x).upper() for x in arr if str(x).strip() != ""]
    if vals and all(v == "PM" for v in vals): return "PM"
    try:
        total = float(numeric_sum); 
        if _pd.isna(total): total = 0.0
    except Exception:
        total = 0.0
    return round2(total)

def merge_raw_lists(*vals):
    import pandas as _pd
    out: List[Any] = []
    for v in vals:
        if isinstance(v, (list, tuple, set)): out.extend(list(v))
        elif isinstance(v, _pd.Series):       out.extend(v.tolist())
        elif v is None or (isinstance(v, float) and _pd.isna(v)) or v == "": continue
        else: out.append(v)
    return out

# ===================== Excel simple =====================
def exportar_excel_simple_valor_control(df_final: pd.DataFrame) -> bytes:
    wb = Workbook(); ws = wb.active; ws.title = "REPORTE DE DOSIS"
    border = Border(left=Side(style='thin'), right=Side(style='thin'),
                    top=Side(style='thin'),  bottom=Side(style='thin'))
    ws['I1'] = f"Fecha de emisión: {datetime.now().strftime('%d/%m/%Y')}"
    ws['I1'].font = Font(size=10, italic=True)
    ws['I1'].alignment = Alignment(horizontal='right', vertical='top')
    ws.merge_cells('A5:J5'); c = ws['A5']; c.value = 'REPORTE DE DOSIMETRÍA'
    c.font = Font(bold=True, size=14); c.alignment = Alignment(horizontal='center')
    headers = [
        'PERIODO DE LECTURA','CLIENTE','CÓDIGO DE DOSÍMETRO','NOMBRE',
        'CÉDULA','FECHA DE LECTURA','TIPO DE DOSÍMETRO','Hp(10)','Hp(0.07)','Hp(3)'
    ]
    for i, h in enumerate(headers, 1):
        cell = ws.cell(row=7, column=i, value=h)
        cell.font = Font(bold=True); cell.alignment = Alignment(horizontal='center')
        cell.fill = PatternFill('solid', fgColor='DDDDDD'); cell.border = border
    start = 8
    for ridx, (_, row) in enumerate(df_final.iterrows()):
        for cidx, h in enumerate(headers, 1):
            val = row.get(h, "")
            cell = ws.cell(row=start + ridx, column=cidx, value=val)
            cell.alignment = Alignment(horizontal='center', wrap_text=True)
            cell.font = Font(size=10); cell.border = border
    for col in ws.columns:
        mx = max(len(str(c.value)) if c.value else 0 for c in col) + 2
        ws.column_dimensions[get_column_letter(col[0].column)].width = mx
    bio = io.BytesIO(); wb.save(bio); bio.seek(0)
    return bio.read()

# ===================== Lectura de dosis (archivo) =====================
def leer_dosis(upload):
    if not upload: return None
    name = upload.name.lower()
    if name.endswith(".csv"):
        try:
            df = pd.read_csv(upload, delimiter=';', engine='python')
        except Exception:
            upload.seek(0); df = pd.read_csv(upload)
    else:
        df = pd.read_excel(upload)

    # Normaliza nombres
    norm = (df.columns.astype(str).str.strip().str.lower()
            .str.replace(' ', '', regex=False)
            .str.replace('(', '').str.replace(')', '')
            .str.replace('.', '', regex=False))
    df.columns = norm

    # Columnas esperadas
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

    # Normalización fuerte de códigos del archivo
    if 'dosimeter' in df.columns:
        df['dosimeter'] = (
            df['dosimeter'].astype(str)
            .str.upper()
            .str.replace(r"[^A-Z0-9]", "", regex=True)
            .str.strip()
        )

    if 'timestamp' in df.columns:
        df['timestamp'] = pd.to_datetime(df['timestamp'], errors='coerce')

    return df

# ===================== Normalizador LISTA DE CODIGO =====================
def normalize_lista_codigo(df_raw: pd.DataFrame) -> pd.DataFrame:
    """
    Normaliza columnas de 'LISTA DE CODIGO' y el código del dosímetro.
    Fallback del código: CÓDIGO_DOSÍMETRO → CÓDIGO USUARIO → CÓDIGO_CLIENTE
    """
    df = df_raw.copy()
    needed = [
        "CÉDULA","CÓDIGO USUARIO","NOMBRE","APELLIDO","FECHA DE NACIMIENTO",
        "CLIENTE","CÓDIGO_CLIENTE","ETIQUETA",
        "CÓDIGO_DOSÍMETRO","PERIODO DE LECTURA","TIPO DE DOSÍMETRO"
    ]
    for c in needed:
        if c not in df.columns: df[c] = ""

    df["NOMBRE"] = df["NOMBRE"].fillna("").astype(str).str.strip()
    ap = df["APELLIDO"].fillna("").astype(str).str.strip()
    df["NOMBRE_COMPLETO"] = (df["NOMBRE"] + " " + ap).str.strip().replace({"^$": ""}, regex=True)

    # ---- Código normalizado con fallback
    def pick_code(row):
        for k in ["CÓDIGO_DOSÍMETRO", "CÓDIGO USUARIO", "CÓDIGO_CLIENTE"]:
            v = str(row.get(k, "") or "").upper()
            v = re.sub(r"[^A-Z0-9]", "", v)
            if v and v != "NAN":
                return v
        return ""
    df["CODIGO"] = df.apply(pick_code, axis=1)

    df["CLIENTE"] = df["CLIENTE"].fillna("").astype(str).str.strip()
    df["PERIODO_NORM"] = (
        df["PERIODO DE LECTURA"].fillna("").astype(str).str.strip().str.upper()
        .str.replace(r"\.+$", "", regex=True)
    )

    def is_control_row(r):
        for k in ["ETIQUETA","NOMBRE","CÉDULA","CÓDIGO USUARIO"]:
            if str(r.get(k, "")).strip().upper() == "CONTROL":
                return True
        return False
    df["CONTROL_FLAG"] = df.apply(is_control_row, axis=1)
    df["TIPO DE DOSÍMETRO"] = df["TIPO DE DOSÍMETRO"].fillna("").astype(str).str.strip()
    df["CÉDULA"] = df["CÉDULA"].fillna("").astype(str).str.strip()
    return df

# ===================== Construcción de registros =====================
def construir_registros_desde_lista_codigo(df_lista: pd.DataFrame,
                                           df_dosis: pd.DataFrame,
                                           periodo_filtro: str = "— TODOS —") -> List[Dict[str, Any]]:
    per_f = (periodo_filtro or "").strip().upper()
    base = df_lista[df_lista["PERIODO_NORM"] == per_f].copy() if (per_f and per_f not in ("— TODOS —","TODOS","TODAS")) else df_lista.copy()

    # Índice dosis por código normalizado
    df_d = df_dosis.copy()
    df_d["dosimeter"] = df_d["dosimeter"].astype(str).str.upper().str.replace(r"[^A-Z0-9]", "", regex=True).str.strip()
    idx = df_d.set_index("dosimeter")

    # Debug de listas de códigos (para mostrar si no hay match)
    codes_ninox = sorted(set(base["CODIGO"]) - {""})
    codes_file  = sorted(set(df_d["dosimeter"]) - {""})

    # Checkbox de debug manual
    if st.checkbox("Mostrar debug de códigos", value=False, key="dbg_codes"):
        inter = sorted(set(codes_ninox).intersection(codes_file))
        st.write(f"Códigos Ninox: {len(codes_ninox)} | Archivo: {len(codes_file)} | Intersección: {len(inter)}")
        st.write("Ejemplos intersección:", inter[:20])

    registros = []
    base = pd.concat([base[base["CONTROL_FLAG"]], base[~base["CONTROL_FLAG"]]], ignore_index=True)

    for _, r in base.iterrows():
        cod = r["CODIGO"]
        if not cod or cod.lower() == "nan": continue
        if cod not in idx.index:           continue

        d = idx.loc[cod]
        if isinstance(d, pd.DataFrame): d = d.sort_values(by="timestamp").iloc[-1]

        ts = d.get("timestamp", pd.NaT)
        try:
            fecha_str = pd.to_datetime(ts).strftime("%d/%m/%Y %H:%M") if pd.notna(ts) else ""
        except Exception:
            fecha_str = ""

        registros.append({
            "PERIODO DE LECTURA": r["PERIODO_NORM"] or "",
            "CLIENTE": r["CLIENTE"],
            "CÓDIGO DE DOSÍMETRO": cod,
            "NOMBRE": r["NOMBRE_COMPLETO"] or r["NOMBRE"],
            "CÉDULA": r["CÉDULA"],
            "FECHA DE LECTURA": fecha_str,
            "TIPO DE DOSÍMETRO": r["TIPO DE DOSÍMETRO"] or "CE",
            "Hp(10)":  float(d.get("hp10dose", 0.0) or 0.0),
            "Hp(0.07)":float(d.get("hp0.07dose", 0.0) or 0.0),
            "Hp(3)":   float(d.get("hp3dose", 0.0) or 0.0),
            "_IS_CONTROL": bool(r["CONTROL_FLAG"]),
        })

    # Si no hubo coincidencias, muestra debug automático
    if not registros:
        with st.expander("Debug de coincidencias (no se encontraron)"):
            inter = sorted(set(codes_ninox).intersection(codes_file))
            st.write(f"Códigos Ninox: {len(codes_ninox)} | Archivo: {len(codes_file)} | Intersección: {len(inter)}")
            st.write("Ejemplos Ninox:", codes_ninox[:30])
            st.write("Ejemplos Archivo:", codes_file[:30])

    registros.sort(key=lambda x: (not x.get("_IS_CONTROL", False), x.get("NOMBRE","")))
    for r in registros: r.pop("_IS_CONTROL", None)
    return registros

# ===================== Valor - Control =====================
def periodo_desde_fecha(periodo_str: str, fecha_str: str) -> str:
    per = (periodo_str or "").strip().upper()
    per = re.sub(r'\.+$', '', per).strip()
    if per and per != "CONTROL": return per
    if not fecha_str:            return per or ""
    try:
        fecha = pd.to_datetime(fecha_str, dayfirst=True, errors="coerce")
        if pd.isna(fecha): return per or ""
        meses = ["ENERO","FEBRERO","MARZO","ABRIL","MAYO","JUNIO","JULIO","AGOSTO","SEPTIEMBRE","OCTUBRE","NOVIEMBRE","DICIEMBRE"]
        return f"{meses[fecha.month - 1]} {fecha.year}"
    except Exception:
        return per or ""

def aplicar_valor_menos_control(registros):
    if not registros: return registros
    base10 = float(registros[0]['Hp(10)']); base07 = float(registros[0]['Hp(0.07)']); base3 = float(registros[0]['Hp(3)'])
    for i, r in enumerate(registros):
        r['PERIODO DE LECTURA'] = periodo_desde_fecha(r.get('PERIODO DE LECTURA',''), r.get('FECHA DE LECTURA',''))
        if i == 0:
            r['NOMBRE'] = "CONTROL"
            r['Hp(10)'] = f"{base10:.2f}"; r['Hp(0.07)'] = f"{base07:.2f}"; r['Hp(3)'] = f"{base3:.2f}"
        else:
            for key, base in [('Hp(10)', base10), ('Hp(0.07)', base07), ('Hp(3)', base3)]:
                diff = float(r[key]) - base
                r[key] = "PM" if diff < 0.005 else f"{diff:.2f}"
    return registros

# ===================== TABS =====================
tab1, tab2 = st.tabs(["📥 Carga, VALOR−CONTROL y Subida", "📊 Reporte Actual / Anual / Vida"])

# ===================== TAB 1 =====================
with tab1:
    st.subheader("📤 Cargar archivo de Dosis ↔ LISTA DE CODIGO (Ninox) → subir a BASE DE DATOS")

    with st.sidebar:
        st.markdown("### ⚙️ Configuración (TAB 1)")
        base_table_id   = st.text_input("Tabla de lectura (personas/códigos)", value=DEFAULT_BASE_TABLE_ID, key="tab1_base")
        report_table_id = st.text_input("Tabla de escritura (salida)", value=DEFAULT_REPORT_TABLE_ID, key="tab1_report")
        periodo_filtro  = st.text_input("Filtro PERIODO (opcional)", value="— TODOS —", key="tab1_per")
        subir_pm_como_texto = st.checkbox("Subir 'PM' como TEXTO (si Hp son texto en Ninox)", value=True, key="tab1_pm_texto")
        debug_uno = st.checkbox("Enviar 1 registro (debug)", value=False, key="tab1_debug")
        show_tables = st.checkbox("Mostrar tablas Ninox (debug)", value=False, key="tab1_show")

    # Leer LISTA DE CODIGO
    try:
        if show_tables:
            st.expander("Tablas Ninox (debug)").json(ninox_list_tables(TEAM_ID, DATABASE_ID))
        df_lista_raw = ninox_fetch_records(TEAM_ID, DATABASE_ID, base_table_id)
        if df_lista_raw.empty:
            st.warning("No hay datos en LISTA DE CODIGO.")
            df_participantes = None
        else:
            df_participantes = normalize_lista_codigo(df_lista_raw)
            st.success(f"Conectado a Ninox. Tabla: {base_table_id}")
            st.dataframe(df_participantes.head(15), use_container_width=True)
    except Exception as e:
        st.error(f"Error leyendo {base_table_id}: {e}")
        df_participantes = None

    st.markdown("#### Archivo de Dosis")
    upload = st.file_uploader("Selecciona CSV/XLS/XLSX", type=["csv","xls","xlsx"], key="tab1_upl")
    df_dosis = leer_dosis(upload) if upload else None
    if df_dosis is not None:
        st.caption("Vista previa dosis (normalizada):")
        st.dataframe(df_dosis.head(15), use_container_width=True)

    col1, col2 = st.columns([1,1])
    with col1:
        nombre_reporte = st.text_input("Nombre archivo (sin extensión)",
                                       value=f"ReporteDosimetria_{datetime.now().strftime('%Y-%m-%d')}",
                                       key="tab1_name")
    with col2:
        btn_proc = st.button("✅ Procesar", type="primary", use_container_width=True, key="tab1_btn_proc")

    if btn_proc:
        if df_participantes is None or df_participantes.empty:
            st.error("No hay filas en LISTA DE CODIGO.")
        elif df_dosis is None or df_dosis.empty:
            st.error("No hay datos de dosis.")
        elif 'dosimeter' not in df_dosis.columns:
            st.error("El archivo de dosis debe tener la columna 'dosimeter'.")
        else:
            with st.spinner("Procesando..."):
                registros = construir_registros_desde_lista_codigo(
                    df_participantes, df_dosis, periodo_filtro=periodo_filtro
                )
                if not registros:
                    st.warning("No hay coincidencias CÓDIGO_DOSÍMETRO ↔ dosis (revisa filtro/códigos).")
                else:
                    registros = aplicar_valor_menos_control(registros)
                    df_final = pd.DataFrame(registros)

                    df_final['PERIODO DE LECTURA'] = (
                        df_final['PERIODO DE LECTURA'].astype(str)
                        .str.replace(r'\.+$', '', regex=True).str.strip()
                    )
                    if not df_final.empty:
                        df_final.loc[df_final.index.min(), 'NOMBRE'] = 'CONTROL'
                        df_final['NOMBRE'] = df_final['NOMBRE'].astype(str).str.replace(r'\.+$', '', regex=True).str.strip()

                    st.session_state.df_final = df_final
                    st.success(f"¡Listo! Registros generados: {len(df_final)}")
                    st.dataframe(df_final, use_container_width=True)

                    try:
                        xlsx = exportar_excel_simple_valor_control(df_final)
                        st.download_button(
                            "⬇️ Descargar Excel (VALOR−CONTROL)",
                            data=xlsx,
                            file_name=f"{(nombre_reporte.strip() or 'ReporteDosimetria')}.xlsx",
                            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                            key="tab1_dl"
                        )
                    except Exception as e:
                        st.error(f"No se pudo generar Excel: {e}")

    st.markdown("---")
    st.subheader("⬆️ Subir TODO a Ninox (tabla BASE DE DATOS)")

    CUSTOM_MAP = {
        "PERIODO DE LECTURA": "PERIODO DE LECTURA",
        "CLIENTE": "CLIENTE",
        "CÓDIGO DE DOSÍMETRO": "CÓDIGO_DOSÍMETRO",
        "NOMBRE": "NOMBRE",
        "CÉDULA": "CÉDULA",
        "FECHA DE LECTURA": "FECHA DE LECTURA",
        "TIPO DE DOSÍMETRO": "TIPO DE DOSÍMETRO",
    }
    SPECIAL_MAP = {"Hp(10)": "Hp (10)", "Hp(0.07)": "Hp (0.07)", "Hp(3)": "Hp (3)"}

    def resolve_dest_name(col_name: str) -> str:
        if col_name in SPECIAL_MAP: return SPECIAL_MAP[col_name]
        if col_name in CUSTOM_MAP:  return CUSTOM_MAP[col_name]
        return col_name

    def _hp_value(v, as_text_pm=True):
        if isinstance(v, str) and v.strip().upper() == "PM":
            return "PM" if as_text_pm else None
        try: return float(v)
        except Exception: return v if v is not None else None

    def _to_str(v):
        if pd.isna(v): return ""
        if isinstance(v, (pd.Timestamp, )): return v.strftime("%Y-%m-%d %H:%M:%S")
        return str(v)

    if st.button("Subir TODO a Ninox (tabla BASE DE DATOS)", key="tab1_btn_upload"):
        df_final = st.session_state.df_final
        if df_final is None or df_final.empty:
            st.error("Primero pulsa 'Procesar'.")
        else:
            try:
                ninox_fields = ninox_get_table_fields(TEAM_ID, DATABASE_ID, DEFAULT_REPORT_TABLE_ID)
                if not ninox_fields:
                    st.warning("No pude leer los campos de la tabla en Ninox. Verifica el nombre/ID.")
            except Exception as e:
                st.error(f"No se pudo leer el esquema de la tabla Ninox: {e}")
                ninox_fields = set()

            with st.expander("Campos detectados en Ninox"):
                st.write(sorted(ninox_fields))

            rows, skipped_cols = [], set()
            iterator = df_final.head(1).iterrows() if debug_uno else df_final.iterrows()

            for _, row in iterator:
                fields_payload = {}
                for col in df_final.columns:
                    dest = resolve_dest_name(col)
                    if ninox_fields and dest not in ninox_fields:
                        skipped_cols.add(dest); continue
                    val = row[col]
                    if dest in {"Hp (10)", "Hp (0.07)", "Hp (3)"}:
                        val = _hp_value(val, as_text_pm=subir_pm_como_texto)
                    else:
                        val = _to_str(val)
                    fields_payload[dest] = val
                rows.append({"fields": fields_payload})

            if debug_uno:
                st.caption("Payload (primer registro):"); st.json(rows[:1])

            with st.spinner("Subiendo a Ninox..."):
                res = ninox_insert_records(TEAM_ID, DATABASE_ID, DEFAULT_REPORT_TABLE_ID, rows, batch_size=300)

            if res.get("ok"):
                st.success(f"✅ Subido a Ninox: {res.get('inserted', 0)} registro(s).")
                if skipped_cols:
                    st.info("Columnas omitidas por no existir en Ninox:\n- " + "\n- ".join(sorted(skipped_cols)))
                try:
                    df_check = ninox_fetch_records(TEAM_ID, DATABASE_ID, DEFAULT_REPORT_TABLE_ID)
                    st.caption("Contenido reciente en BASE DE DATOS:")
                    st.dataframe(df_check.tail(len(rows)), use_container_width=True)
                except Exception:
                    pass
            else:
                st.error(f"❌ Error al subir: {res.get('error')}")
                if skipped_cols:
                    st.info("Revisa/crea en Ninox los campos omitidos:\n- " + "\n- ".join(sorted(skipped_cols)))

# ===================== TAB 2 =====================
with tab2:
    st.subheader("📊 Reporte — Actual, Anual y de por Vida (desde Ninox BASE DE DATOS)")

    def fetch_all_records(table_hint: str, page_size: int = 1000):
        table_id = ninox_resolve_table_id(TEAM_ID, DATABASE_ID, table_hint)
        url = f"{BASE_URL}/teams/{TEAM_ID}/databases/{DATABASE_ID}/tables/{table_id}/records"
        skip, out = 0, []
        while True:
            r = requests.get(url, headers=ninox_headers(), params={"limit": page_size, "skip": skip}, timeout=60)
            r.raise_for_status()
            chunk = r.json()
            if not chunk: break
            out.extend(chunk)
            if len(chunk) < page_size: break
            skip += page_size
        return out

    def normalize_df(records):
        rows = []
        for r in records:
            f = r.get("fields", {}) or {}
            rows.append({
                "_id": r.get("id"),
                "PERIODO DE LECTURA": f.get("PERIODO DE LECTURA"),
                "CLIENTE": f.get("CLIENTE"),
                "CÓDIGO DE DOSÍMETRO": str(f.get("CÓDIGO_DOSÍMETRO") or f.get("CÓDIGO DE DOSÍMETRO") or "").strip(),
                "NOMBRE": f.get("NOMBRE"),
                "CÉDULA": f.get("CÉDULA"),
                "FECHA DE LECTURA": f.get("FECHA DE LECTURA"),
                "TIPO DE DOSÍMETRO": f.get("TIPO DE DOSÍMETRO"),
                "Hp10_RAW":  as_value(f.get("Hp (10)")),
                "Hp007_RAW": as_value(f.get("Hp (0.07)")),
                "Hp3_RAW":   as_value(f.get("Hp (3)")),
                "Hp10_NUM":  as_num(f.get("Hp (10)")),
                "Hp007_NUM": as_num(f.get("Hp (0.07)")),
                "Hp3_NUM":   as_num(f.get("Hp (3)")),
            })
        df = pd.DataFrame(rows)
        df["FECHA_DE_LECTURA_DT"] = pd.to_datetime(
            df["FECHA DE LECTURA"].apply(
                lambda x: dtparse(str(x), dayfirst=True) if pd.notna(x) and str(x).strip() != "" else pd.NaT
            ), errors="coerce"
        )
        df["NOMBRE_NORM"] = df["NOMBRE"].fillna("").astype(str).str.strip()
        df["CÉDULA_NORM"] = df["CÉDULA"].fillna("").astype(str).str.strip()
        return df

    def fmt_fecha(dtval):
        if pd.isna(dtval): return ""
        try: return pd.to_datetime(dtval).strftime("%d/%m/%Y %H:%M")
        except Exception: return str(dtval)

    with st.spinner("Cargando datos desde Ninox (BASE DE DATOS)…"):
        base_records = fetch_all_records(DEFAULT_REPORT_TABLE_ID)
        base = normalize_df(base_records)

    if base.empty:
        st.warning("No hay registros en la tabla BASE DE DATOS.")
        st.stop()

    with st.sidebar:
        st.markdown("### ⚙️ Configuración (TAB 2)")
        per_order = (base.groupby("PERIODO DE LECTURA")["FECHA_DE_LECTURA_DT"].max()
                    .sort_values(ascending=False).index.astype(str).tolist())
        per_valid = [p for p in per_order if p.strip().upper() != "CONTROL"]
        periodo_actual = st.selectbox("Periodo actual", per_valid, index=0 if per_valid else None, key="tab2_periodo")

        st.checkbox("ANUAL automático (mismo año del periodo actual)", value=True, key="tab2_auto")

        st.multiselect("Periodos anteriores (solo si ANUAL automático está desmarcado)",
            [p for p in per_valid if p != periodo_actual],
            default=[per_valid[1]] if (len(per_valid) > 1) else [], key="tab2_prev")

        comp_opts = ["(todas)"] + sorted(base["CLIENTE"].dropna().astype(str).unique().tolist())
        compania = st.selectbox("Cliente", comp_opts, index=0, key="tab2_comp")

        tipo_opts = ["(todos)"] + sorted(base["TIPO DE DOSÍMETRO"].dropna().astype(str).unique().tolist())
        tipo = st.selectbox("Tipo de dosímetro", tipo_opts, index=0, key="tab2_tipo")

        files = st.file_uploader("Archivos de dosis (para filtrar por CÓDIGO DE DOSÍMETRO) — Opcional",
                                 type=["csv","xlsx","xls"], accept_multiple_files=True, key="tab2_codes")

    df_company_type = base.copy()
    if compania != "(todas)":
        df_company_type = df_company_type[df_company_type["CLIENTE"].astype(str) == compania]
    if tipo != "(todos)":
        df_company_type = df_company_type[df_company_type["TIPO DE DOSÍMETRO"].astype(str) == tipo]

    if df_company_type.empty:
        st.warning("No hay registros que cumplan los filtros de Cliente y/o Tipo de dosímetro.")
        st.stop()

    def read_codes_from_files(files) -> Set[str]:
        codes: Set[str] = set()
        for f in files or []:
            raw = f.read(); f.seek(0)
            name = f.name.lower()
            try:
                if name.endswith((".xlsx", ".xls")):
                    df = pd.read_excel(BytesIO(raw))
                else:
                    df = None
                    for enc in ("utf-8-sig","latin-1"):
                        try:
                            df = pd.read_csv(BytesIO(raw), sep=None, engine="python", encoding=enc); break
                        except Exception: continue
                    if df is None: df = pd.read_csv(BytesIO(raw))
            except Exception:
                continue
            if df is None or df.empty: continue
            cand = None
            for c in df.columns:
                cl = str(c).lower()
                if any(k in cl for k in ["dosim","código","codigo","wb","dosímetro","dosimetro"]):
                    cand = c; break
            if cand is None:
                for c in df.columns:
                    if df[c].astype(str).str.contains(r"^WB\d{5,}$", case=False, na=False).any():
                        cand = c; break
            if cand is None: cand = df.columns[0]
            codes |= set(df[cand].astype(str).str.strip())
        return {c for c in codes if c and c.lower() != "nan"}

    codes_filter: Optional[Set[str]] = read_codes_from_files(files) if files else None
    if codes_filter: st.success(f"Códigos detectados: {len(codes_filter)}")

    keys = ["NOMBRE_NORM", "CÉDULA_NORM"]
    df_curr = df_company_type[df_company_type["PERIODO DE LECTURA"].astype(str) == str(periodo_actual)].copy()
    if codes_filter:
        df_curr = df_curr[df_curr["CÓDIGO DE DOSÍMETRO"].isin(codes_filter)]
    if df_curr.empty:
        st.warning("No hay registros en el período actual con los filtros seleccionados.")
        st.stop()

    df_curr = df_curr.sort_values("FECHA_DE_LECTURA_DT")
    personas_actual = set(zip(df_curr["NOMBRE_NORM"], df_curr["CÉDULA_NORM"]))

    df_all_for_people = df_company_type.copy()
    df_all_for_people["_pair"] = list(zip(df_all_for_people["NOMBRE_NORM"], df_all_for_people["CÉDULA_NORM"]))
    df_all_for_people = df_all_for_people[df_all_for_people["_pair"].isin(personas_actual)]

    if df_all_for_people.empty:
        st.warning("No se encontró historial para las personas detectadas en el período actual.")
        st.stop()

    # ACTUAL
    gb_curr_sum = df_curr.groupby(keys, as_index=False).agg({
        "PERIODO DE LECTURA": "last",
        "CLIENTE": "last",
        "CÓDIGO DE DOSÍMETRO": "last",
        "NOMBRE": "last",
        "CÉDULA": "last",
        "FECHA_DE_LECTURA_DT": "max",
        "TIPO DE DOSÍMETRO": "last",
        "Hp10_NUM": "sum", "Hp007_NUM": "sum", "Hp3_NUM": "sum",
    })
    gb_curr_raw = df_curr.groupby(keys).agg({
        "Hp10_RAW": list, "Hp007_RAW": list, "Hp3_RAW": list
    }).rename(columns={
        "Hp10_RAW": "Hp10_ACTUAL_RAW_LIST",
        "Hp007_RAW": "Hp007_ACTUAL_RAW_LIST",
        "Hp3_RAW": "Hp3_ACTUAL_RAW_LIST"
    }).reset_index()
    out = gb_curr_sum.merge(gb_curr_raw, on=keys, how="left").rename(columns={
        "Hp10_NUM": "Hp10_ACTUAL_NUM_SUM",
        "Hp007_NUM": "Hp007_ACTUAL_NUM_SUM",
        "Hp3_NUM": "Hp3_ACTUAL_NUM_SUM",
    })

    # ANUAL (suma Actual + Previos)
    usar_anual_automatico = st.session_state.get("tab2_auto", True)
    if usar_anual_automatico:
        if df_curr["FECHA_DE_LECTURA_DT"].notna().any():
            current_year = int(df_curr["FECHA_DE_LECTURA_DT"].dt.year.mode().iloc[0])
        else:
            m = re.search(r"\b(20\d{2}|19\d{2})\b", str(periodo_actual))
            current_year = int(m.group(1)) if m else datetime.now().year
        df_same_year = df_all_for_people[df_all_for_people["FECHA_DE_LECTURA_DT"].dt.year == current_year].copy()
        df_prev_same_year = df_same_year[df_same_year["PERIODO DE LECTURA"].astype(str) != str(periodo_actual)].copy()
        gb_prev_sum = df_prev_same_year.groupby(keys).agg({
            "Hp10_NUM": "sum", "Hp007_NUM": "sum", "Hp3_NUM": "sum"
        }).rename(columns={
            "Hp10_NUM": "Hp10_PREV_NUM_SUM", "Hp007_NUM": "Hp007_PREV_NUM_SUM", "Hp3_NUM": "Hp3_PREV_NUM_SUM",
        }).reset_index()
        gb_prev_raw = df_prev_same_year.groupby(keys).agg({
            "Hp10_RAW": list, "Hp007_RAW": list, "Hp3_RAW": list
        }).rename(columns={
            "Hp10_RAW": "Hp10_PREV_RAW_LIST", "Hp007_RAW": "Hp007_PREV_RAW_LIST", "Hp3_RAW": "Hp3_PREV_RAW_LIST"
        }).reset_index()
        out = out.merge(gb_prev_sum, on=keys, how="left").merge(gb_prev_raw, on=keys, how="left")
    else:
        df_prev = df_all_for_people[df_all_for_people["PERIODO DE LECTURA"].astype(str).isin(st.session_state.get("tab2_prev", []))].copy()
        gb_prev_sum = df_prev.groupby(keys).agg({
            "Hp10_NUM": "sum", "Hp007_NUM": "sum", "Hp3_NUM": "sum"
        }).rename(columns={
            "Hp10_NUM": "Hp10_PREV_NUM_SUM", "Hp007_NUM": "Hp007_PREV_NUM_SUM", "Hp3_NUM": "Hp3_PREV_NUM_SUM",
        }).reset_index()
        gb_prev_raw = df_prev.groupby(keys).agg({
            "Hp10_RAW": list, "Hp007_RAW": list, "Hp3_RAW": list
        }).rename(columns={
            "Hp10_RAW": "Hp10_PREV_RAW_LIST", "Hp007_RAW": "Hp007_PREV_RAW_LIST", "Hp3_RAW": "Hp3_PREV_RAW_LIST"
        }).reset_index()
        out = out.merge(gb_prev_sum, on=keys, how="left").merge(gb_prev_raw, on=keys, how="left")

    # VIDA
    gb_life_sum = df_all_for_people.groupby(keys).agg({
        "Hp10_NUM": "sum", "Hp007_NUM": "sum", "Hp3_NUM": "sum"
    }).rename(columns={
        "Hp10_NUM": "Hp10_LIFE_NUM_SUM", "Hp007_NUM": "Hp007_LIFE_NUM_SUM", "Hp3_NUM": "Hp3_LIFE_NUM_SUM",
    }).reset_index()
    gb_life_raw = df_all_for_people.groupby(keys).agg({
        "Hp10_RAW": list, "Hp007_RAW": list, "Hp3_RAW": list
    }).rename(columns={
        "Hp10_RAW": "Hp10_LIFE_RAW_LIST", "Hp007_RAW": "Hp007_LIFE_RAW_LIST", "Hp3_RAW": "Hp3_LIFE_RAW_LIST"
    }).reset_index()
    out = out.merge(gb_life_sum, on=keys, how="left").merge(gb_life_raw, on=keys, how="left")

    # Totales finales
    for c in ["Hp10_ACTUAL_NUM_SUM","Hp007_ACTUAL_NUM_SUM","Hp3_ACTUAL_NUM_SUM",
              "Hp10_PREV_NUM_SUM","Hp007_PREV_NUM_SUM","Hp3_PREV_NUM_SUM",
              "Hp10_LIFE_NUM_SUM","Hp007_LIFE_NUM_SUM","Hp3_LIFE_NUM_SUM"]:
        if c not in out.columns: out[c] = 0.0
        out[c] = out[c].fillna(0.0)

    out["Hp (10) ACTUAL"]   = out.apply(lambda r: pm_or_sum(r.get("Hp10_ACTUAL_RAW_LIST", []), r["Hp10_ACTUAL_NUM_SUM"]), axis=1)
    out["Hp (0.07) ACTUAL"] = out.apply(lambda r: pm_or_sum(r.get("Hp007_ACTUAL_RAW_LIST", []), r["Hp007_ACTUAL_NUM_SUM"]), axis=1)
    out["Hp (3) ACTUAL"]    = out.apply(lambda r: pm_or_sum(r.get("Hp3_ACTUAL_RAW_LIST",  []), r["Hp3_ACTUAL_NUM_SUM"]),  axis=1)

    out["Hp (10) ANUAL"] = out.apply(
        lambda r: pm_or_sum(
            merge_raw_lists(r.get("Hp10_ACTUAL_RAW_LIST"), r.get("Hp10_PREV_RAW_LIST")),
            float(r["Hp10_ACTUAL_NUM_SUM"]) + float(r["Hp10_PREV_NUM_SUM"])
        ),
        axis=1
    )
    out["Hp (0.07) ANUAL"] = out.apply(
        lambda r: pm_or_sum(
            merge_raw_lists(r.get("Hp007_ACTUAL_RAW_LIST"), r.get("Hp007_PREV_RAW_LIST")),
            float(r["Hp007_ACTUAL_NUM_SUM"]) + float(r["Hp007_PREV_NUM_SUM"])
        ),
        axis=1
    )
    out["Hp (3) ANUAL"] = out.apply(
        lambda r: pm_or_sum(
            merge_raw_lists(r.get("Hp3_ACTUAL_RAW_LIST"), r.get("Hp3_PREV_RAW_LIST")),
            float(r["Hp3_ACTUAL_NUM_SUM"]) + float(r["Hp3_PREV_NUM_SUM"])
        ),
        axis=1
    )

    out["Hp (10) VIDA"]   = out.apply(lambda r: pm_or_sum(r.get("Hp10_LIFE_RAW_LIST", []), r["Hp10_LIFE_NUM_SUM"]), axis=1)
    out["Hp (0.07) VIDA"] = out.apply(lambda r: pm_or_sum(r.get("Hp007_LIFE_RAW_LIST", []), r["Hp007_LIFE_NUM_SUM"]), axis=1)
    out["Hp (3) VIDA"]    = out.apply(lambda r: pm_or_sum(r.get("Hp3_LIFE_RAW_LIST",  []), r["Hp3_LIFE_NUM_SUM"]),  axis=1)

    out["FECHA Y HORA DE LECTURA"] = out["FECHA_DE_LECTURA_DT"].apply(fmt_fecha)
    out["PERIODO DE LECTURA"] = st.session_state.get("tab2_periodo", "")
    out["__is_control"] = out["NOMBRE"].fillna("").astype(str).str.strip().str.upper().eq("CONTROL")
    out = out.sort_values(["__is_control","NOMBRE","CÉDULA"], ascending=[False, True, True])

    FINAL_COLS = [
        "PERIODO DE LECTURA","CLIENTE","CÓDIGO DE DOSÍMETRO","NOMBRE","CÉDULA",
        "FECHA Y HORA DE LECTURA","TIPO DE DOSÍMETRO",
        "Hp (10) ACTUAL","Hp (0.07) ACTUAL","Hp (3) ACTUAL",
        "Hp (10) ANUAL","Hp (0.07) ANUAL","Hp (3) ANUAL",
        "Hp (10) VIDA","Hp (0.07) VIDA","Hp (3) VIDA",
    ]
    for c in FINAL_COLS:
        if c not in out.columns: out[c] = ""
    out = out[FINAL_COLS]

    st.markdown("#### Reporte final (vista previa)")
    st.dataframe(out, use_container_width=True, hide_index=True)

    csv_bytes = out.to_csv(index=False).encode("utf-8-sig")
    st.download_button("⬇️ Descargar CSV (UTF-8 con BOM)", data=csv_bytes,
                       file_name=f"reporte_dosimetria_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
                       mime="text/csv", key="tab2_dl_csv")

    def to_excel_simple(df: pd.DataFrame, sheet_name="Reporte"):
        bio = BytesIO()
        with pd.ExcelWriter(bio, engine="openpyxl") as w:
            df.to_excel(w, index=False, sheet_name=sheet_name)
        bio.seek(0); return bio.getvalue()

    xlsx_simple = to_excel_simple(out)
    st.download_button("⬇️ Descargar Excel (tabla simple)", data=xlsx_simple,
                       file_name=f"reporte_dosimetria_tabla_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx",
                       mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                       key="tab2_dl_xlsx_simple")
