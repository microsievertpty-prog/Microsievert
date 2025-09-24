# archivo: tab1_microsievert.py
import io, re, requests, pandas as pd, streamlit as st
from io import BytesIO
from datetime import datetime
from openpyxl import Workbook
from openpyxl.styles import Font, Alignment, Border, Side, PatternFill
from openpyxl.utils import get_column_letter

# ========= CONFIG NINOX =========
API_TOKEN   = "edf312a0-98b8-11f0-883e-db77626d62e5"
TEAM_ID     = "YrsYfTegptdZcHJEj"
DATABASE_ID = "ow1geqnkz00e"
BASE_URL    = "https://api.ninox.com/v1"

DEFAULT_BASE_TABLE_ID   = "J"   # LISTA DE CÓDIGO  (origen)
DEFAULT_REPORT_TABLE_ID = "C"   # BASE DE DATOS    (destino)

# ========= STREAMLIT =========
st.set_page_config(page_title="Microsievert — TAB 1", page_icon="🧪", layout="wide")
st.title("TAB 1 — Cargar Dosis, VALOR−CONTROL y Subida a Ninox")

# ========= HELPERS NINOX =========
def ninox_headers():
    return {"Authorization": f"Bearer {API_TOKEN}", "Content-Type": "application/json"}

@st.cache_data(ttl=300)
def ninox_fetch_records(team_id, db_id, table_id, per_page: int = 1000):
    url = f"{BASE_URL}/teams/{team_id}/databases/{db_id}/tables/{table_id}/records"
    out, offset = [], 0
    while True:
        r = requests.get(url, headers=ninox_headers(), params={"perPage": per_page, "offset": offset}, timeout=60)
        r.raise_for_status()
        batch = r.json()
        if not batch: break
        out.extend(batch)
        if len(batch) < per_page: break
        offset += per_page
    rows = [x.get("fields", {}) for x in out]
    df = pd.DataFrame(rows) if rows else pd.DataFrame()
    df.columns = [str(c) for c in df.columns]  # conserva acentos/espacios
    return df

def ninox_insert_records(team_id, db_id, table_id, rows: list, batch_size: int = 400):
    url = f"{BASE_URL}/teams/{team_id}/databases/{db_id}/tables/{table_id}/records"
    inserted = 0
    for i in range(0, len(rows), batch_size):
        chunk = rows[i:i+batch_size]
        r = requests.post(url, headers=ninox_headers(), json=chunk, timeout=60)
        if r.status_code != 200:
            return {"ok": False, "inserted": inserted, "error": f"{r.status_code} {r.text}"}
        inserted += len(chunk)
    return {"ok": True, "inserted": inserted}

@st.cache_data(ttl=120)
def ninox_get_table_fields(team_id, db_id, table_id):
    url = f"{BASE_URL}/teams/{team_id}/databases/{db_id}/tables"
    r = requests.get(url, headers=ninox_headers(), timeout=30)
    r.raise_for_status()
    info = r.json()
    fields = set()
    for t in info:
        if str(t.get("id")) == str(table_id):
            cols = t.get("fields") or t.get("columns") or []
            for c in cols:
                if isinstance(c, dict) and c.get("name"):
                    fields.add(c["name"])
            break
    return fields

# ========= LECTURA ARCHIVO DE DOSIS =========
def leer_dosis(upload):
    if not upload: return None
    name = upload.name.lower()
    if name.endswith(".csv"):
        try: df = pd.read_csv(upload, delimiter=';', engine='python')
        except Exception:
            upload.seek(0); df = pd.read_csv(upload)
    else:
        df = pd.read_excel(upload)

    norm = (df.columns.astype(str).str.strip().str.lower()
            .str.replace(' ', '', regex=False).str.replace('(', '').str.replace(')', '')
            .str.replace('.', '', regex=False))
    df.columns = norm

    if 'dosimeter' not in df.columns:
        for alt in ['dosimetro','codigo','codigodosimetro','codigo_dosimetro']:
            if alt in df.columns:
                df.rename(columns={alt:'dosimeter'}, inplace=True); break

    for cand in ['hp10dosecorr','hp10dose','hp10']:
        if cand in df.columns: df.rename(columns={cand:'hp10dose'}, inplace=True); break
    for cand in ['hp007dosecorr','hp007dose','hp007']:
        if cand in df.columns: df.rename(columns={cand:'hp0.07dose'}, inplace=True); break
    for cand in ['hp3dosecorr','hp3dose','hp3']:
        if cand in df.columns: df.rename(columns={cand:'hp3dose'}, inplace=True); break

    for k in ['hp10dose','hp0.07dose','hp3dose']:
        df[k] = pd.to_numeric(df[k], errors='coerce').fillna(0.0) if k in df.columns else 0.0

    if 'dosimeter' in df.columns:
        df['dosimeter'] = df['dosimeter'].astype(str).str.strip().str.upper()

    if 'timestamp' in df.columns:
        df['timestamp'] = pd.to_datetime(df['timestamp'], errors='coerce')

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

# ========= CRUCE LISTA DE CÓDIGO -> DOSIS =========
def construir_registros(dfp: pd.DataFrame, dfd: pd.DataFrame, periodo_filtro: str = "— TODOS —"):
    if dfp is None or dfp.empty or dfd is None or dfd.empty: return []

    # columnas mínimas
    for c in ["NOMBRE","APELLIDO","CÉDULA","CLIENTE","COMPAÑÍA","PERIODO DE LECTURA","TIPO DE DOSÍMETRO"]:
        if c not in dfp.columns: dfp[c] = ""

    dfp = dfp.copy()
    dfd = dfd.copy()
    dfd["dosimeter"] = dfd["dosimeter"].astype(str).str.strip().str.upper()

    # A) Detecta columna de código en LISTA (CÓDIGO_DOSÍMETRO o CÓDIGO DE DOSÍMETRO, etc.)
    codigo_cols = [c for c in dfp.columns if str(c).strip().upper() in (
        "CÓDIGO_DOSÍMETRO","CODIGO_DOSÍMETRO","CÓDIGO DE DOSÍMETRO","CODIGO DE DOSIMETRO","CODIGO DE DOSÍMETRO"
    )]
    if not codigo_cols:
        st.error("En LISTA DE CÓDIGO falta la columna del código (CÓDIGO_DOSÍMETRO o CÓDIGO DE DOSÍMETRO).")
        return []
    codigo_col = codigo_cols[0]
    dfp[codigo_col] = dfp[codigo_col].astype(str).str.strip().str.upper()

    # filtro de periodo
    pf = (periodo_filtro or "").strip().upper()
    if pf not in ("","— TODOS —"):
        dfp = dfp[dfp["PERIODO DE LECTURA"].astype(str).str.upper()==pf]
        if dfp.empty: return []

    registros = []
    for _, fila in dfp.iterrows():
        cod = str(fila.get(codigo_col,"")).strip().upper()
        if not cod or cod=="NAN": continue

        block = dfd[dfd["dosimeter"]==cod]
        if block.empty: continue

        hp10 = float(block.get("hp10dose", pd.Series([0])).sum())
        hp07 = float(block.get("hp0.07dose", pd.Series([0])).sum())
        hp3  = float(block.get("hp3dose", pd.Series([0])).sum())

        fecha_str = ""
        if "timestamp" in block.columns and block["timestamp"].notna().any():
            fecha_str = pd.to_datetime(block["timestamp"]).max().strftime("%d/%m/%Y %H:%M")

        periodo_i = periodo_desde_fecha(str(fila.get("PERIODO DE LECTURA","")), fecha_str)
        nombre_raw = f"{str(fila.get('NOMBRE','')).strip()} {str(fila.get('APELLIDO','')).strip()}".strip()

        # B) Cliente/Compañía flexible
        compania = str(fila.get("CLIENTE_CLIENTE","") or fila.get("CLIENTE","") or fila.get("COMPAÑÍA","")).strip()
        tipo_dos = str(fila.get("TIPO DE DOSÍMETRO","") or "CE").strip()

        registros.append({
            "PERIODO DE LECTURA": periodo_i,
            "COMPAÑÍA": compania,
            "CÓDIGO DE DOSÍMETRO": cod,
            "NOMBRE": nombre_raw,
            "CÉDULA": str(fila.get("CÉDULA","")).strip(),
            "FECHA DE LECTURA": fecha_str,
            "TIPO DE DOSÍMETRO": tipo_dos,
            "Hp(10)":  hp10,
            "Hp(0.07)": hp07,
            "Hp(3)":   hp3,
        })
    return registros

# ========= VALOR - CONTROL =========
def aplicar_valor_menos_control(registros):
    if not registros: return registros
    df = pd.DataFrame(registros).copy()

    # C) Más señales de CONTROL
    control_mask = (
        df["NOMBRE"].fillna("").str.strip().str.upper().eq("CONTROL") |
        df["CÉDULA"].fillna("").str.strip().str.upper().eq("CONTROL") |
        df["CÓDIGO DE DOSÍMETRO"].astype(str).str.strip().str.upper().eq("CONTROL")
    )
    for extra in ("ETIQUETA","OBSERVACIÓN","OBSERVACION"):
        if extra in df.columns:
            control_mask |= df[extra].fillna("").str.strip().str.upper().eq("CONTROL")

    ctrl = df[control_mask]
    if not ctrl.empty:
        base10 = float(pd.to_numeric(ctrl["Hp(10)"], errors="coerce").fillna(0).mean())
        base07 = float(pd.to_numeric(ctrl["Hp(0.07)"], errors="coerce").fillna(0).mean())
        base3  = float(pd.to_numeric(ctrl["Hp(3)"], errors="coerce").fillna(0).mean())
    else:
        base10 = base07 = base3 = 0.0  # fallback

    out = []
    for r in registros:
        rr = r.copy()
        rr["PERIODO DE LECTURA"] = periodo_desde_fecha(r.get("PERIODO DE LECTURA",""), r.get("FECHA DE LECTURA",""))
        for key, base in [("Hp(10)",base10),("Hp(0.07)",base07),("Hp(3)",base3)]:
            try:
                diff = float(r[key]) - float(base)
                rr[key] = "PM" if diff < 0.005 else f"{diff:.2f}"
            except Exception:
                rr[key] = r[key]
        if str(r.get("NOMBRE","")).strip().upper()=="CONTROL":
            rr["NOMBRE"]="CONTROL"
        out.append(rr)
    return out

# ========= EXCEL =========
def exportar_excel_simple_valor_control(df_final: pd.DataFrame) -> bytes:
    wb = Workbook(); ws = wb.active; ws.title = "REPORTE DE DOSIS"
    border = Border(left=Side(style='thin'), right=Side(style='thin'),
                    top=Side(style='thin'),  bottom=Side(style='thin'))
    ws['I1'] = f"Fecha de emisión: {datetime.now().strftime('%d/%m/%Y')}"
    ws['I1'].font = Font(size=10, italic=True)
    ws['I1'].alignment = Alignment(horizontal='right', vertical='top')
    ws.merge_cells('A5:J5')
    c = ws['A5']; c.value = 'REPORTE DE DOSIMETRÍA'
    c.font = Font(bold=True, size=14); c.alignment = Alignment(horizontal='center')
    headers = ['PERIODO DE LECTURA','COMPAÑÍA','CÓDIGO DE DOSÍMETRO','NOMBRE',
               'CÉDULA','FECHA DE LECTURA','TIPO DE DOSÍMETRO','Hp(10)','Hp(0.07)','Hp(3)']
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

# ========= UI =========
with st.sidebar:
    st.markdown("### ⚙️ Configuración")
    base_table_id   = st.text_input("Table ID LISTA DE CÓDIGO (origen)", value=DEFAULT_BASE_TABLE_ID)
    report_table_id = st.text_input("Table ID BASE DE DATOS (destino)", value=DEFAULT_REPORT_TABLE_ID)
    periodo_filtro  = st.text_input("Filtro PERIODO (opcional)", value="— TODOS —")
    subir_pm_como_texto = st.checkbox("Subir 'PM' como TEXTO (si Hp son Texto en Ninox)", value=True)
    debug_uno = st.checkbox("Enviar 1 registro (debug)", value=False)

# 1) LISTA DE CÓDIGO
try:
    df_lista = ninox_fetch_records(TEAM_ID, DATABASE_ID, base_table_id)
    if df_lista.empty:
        st.warning("No hay datos en LISTA DE CÓDIGO.")
    else:
        st.success("✓ LISTA DE CÓDIGO cargada")
        st.dataframe(df_lista.head(15), use_container_width=True)
except Exception as e:
    st.error(f"Error leyendo LISTA DE CÓDIGO: {e}")
    df_lista = None

# 2) Archivo de dosis
st.markdown("#### Archivo de Dosis")
upload = st.file_uploader("Selecciona CSV/XLS/XLSX", type=["csv","xls","xlsx"])
df_dosis = leer_dosis(upload) if upload else None
if df_dosis is not None:
    st.caption("Vista previa dosis (normalizada):")
    st.dataframe(df_dosis.head(15), use_container_width=True)

# 3) Procesar
col1, col2 = st.columns([1,1])
with col1:
    nombre_reporte = st.text_input("Nombre archivo (sin extensión)",
                                   value=f"ReporteDosimetria_{datetime.now().strftime('%Y-%m-%d')}")
with col2:
    btn_proc = st.button("✅ Procesar", type="primary", use_container_width=True)

if btn_proc:
    if df_lista is None or df_lista.empty:
        st.error("No hay filas en LISTA DE CÓDIGO.")
    elif df_dosis is None or df_dosis.empty:
        st.error("No hay datos de dosis.")
    elif 'dosimeter' not in df_dosis.columns:
        st.error("El archivo de dosis debe tener la columna 'dosimeter'.")
    else:
        with st.spinner("Procesando…"):
            registros = construir_registros(df_lista, df_dosis, periodo_filtro=periodo_filtro)
            if not registros:
                st.warning("No hay coincidencias CÓDIGO_DOSÍMETRO ↔ dosimeter (revisa filtro/códigos).")
            else:
                registros = aplicar_valor_menos_control(registros)
                df_final = pd.DataFrame(registros)
                # limpieza ligera
                df_final['PERIODO DE LECTURA'] = df_final['PERIODO DE LECTURA'].astype(str).str.replace(r'\.+$', '', regex=True).str.strip()
                df_final['NOMBRE'] = df_final['NOMBRE'].astype(str).str.replace(r'\.+$', '', regex=True).str.strip()
                st.session_state["df_final"] = df_final
                st.success(f"¡Listo! Registros generados: {len(df_final)}")
                st.dataframe(df_final, use_container_width=True)
                try:
                    xlsx = exportar_excel_simple_valor_control(df_final)
                    st.download_button(
                        "⬇️ Descargar Excel (VALOR−CONTROL)",
                        data=xlsx,
                        file_name=f"{(nombre_reporte.strip() or 'ReporteDosimetria')}.xlsx",
                        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                        key="dl_xlsx"
                    )
                except Exception as e:
                    st.error(f"No se pudo generar Excel: {e}")

# 4) Subir a Ninox (BASE DE DATOS)
st.markdown("---")
st.subheader("⬆️ Subir TODO a Ninox (tabla BASE DE DATOS)")

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

def resolve_dest_name(col_name: str) -> str:
    return SPECIAL_MAP.get(col_name, CUSTOM_MAP.get(col_name, col_name))

def _hp_value(v, as_text_pm=True):
    if isinstance(v, str) and v.strip().upper()=="PM":
        return "PM" if as_text_pm else None
    try: return float(v)
    except Exception: return v if v is not None else None

def _to_str(v):
    return "" if pd.isna(v) else str(v)

if st.button("Subir TODO a Ninox (BASE DE DATOS)"):
    df_final = st.session_state.get("df_final")
    if df_final is None or df_final.empty:
        st.error("Primero pulsa 'Procesar'.")
    else:
        try:
            ninox_fields = ninox_get_table_fields(TEAM_ID, DATABASE_ID, DEFAULT_REPORT_TABLE_ID)
        except Exception as e:
            ninox_fields = set()
            st.error(f"No pude leer campos de la tabla destino: {e}")

        rows, skipped = [], set()
        it = df_final.head(1).iterrows() if st.session_state.get("debug_uno", False) else df_final.iterrows()
        for _, row in it:
            payload = {}
            for col in df_final.columns:
                dest = resolve_dest_name(col)
                if ninox_fields and dest not in ninox_fields:
                    skipped.add(dest); continue
                val = row[col]
                if dest in {"Hp (10)","Hp (0.07)","Hp (3)"}:
                    val = _hp_value(val, as_text_pm=st.session_state.get("subir_pm_como_texto", True))
                else:
                    val = _to_str(val)
                payload[dest] = val
            rows.append({"fields": payload})

        with st.spinner("Subiendo…"):
            res = ninox_insert_records(TEAM_ID, DATABASE_ID, DEFAULT_REPORT_TABLE_ID, rows, batch_size=300)

        if res.get("ok"):
            st.success(f"Subido a Ninox: {res.get('inserted',0)} registro(s).")
            if skipped:
                st.info("Omitidos por no existir en la tabla destino:\n- " + "\n- ".join(sorted(skipped)))
        else:
            st.error(f"Error al subir: {res.get('error')}")
            if skipped:
                st.info("Revisa/crea estos campos:\n- " + "\n- ".join(sorted(skipped)))
