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
from openpyxl.drawing.image import Image as XLImage

# Logo de ejemplo si no subes uno (opcional)
try:
    from PIL import Image as PILImage, ImageDraw, ImageFont
except Exception:
    PILImage = None
    ImageDraw = None
    ImageFont = None

# ===================== NINOX CONFIG =====================
API_TOKEN   = "edf312a0-98b8-11f0-883e-db77626d62e5"   
TEAM_ID     = "YrsYfTegptdZcHJEj"
DATABASE_ID = "ow1geqnkz00e"
BASE_URL    = "https://api.ninox.com/v1"

# IDs por defecto
DEFAULT_BASE_TABLE_ID   = "J"   # BASE DE DATOS
DEFAULT_REPORT_TABLE_ID = "C"   # REPORTE

# ===================== STREAMLIT (global) =====================
st.set_page_config(page_title="Microsievert - Dosimetría", page_icon="🧪", layout="wide")
st.title("🧪 Sistema de Gestión de Dosimetría — Microsievert")
st.caption("Ninox + Procesamiento VALOR − CONTROL + Reporte Actual/Anual/Vida + Exportación")

if "df_final" not in st.session_state:
    st.session_state.df_final = None

# ===================== Ninox helpers (compartidos) =====================
def ninox_headers():
    return {"Authorization": f"Bearer {API_TOKEN}", "Content-Type": "application/json"}

@st.cache_data(ttl=300, show_spinner=False)
def ninox_list_tables(team_id: str, db_id: str):
    url = f"{BASE_URL}/teams/{team_id}/databases/{db_id}/tables"
    r = requests.get(url, headers=ninox_headers(), timeout=30)
    r.raise_for_status()
    return r.json()

@st.cache_data(ttl=300, show_spinner=False)
def ninox_fetch_records(team_id: str, db_id: str, table_id: str, per_page: int = 1000):
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
    df.columns = [str(c) for c in df.columns]  # conservar acentos/espacios
    return df

def ninox_insert_records(team_id: str, db_id: str, table_id: str, rows: list, batch_size: int = 400):
    url = f"{BASE_URL}/teams/{team_id}/databases/{db_id}/tables/{table_id}/records"
    n = len(rows)
    if n == 0:
        return {"ok": True, "inserted": 0}
    inserted = 0
    for i in range(0, n, batch_size):
        chunk = rows[i:i+batch_size]
        r = requests.post(url, headers=ninox_headers(), json=chunk, timeout=60)
        if r.status_code != 200:
            return {"ok": False, "inserted": inserted, "error": f"{r.status_code} {r.text}"}
        inserted += len(chunk)
    return {"ok": True, "inserted": inserted}

@st.cache_data(ttl=120, show_spinner=False)
def ninox_get_table_fields(team_id: str, db_id: str, table_id: str):
    """Devuelve el conjunto de nombres de campos existentes en la tabla Ninox."""
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
                if name:
                    fields.add(name)
            break
    return fields

# ===================== Utilidades genéricas =====================
def round2(x: float) -> float:
    return float(f"{x:.2f}")

# --------- Helpers para reporte (parte 2) ---------
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
    if isinstance(raws, (list, tuple, set)):
        arr = list(raws)
    elif isinstance(raws, _pd.Series):
        arr = raws.tolist()
    elif raws is None or (isinstance(raws, float) and _pd.isna(raws)) or raws == "":
        arr = []
    else:
        arr = [raws]
    vals = [str(x).upper() for x in arr if str(x).strip() != ""]
    if vals and all(v == "PM" for v in vals):
        return "PM"
    try:
        total = float(numeric_sum)
        if _pd.isna(total):
            total = 0.0
    except Exception:
        total = 0.0
    return round2(total)

def merge_raw_lists(*vals):
    import pandas as _pd
    out: List[Any] = []
    for v in vals:
        if isinstance(v, (list, tuple, set)):
            out.extend(list(v))
        elif isinstance(v, _pd.Series):
            out.extend(v.tolist())
        elif v is None or (isinstance(v, float) and _pd.isna(v)) or v == "":
            continue
        else:
            out.append(v)
    return out

def fetch_all_records(table_id: str, page_size: int = 1000):
    # versión usada en parte 2 (usa limit/skip de la otra ruta)
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
            "COMPAÑÍA": f.get("COMPAÑÍA"),
            "CÓDIGO DE DOSÍMETRO": str(f.get("CÓDIGO DE DOSÍMETRO") or "").strip(),
            "NOMBRE": f.get("NOMBRE"),
            "CÉDULA": f.get("CÉDULA"),
            "FECHA DE LECTURA": f.get("FECHA DE LECTURA"),
            "TIPO DE DOSÍMETRO": f.get("TIPO DE DOSÍMETRO"),
            "Hp10_RAW":  as_value(f.get("Hp (10)")),
            "Hp007_RAW": as_value(f.get("Hp (0.07)")),
            "Hp3_RAW":  as_value(f.get("Hp (3)")),
            "Hp10_NUM":  as_num(f.get("Hp (10)")),
            "Hp007_NUM": as_num(f.get("Hp (0.07)")),
            "Hp3_NUM":  as_num(f.get("Hp (3)")),
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

def col_pixels(ws, col_letter: str) -> int:
    w = ws.column_dimensions[col_letter].width
    if w is None: w = 8.43
    return int(w * 7 + 5)

def row_pixels(ws, row_idx: int) -> int:
    h = ws.row_dimensions[row_idx].height
    if h is None: h = 15
    return int(h * 96 / 72)

def fit_logo(ws, logo_bytes: bytes, top_left: str = "C1", bottom_right: str = "F4", padding: int = 6):
    if not logo_bytes:
        return
    img = XLImage(BytesIO(logo_bytes))
    def parse_cell(cell_ref: str):
        letters = ''.join([c for c in cell_ref if c.isalpha()]) or "A"
        digits  = ''.join([c for c in cell_ref if c.isdigit()]) or "1"
        col_idx = column_index_from_string(letters.upper())
        row_idx = int(digits)
        return col_idx, row_idx
    tl_col, tl_row = parse_cell(top_left)
    br_col, br_row = parse_cell(bottom_right)
    if br_col < tl_col: tl_col, br_col = br_col, tl_col
    if br_row < tl_row: tl_row, br_row = br_row, tl_row
    box_w = sum(col_pixels(ws, get_column_letter(c)) for c in range(tl_col, br_col + 1))
    box_h = sum(row_pixels(ws, r) for r in range(tl_row, br_row + 1))
    max_w = max(10, box_w - 2 * padding)
    max_h = max(10, box_h - 2 * padding)
    try:
        scale = min(max_w / float(img.width), max_h / float(img.height), 1.0)
    except Exception:
        scale = 1.0
    img.width  = int(img.width  * scale)
    img.height = int(img.height * scale)
    img.anchor = top_left
    ws.add_image(img)

def sample_logo_bytes(text="µSv  MICROSIEVERT, S.A."):
    if PILImage is None or ImageDraw is None or ImageFont is None: return None
    img = PILImage.new("RGBA", (420, 110), (255, 255, 255, 0))
    d = ImageDraw.Draw(img)
    try:
        font = ImageFont.truetype("arial.ttf", 36)
    except Exception:
        font = ImageFont.load_default()
    d.text((12, 30), text, fill=(0, 70, 140, 255), font=font)
    bio = BytesIO(); img.save(bio, format="PNG"); return bio.getvalue()

# ===================== Dosis (parte 1) =====================
def leer_dosis(upload):
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
        for alt in ['dosimetro', 'codigo', 'codigodosimetro', 'codigo_dosimetro']:
            if alt in df.columns:
                df.rename(columns={alt: 'dosimeter'}, inplace=True); break

    for cand in ['hp10dosecorr', 'hp10dose', 'hp10']:
        if cand in df.columns: df.rename(columns={cand: 'hp10dose'}, inplace=True); break
    for cand in ['hp007dosecorr', 'hp007dose', 'hp007']:
        if cand in df.columns: df.rename(columns={cand: 'hp0.07dose'}, inplace=True); break
    for cand in ['hp3dosecorr', 'hp3dose', 'hp3']:
        if cand in df.columns: df.rename(columns={cand: 'hp3dose'}, inplace=True); break

    for k in ['hp10dose', 'hp0.07dose', 'hp3dose']:
        if k in df.columns: df[k] = pd.to_numeric(df[k], errors='coerce').fillna(0.0)
        else: df[k] = 0.0

    if 'dosimeter' in df.columns:
        df['dosimeter'] = df['dosimeter'].astype(str).str.strip().str.upper()

    if 'timestamp' in df.columns:
        df['timestamp'] = pd.to_datetime(df['timestamp'], errors='coerce')

    return df

def periodo_desde_fecha(periodo_str: str, fecha_str: str) -> str:
    per = (periodo_str or "").strip().upper()
    per = re.sub(r'\.+$', '', per).strip()
    if per and per != "CONTROL":
        return per
    if not fecha_str:
        return per or ""
    try:
        fecha = pd.to_datetime(fecha_str, dayfirst=True, errors="coerce")
        if pd.isna(fecha):
            return per or ""
        meses = ["ENERO","FEBRERO","MARZO","ABRIL","MAYO","JUNIO",
                 "JULIO","AGOSTO","SEPTIEMBRE","OCTUBRE","NOVIEMBRE","DICIEMBRE"]
        mes = meses[fecha.month - 1]
        return f"{mes} {fecha.year}"
    except Exception:
        return per or ""

def construir_registros(dfp, dfd, periodo_filtro="— TODOS —"):
    registros = []
    needed = ["NOMBRE","APELLIDO","CÉDULA","COMPAÑÍA"] + \
             [f"DOSIMETRO {i}" for i in range(1,6)] + \
             [f"PERIODO {i}" for i in range(1,6)]
    for c in needed:
        if c not in dfp.columns: dfp[c] = ""

    for _, fila in dfp.iterrows():
        nombre_raw = f"{str(fila.get('NOMBRE','')).strip()} {str(fila.get('APELLIDO','')).strip()}".strip()
        for i in range(1, 6):
            cod = str(fila.get(f'DOSIMETRO {i}', '')).strip().upper()
            per = str(fila.get(f'PERIODO {i}', '')).upper()
            if not cod or cod == "NAN": continue

            periodo_i = "CONTROL" if re.match(r'^\s*CONTROL\b', per) else re.sub(r'\.+', '.', per).strip()

            pf = (periodo_filtro or "").strip().upper()
            if pf not in ("", "— TODOS —") and periodo_i != pf:
                continue

            row = dfd.loc[dfd['dosimeter'] == cod]
            if row.empty: continue

            r0 = row.iloc[0]
            fecha = r0.get('timestamp', pd.NaT)
            fecha_str = ""
            try:
                if pd.notna(fecha): fecha_str = pd.to_datetime(fecha).strftime('%d/%m/%Y %H:%M')
            except Exception:
                fecha_str = ""

            registros.append({
                'PERIODO DE LECTURA': periodo_i,
                'COMPAÑÍA': fila.get('COMPAÑÍA',''),
                'CÓDIGO DE DOSÍMETRO': cod,
                'NOMBRE': nombre_raw,
                'CÉDULA': fila.get('CÉDULA',''),
                'FECHA DE LECTURA': fecha_str,
                'TIPO DE DOSÍMETRO': 'CE',
                'Hp(10)': float(r0.get('hp10dose', 0.0)),
                'Hp(0.07)': float(r0.get('hp0.07dose', 0.0)),
                'Hp(3)': float(r0.get('hp3dose', 0.0))
            })
    return registros

def aplicar_valor_menos_control(registros):
    if not registros: return registros
    base10 = float(registros[0]['Hp(10)'])
    base07 = float(registros[0]['Hp(0.07)'])
    base3  = float(registros[0]['Hp(3)'])
    for i, r in enumerate(registros):
        r['PERIODO DE LECTURA'] = periodo_desde_fecha(
            r.get('PERIODO DE LECTURA', ''), r.get('FECHA DE LECTURA', '')
        )
        if i == 0:
            r['NOMBRE']  = "CONTROL"
            r['Hp(10)']  = f"{base10:.2f}"
            r['Hp(0.07)'] = f"{base07:.2f}"
            r['Hp(3)']   = f"{base3:.2f}"
        else:
            for key, base in [('Hp(10)', base10), ('Hp(0.07)', base07), ('Hp(3)', base3)]:
                diff = float(r[key]) - base
                r[key] = "PM" if diff < 0.005 else f"{diff:.2f}"
    return registros

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

    headers = [
        'PERIODO DE LECTURA','COMPAÑÍA','CÓDIGO DE DOSÍMETRO','NOMBRE',
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

# ===================== Excel formato plantilla (parte 2) =====================
def build_formatted_excel(df_final: pd.DataFrame,
                          header_lines: List[str],
                          logo_bytes: Optional[bytes]) -> bytes:
    wb = Workbook(); ws = wb.active; ws.title = "Reporte"
    bold = Font(bold=True)
    center = Alignment(horizontal="center", vertical="center", wrap_text=True)
    thin = Side(style="thin")
    border = Border(top=thin, bottom=thin, left=thin, right=thin)
    gray = PatternFill("solid", fgColor="DDDDDD")
    group_fill = PatternFill("solid", fgColor="EEEEEE")

    widths = {"A":24,"B":28,"C":16,"D":16,"E":16,"F":16,"G":10,"H":12,"I":12,"J":12,"K":12,"L":12,"M":12,"N":12,"O":12,"P":12}
    for k, v in widths.items():
        ws.column_dimensions[k].width = v
    for r in range(1, 5):
        ws.row_dimensions[r].height = 20

    # Encabezado texto (A1:B4)
    for i, line in enumerate(header_lines[:4], start=1):
        ws.merge_cells(f"A{i}:B{i}")
        c = ws[f"A{i}"]; c.value = line; c.fill = gray
        c.font = Font(bold=True); c.alignment = Alignment(horizontal="left", vertical="center")
        for col in ("A","B"): ws.cell(row=i, column=ord(col)-64).border = border

    # Fecha de emisión (I1:P1)
    ws.merge_cells("I1:J1"); ws["I1"] = "Fecha de emisión"
    ws["I1"].font = Font(bold=True, size=10); ws["I1"].alignment = center; ws["I1"].fill = gray
    ws.merge_cells("K1:P1"); ws["K1"] = datetime.now().strftime("%d-%b-%y").lower()
    ws["K1"].font = Font(bold=True, size=10); ws["K1"].alignment = center
    for col_idx in range(ord("I")-64, ord("P")-64+1):
        ws.cell(row=1, column=col_idx).border = border

    # Cliente (I2:P2)
    ws.merge_cells("I2:P2")
    ws["I2"] = "Cliente: ____________________________"
    ws["I2"].font = Font(bold=True, size=10)
    ws["I2"].alignment = Alignment(horizontal="left", vertical="center")
    for col_idx in range(ord("I")-64, ord("P")-64+1):
        ws.cell(row=2, column=col_idx).border = border

    # Logo
    if logo_bytes is None:
        logo_bytes = sample_logo_bytes()
    if logo_bytes:
        fit_logo(ws, logo_bytes, top_left="C1", bottom_right="F4", padding=6)

    # Título
    ws.merge_cells("A6:P6"); ws["A6"] = "REPORTE DE DOSIMETRÍA"
    ws["A6"].font = Font(bold=True, size=14); ws["A6"].alignment = center

    # Bloques
    ws.merge_cells("H7:J7"); ws["H7"] = "DOSIS ACTUAL (mSv)"
    ws.merge_cells("K7:M7"); ws["K7"] = "DOSIS ANUAL (mSv)"
    ws.merge_cells("N7:P7"); ws["N7"] = "DOSIS DE POR VIDA (mSv)"
    for rng in (("H7","J7"), ("K7","M7"), ("N7","P7")):
        start_col = column_index_from_string(rng[0][0]); end_col = column_index_from_string(rng[1][0]); row = 7
        ws[rng[0]].font = bold; ws[rng[0]].alignment = center; ws[rng[0]].fill = group_fill
        for col in range(start_col, end_col + 1):
            c = ws.cell(row=row, column=col); c.border = border; c.fill = group_fill

    # Encabezados
    headers = [
        "PERIODO DE LECTURA","COMPAÑÍA","CÓDIGO DE DOSÍMETRO","NOMBRE","CÉDULA",
        "FECHA Y HORA DE LECTURA","TIPO DE DOSÍMETRO",
        "Hp (10) ACTUAL","Hp (0.07) ACTUAL","Hp (3) ACTUAL",
        "Hp (10) ANUAL","Hp (0.07) ANUAL","Hp (3) ANUAL",
        "Hp (10) VIDA","Hp (0.07) VIDA","Hp (3) VIDA",
    ]
    header_row = 8
    for col_idx, h in enumerate(headers, start=1):
        cell = ws.cell(row=header_row, column=col_idx, value=h)
        cell.font = bold; cell.alignment = center; cell.border = border; cell.fill = gray

    # Datos
    start_row = header_row + 1
    for _, r in df_final[headers].iterrows():
        ws.append(list(r.values))
    last_row = ws.max_row

    # Formato filas
    for row in ws.iter_rows(min_row=header_row, max_row=last_row, min_col=1, max_col=len(headers)):
        for cell in row:
            cell.border = border
            if cell.row >= start_row:
                cell.alignment = Alignment(vertical="center", horizontal="center", wrap_text=True)
    for rr in range(start_row, last_row + 1):
        ws.row_dimensions[rr].height = 20

    ws.freeze_panes = f"A{start_row}"

    # Auto-ancho
    for col_cells in ws.iter_cols(min_col=1, max_col=16, min_row=header_row, max_row=last_row):
        col_letter = get_column_letter(col_cells[0].column)
        max_len = max(len("" if c.value is None else str(c.value)) for c in col_cells)
        ws.column_dimensions[col_letter].width = max(ws.column_dimensions[col_letter].width, min(max_len + 2, 42))

    # Sección informativa
    row = last_row + 2
    ws.merge_cells(f"A{row}:P{row}"); ws[f"A{row}"] = "INFORMACIÓN DEL REPORTE DE DOSIMETRÍA"
    ws[f"A{row}"].font = Font(bold=True); ws[f"A{row}"].alignment = Alignment(horizontal="center"); row += 1

    bullets = [
        "‒ Periodo de lectura: periodo de uso del dosímetro personal.",
        "‒ Fecha de lectura: fecha en que se realizó la lectura.",
        "‒ Tipo de dosímetro:",
    ]
    for text in bullets:
        ws.merge_cells(f"A{row}:D{row}")
        c = ws[f"A{row}"]; c.value = text
        c.font = Font(size=10, bold=True); c.alignment = Alignment(horizontal="left")
        row += 2

    tipos = [("CE","Cuerpo Entero"), ("A","Anillo"), ("B","Brazalete"), ("CR","Cristalino")]
    for clave, desc in tipos:
        ws.merge_cells(f"C{row}:D{row}"); ws[f"C{row}"] = f"{clave} = {desc}"
        ws[f"C{row}"].font = Font(size=10, bold=True); ws[f"C{row}"].alignment = Alignment(horizontal="left")
        for col in ("C","D"): ws.cell(row=row, column=ord(col)-64).border = border
        row += 1
    row += 1

    ws.merge_cells(f"F{row}:I{row}"); ws[f"F{row}"] = "LÍMITES ANUALES DE EXPOSICIÓN A RADIACIONES"
    ws[f"F{row}"].font = Font(bold=True, size=10); ws[f"F{row}"].alignment = Alignment(horizontal="center")
    row += 1

    limites = [
        ("Cuerpo Entero", "20 mSv/año"),
        ("Cristalino", "150 mSv/año"),
        ("Extremidades y piel", "500 mSv/año"),
        ("Fetal", "1 mSv/periodo de gestación"),
        ("Público", "1 mSv/año"),
    ]
    for cat, val in limites:
        ws.merge_cells(f"F{row}:G{row}"); ws[f"F{row}"] = cat
        ws[f"F{row}"].font = Font(size=10); ws[f"F{row}"].alignment = Alignment(horizontal="left")
        ws.merge_cells(f"H{row}:I{row}"); ws[f"H{row}"] = val
        ws[f"H{row}"].font = Font(size=10); ws[f"H{row}"].alignment = Alignment(horizontal="right")
        for col in ("F","G","H","I"): ws.cell(row=row, column=ord(col)-64).border = border
        row += 1
    row += 2

    ws.merge_cells(f"A{row}:P{row}"); ws[f"A{row}"] = "‒ DATOS DEL PARTICIPANTE:"
    ws[f"A{row}"].font = Font(bold=True, size=10); ws[f"A{row}"].alignment = Alignment(horizontal="left"); row += 1

    datos = [
        "‒ Código de usuario: Número único asignado al usuario por Microsievert, S.A.",
        "‒ Nombre: Persona a la cual se le asigna el dosímetro personal.",
        "‒ Cédula: Número del documento de identidad personal del usuario.",
    ]
    for txt in datos:
        ws.merge_cells(f"A{row}:P{row}"); ws[f"A{row}"] = txt
        ws[f"A{row}"].font = Font(size=10); ws[f"A{row}"].alignment = Alignment(horizontal="left")
        row += 1
    row += 2

    ws.merge_cells(f"A{row}:P{row}"); ws[f"A{row}"] = "‒ DOSIS EN MILISIEVERT:"
    ws[f"A{row}"].font = Font(bold=True, size=10); ws[f"A{row}"].alignment = Alignment(horizontal="left"); row += 1

    shade = PatternFill("solid", fgColor="DDDDDD")
    ws.merge_cells(f"B{row}:C{row}"); ws[f"B{row}"] = "Nombre"
    ws[f"B{row}"].font = Font(bold=True, size=10)
    ws[f"B{row}"].alignment = Alignment(horizontal="center", vertical="center", wrap_text=True); ws[f"B{row}"].fill = shade
    ws.merge_cells(f"D{row}:I{row}"); ws[f"D{row}"] = "Definición"
    ws[f"D{row}"].font = Font(bold=True, size=10)
    ws[f"D{row}"].alignment = Alignment(horizontal="center", vertical="center", wrap_text=True); ws[f"D{row}"].fill = shade
    ws.merge_cells(f"J{row}:J{row}"); ws[f"J{row}"] = "Unidad"
    ws[f"J{row}"].font = Font(bold=True, size=10)
    ws[f"J{row}"].alignment = Alignment(horizontal="center", vertical="center", wrap_text=True); ws[f"J{row}"].fill = shade
    for col in ("B","C","D","E","F","G","H","I","J"):
        ws.cell(row=row, column=ord(col)-64).border = border
    ws.row_dimensions[row].height = 30; row += 1

    definitions = [
        ("Dosis efectiva Hp(10)",  "Es la dosis equivalente en tejido blando, J·kg⁻¹ o Sv a una profundidad de 10 mm, bajo determinado punto del cuerpo.", "mSv"),
        ("Dosis superficial Hp(0,07)", "Es la dosis equivalente en tejido blando, J·kg⁻¹ o Sv a una profundidad de 0,07 mm, bajo determinado punto del cuerpo.", "mSv"),
        ("Dosis cristalino Hp(3)", "Es la dosis equivalente en tejido blando, J·kg⁻¹ o Sv a una profundidad de 3 mm, bajo determinado punto del cuerpo.", "mSv"),
    ]
    for nom, desc, uni in definitions:
        ws.merge_cells(f"B{row}:C{row}"); ws[f"B{row}"] = nom
        ws[f"B{row}"].font = Font(size=10, bold=True); ws[f"B{row}"].alignment = Alignment(horizontal="left", wrap_text=True)
        ws.merge_cells(f"D{row}:I{row}"); ws[f"D{row}"] = desc
        ws[f"D{row}"].font = Font(size=10); ws[f"D{row}"].alignment = Alignment(horizontal="left", wrap_text=True)
        ws.merge_cells(f"J{row}:J{row}"); ws[f"J{row}"] = uni
        ws[f"J{row}"].font = Font(size=10); ws[f"J{row}"].alignment = Alignment(horizontal="center", wrap_text=True)
        for col in ("B","C","D","E","F","G","H","I","J"):
            ws.cell(row=row, column=ord(col)-64).border = border
        ws.row_dimensions[row].height = 30; row += 1

    row += 1
    ws.merge_cells(f"A{row}:P{row}")
    ws[f"A{row}"] = "LECTURAS DE ANILLO: las lecturas del dosímetro de anillo son registradas como una dosis equivalente superficial Hp(0,07)."
    ws[f"A{row}"].font = Font(size=10, bold=True); ws[f"A{row}"].alignment = Alignment(horizontal="left", wrap_text=True); row += 1

    ws.merge_cells(f"A{row}:P{row}")
    ws[f"A{row}"] = "Los resultados de las dosis individuales de radiación son reportados para diferentes periodos de tiempo:"
    ws[f"A{row}"].font = Font(size=10); ws[f"A{row}"].alignment = Alignment(horizontal="left", wrap_text=True); row += 1

    blocks = [
        ("DOSIS ACTUAL",      "Es el correspondiente de dosis acumulada durante el período de lectura definido."),
        ("DOSIS ANUAL",       "Es el correspondiente de dosis acumulada desde el inicio del año hasta la fecha."),
        ("DOSIS DE POR VIDA", "Es el correspondiente de dosis acumulada desde el inicio del servicio dosimétrico hasta la fecha."),
    ]
    for clave, texto in blocks:
        ws.merge_cells(f"B{row}:C{row}"); ws[f"B{row}"] = clave
        ws[f"B{row}"].font = Font(bold=True, size=10); ws[f"B{row}"].alignment = Alignment(horizontal="center")
        ws.merge_cells(f"D{row}:P{row}"); ws[f"D{row}"] = texto
        ws[f"D{row}"].font = Font(size=10); ws[f"D{row}"].alignment = Alignment(horizontal="left", wrap_text=True)
        for col_idx in range(ord("B")-64, ord("P")-64+1):
            ws.cell(row=row, column=col_idx).border = border
        row += 1

    row += 2
    ws.merge_cells(f"A{row}:P{row}")
    ws[f"A{row}"] = ("DOSÍMETRO DE CONTROL: incluido en cada paquete entregado para monitorear la exposición a la radiación "
                     "recibida durante el tránsito y almacenamiento. Este dosímetro debe ser guardado por el cliente en un "
                     "área libre de radiación durante el período de uso.")
    ws[f"A{row}"].font = Font(size=10, bold=True); ws[f"A{row}"].alignment = Alignment(horizontal="left", wrap_text=True); row += 2

    ws.merge_cells(f"A{row}:P{row}")
    ws[f"A{row}"] = ("POR DEBAJO DEL MÍNIMO DETECTADO: es la dosis por debajo de la cantidad mínima reportada para el período "
                     "de uso y son registradas como \"PM\".")
    ws[f"A{row}"].font = Font(size=10, bold=True); ws[f"A{row}"].alignment = Alignment(horizontal="left", wrap_text=True)

    bio = BytesIO(); wb.save(bio); bio.seek(0)
    return bio.getvalue()

# ===================== TABS =====================
tab1, tab2 = st.tabs(["📥 Carga, VALOR−CONTROL y Subida a Ninox", "📊 Reporte Actual / Anual / Vida (desde Ninox)"])

# ===================== TAB 1 =====================
with tab1:
    st.subheader("📤 Cargar archivo de Dosis y cruzar con BASE DE DATOS (Ninox)")

    with st.sidebar:
        st.markdown("### ⚙️ Configuración (TAB 1)")
        base_table_id   = st.text_input("Table ID BASE DE DATOS", value=DEFAULT_BASE_TABLE_ID, key="tab1_base")
        report_table_id = st.text_input("Table ID REPORTE", value=DEFAULT_REPORT_TABLE_ID, key="tab1_report")
        periodo_filtro  = st.text_input("Filtro PERIODO (opcional)", value="— TODOS —", key="tab1_per")
        subir_pm_como_texto = st.checkbox("Subir 'PM' como TEXTO (si campos Hp son Texto en Ninox)", value=True, key="tab1_pm_texto")
        debug_uno = st.checkbox("Enviar 1 registro (debug)", value=False, key="tab1_debug")
        show_tables = st.checkbox("Mostrar tablas Ninox (debug)", value=False, key="tab1_show")

    try:
        if show_tables:
            st.expander("Tablas Ninox (debug)").json(ninox_list_tables(TEAM_ID, DATABASE_ID))
        df_participantes = ninox_fetch_records(TEAM_ID, DATABASE_ID, base_table_id)
        if df_participantes.empty:
            st.warning("No hay datos en BASE DE DATOS (Ninox).")
        else:
            st.success(f"Conectado a Ninox. Tabla BASE DE DATOS: {base_table_id}")
            st.dataframe(df_participantes.head(15), use_container_width=True)
    except Exception as e:
        st.error(f"Error leyendo BASE DE DATOS: {e}")
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
            st.error("No hay participantes desde Ninox.")
        elif df_dosis is None or df_dosis.empty:
            st.error("No hay datos de dosis.")
        elif 'dosimeter' not in df_dosis.columns:
            st.error("El archivo de dosis debe tener la columna 'dosimeter'.")
        else:
            with st.spinner("Procesando..."):
                registros = construir_registros(df_participantes, df_dosis, periodo_filtro=periodo_filtro)
                if not registros:
                    st.warning("No hay coincidencias DOSÍMETRO ↔ dosis (revisa filtro/códigos).")
                else:
                    registros = aplicar_valor_menos_control(registros)
                    df_final = pd.DataFrame(registros)

                    # Limpieza suave
                    df_final['PERIODO DE LECTURA'] = (
                        df_final['PERIODO DE LECTURA'].astype(str)
                        .str.replace(r'\.+$', '', regex=True).str.strip()
                    )
                    df_final.loc[df_final.index.min(), 'NOMBRE'] = 'CONTROL'
                    df_final['NOMBRE'] = (
                        df_final['NOMBRE'].astype(str)
                        .str.replace(r'\.+$', '', regex=True).str.strip()
                    )

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
    st.subheader("⬆️ Subir TODO a Ninox (tabla REPORTE)")

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
        if col_name in SPECIAL_MAP: return SPECIAL_MAP[col_name]
        if col_name in CUSTOM_MAP:  return CUSTOM_MAP[col_name]
        return col_name

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

    if st.button("Subir TODO a Ninox (tabla REPORTE)", key="tab1_btn_upload"):
        df_final = st.session_state.df_final
        if df_final is None or df_final.empty:
            st.error("Primero pulsa 'Procesar'.")
        else:
            try:
                ninox_fields = ninox_get_table_fields(TEAM_ID, DATABASE_ID, report_table_id)
                if not ninox_fields:
                    st.warning("No pude leer los campos de la tabla en Ninox. Verifica el ID de tabla.")
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
                st.caption("Payload (primer registro):")
                st.json(rows[:1])

            with st.spinner("Subiendo a Ninox..."):
                res = ninox_insert_records(TEAM_ID, DATABASE_ID, report_table_id, rows, batch_size=300)

            if res.get("ok"):
                st.success(f"✅ Subido a Ninox: {res.get('inserted', 0)} registro(s).")
                if skipped_cols:
                    st.info("Columnas omitidas por no existir en Ninox:\n- " + "\n- ".join(sorted(skipped_cols)))
                try:
                    df_check = ninox_fetch_records(TEAM_ID, DATABASE_ID, report_table_id)
                    st.caption("Contenido reciente en REPORTE:")
                    st.dataframe(df_check.tail(len(rows)), use_container_width=True)
                except Exception:
                    pass
            else:
                st.error(f"❌ Error al subir: {res.get('error')}")
                if skipped_cols:
                    st.info("Revisa/crea en Ninox los campos omitidos:\n- " + "\n- ".join(sorted(skipped_cols)))

# ===================== TAB 2 =====================
with tab2:
    st.subheader("📊 Reporte — Actual, Anual y de por Vida (por persona) desde Ninox REPORTE")

    # Sidebar específica de TAB 2
    with st.sidebar:
        st.markdown("### ⚙️ Configuración (TAB 2)")
        header_line1 = st.text_input("Encabezado Excel — Línea 1", "MICROSIEVERT, S.A.", key="tab2_h1")
        header_line2 = st.text_input("Encabezado Excel — Línea 2", "PH Conardo", key="tab2_h2")
        header_line3 = st.text_input("Encabezado Excel — Línea 3", "Calle 41 Este, Panamá", key="tab2_h3")
        header_line4 = st.text_input("Encabezado Excel — Línea 4", "PANAMÁ", key="tab2_h4")
        logo_file = st.file_uploader("Logo (PNG/JPG) opcional", type=["png","jpg","jpeg"], key="tab2_logo")

    with st.spinner("Cargando datos desde Ninox (REPORTE)…"):
        base_records = fetch_all_records(DEFAULT_REPORT_TABLE_ID)
        base = normalize_df(base_records)

    if base.empty:
        st.warning("No hay registros en la tabla REPORTE.")
        st.stop()

    with st.sidebar:
        st.markdown("---")
        per_order = (base.groupby("PERIODO DE LECTURA")["FECHA_DE_LECTURA_DT"].max()
                    .sort_values(ascending=False).index.astype(str).tolist())
        per_valid = [p for p in per_order if p.strip().upper() != "CONTROL"]
        periodo_actual = st.selectbox("Periodo actual", per_valid, index=0 if per_valid else None, key="tab2_periodo")

        usar_anual_automatico = st.checkbox("ANUAL automático (mismo año del periodo actual)", value=True, key="tab2_auto")

        periodos_anteriores = st.multiselect(
            "Periodos anteriores (solo si ANUAL automático está desmarcado)",
            [p for p in per_valid if p != periodo_actual],
            default=[per_valid[1]] if (len(per_valid) > 1) else [], key="tab2_prev"
        )
        comp_opts = ["(todas)"] + sorted(base["COMPAÑÍA"].dropna().astype(str).unique().tolist())
        compania = st.selectbox("Compañía", comp_opts, index=0, key="tab2_comp")
        tipo_opts = ["(todos)"] + sorted(base["TIPO DE DOSÍMETRO"].dropna().astype(str).unique().tolist())
        tipo = st.selectbox("Tipo de dosímetro", tipo_opts, index=0, key="tab2_tipo")

        files = st.file_uploader("Archivos de dosis (para filtrar por CÓDIGO DE DOSÍMETRO) — Opcional", type=["csv","xlsx","xls"], accept_multiple_files=True, key="tab2_codes")

    # Filtro compañía/tipo
    df_company_type = base.copy()
    if compania != "(todas)":
        df_company_type = df_company_type[df_company_type["COMPAÑÍA"].astype(str) == compania]
    if tipo != "(todos)":
        df_company_type = df_company_type[df_company_type["TIPO DE DOSÍMETRO"].astype(str) == tipo]

    if df_company_type.empty:
        st.warning("No hay registros que cumplan los filtros de Compañía y/o Tipo de dosímetro.")
        st.stop()

    # Leer códigos (opcional)
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
    if codes_filter:
        st.success(f"Códigos detectados: {len(codes_filter)}")

    # Construcción DF ACTUAL
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
        "COMPAÑÍA": "last",
        "CÓDIGO DE DOSÍMETRO": "last",
        "NOMBRE": "last",
        "CÉDULA": "last",
        "FECHA_DE_LECTURA_DT": "max",
        "TIPO DE DOSÍMETRO": "last",
        "Hp10_NUM": "sum",
        "Hp007_NUM": "sum",
        "Hp3_NUM": "sum",
    })
    gb_curr_raw = df_curr.groupby(keys).agg({
        "Hp10_RAW": list,
        "Hp007_RAW": list,
        "Hp3_RAW": list
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

    # ANUAL
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
            "Hp10_NUM": "sum",
            "Hp007_NUM": "sum",
            "Hp3_NUM": "sum"
        }).rename(columns={
            "Hp10_NUM": "Hp10_PREV_NUM_SUM",
            "Hp007_NUM": "Hp007_PREV_NUM_SUM",
            "Hp3_NUM": "Hp3_PREV_NUM_SUM",
        }).reset_index()

        gb_prev_raw = df_prev_same_year.groupby(keys).agg({
            "Hp10_RAW": list,
            "Hp007_RAW": list,
            "Hp3_RAW": list
        }).rename(columns={
            "Hp10_RAW": "Hp10_PREV_RAW_LIST",
            "Hp007_RAW": "Hp007_PREV_RAW_LIST",
            "Hp3_RAW": "Hp3_PREV_RAW_LIST"
        }).reset_index()

        out = out.merge(gb_prev_sum, on=keys, how="left").merge(gb_prev_raw, on=keys, how="left")

    else:
        df_prev = df_all_for_people[df_all_for_people["PERIODO DE LECTURA"].astype(str).isin(st.session_state.get("tab2_prev", []))].copy()

        gb_prev_sum = df_prev.groupby(keys).agg({
            "Hp10_NUM": "sum",
            "Hp007_NUM": "sum",
            "Hp3_NUM": "sum"
        }).rename(columns={
            "Hp10_NUM": "Hp10_PREV_NUM_SUM",
            "Hp007_NUM": "Hp007_PREV_NUM_SUM",
            "Hp3_NUM": "Hp3_PREV_NUM_SUM",
        }).reset_index()

        gb_prev_raw = df_prev.groupby(keys).agg({
            "Hp10_RAW": list,
            "Hp007_RAW": list,
            "Hp3_RAW": list
        }).rename(columns={
            "Hp10_RAW": "Hp10_PREV_RAW_LIST",
            "Hp007_RAW": "Hp007_PREV_RAW_LIST",
            "Hp3_RAW": "Hp3_PREV_RAW_LIST"
        }).reset_index()

        out = out.merge(gb_prev_sum, on=keys, how="left").merge(gb_prev_raw, on=keys, how="left")

    # VIDA
    gb_life_sum = df_all_for_people.groupby(keys).agg({
        "Hp10_NUM": "sum",
        "Hp007_NUM": "sum",
        "Hp3_NUM": "sum"
    }).rename(columns={
        "Hp10_NUM": "Hp10_LIFE_NUM_SUM",
        "Hp007_NUM": "Hp007_LIFE_NUM_SUM",
        "Hp3_NUM": "Hp3_LIFE_NUM_SUM",
    }).reset_index()

    gb_life_raw = df_all_for_people.groupby(keys).agg({
        "Hp10_RAW": list,
        "Hp007_RAW": list,
        "Hp3_RAW": list
    }).rename(columns={
        "Hp10_RAW": "Hp10_LIFE_RAW_LIST",
        "Hp007_RAW": "Hp007_LIFE_RAW_LIST",
        "Hp3_RAW": "Hp3_LIFE_RAW_LIST"
    }).reset_index()

    out = out.merge(gb_life_sum, on=keys, how="left").merge(gb_life_raw, on=keys, how="left")

    # Rellenos y columnas finales
    for c in [
        "Hp10_ACTUAL_NUM_SUM","Hp007_ACTUAL_NUM_SUM","Hp3_ACTUAL_NUM_SUM",
        "Hp10_PREV_NUM_SUM","Hp007_PREV_NUM_SUM","Hp3_PREV_NUM_SUM",
        "Hp10_LIFE_NUM_SUM","Hp007_LIFE_NUM_SUM","Hp3_LIFE_NUM_SUM",
    ]:
        if c not in out.columns: out[c] = 0.0
        out[c] = out[c].fillna(0.0)

    def fmt_fecha(dtval):
        if pd.isna(dtval): return ""
        try: return pd.to_datetime(dtval).strftime("%d/%m/%Y %H:%M")
        except Exception: return str(dtval)

    out["Hp (10) ACTUAL"]   = out.apply(lambda r: pm_or_sum(r.get("Hp10_ACTUAL_RAW_LIST", []), r["Hp10_ACTUAL_NUM_SUM"]), axis=1)
    out["Hp (0.07) ACTUAL"] = out.apply(lambda r: pm_or_sum(r.get("Hp007_ACTUAL_RAW_LIST", []), r["Hp007_ACTUAL_NUM_SUM"]), axis=1)
    out["Hp (3) ACTUAL"]    = out.apply(lambda r: pm_or_sum(r.get("Hp3_ACTUAL_RAW_LIST",  []), r["Hp3_ACTUAL_NUM_SUM"]),  axis=1)

    out["Hp (10) ANUAL"] = out.apply(lambda r: pm_or_sum(
        merge_raw_lists(r.get("Hp10_ACTUAL_RAW_LIST"), r.get("Hp10_PREV_RAW_LIST")),
        float(r["Hp10_ACTUAL_NUM_SUM"]) + float(r["Hp10_PREV_NUM_SUM"])
    ), axis=1)
    out["Hp (0.07) ANUAL"] = out.apply(lambda r: pm_or_sum(
        merge_raw_lists(r.get("Hp007_ACTUAL_RAW_LIST"), r.get("Hp007_PREV_RAW_LIST")),
        float(r["Hp007_ACTUAL_NUM_SUM"]) + float(r["Hp007_PREV_NUM_SUM"])
    ), axis=1)
    out["Hp (3) ANUAL"] = out.apply(lambda r: pm_or_sum(
        merge_raw_lists(r.get("Hp3_ACTUAL_RAW_LIST"), r.get("Hp3_PREV_RAW_LIST")),
        float(r["Hp3_ACTUAL_NUM_SUM"]) + float(r["Hp3_PREV_NUM_SUM"])
    ), axis=1)

    out["Hp (10) VIDA"]   = out.apply(lambda r: pm_or_sum(r.get("Hp10_LIFE_RAW_LIST", []), r["Hp10_LIFE_NUM_SUM"]), axis=1)
    out["Hp (0.07) VIDA"] = out.apply(lambda r: pm_or_sum(r.get("Hp007_LIFE_RAW_LIST", []), r["Hp007_LIFE_NUM_SUM"]), axis=1)
    out["Hp (3) VIDA"]    = out.apply(lambda r: pm_or_sum(r.get("Hp3_LIFE_RAW_LIST",  []), r["Hp3_LIFE_NUM_SUM"]),  axis=1)

    out["FECHA Y HORA DE LECTURA"] = out["FECHA_DE_LECTURA_DT"].apply(fmt_fecha)
    out["PERIODO DE LECTURA"] = st.session_state.get("tab2_periodo", "")

    out["__is_control"] = out["NOMBRE"].fillna("").astype(str).str.strip().str.upper().eq("CONTROL")
    out = out.sort_values(["__is_control","NOMBRE","CÉDULA"], ascending=[False, True, True])

    FINAL_COLS = [
        "PERIODO DE LECTURA","COMPAÑÍA","CÓDIGO DE DOSÍMETRO","NOMBRE","CÉDULA",
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

    # Descargas
    csv_bytes = out.to_csv(index=False).encode("utf-8-sig")
    st.download_button(
        "⬇️ Descargar CSV (UTF-8 con BOM)",
        data=csv_bytes,
        file_name=f"reporte_dosimetria_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
        mime="text/csv",
        key="tab2_dl_csv"
    )

    def to_excel_simple(df: pd.DataFrame, sheet_name="Reporte"):
        bio = BytesIO()
        with pd.ExcelWriter(bio, engine="openpyxl") as w:
            df.to_excel(w, index=False, sheet_name=sheet_name)
        bio.seek(0); return bio.getvalue()

    xlsx_simple = to_excel_simple(out)
    st.download_button(
        "⬇️ Descargar Excel (tabla simple)",
        data=xlsx_simple,
        file_name=f"reporte_dosimetria_tabla_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx",
        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        key="tab2_dl_xlsx_simple"
    )

    # Excel plantilla (con Cliente en I2:P2)
    header_lines = [st.session_state.get("tab2_h1",""), st.session_state.get("tab2_h2",""),
                    st.session_state.get("tab2_h3",""), st.session_state.get("tab2_h4","")]
    logo_bytes = st.session_state.get("tab2_logo").read() if st.session_state.get("tab2_logo") is not None else None

    xlsx_fmt = build_formatted_excel(out.copy(), header_lines, logo_bytes)
    st.download_button(
        "⬇️ Descargar Excel (formato plantilla)",
        data=xlsx_fmt,
        file_name=f"reporte_dosimetria_plantilla_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx",
        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        key="tab2_dl_xlsx_fmt"
    )







