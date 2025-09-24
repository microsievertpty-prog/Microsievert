# -*- coding: utf-8 -*-
# Streamlit app: CRUCE EXCLUSIVO por CÓDIGO_DOSÍMETRO (LISTA) ↔ dosimeter (Dosis)
# Lee AMBAS tablas desde ARCHIVOS (CSV/XLS/XLSX), normaliza códigos y periodos (AGO-25 → AGOSTO 2025),
# filtra por uno o varios periodos (vacío = TODOS), calcula VALOR−CONTROL y exporta Excel.

import re
from io import BytesIO
from datetime import datetime
from typing import List, Dict, Any, Optional

import pandas as pd
import streamlit as st

# ===================== UI / App =====================
st.set_page_config(page_title="Dosimetría — Match CÓDIGO_DOSÍMETRO ↔ dosimeter", page_icon="🧪", layout="wide")
st.title("🧪 Dosimetría — Match exacto CÓDIGO_DOSÍMETRO ↔ dosimeter (archivos)")
st.caption("Sube tu LISTA DE CÓDIGO y el archivo de dosis. El cruce se hace SOLO por el código normalizado.")

# ===================== Helpers =====================
def _norm_code(x: str) -> str:
    """
    Normaliza un código a formato estándar 'WB' + 6 dígitos.
    Acepta: '57', 'WB57', 'WB000057', '  /WB000057 ' → 'WB000057'
    """
    if x is None:
        return ""
    s = str(x).strip().upper()
    s = s.replace("\u00A0", " ").strip()      # NBSP
    s = re.sub(r"[^A-Z0-9]", "", s)           # quita todo lo no alfanumérico

    m_dig = re.fullmatch(r"(\d+)", s)
    if m_dig:
        return f"WB{m_dig.group(1).zfill(6)}"

    m_wb = re.fullmatch(r"WB(\d+)", s)
    if m_wb:
        return f"WB{m_wb.group(1).zfill(6)}"

    if re.fullmatch(r"WB\d{6}", s):
        return s
    return s  # deja pasar códigos no WB (si existieran)

def _read_csv_robusto(upload) -> pd.DataFrame:
    """
    Lectura robusta de CSV probando codificaciones y separadores para evitar UnicodeDecodeError.
    """
    raw = upload.read()
    upload.seek(0)
    codificaciones = ["utf-8-sig", "utf-8", "latin-1", "cp1252"]
    separadores = [None, ",", ";", "\t", "|"]   # None => autodetección (engine='python')
    ultimo_error = None
    for enc in codificaciones:
        for sep in separadores:
            try:
                return pd.read_csv(BytesIO(raw), sep=sep, engine="python", encoding=enc)
            except Exception as e:
                ultimo_error = e
                continue
    raise ultimo_error if ultimo_error else RuntimeError("No se pudo leer el CSV.")

# ===================== Lectores =====================
def leer_lista_codigo_archivo(upload) -> Optional[pd.DataFrame]:
    """
    Lee LISTA DE CÓDIGO desde CSV/XLS/XLSX y devuelve columnas estándar:
    CÉDULA, CÓDIGO USUARIO, NOMBRE, APELLIDO, FECHA DE NACIMIENTO, CLIENTE,
    CÓDIGO_CLIENTE, ETIQUETA, CÓDIGO_DOSÍMETRO, PERIODO DE LECTURA, TIPO DE DOSÍMETRO
    """
    if not upload:
        return None

    name = (upload.name or "").lower()
    if name.endswith((".xlsx", ".xls")):
        df = pd.read_excel(upload, sheet_name=0)
    else:
        df = _read_csv_robusto(upload)

    # Normaliza encabezados
    norm = (df.columns.astype(str).str.strip().str.lower().str.replace(r"\s+", " ", regex=True))
    df.columns = norm

    # Mapeo flexible (incluye variantes)
    candidates = {
        "cédula":             ["cédula","cedula","id","documento","ced"],
        "código usuario":     ["código usuario","codigo usuario","codigo_usuario","codigo de usuario","usuario"],
        "nombre":             ["nombre","nombres"],
        "apellido":           ["apellido","apellidos"],
        "fecha de nacimiento":["fecha de nacimiento","f. nacimiento","fecha nacimiento"],
        "cliente":            ["cliente","compañía","compania","empresa"],
        "código_cliente":     ["código cliente","codigo cliente","codigo_cliente","id cliente","cliente id"],
        "etiqueta":           ["etiqueta","tag","label"],
        "código_dosímetro":   ["código dosímetro","codigo dosimetro","codigo_dosimetro","dosímetro","dosimetro","dosimeter","codigo"],
        "periodo de lectura": ["periodo de lectura","período de lectura","periodo","período","periodo lectura","lectura periodo","periodo (ej. agosto 2025)"],
        "tipo de dosímetro":  ["tipo de dosímetro","tipo dosimetro","tipo_dosimetro","tipo"],
    }

    out = pd.DataFrame()
    for target, opts in candidates.items():
        found = next((opt for opt in opts if opt in df.columns), None)
        out[target.upper()] = df[found] if found is not None else ""

    return out

def normalize_lista_codigo(df: pd.DataFrame) -> pd.DataFrame:
    """Estándar + derivados para la LISTA DE CÓDIGO, con normalización de periodos (AGO-25 → AGOSTO 2025)."""
    needed = [
        "CÉDULA","CÓDIGO USUARIO","NOMBRE","APELLIDO","FECHA DE NACIMIENTO",
        "CLIENTE","CÓDIGO_CLIENTE","ETIQUETA","CÓDIGO_DOSÍMETRO",
        "PERIODO DE LECTURA","TIPO DE DOSÍMETRO"
    ]
    for c in needed:
        if c not in df.columns:
            df[c] = ""

    ap = df["APELLIDO"].fillna("").astype(str).str.strip()
    df["NOMBRE_COMPLETO"] = (df["NOMBRE"].fillna("").astype(str).str.strip() + " " + ap).str.strip()

    # Código estandarizado
    df["CODIGO"] = df["CÓDIGO_DOSÍMETRO"].fillna("").astype(str).map(_norm_code)

    # Periodo normalizado
    meses_largos = ["ENERO","FEBRERO","MARZO","ABRIL","MAYO","JUNIO",
                    "JULIO","AGOSTO","SEPTIEMBRE","OCTUBRE","NOVIEMBRE","DICIEMBRE"]
    meses_cortos = ["ENE","FEB","MAR","ABR","MAY","JUN","JUL","AGO","SEP","OCT","NOV","DIC"]
    mapa_mes = {m: meses_largos[i] for i, m in enumerate(meses_cortos)}

    def parse_periodo(raw: str) -> str:
        if raw is None:
            return ""
        s = str(raw).strip().upper()
        s = s.replace("\u00A0", " ").strip()
        s = re.sub(r"\s+", " ", s)
        s = re.sub(r"\.+$", "", s)

        # Formatos tipo "AGO-25" / "ago 25" / "ago25"
        m = re.match(r"^(ENE|FEB|MAR|ABR|MAY|JUN|JUL|AGO|SEP|OCT|NOV|DIC)[\s\-]?(\d{2}|\d{4})$", s)
        if m:
            mes_3 = m.group(1)
            yy = m.group(2)
            yyyy = int(yy) + 2000 if len(yy) == 2 else int(yy)
            return f"{mapa_mes[mes_3]} {yyyy}"

        # Ya “AGOSTO 2025” completo
        m2 = re.match(r"^(ENERO|FEBRERO|MARZO|ABRIL|MAYO|JUNIO|JULIO|AGOSTO|SEPTIEMBRE|OCTUBRE|NOVIEMBRE|DICIEMBRE)\s+(\d{4})$", s)
        if m2:
            return f"{m2.group(1)} {m2.group(2)}"

        # Último recurso: intentar parsear fecha
        try:
            dt = pd.to_datetime(s, errors="raise", dayfirst=True)
            return f"{meses_largos[dt.month-1]} {dt.year}"
        except Exception:
            return s  # lo deja como está

    df["PERIODO_NORM"] = df["PERIODO DE LECTURA"].apply(parse_periodo)

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
    """
    Lee archivo de dosis con columnas (dosimeter, hp10dose, hp0.07dose, hp3dose, timestamp opcional).
    Soporta CSV (codificaciones/separadores varios) y Excel.
    """
    if not upload:
        return None

    name = (upload.name or "").lower()
    if name.endswith(".csv"):
        df = _read_csv_robusto(upload)
    else:
        df = pd.read_excel(upload)

    # Encabezados compactos
    norm = (df.columns.astype(str).str.strip().str.lower()
            .str.replace(' ', '', regex=False)
            .str.replace('(', '').str.replace(')', '')
            .str.replace('.', '', regex=False))
    df.columns = norm

    # Mapas
    if 'dosimeter' not in df.columns:
        for alt in ['dosimetro','codigo','codigodosimetro','codigo_dosimetro']:
            if alt in df.columns:
                df.rename(columns={alt: 'dosimeter'}, inplace=True); break

    for cand in ['hp10dosecorr','hp10dose','hp10']:
        if cand in df.columns: df.rename(columns={cand:'hp10dose'}, inplace=True); break
    for cand in ['hp007dosecorr','hp007dose','hp007']:
        if cand in df.columns: df.rename(columns={cand:'hp0.07dose'}, inplace=True); break
    for cand in ['hp3dosecorr','hp3dose','hp3']:
        if cand in df.columns: df.rename(columns={cand:'hp3dose'}, inplace=True); break

    # Numéricos
    for k in ['hp10dose','hp0.07dose','hp3dose']:
        if k in df.columns:
            df[k] = pd.to_numeric(df[k], errors='coerce').fillna(0.0)
        else:
            df[k] = 0.0

    # Timestamp opcional
    if 'timestamp' in df.columns:
        df['timestamp'] = pd.to_datetime(df['timestamp'], errors='coerce')

    # Normaliza códigos
    if 'dosimeter' in df.columns:
        df['dosimeter'] = df['dosimeter'].astype(str).map(_norm_code)

    return df

# ===================== Valor − Control =====================
def periodo_desde_fecha(periodo_str: str, fecha_str: str) -> str:
    per = (periodo_str or "").strip().upper()
    per = re.sub(r'\.+$', '', per).strip()
    if per and per != "CONTROL":
        return per
    if not fecha_str:
        return per or ""
    try:
        fecha = pd.to_datetime(fecha_str, dayfirst=True, errors="coerce")
        if pd.isna(fecha): return per or ""
        meses = ["ENERO","FEBRERO","MARZO","ABRIL","MAYO","JUNIO",
                 "JULIO","AGOSTO","SEPTIEMBRE","OCTUBRE","NOVIEMBRE","DICIEMBRE"]
        return f"{meses[fecha.month-1]} {fecha.year}"
    except Exception:
        return per or ""

def aplicar_valor_menos_control(registros: List[Dict[str,Any]]):
    """Asume primer registro = control. Resta control; si diff < 0.005 ⇒ 'PM'."""
    if not registros: return registros
    base10 = float(registros[0]['Hp(10)'])
    base07 = float(registros[0]['Hp(0.07)'])
    base3  = float(registros[0]['Hp(3)'])
    for i, r in enumerate(registros):
        r['PERIODO DE LECTURA'] = periodo_desde_fecha(r.get('PERIODO DE LECTURA',''), r.get('FECHA DE LECTURA',''))
        if i == 0:
            r['NOMBRE']   = "CONTROL"
            r['Hp(10)']   = f"{base10:.2f}"
            r['Hp(0.07)'] = f"{base07:.2f}"
            r['Hp(3)']    = f"{base3:.2f}"
        else:
            for key, base in [('Hp(10)', base10), ('Hp(0.07)', base07), ('Hp(3)', base3)]:
                diff = float(r[key]) - base
                r[key] = "PM" if diff < 0.005 else f"{diff:.2f}"
    return registros

# ===================== UI: Subidas y flujo =====================
st.markdown("### 1) Subir **LISTA DE CÓDIGO**")
upl_lista = st.file_uploader("Selecciona CSV/XLS/XLSX (LISTA DE CÓDIGO)", type=["csv","xls","xlsx"], key="upl_lista")
df_lista_raw = leer_lista_codigo_archivo(upl_lista) if upl_lista else None

if df_lista_raw is not None:
    st.success(f"Lista cargada: {len(df_lista_raw)} fila(s)")
    st.dataframe(df_lista_raw.head(20), use_container_width=True)

    # Normaliza lista
    df_lista = normalize_lista_codigo(df_lista_raw)

    # Periodos (de la LISTA, ya normalizados)
    periodos = sorted([p for p in df_lista["PERIODO_NORM"].dropna().astype(str).unique() if p.strip() != ""])
    st.markdown("#### Filtrar por PERIODO DE LECTURA (multi; vacío = TODOS)")
    periods_sel = st.multiselect("PERIODO DE LECTURA", options=periodos, default=[])

    df_lista_f = df_lista[df_lista["PERIODO_NORM"].isin(periods_sel)] if periods_sel else df_lista.copy()

    with st.expander("Resumen de periodos detectados (LISTA)"):
        st.write(df_lista.groupby("PERIODO_NORM").size().sort_values(ascending=False))

    st.markdown("### 2) Subir **Archivo de Dosis**")
    upl_dosis = st.file_uploader("Selecciona CSV/XLS/XLSX (dosis)", type=["csv","xls","xlsx"], key="upl_dosis")
    df_dosis = leer_dosis(upl_dosis) if upl_dosis else None
    if df_dosis is not None:
        st.success(f"Dosis cargadas: {len(df_dosis)} fila(s)")
        st.dataframe(df_dosis.head(20), use_container_width=True)

    # Opciones
    c1, c2 = st.columns([1,1])
    with c1:
        nombre_out = st.text_input("Nombre archivo (sin extensión)", value=f"ReporteDosimetria_{datetime.now().strftime('%Y-%m-%d')}")
    with c2:
        btn_proc = st.button("✅ Procesar (match por código)", type="primary", use_container_width=True)

    show_debug = st.checkbox("🔎 Debug: sets y coincidencias", value=False)

    if btn_proc:
        if df_lista_f.empty:
            st.error("No hay filas en LISTA DE CÓDIGO (tras el filtro).")
        elif df_dosis is None or df_dosis.empty:
            st.error("No hay datos de dosis.")
        elif 'dosimeter' not in df_dosis.columns:
            st.error("El archivo de dosis debe tener la columna 'dosimeter'.")
        else:
            with st.spinner("Cruzando por CÓDIGO_DOSÍMETRO ↔ dosimeter…"):
                # ========== MATCH EXCLUSIVO: CODIGO (LISTA) ↔ dosimeter (Dosis) ==========
                set_lista  = set(df_lista_f["CODIGO"].dropna().astype(str))
                set_dosis  = set(df_dosis["dosimeter"].dropna().astype(str))
                inter = set_lista & set_dosis

                if show_debug:
                    with st.expander("Detalle de coincidencias"):
                        st.write(f"Códigos en LISTA (filtrada): {len(set_lista)}")
                        st.write(f"Códigos en DOSIS: {len(set_dosis)}")
                        st.write(f"Intersección: {len(inter)}")
                        st.write("Ejemplos intersección:", sorted(list(inter))[:50])
                        st.write("En dosis pero NO en lista (ejemplos):", sorted(list(set_dosis - set_lista))[:50])
                        st.write("En lista pero NO en dosis (ejemplos):", sorted(list(set_lista - set_dosis))[:50])

                df_merge = pd.merge(
                    df_lista_f,
                    df_dosis,
                    left_on="CODIGO",
                    right_on="dosimeter",
                    how="inner"   # SOLO coincidencias reales
                )

                if df_merge.empty:
                    st.warning("⚠️ No se encontraron coincidencias entre CÓDIGO_DOSÍMETRO y dosimeter. Revisa periodos/códigos.")
                else:
                    st.success(f"✅ {len(df_merge)} coincidencia(s) por código.")
                    st.dataframe(df_merge.head(50), use_container_width=True)

                    # ========== Construcción registros + VALOR−CONTROL ==========
                    # Ordenar CONTROL primero (si hay)
                    df_merge["_is_control"] = (
                        df_merge["NOMBRE_COMPLETO"].fillna("").astype(str).str.strip().str.upper().eq("CONTROL") |
                        df_merge["ETIQUETA"].fillna("").astype(str).str.strip().str.upper().eq("CONTROL")
                    )
                    df_merge = pd.concat([df_merge[df_merge["_is_control"]], df_merge[~df_merge["_is_control"]]], ignore_index=True)

                    registros: List[Dict[str, Any]] = []
                    for _, r in df_merge.iterrows():
                        ts = r.get("timestamp", pd.NaT)
                        fecha_str = ""
                        try:
                            if pd.notna(ts):
                                fecha_str = pd.to_datetime(ts).strftime("%d/%m/%Y %H:%M")
                        except Exception:
                            fecha_str = ""

                        registros.append({
                            "PERIODO DE LECTURA": r.get("PERIODO_NORM",""),
                            "CLIENTE": r.get("CLIENTE",""),
                            "CÓDIGO DE DOSÍMETRO": r.get("CODIGO",""),
                            "NOMBRE": r.get("NOMBRE_COMPLETO") or r.get("NOMBRE",""),
                            "CÉDULA": r.get("CÉDULA",""),
                            "FECHA DE LECTURA": fecha_str,
                            "TIPO DE DOSÍMETRO": r.get("TIPO DE DOSÍMETRO","") or "CE",
                            "Hp(10)": float(r.get("hp10dose", 0.0) or 0.0),
                            "Hp(0.07)": float(r.get("hp0.07dose", 0.0) or 0.0),
                            "Hp(3)": float(r.get("hp3dose", 0.0) or 0.0),
                        })

                    # Aplicar VALOR−CONTROL
                    if registros:
                        registros = aplicar_valor_menos_control(registros)
                        df_final = pd.DataFrame(registros)

                        # Limpieza suave
                        df_final['PERIODO DE LECTURA'] = (
                            df_final['PERIODO DE LECTURA'].astype(str)
                            .str.replace(r'\.+$', '', regex=True).str.strip()
                        )
                        if not df_final.empty:
                            df_final.loc[df_final.index.min(), 'NOMBRE'] = 'CONTROL'
                            df_final['NOMBRE'] = (
                                df_final['NOMBRE'].astype(str)
                                .str.replace(r'\.+$', '', regex=True).str.strip()
                            )

                        st.markdown("### Resultado (VALOR−CONTROL)")
                        st.dataframe(df_final, use_container_width=True)
                        st.session_state["df_final"] = df_final

                        # Exportar Excel simple
                        def to_excel_simple(df: pd.DataFrame):
                            bio = BytesIO()
                            with pd.ExcelWriter(bio, engine="openpyxl") as w:
                                df.to_excel(w, index=False, sheet_name="REPORTE")
                            bio.seek(0); return bio.getvalue()

                        xlsx = to_excel_simple(df_final)
                        st.download_button(
                            "⬇️ Descargar Excel (VALOR−CONTROL)",
                            data=xlsx,
                            file_name=f"{(nombre_out.strip() or 'ReporteDosimetria')}.xlsx",
                            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
                        )
else:
    st.info("Sube primero la **LISTA DE CÓDIGO** para continuar.")
