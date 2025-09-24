# -*- coding: utf-8 -*-
import re
from io import BytesIO
from datetime import datetime
from typing import List, Dict, Any, Optional

import pandas as pd
import streamlit as st

# ===================== App =====================
st.set_page_config(page_title="Dosimetría — Solo archivos", page_icon="🧪", layout="wide")
st.title("🧪 Dosimetría — Cargar LISTA DE CÓDIGO y Dosis (sin Ninox)")
st.caption("Sube tu LISTA DE CÓDIGO (tabla de lectura) y tu archivo de dosis. Cruce + VALOR−CONTROL + exportación.")

# ===================== Lectores / Normalizadores =====================
def leer_lista_codigo_archivo(upload) -> Optional[pd.DataFrame]:
    """
    Lee LISTA DE CÓDIGO desde CSV/XLS/XLSX y devuelve un DataFrame
    con nombres estándar para el pipeline.
    """
    if not upload:
        return None

    name = (upload.name or "").lower()
    if name.endswith((".xlsx", ".xls")):
        df = pd.read_excel(upload, sheet_name=0)
    else:
        # Intenta autodetectar separador
        try:
            df = pd.read_csv(upload, sep=None, engine="python")
        except Exception:
            upload.seek(0)
            df = pd.read_csv(upload)

    # Normaliza encabezados a minúsculas y con espacios simples
    norm = (df.columns.astype(str)
            .str.strip().str.lower()
            .str.replace(r"\s+", " ", regex=True))
    df.columns = norm

    # Mapeo flexible: (target -> posibles nombres en el archivo)
    candidates = {
        "cédula":             ["cédula","cedula","id","documento","ced"],
        "código usuario":     ["código usuario","codigo usuario","codigo_usuario","codigo de usuario"],
        "nombre":             ["nombre","nombres"],
        "apellido":           ["apellido","apellidos"],
        "fecha de nacimiento":["fecha de nacimiento","f. nacimiento","fecha nacimiento"],
        "cliente":            ["cliente","compañía","compania","empresa"],
        "código_cliente":     ["código cliente","codigo cliente","codigo_cliente","id cliente"],
        "etiqueta":           ["etiqueta","tag","label"],
        # Aceptamos varias formas, incl. 'dosimeter' del laboratorio:
        "código_dosímetro":   ["código dosímetro","codigo dosimetro","codigo_dosimetro","dosímetro","dosimetro","dosimeter","codigo"],
        "periodo de lectura": ["periodo de lectura","período de lectura","periodo","período","periodo lectura","lectura periodo"],
        "tipo de dosímetro":  ["tipo de dosímetro","tipo dosimetro","tipo_dosimetro","tipo"],
    }

    out = pd.DataFrame()
    for target, opts in candidates.items():
        found = next((opt for opt in opts if opt in df.columns), None)
        out[target.upper()] = df[found] if found is not None else ""

    return out

def normalize_lista_codigo(df: pd.DataFrame) -> pd.DataFrame:
    """
    Asegura columnas y crea derivados:
    - NOMBRE_COMPLETO
    - CODIGO (CÓDIGO_DOSÍMETRO normalizado)
    - PERIODO_NORM (mayúsculas, espacios simples, sin puntos al final)
    - CONTROL_FLAG (si la fila es control)
    """
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
    """
    Lee archivo de dosis con columnas (dosimeter, hp10dose, hp0.07dose, hp3dose, timestamp opcional).
    Acepta CSV/XLS/XLSX y nombres alternativos.
    """
    if not upload:
        return None

    name = (upload.name or "").lower()
    if name.endswith(".csv"):
        try:
            df = pd.read_csv(upload, delimiter=';', engine='python')
        except Exception:
            upload.seek(0)
            df = pd.read_csv(upload)
    else:
        df = pd.read_excel(upload)

    # Normaliza encabezados compactos
    norm = (df.columns.astype(str).str.strip().str.lower()
            .str.replace(' ', '', regex=False)
            .str.replace('(', '').str.replace(')', '')
            .str.replace('.', '', regex=False))
    df.columns = norm

    # Mapear campos comunes
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

    for k in ['hp10dose','hp0.07dose','hp3dose']:
        if k in df.columns:
            df[k] = pd.to_numeric(df[k], errors='coerce').fillna(0.0)
        else:
            df[k] = 0.0

    if 'timestamp' in df.columns:
        df['timestamp'] = pd.to_datetime(df['timestamp'], errors='coerce')

    if 'dosimeter' in df.columns:
        df['dosimeter'] = df['dosimeter'].astype(str).str.strip().str.upper()

    return df

# ===================== Lógica de VALOR − CONTROL =====================
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
    """
    El primer registro debe ser CONTROL. Resta su Hp a los demás.
    Si la diferencia < 0.005 → 'PM'.
    """
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

# ===================== UI: Subidas =====================
st.markdown("### 1) Subir **LISTA DE CÓDIGO** (tabla de lectura)")
upl_lista = st.file_uploader("Selecciona CSV/XLS/XLSX", type=["csv","xls","xlsx"], key="upl_lista")
df_lista_raw = leer_lista_codigo_archivo(upl_lista) if upl_lista else None

if df_lista_raw is not None:
    st.success(f"Lista de código cargada: {len(df_lista_raw)} fila(s)")
    st.dataframe(df_lista_raw.head(20), use_container_width=True)

    # Normaliza lista
    df_lista = normalize_lista_codigo(df_lista_raw)

    # Selector multi-periodo (vacío = todos)
    periodos = sorted([p for p in df_lista["PERIODO_NORM"].dropna().astype(str).unique() if p.strip() != ""])
    st.markdown("#### Filtrar por PERIODO DE LECTURA (elige uno o varios; en blanco = TODOS)")
    periods_sel = st.multiselect("PERIODO DE LECTURA", options=periodos, default=[])

    df_lista_f = df_lista[df_lista["PERIODO_NORM"].isin(periods_sel)] if periods_sel else df_lista.copy()

    with st.expander("Resumen de periodos detectados"):
        st.write(df_lista.groupby("PERIODO_NORM").size().sort_values(ascending=False))

    st.markdown("### 2) Subir **Archivo de Dosis**")
    upl_dosis = st.file_uploader("Selecciona CSV/XLS/XLSX (dosis)", type=["csv","xls","xlsx"], key="upl_dosis")
    df_dosis = leer_dosis(upl_dosis) if upl_dosis else None
    if df_dosis is not None:
        st.success(f"Dosis cargadas: {len(df_dosis)} fila(s)")
        st.dataframe(df_dosis.head(20), use_container_width=True)

    # ============ Procesar ============
    c1, c2 = st.columns([1,1])
    with c1:
        nombre_out = st.text_input("Nombre archivo (sin extensión)", value=f"ReporteDosimetria_{datetime.now().strftime('%Y-%m-%d')}")
    with c2:
        btn_proc = st.button("✅ Procesar", type="primary", use_container_width=True)

    def construir_registros(df_lista_use: pd.DataFrame, df_dosis_use: pd.DataFrame) -> List[Dict[str,Any]]:
        if df_lista_use.empty or df_dosis_use is None or df_dosis_use.empty:
            return []
        # Índice por dosímetro del archivo de dosis
        idx = df_dosis_use.set_index("dosimeter")
        registros, miss = [], []

        # Control primero
        base = pd.concat([df_lista_use[df_lista_use["CONTROL_FLAG"]],
                          df_lista_use[~df_lista_use["CONTROL_FLAG"]]], ignore_index=True)

        for _, r in base.iterrows():
            cod = r["CODIGO"]
            if not cod or cod.lower() == "nan":
                continue
            if cod not in idx.index:
                miss.append(cod)
                continue

            d = idx.loc[cod]
            if isinstance(d, pd.DataFrame):
                # Si hay varias lecturas, toma la última por timestamp (si existe)
                if "timestamp" in d.columns:
                    d = d.sort_values(by="timestamp").iloc[-1]
                else:
                    d = d.iloc[-1]

            # Formato fecha lectura (opcional)
            fecha_str = ""
            if "timestamp" in d and pd.notna(d["timestamp"]):
                try:
                    fecha_str = pd.to_datetime(d["timestamp"]).strftime("%d/%m/%Y %H:%M")
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
                "Hp(10)": float(d.get("hp10dose", 0.0) or 0.0),
                "Hp(0.07)": float(d.get("hp0.07dose", 0.0) or 0.0),
                "Hp(3)": float(d.get("hp3dose", 0.0) or 0.0),
            })

        # Debug opcional
        if st.checkbox("📎 Mostrar debug de códigos no coincidentes"):
            st.write("Códigos en dosis NO encontrados en LISTA:", sorted(set(idx.index) - set(base["CODIGO"])))
            st.write("Códigos en LISTA sin dosis:", sorted(set(miss)))

        # Orden: CONTROL primero
        registros.sort(key=lambda x: (x.get("NOMBRE","").strip().upper() != "CONTROL", x.get("NOMBRE","")))
        return registros

    if btn_proc:
        if df_lista_f.empty:
            st.error("No hay filas en LISTA DE CÓDIGO (tras el filtro).")
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
                        df_final['PERIODO DE LECTURA'].astype(str)
                        .str.replace(r'\.+$', '', regex=True).str.strip()
                    )
                    if not df_final.empty:
                        df_final.loc[df_final.index.min(), 'NOMBRE'] = 'CONTROL'
                        df_final['NOMBRE'] = (
                            df_final['NOMBRE'].astype(str)
                            .str.replace(r'\.+$', '', regex=True).str.strip()
                        )

                    st.success(f"¡Listo! Registros generados: {len(df_final)}")
                    st.dataframe(df_final, use_container_width=True)
                    st.session_state["df_final"] = df_final

                    # ===================== Descargar Excel =====================
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
