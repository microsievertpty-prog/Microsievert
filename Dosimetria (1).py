# -*- coding: utf-8 -*-
from typing import Optional
from io import BytesIO
from datetime import datetime

import pandas as pd
from openpyxl import Workbook
from openpyxl.styles import Alignment, Font, PatternFill, Border, Side
from openpyxl.drawing.image import Image as XLImage


def build_excel_like_example(
    df_reporte: pd.DataFrame,
    fecha_emision: Optional[str] = None,
    cliente: Optional[str] = None,
    codigo_reporte: Optional[str] = None,
    logo_bytes: Optional[bytes] = None,
) -> bytes:
    """
    Genera el Excel del Reporte Final:
    - Cabecera sin duplicados (bloques agrupados y subcabeceras).
    - Tabla con estilos y bordes.
    - Logo opcional (arriba-izquierda).
    - Bloque informativo debajo de la tabla, con textos y cajas.

    Retorna: bytes del archivo .xlsx
    """

    # ------------------------ estilos reutilizables ------------------------
    THIN = Side(style="thin", color="000000")
    BORDER_ALL = Border(left=THIN, right=THIN, top=THIN, bottom=THIN)

    H_CENTER = Alignment(horizontal="center", vertical="center", wrap_text=True)
    H_LEFT   = Alignment(horizontal="left",   vertical="center", wrap_text=True)

    FONT_TITLE   = Font(bold=True, size=14, color="000000")
    FONT_HEADER  = Font(bold=True, size=11, color="000000")
    FONT_TEXT    = Font(size=10, color="000000")

    FILL_HEADER  = PatternFill("solid", fgColor="DDDDDD")
    FILL_LIGHT   = PatternFill("solid", fgColor="F2F2F2")

    def _box(ws, r0, c0, r1, c1, *, header=False, fill=None):
        for r in range(r0, r1 + 1):
            for c in range(c0, c1 + 1):
                cell = ws.cell(r, c)
                cell.border = BORDER_ALL
                cell.alignment = H_CENTER if header else H_LEFT
                if fill is not None:
                    cell.fill = fill
                if header:
                    cell.font = FONT_HEADER
                else:
                    if not cell.font or not cell.font.bold:
                        cell.font = FONT_TEXT

    # ------------------------ workbook & hoja ------------------------
    wb = Workbook()
    ws = wb.active
    ws.title = "REPORTE"

    # Anchos de columnas (A..O -> 15 columnas)
    widths = [16, 16, 30, 18, 20, 18, 10, 10, 10, 10, 10, 10, 10, 10, 10]
    for col, w in enumerate(widths, start=1):
        ws.column_dimensions[chr(ord("A") + col - 1)].width = w

    row = 1

    # ------------------------ Encabezado superior (logo + datos) ------------------------
    # Logo
    if logo_bytes:
        try:
            img = XLImage(BytesIO(logo_bytes))
            img.width = 140
            img.height = 140
            ws.add_image(img, "A1")
        except Exception:
            pass

    # Datos empresa (como en tu ejemplo)
    ws.merge_cells(start_row=row, start_column=1, end_row=row, end_column=7)
    ws.cell(row, 1, "MICROSIEVERT, S.A.").font = Font(bold=True, size=12)
    row += 1
    ws.merge_cells(start_row=row, start_column=1, end_row=row, end_column=7); ws.cell(row,1,"PH Conardo")
    row += 1
    ws.merge_cells(start_row=row, start_column=1, end_row=row, end_column=7); ws.cell(row,1,"Calle 41 Este, Panamá")
    row += 1
    ws.merge_cells(start_row=row, start_column=1, end_row=row, end_column=7); ws.cell(row,1,"PANAMÁ")
    row += 2

    # Fecha, Cliente, Código (a la derecha)
    fecha_emision = fecha_emision or datetime.today().strftime("%d/%m/%Y")
    ws.merge_cells(start_row=row, start_column=11, end_row=row, end_column=12); ws.cell(row,11,"Fecha de emisión").alignment = H_CENTER
    ws.merge_cells(start_row=row, start_column=13, end_row=row, end_column=15); ws.cell(row,13, fecha_emision).alignment = H_CENTER
    row += 1
    ws.merge_cells(start_row=row, start_column=11, end_row=row, end_column=12); ws.cell(row,11,"Cliente").alignment = H_CENTER
    ws.merge_cells(start_row=row, start_column=13, end_row=row, end_column=15); ws.cell(row,13, (cliente or "")).alignment = H_CENTER
    row += 1
    ws.merge_cells(start_row=row, start_column=11, end_row=row, end_column=12); ws.cell(row,11,"Código").alignment = H_CENTER
    ws.merge_cells(start_row=row, start_column=13, end_row=row, end_column=15); ws.cell(row,13, (codigo_reporte or "SIN-CÓDIGO")).alignment = H_CENTER
    row += 2

    # ------------------------ Título de reporte ------------------------
    ws.merge_cells(start_row=row, start_column=1, end_row=row, end_column=15)
    t = ws.cell(row, 1, "REPORTE DE DOSIMETRÍA")
    t.font = FONT_TITLE
    t.alignment = H_CENTER
    row += 1

    # ------------------------ Cabecera de la tabla (sin duplicados) ------------------------
    # Fila 1 de cabecera (solo bloques agrupados)
    _box(ws, row, 1, row, 15, header=True, fill=FILL_LIGHT)

    ws.merge_cells(start_row=row, start_column=7,  end_row=row, end_column=9)
    ws.cell(row,7,  "DOSIS EN MILISIEVERT (mSv) — DOSIS").alignment = H_CENTER

    ws.merge_cells(start_row=row, start_column=10, end_row=row, end_column=12)
    ws.cell(row,10, "DOSIS ANUAL").alignment = H_CENTER

    ws.merge_cells(start_row=row, start_column=13, end_row=row, end_column=15)
    ws.cell(row,13, "DOSIS DE POR VIDA").alignment = H_CENTER

    row += 1

    # Fila 2 de cabecera (nombres de columnas)
    headers = [
        "PERIODO DE LECTURA", "CÓDIGO DE USUARIO", "NOMBRE", "CÉDULA", "FECHA DE LECTURA",
        "TIPO DE DOSÍMETRO",
        "Hp(10)", "Hp(0.07)", "Hp(3)",
        "Hp(10)", "Hp(0.07)", "Hp(3)",
        "Hp(10)", "Hp(0.07)", "Hp(3)"
    ]
    for c, h in enumerate(headers, start=1):
        cell = ws.cell(row, c, h)
        cell.alignment = H_CENTER
        cell.font = FONT_HEADER
        cell.fill = FILL_HEADER
        cell.border = BORDER_ALL

    # ------------------------ Datos ------------------------
    start_data_row = row + 1
    if not df_reporte.empty:
        # Asegura orden y existencia de columnas esperadas
        expected_cols = [
            "PERIODO DE LECTURA","CÓDIGO DE USUARIO","NOMBRE","CÉDULA","FECHA DE LECTURA","TIPO DE DOSÍMETRO",
            "Hp (10)","Hp (0.07)","Hp (3)",
            "Hp (10) ANUAL","Hp (0.07) ANUAL","Hp (3) ANUAL",
            "Hp (10) DE POR VIDA","Hp (0.07) DE POR VIDA","Hp (3) DE POR VIDA"
        ]
        data_cols = []
        for col in expected_cols:
            if col in df_reporte.columns:
                data_cols.append(col)
            else:
                # columna faltante -> crea vacía
                df_reporte[col] = ""
                data_cols.append(col)

        view = df_reporte[data_cols].copy()

        for i, (_, r) in enumerate(view.iterrows(), start=start_data_row):
            values = [
                r["PERIODO DE LECTURA"], r["CÓDIGO DE USUARIO"], r["NOMBRE"], r["CÉDULA"],
                r["FECHA DE LECTURA"], r["TIPO DE DOSÍMETRO"],
                r["Hp (10)"], r["Hp (0.07)"], r["Hp (3)"],
                r["Hp (10) ANUAL"], r["Hp (0.07) ANUAL"], r["Hp (3) ANUAL"],
                r["Hp (10) DE POR VIDA"], r["Hp (0.07) DE POR VIDA"], r["Hp (3) DE POR VIDA"],
            ]
            for c, v in enumerate(values, start=1):
                cell = ws.cell(i, c, v)
                cell.alignment = H_CENTER if c >= 7 else H_LEFT
                cell.border = BORDER_ALL
                cell.font = FONT_TEXT

        last_data_row = start_data_row + len(view) - 1
    else:
        last_data_row = start_data_row - 1  # no rows

    row = last_data_row + 2  # espacio después de la tabla

    # ------------------------ BLOQUE INFORMATIVO (debajo de la tabla) ------------------------
    # Título del bloque
    ws.merge_cells(start_row=row, start_column=1, end_row=row, end_column=15)
    ws.cell(row,1,"INFORMACIÓN DEL REPORTE DE DOSIMETRÍA").font = FONT_HEADER
    ws.cell(row,1).alignment = H_CENTER
    row += 2

    # 1) Periodo de lectura
    ws.merge_cells(start_row=row, start_column=1, end_row=row, end_column=15)
    ws.cell(row,1,"– Periodo de lectura: periodo de uso del dosímetro personal.")
    _box(ws, row, 1, row, 15)
    row += 2

    # 2) Fecha de lectura
    ws.merge_cells(start_row=row, start_column=1, end_row=row, end_column=15)
    ws.cell(row,1,"– Fecha de lectura: corresponde a la fecha en que fue realizada la lectura del dosímetro.")
    _box(ws, row, 1, row, 15)
    row += 2

    # 3) Tipo de dosímetro (caja con equivalencias + límites anuales a la derecha)
    # Título
    ws.merge_cells(start_row=row, start_column=1, end_row=row, end_column=15)
    ws.cell(row,1,"– Tipo de dosímetro:")
    _box(ws, row, 1, row, 15)
    row += 2

    # Caja izquierda: equivalencias
    left_r0 = row
    ws.merge_cells(start_row=row, start_column=2, end_row=row, end_column=6)
    ws.cell(row,2,"CE = Cuerpo Entero").alignment = H_CENTER; _box(ws, row, 2, row, 6)
    row += 1
    ws.merge_cells(start_row=row, start_column=2, end_row=row, end_column=6)
    ws.cell(row,2,"A = Anillo").alignment = H_CENTER; _box(ws, row, 2, row, 6)
    row += 1
    ws.merge_cells(start_row=row, start_column=2, end_row=row, end_column=6)
    ws.cell(row,2,"B = Brazalete").alignment = H_CENTER; _box(ws, row, 2, row, 6)
    row += 1
    ws.merge_cells(start_row=row, start_column=2, end_row=row, end_column=6)
    ws.cell(row,2,"CR = Cristalino").alignment = H_CENTER; _box(ws, row, 2, row, 6)
    left_r1 = row
    # Marco de la caja
    _box(ws, left_r0, 2, left_r1, 6)

    # Caja derecha: límites anuales
    row = left_r0
    ws.merge_cells(start_row=row, start_column=8, end_row=row, end_column=15)
    ws.cell(row,8,"LÍMITES ANUALES DE EXPOSICIÓN A RADIACIONES").alignment = H_CENTER
    _box(ws, row, 8, row, 15, header=True, fill=FILL_LIGHT)
    row += 1

    limites = [
        ("Cuerpo Entero", "20mSv/año"),
        ("Cristalino", "150 mSv/año"),
        ("Extremidades y piel", "500 mSv/año"),
        ("Fetal", "1 mSv/periodo de gestación"),
        ("Público", "1 mSv/año"),
    ]
    for nom, val in limites:
        ws.merge_cells(start_row=row, start_column=8, end_row=row, end_column=11)
        ws.cell(row,8, nom).alignment = H_CENTER
        ws.merge_cells(start_row=row, start_column=12, end_row=row, end_column=15)
        ws.cell(row,12, val).alignment = H_CENTER
        _box(ws, row, 8, row, 15)
        row += 1

    row = left_r1 + 2

    # 4) Datos del participante (lista de bullet points)
    ws.merge_cells(start_row=row, start_column=1, end_row=row, end_column=15)
    ws.cell(row,1,"– DATOS DEL PARTICIPANTE:"); _box(ws, row, 1, row, 15, header=True, fill=FILL_LIGHT)
    row += 1

    bullets = [
        "- Código de usuario: Número único asignado al usuario por Microsievert, S.A.",
        "- Nombre: Persona a la cual se le asigna el dosímetro personal.",
        "- Cédula: Número del documento de identidad personal del usuario.",
        "- Fecha de nacimiento: Registro de la fecha de nacimiento del usuario."
    ]
    for b in bullets:
        ws.merge_cells(start_row=row, start_column=1, end_row=row, end_column=15)
        ws.cell(row,1,b); _box(ws, row, 1, row, 15)
        row += 1

    row += 1

    # 5) Dosis en mSv (tabla: Nombre/Definición/Unidad)
    ws.merge_cells(start_row=row, start_column=1, end_row=row, end_column=15)
    ws.cell(row,1,"– DOSIS EN MILISIEVERT:"); _box(ws, row, 1, row, 15, header=True, fill=FILL_LIGHT)
    row += 2

    # cabecera local
    ws.merge_cells(start_row=row, start_column=2, end_row=row, end_column=6);  ws.cell(row,2,"Nombre").alignment = H_CENTER
    ws.merge_cells(start_row=row, start_column=7, end_row=row, end_column=13); ws.cell(row,7,"Definición").alignment = H_CENTER
    ws.merge_cells(start_row=row, start_column=14, end_row=row, end_column=15); ws.cell(row,14,"Unidad").alignment = H_CENTER
    _box(ws, row, 2, row, 15, header=True, fill=FILL_HEADER)
    row += 1

    dosis_tabla = [
        ("Dosis efectiva",
         "Es la dosis equivalente en tejido blando, J·kg-1 ó Sv a una profundidad de 10 mm, bajo determinado punto", "mSv"),
        ("Dosis equivalente superficial",
         "Es la dosis equivalente en tejido blando, J·kg-1 ó Sv a una profundidad de 0,07 mm, bajo determinado punto", "mSv"),
        ("Dosis equivalente a cristalino",
         "Es la dosis equivalente en tejido blando, J·kg-1 ó Sv a una profundidad de 3 mm, bajo determinado punto del", "mSv"),
    ]
    for n, d, u in dosis_tabla:
        ws.merge_cells(start_row=row, start_column=2, end_row=row, end_column=6);  ws.cell(row,2,n)
        ws.merge_cells(start_row=row, start_column=7, end_row=row, end_column=13); ws.cell(row,7,d)
        ws.merge_cells(start_row=row, start_column=14, end_row=row, end_column=15); ws.cell(row,14,u).alignment = H_CENTER
        _box(ws, row, 2, row, 15)
        row += 1

    row += 1

    # 6) Lecturas de anillo + periodos
    ws.merge_cells(start_row=row, start_column=1, end_row=row, end_column=15)
    ws.cell(row,1,"LECTURAS DE ANILLO: las lecturas del dosímetro de anillo son registradas como una dosis equivalente superficial Hp(0.7)")
    _box(ws, row, 1, row, 15)
    row += 2

    ws.merge_cells(start_row=row, start_column=1, end_row=row, end_column=15)
    ws.cell(row,1,"Los resultados de las dosis individuales de radiación son reportados para diferentes periodos de tiempo:")
    _box(ws, row, 1, row, 15)
    row += 2

    # mini tabla de periodos
    ws.merge_cells(start_row=row, start_column=2, end_row=row, end_column=6);  ws.cell(row,2,"DOSIS ACTUAL").alignment = H_CENTER
    ws.merge_cells(start_row=row, start_column=7, end_row=row, end_column=15); ws.cell(row,7,"Es el correspondiente de dosis acumulada durante el periodo de lectura definido.")
    _box(ws, row, 2, row, 15); row += 1

    ws.merge_cells(start_row=row, start_column=2, end_row=row, end_column=6);  ws.cell(row,2,"DOSIS ANUAL").alignment = H_CENTER
    ws.merge_cells(start_row=row, start_column=7, end_row=row, end_column=15); ws.cell(row,7,"Es el correspondiente de dosis acumulada desde el inicio del año hasta la fecha.")
    _box(ws, row, 2, row, 15); row += 1

    ws.merge_cells(start_row=row, start_column=2, end_row=row, end_column=6);  ws.cell(row,2,"DOSIS DE POR VIDA").alignment = H_CENTER
    ws.merge_cells(start_row=row, start_column=7, end_row=row, end_column=15); ws.cell(row,7,"Es el correspondiente de DOSIS acumulada desde el inicio del servicio dosimétrico hasta la fecha.")
    _box(ws, row, 2, row, 15); row += 2

    # 7) Control y PM
    ws.merge_cells(start_row=row, start_column=1, end_row=row, end_column=15)
    ws.cell(row,1,"DOSÍMETRO DE CONTROL: incluido en cada paquete entregado para monitorear la exposición a la radiación recibida durante el tránsito y almacenamiento. Este dosímetro")
    _box(ws, row, 1, row, 15); row += 2

    ws.merge_cells(start_row=row, start_column=1, end_row=row, end_column=15)
    ws.cell(row,1,'POR DEBAJO DEL MÍNIMO DETECTADO: es la dosis por debajo de la cantidad mínima reportada para el período de uso y son registradas como "PM".')
    _box(ws, row, 1, row, 15); row += 1

    # ------------------------ guardar a bytes ------------------------
    bio = BytesIO()
    wb.save(bio)
    return bio.getvalue()

