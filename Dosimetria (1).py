def consolidar_para_upload(df_vista: pd.DataFrame, df_num: pd.DataFrame, umbral_pm: float = 0.005) -> pd.DataFrame:
    """
    Consolida para subir a Ninox.
    - PERSONAS: suma por periodo/usuario de las dosis corregidas.
    - CONTROL: promedio por periodo de las filas de control,
      conservando CÓDIGO DE USUARIO y CÉDULA si existen (último no vacío).
      Si el CÓDIGO DE USUARIO del control está vacío, se rellena con:
         1) CÓDIGO DE DOSÍMETRO del control; si no hay,
         2) 'CONTROL'.
    """
    if df_vista is None or df_vista.empty or df_num is None or df_num.empty:
        return pd.DataFrame()

    # ============== PERSONAS (no control) ==============
    personas_num = df_num[df_num["NOMBRE"].astype(str).str.upper().str.startswith("CONTROL") == False].copy()
    per_consol = pd.DataFrame()
    if not personas_num.empty:
        per_consol = personas_num.groupby(["PERIODO DE LECTURA","CÓDIGO DE USUARIO"], as_index=False).agg({
            "CLIENTE":"last",
            "NOMBRE":"last",
            "CÉDULA":"last",
            "CÓDIGO DE DOSÍMETRO":"last",
            "TIPO DE DOSÍMETRO":"last",
            "FECHA DE LECTURA":"last",
            "_Hp10_NUM":"sum",
            "_Hp007_NUM":"sum",
            "_Hp3_NUM":"sum"
        }).rename(columns={
            "_Hp10_NUM":"Hp (10)",
            "_Hp007_NUM":"Hp (0.07)",
            "_Hp3_NUM":"Hp (3)"
        })

    # ============== CONTROL ==============
    control_v = df_vista[df_vista["NOMBRE"].astype(str).str.upper().str.startswith("CONTROL")].copy()
    ctrl_consol = pd.DataFrame()
    if not control_v.empty:
        # Convertir Hp de control (que en df_vista vienen ya formateados) a num para poder promediar
        for h in ["Hp (10)","Hp (0.07)","Hp (3)"]:
            control_v[h] = control_v[h].apply(hp_to_num)

        # último no-vacío helper
        def _last_nonempty(series: pd.Series) -> str:
            for v in series.iloc[::-1]:
                s = str(v).strip()
                if s:
                    return s
            return ""

        ctrl_consol = control_v.groupby(["PERIODO DE LECTURA"], as_index=False).agg({
            "CLIENTE":"last",
            "CÓDIGO DE DOSÍMETRO":"first",
            "CÓDIGO DE USUARIO": _last_nonempty,     # << conservar si existe
            "CÉDULA": _last_nonempty,                # idem
            "TIPO DE DOSÍMETRO":"last",
            "FECHA DE LECTURA":"last",
            "Hp (10)":"mean",
            "Hp (0.07)":"mean",
            "Hp (3)":"mean"
        })

        # Si CÓDIGO DE USUARIO quedó vacío, rellenar con CÓDIGO DE DOSÍMETRO; si tampoco hay, poner 'CONTROL'
        def _fill_usercode(row):
            cu = str(row.get("CÓDIGO DE USUARIO","") or "").strip()
            if cu:
                return cu
            cd = str(row.get("CÓDIGO DE DOSÍMETRO","") or "").strip()
            return cd if cd else "CONTROL"

        ctrl_consol["CÓDIGO DE USUARIO"] = ctrl_consol.apply(_fill_usercode, axis=1)
        ctrl_consol["NOMBRE"] = "CONTROL"

    # ============== UNIÓN Y FORMATO ==============
    out = pd.concat([ctrl_consol, per_consol], ignore_index=True, sort=False)
    if out.empty:
        return out

    def _fmt(v: float) -> str:
        v = float(v or 0.0)
        return "PM" if v < umbral_pm else f"{v:.2f}"

    for h in ["Hp (10)","Hp (0.07)","Hp (3)"]:
        out[h] = out[h].map(_fmt)

    orden_pref = [
        "PERIODO DE LECTURA","CLIENTE","CÓDIGO DE DOSÍMETRO","CÓDIGO DE USUARIO","NOMBRE",
        "CÉDULA","FECHA DE LECTURA","TIPO DE DOSÍMETRO",
        "Hp (10)","Hp (0.07)","Hp (3)"
    ]
    cols = [c for c in orden_pref if c in out.columns] + [c for c in out.columns if c not in orden_pref]
    out = out[cols]

    sort_keys = [c for c in ["PERIODO DE LECTURA","NOMBRE","CÓDIGO DE USUARIO","CÓDIGO DE DOSÍMETRO"] if c in out.columns]
    out = out.sort_values(sort_keys).reset_index(drop=True)
    return out
