# -*- coding: utf-8 -*-
"""
UTPL - Administración de Empresas
Registro y Edición de Horarios (Streamlit) + Dashboard + Backups + Franjas por día

Cambios clave en esta versión:
- Sábado deshabilitado por defecto. SOLO se habilita si en docentes.xlsx 'dias_permitidos' incluye "Sábado".
  (No hay auto-inclusión por tipo, ni por columnas sabado_ini/fin.)
- Conflictos:
  * Global: SOLO se validan cruces de SINCRONÍA entre ASIGNATURAS DISTINTAS en el mismo ciclo y día.
    (Sincronías de la MISMA asignatura sí pueden solaparse, p.ej. paralelos distintos.)
  * Self: para un MISMO docente, NO se permiten cruces/solapes de NINGÚN tipo (ni SINC ni TUT) con sus propios registros,
    en el mismo día (independientemente del ciclo o asignatura). Se valida al sugerir y al guardar/editar.
- Sugerencias (sincronía/tutoría) ya filtran conflictos propios del docente.
- Backups automáticos al registrar/editar (retención configurable).
- DATA_DIR configurable por variable de entorno (para Render). Escrituras con FileLock para evitar corrupción.
"""

import os
import io
import re
import uuid
import shutil
import unicodedata
from datetime import datetime, timedelta, datetime as _dt

import numpy as np
import pandas as pd
import streamlit as st
from filelock import FileLock, Timeout

# =========================
# CONSTANTES / RUTAS
# =========================
DATA_DIR = os.getenv("DATA_DIR", "data")  # en Render configura DATA_DIR=/var/data
DOCENTES_XLSX = os.path.join(DATA_DIR, "docentes.xlsx")
DOCENTES_SHEET = "docentes"
MASTER_XLSX = os.path.join(DATA_DIR, "horarios_master.xlsx")
MASTER_SHEET = "horarios"

BACKUP_DIR = os.path.join(DATA_DIR, "backups")
LOCK_PATH = os.path.join(DATA_DIR, ".master.lock")  # lock para escrituras concurrentes
MAX_BACKUPS = 60  # retención de copias

# --- ADMIN / UPLOADER ---
ADMIN_PIN = os.getenv("ADMIN_PIN", "1234")  # define ADMIN_PIN en el entorno en producción
DOCENTES_LOCK_PATH = os.path.join(DATA_DIR, ".docentes.lock")  # lock para docentes.xlsx

TIME_FMT = "%H:%M"
STEP_MIN = 60         # Intervalo base: 60 minutos
SYNC_SLOT_MIN = 60    # sincronía = 1 hora
TUTOR_SLOT_MIN = 120  # tutoría = 2 horas

DAYS_FULL = ["Lunes","Martes","Miércoles","Jueves","Viernes","Sábado","Domingo"]
WEEKDAYS = ["Lunes","Martes","Miércoles","Jueves","Viernes"]

# Mapeo columnas por día (si existen en el Excel)
DAY_COLS = {
    "Lunes": ("lunes_ini", "lunes_fin"),
    "Martes": ("martes_ini", "martes_fin"),
    "Miércoles": ("miercoles_ini", "miercoles_fin"),
    "Jueves": ("jueves_ini", "jueves_fin"),
    "Viernes": ("viernes_ini", "viernes_fin"),
    "Sábado": ("sabado_ini", "sabado_fin"),
    "Domingo": ("domingo_ini", "domingo_fin"),
}

# =========================
# NORMALIZACIÓN / TIEMPO (ROBUSTO)
# =========================
def normalize_key(s: str) -> str:
    if s is None:
        return ""
    s = str(s)
    s = unicodedata.normalize("NFKD", s)
    s = "".join(ch for ch in s if not unicodedata.combining(ch))
    s = s.replace("\u200b", " ")
    s = re.sub(r"\s+", " ", s).strip().lower()
    return s

# Aliases para tolerar faltas de tildes/variantes en dias_permitidos
DAY_ALIASES = {
    "lunes":"Lunes",
    "martes":"Martes",
    "miercoles":"Miércoles", "miércoles":"Miércoles",
    "jueves":"Jueves",
    "viernes":"Viernes",
    "sabado":"Sábado", "sábado":"Sábado",
    "domingo":"Domingo"
}

def normalize_day_token(tok: str) -> str:
    base = normalize_key(tok).replace("-", " ").strip()
    return DAY_ALIASES.get(base, tok.strip())

HHMM_RE = re.compile(r"^[0-2]\d:[0-5]\d$")  # validación básica HH:MM (24h)

def is_hhmm(s: str) -> bool:
    return isinstance(s, str) and bool(HHMM_RE.match(s or ""))

def _t(hhmm: str) -> _dt:
    return _dt.strptime(hhmm, TIME_FMT)

def _t_safe(hhmm: str) -> _dt | None:
    if not is_hhmm(hhmm):
        return None
    return _dt.strptime(hhmm, TIME_FMT)

def inside_interval(hhmm: str, start_str: str, end_str: str) -> bool:
    t = _t_safe(hhmm); s = _t_safe(start_str); e = _t_safe(end_str)
    if t is None or s is None or e is None:
        return False
    return s <= t < e

def overlaps(a_start, a_end, b_start, b_end) -> bool:
    As = _t_safe(a_start); Ae = _t_safe(a_end); Bs = _t_safe(b_start); Be = _t_safe(b_end)
    if None in (As, Ae, Bs, Be):
        return False
    return As < Be and Bs < Ae

def _intersect(a_ini, a_fin, b_ini, b_fin):
    As = _t_safe(a_ini); Ae = _t_safe(a_fin); Bs = _t_safe(b_ini); Be = _t_safe(b_fin)
    if None in (As, Ae, Bs, Be):
        return (False, "00:00", "00:00")
    ini = max(As, Bs); fin = min(Ae, Be)
    return (ini < fin, ini.strftime(TIME_FMT), fin.strftime(TIME_FMT))

def time_range(start_str, end_str, step_min=STEP_MIN):
    s = _t_safe(start_str); e = _t_safe(end_str)
    if s is None or e is None or s >= e:
        return []
    slots, t = [], s
    while t + timedelta(minutes=step_min) <= e:
        slots.append(t.strftime(TIME_FMT))
        t += timedelta(minutes=step_min)
    return slots

def label_block(docente, asignatura, paralelo_codigo, tipo):
    short = "SINC" if tipo == "SINC" else "TUT"
    return f"{short} · {asignatura} ({paralelo_codigo}) · {docente}"

# =========================
# ARCHIVOS + BACKUPS (CON LOCK)
# =========================
def ensure_data_dir():
    os.makedirs(DATA_DIR, exist_ok=True)
    os.makedirs(BACKUP_DIR, exist_ok=True)

def create_docentes_template_if_missing():
    if not os.path.exists(DOCENTES_XLSX):
        ensure_data_dir()
        cols = [
            "docente","tipo_docente","asignatura","paralelo_codigo","ciclo",
            "dias_permitidos","franja_inicio","franja_fin",
            "lunes_ini","lunes_fin","martes_ini","martes_fin","miercoles_ini","miercoles_fin",
            "jueves_ini","jueves_fin","viernes_ini","viernes_fin",
            "sabado_ini","sabado_fin","domingo_ini","domingo_fin"
        ]
        df = pd.DataFrame(columns=cols)
        with pd.ExcelWriter(DOCENTES_XLSX, engine="openpyxl") as writer:
            df.to_excel(writer, index=False, sheet_name=DOCENTES_SHEET)

def _ensure_row_ids(dfm: pd.DataFrame) -> pd.DataFrame:
    if "row_id" not in dfm.columns:
        dfm["row_id"] = None
    needs = dfm["row_id"].isna() | (dfm["row_id"].astype(str).str.len() == 0)
    if needs.any():
        dfm.loc[needs, "row_id"] = [str(uuid.uuid4()) for _ in range(needs.sum())]
    return dfm

def create_master_if_missing():
    if not os.path.exists(MASTER_XLSX):
        ensure_data_dir()
        cols = [
            "row_id","timestamp","docente","tipo_docente",
            "asignatura","paralelo","paralelo_codigo","ciclo","dia",
            "sincronía_inicio","sincronía_fin","tutoría_inicio","tutoría_fin"
        ]
        df = pd.DataFrame(columns=cols)
        # Guardado inicial
        lock = FileLock(LOCK_PATH, timeout=10)
        try:
            with lock:
                with pd.ExcelWriter(MASTER_XLSX, engine="openpyxl") as writer:
                    df.to_excel(writer, index=False, sheet_name=MASTER_SHEET)
        except Timeout:
            st.error("Archivo en uso. Intenta nuevamente.")
            raise
        backup_master(reason="init")

def validate_docentes_df(df: pd.DataFrame) -> tuple[bool, list[str], list[str]]:
    """
    Valida el nuevo docentes.xlsx (pestaña 'docentes'):
    - Columnas obligatorias presentes
    - Formato HH:MM válido en franjas si vienen llenas
    Devuelve: (is_ok, errors, warnings)
    """
    required = [
        "docente","tipo_docente","asignatura","paralelo_codigo","ciclo",
        "dias_permitidos","franja_inicio","franja_fin"
    ]
    errors, warns = [], []

    # Columnas requeridas
    missing = [c for c in required if c not in df.columns]
    if missing:
        errors.append(f"Faltan columnas obligatorias: {', '.join(missing)}")

    # Validación horaria básica (si hay valores)
    def _check_hhmm_series(s, name):
        bad = []
        for i, v in enumerate(s.fillna("").astype(str).tolist()):
            v = v.strip()
            if v and not is_hhmm(v):
                bad.append((i+2, v))  # +2 ~ header + base 1
        if bad:
            warns.append(f"Formato no HH:MM en '{name}' (filas: {', '.join([str(r) for r,_ in bad])}).")

    for c in ["franja_inicio","franja_fin",
              "lunes_ini","lunes_fin","martes_ini","martes_fin","miercoles_ini","miercoles_fin",
              "jueves_ini","jueves_fin","viernes_ini","viernes_fin","sabado_ini","sabado_fin","domingo_ini","domingo_fin"]:
        if c in df.columns:
            _check_hhmm_series(df[c], c)

    # Ciclo numérico
    if "ciclo" in df.columns:
        try:
            pd.to_numeric(df["ciclo"])
        except Exception:
            errors.append("La columna 'ciclo' debe ser numérica (enteros).")

    return (len(errors)==0, errors, warns)

@st.cache_data(ttl=15)
def load_docentes():
    if not os.path.exists(DOCENTES_XLSX):
        create_docentes_template_if_missing()
    df = pd.read_excel(DOCENTES_XLSX, sheet_name=DOCENTES_SHEET, engine="openpyxl")

    # Limpieza robusta de strings
    str_cols = [
        "docente","tipo_docente","asignatura","paralelo_codigo","dias_permitidos",
        "franja_inicio","franja_fin",
        "lunes_ini","lunes_fin","martes_ini","martes_fin","miercoles_ini","miercoles_fin",
        "jueves_ini","jueves_fin","viernes_ini","viernes_fin",
        "sabado_ini","sabado_fin","domingo_ini","domingo_fin"
    ]
    for c in str_cols:
        if c in df.columns:
            df[c] = (
                df[c]
                .astype(str)
                .replace({"nan":"", "NaT":"", "None":"", "NONE":"", "NAN":""})
                .str.strip()
            )

    if "ciclo" in df.columns:
        df["ciclo"] = pd.to_numeric(df["ciclo"], errors="coerce").fillna(0).astype(int)
    if "docente" not in df.columns:
        df["docente"] = ""
    df["docente_key"] = df["docente"].apply(normalize_key)
    return df

@st.cache_data(ttl=5)
def load_master():
    if not os.path.exists(MASTER_XLSX):
        create_master_if_missing()
    df = pd.read_excel(MASTER_XLSX, sheet_name=MASTER_SHEET, engine="openpyxl")
    if not df.empty:
        for c in ["docente","tipo_docente","asignatura","paralelo_codigo","dia",
                  "sincronía_inicio","sincronía_fin","tutoría_inicio","tutoría_fin","row_id"]:
            if c in df.columns:
                df[c] = df[c].astype(str)
        if "ciclo" in df.columns:
            df["ciclo"] = pd.to_numeric(df["ciclo"], errors="coerce")
    # NO escribimos aquí (lectura pura). row_id se garantiza al guardar.
    return _ensure_row_ids(df)

def save_master(df_master):
    """Escritura segura con lock."""
    ensure_data_dir()
    df_master = _ensure_row_ids(df_master.copy())
    lock = FileLock(LOCK_PATH, timeout=10)
    try:
        with lock:
            with pd.ExcelWriter(MASTER_XLSX, engine="openpyxl") as writer:
                df_master.to_excel(writer, index=False, sheet_name=MASTER_SHEET)
    except Timeout:
        st.error("El archivo está en uso. Intenta en unos segundos.")
        raise

def download_excel_bytes(df):
    buf = io.BytesIO()
    with pd.ExcelWriter(buf, engine="openpyxl") as writer:
        df.to_excel(writer, index=False, sheet_name=MASTER_SHEET)
    buf.seek(0)
    return buf

# =========================
# BACKUPS
# =========================
def _prune_backups():
    try:
        files = [os.path.join(BACKUP_DIR, f) for f in os.listdir(BACKUP_DIR) if f.lower().endswith(".xlsx")]
        files = sorted(files, key=lambda p: os.path.getmtime(p), reverse=True)
        for f in files[MAX_BACKUPS:]:
            os.remove(f)
    except Exception as e:
        print(f"[WARN] prune backups: {e}")

def backup_master(reason: str = "manual"):
    try:
        ensure_data_dir()
        if not os.path.exists(MASTER_XLSX):
            return
        lock = FileLock(LOCK_PATH, timeout=10)
        with lock:
            ts = datetime.now().strftime("%Y%m%d-%H%M%S")
            base_name = f"horarios_master_{ts}_{reason}.xlsx"
            dest = os.path.join(BACKUP_DIR, base_name)
            shutil.copy2(MASTER_XLSX, dest)
        _prune_backups()
    except Exception as e:
        print(f"[ERROR] backup_master: {e}")

def backup_docentes(reason: str = "upload"):
    """Copia de seguridad del docentes.xlsx actual (si existe)."""
    try:
        ensure_data_dir()
        if not os.path.exists(DOCENTES_XLSX):
            return None
        ts = datetime.now().strftime("%Y%m%d-%H%M%S")
        base_name = f"docentes_{ts}_{reason}.xlsx"
        dest = os.path.join(BACKUP_DIR, base_name)
        shutil.copy2(DOCENTES_XLSX, dest)
        return dest
    except Exception as e:
        print(f"[ERROR] backup_docentes: {e}")
        return None

def write_docentes_atomic(df_new: pd.DataFrame):
    """Escritura segura del docentes.xlsx (sheet 'docentes') con lock + reemplazo atómico."""
    ensure_data_dir()
    tmp_path = DOCENTES_XLSX + ".tmp"
    lock = FileLock(DOCENTES_LOCK_PATH, timeout=10)
    with lock:
        # 1) Backup del archivo actual (si existía)
        backup_docentes(reason="prewrite")
        # 2) Escribir a .tmp
        with pd.ExcelWriter(tmp_path, engine="openpyxl") as writer:
            df_new.to_excel(writer, index=False, sheet_name=DOCENTES_SHEET)
        # 3) Reemplazo atómico
        os.replace(tmp_path, DOCENTES_XLSX)

def list_docentes_backups(limit: int = 15) -> list[str]:
    """Lista backups recientes que empiezan con 'docentes_'."""
    try:
        files = [f for f in os.listdir(BACKUP_DIR) if f.startswith("docentes_") and f.endswith(".xlsx")]
        files = sorted(files, key=lambda n: os.path.getmtime(os.path.join(BACKUP_DIR, n)), reverse=True)
        return files[:limit]
    except Exception:
        return []

def docentes_last_modified_str() -> str:
    try:
        ts = datetime.fromtimestamp(os.path.getmtime(DOCENTES_XLSX))
        return ts.strftime("%Y-%m-%d %H:%M:%S")
    except Exception:
        return "—"

def to_excel_bytes(df: pd.DataFrame, sheet_name: str) -> io.BytesIO:
    """Convierte un DataFrame a bytes Excel con el nombre de hoja indicado."""
    buf = io.BytesIO()
    with pd.ExcelWriter(buf, engine="openpyxl") as writer:
        df.to_excel(writer, index=False, sheet_name=sheet_name)
    buf.seek(0)
    return buf


# =========================
# REGLAS Y LÓGICA
# =========================
def ventanas_tipo_docente(tipo: str):
    # Nota: aunque ciertos tipos definan sábado, SOLO se usará si 'dias_permitidos' lo incluye.
    if tipo == "tiempo_completo":
        return {d:[("17:00","22:00")] for d in WEEKDAYS}
    if tipo == "tiempo_completo_6+":
        return {d:[("15:00","22:00")] for d in WEEKDAYS}
    if tipo == "medio_tiempo":
        return {d:[("18:00","22:00")] for d in WEEKDAYS}
    if tipo == "asignacion_extra":
        reglas = {d:[("19:00","21:00")] for d in WEEKDAYS}
        reglas["Sábado"] = [("08:00","14:00")]
        return reglas
    if tipo == "administrativo":
        reglas = {d:[("19:00","22:00")] for d in WEEKDAYS}
        reglas["Sábado"] = [("08:00","14:00")]
        return reglas
    # fallback amplio
    return {d:[("07:00","22:00")] for d in WEEKDAYS}

def sincronias_requeridas(n_paralelos_total: int) -> int:
    if n_paralelos_total <= 2: return 1
    if n_paralelos_total <= 5: return 2
    if n_paralelos_total <= 8: return 3
    if n_paralelos_total <= 10: return 4
    return 5

def _get_excel_day_window(row, day_name):
    """Devuelve (ini, fin) si hay columnas por día válidas; si no, None."""
    if day_name not in DAY_COLS:
        return None
    ini_col, fin_col = DAY_COLS[day_name]
    ini = str(row.get(ini_col, "") or "").strip()
    fin = str(row.get(fin_col, "") or "").strip()
    if is_hhmm(ini) and is_hhmm(fin):
        return (ini, fin)
    return None

def effective_windows_by_day(row_docente):
    """
    Ventanas efectivas por día (con franjas por día y fallbacks):
      - 'dias_permitidos' manda SIEMPRE. Si se especifica, solo se consideran esos días (incluyendo Sábado SOLO si aparece).
      - Si 'dias_permitidos' está vacío: se consideran L–V (NO se auto-incluye sábado ni domingo).
      - Para cada día elegido:
          * Si existe <dia>_ini/<dia>_fin en Excel -> base del día = esas columnas.
          * Si no existen y es L–V, se usa franja_inicio/franja_fin (si existen).
      - SIEMPRE se intersecta con la ventana del TIPO para ese día (si la hay).
    """
    tipo_val = str(row_docente.get("tipo_docente","")).strip()
    f_ini = str(row_docente.get("franja_inicio","") or "").strip()
    f_fin = str(row_docente.get("franja_fin","") or "").strip()

    raw = [d.strip() for d in str(row_docente.get("dias_permitidos","")).split(",") if d.strip()]
    dias_raw = [normalize_day_token(d) for d in raw]

    vtipo = ventanas_tipo_docente(tipo_val)

    if dias_raw:
        dias_base = dias_raw.copy()
    else:
        dias_base = WEEKDAYS.copy()  # SOLO L–V por defecto

    out = {}

    for d in dias_base:
        vtipo_list = vtipo.get(d, [])
        base_day = _get_excel_day_window(row_docente, d)

        # Fallback L–V a franja global si no hay por día
        if base_day is None and d in WEEKDAYS and is_hhmm(f_ini) and is_hhmm(f_fin):
            base_day = (f_ini, f_fin)

        if base_day is not None:
            # Intersección base_day con el TIPO
            if vtipo_list:
                for (vt_ini, vt_fin) in vtipo_list:
                    ok, i_ini, i_fin = _intersect(base_day[0], base_day[1], vt_ini, vt_fin)
                    if ok:
                        out.setdefault(d, []).append((i_ini, i_fin))
            else:
                out.setdefault(d, []).append(base_day)
        else:
            # Sin base_day: usar ventana del TIPO (si existe) para ese día
            for (vt_ini, vt_fin) in vtipo_list:
                ok, i_ini, i_fin = _intersect(vt_ini, vt_fin, vt_ini, vt_fin)
                if ok:
                    out.setdefault(d, []).append((i_ini, i_fin))

    # Limpieza
    for d in list(out.keys()):
        uniq = []
        seen = set()
        for (a,b) in out[d]:
            if not (is_hhmm(a) and is_hhmm(b) and a < b):
                continue
            key = f"{a}-{b}"
            if key not in seen:
                seen.add(key)
                uniq.append((a,b))
        if not uniq:
            del out[d]
        else:
            out[d] = uniq

    return out

def ventanas_tipo_for_day(tipo_docente, dia):
    return ventanas_tipo_docente(tipo_docente).get(dia, [])

# -------- CONFLICTOS --------
def hay_conflicto_sync_global(df_master, ciclo, dia, asignatura, s_ini, s_fin, exclude_row_id=None):
    """
    Conflicto si en el mismo ciclo y mismo día existe OTRA asignatura cuya sincronía se solape.
    - Mismo ciclo + mismo día + asignatura distinta -> NO puede solaparse la sincronía.
    - Mismo ciclo + mismo día + misma asignatura -> SÍ puede solaparse (paralelos distintos permitidos).
    Se ignoran tutorías completamente aquí.
    """
    if s_ini is None or s_fin is None:
        return False, ""

    df_check = df_master.copy()
    if exclude_row_id is not None and "row_id" in df_check.columns:
        df_check = df_check[df_check["row_id"] != exclude_row_id]

    mask = (df_check["ciclo"]==ciclo) & (df_check["dia"]==dia)
    df_same_cycle_day = df_check[mask]
    for _, row in df_same_cycle_day.iterrows():
        asig_row = str(row.get("asignatura",""))
        # misma asignatura => permitido
        if normalize_key(asig_row) == normalize_key(asignatura or ""):
            continue
        if overlaps(s_ini, s_fin, row.get("sincronía_inicio",""), row.get("sincronía_fin","")):
            return True, f"Cruce de sincronía con otra asignatura en ciclo {ciclo} ({asig_row})."
    return False, ""

def hay_conflicto_self(docente, df_master, dia, s_ini, s_fin, t_ini, t_fin, exclude_row_id=None):
    """
    Conflictos propios del mismo docente (independiente de ciclo/asignatura), para el MISMO día:
    - La nueva SINCRONÍA no puede solaparse con NINGÚN bloque propio (ni sinc ni tut) ya existente ese día.
    - La nueva TUTORÍA tampoco puede solaparse con NINGÚN bloque propio (ni sinc ni tut) ya existente ese día.
    """
    if not docente:
        return False, ""
    df_check = df_master.copy()
    if exclude_row_id is not None and "row_id" in df_check.columns:
        df_check = df_check[df_check["row_id"] != exclude_row_id]

    mask_self = (df_check["docente"].str.lower()==docente.lower()) & (df_check["dia"]==dia)
    df_self = df_check[mask_self]

    # Verificar solapes de la nueva sincronía con cualquiera de mis bloques existentes
    for _, r in df_self.iterrows():
        if overlaps(s_ini, s_fin, r.get("sincronía_inicio",""), r.get("sincronía_fin","")):
            return True, "Cruce con tu propia sincronía."
        if overlaps(s_ini, s_fin, r.get("tutoría_inicio",""), r.get("tutoría_fin","")):
            return True, "Cruce con tu propia tutoría."

    # Verificar solapes de la nueva tutoría con cualquiera de mis bloques existentes
    for _, r in df_self.iterrows():
        if overlaps(t_ini, t_fin, r.get("sincronía_inicio",""), r.get("sincronía_fin","")):
            return True, "La tutoría propuesta cruza con tu propia sincronía."
        if overlaps(t_ini, t_fin, r.get("tutoría_inicio",""), r.get("tutoría_fin","")):
            return True, "La tutoría propuesta cruza con tu propia tutoría."

    return False, ""

def tutorias_cumple_18_19(df_master, docente):
    mask = (df_master["docente"].str.lower()==(docente or "").lower())
    for _, row in df_master[mask].iterrows():
        if is_hhmm(row["tutoría_inicio"]) and row["tutoría_inicio"] >= "18:00":
            return True
    return False

# -------- SUGERENCIAS --------
def sugerir_sincronia(row_docente, df_master):
    """
    Propone sincronías válidas (1h) por día según ventanas efectivas y sin conflictos.
    Valida:
      - Conflicto GLOBAL de sincronía (asignaturas distintas en mismo ciclo/día).
      - Auto-conflicto con bloques del mismo docente (sinc y tut) en ese día.
    """
    ciclo_val = int(row_docente["ciclo"]) if "ciclo" in row_docente else None
    asignatura_val = str(row_docente.get("asignatura",""))
    docente_val = str(row_docente.get("docente",""))

    ventanas = effective_windows_by_day(row_docente)
    opciones = []
    for d, rangos in ventanas.items():
        for (i_ini, i_fin) in rangos:
            for s in time_range(i_ini, i_fin, STEP_MIN):
                fin = (_t(s) + timedelta(minutes=SYNC_SLOT_MIN)).strftime(TIME_FMT)
                # Debe caber completo
                if not inside_interval(fin, i_ini, i_fin) and fin != i_fin:
                    continue

                # Global (solo sinc)
                conflict_g, _ = hay_conflicto_sync_global(
                    df_master=df_master, ciclo=ciclo_val, dia=d,
                    asignatura=asignatura_val, s_ini=s, s_fin=fin
                )
                if conflict_g:
                    continue

                # Self-conflict (probar con tut ficticia vacía hasta elegir tutoría)
                conflict_s, _ = hay_conflicto_self(
                    docente=docente_val, df_master=df_master, dia=d,
                    s_ini=s, s_fin=fin, t_ini="00:00", t_fin="00:00"
                )
                if conflict_s:
                    continue

                opciones.append((d, s, fin))
    return opciones

def tutorias_posibles(tipo_docente, dia, sincronia_inicio, row_context_for_excel=None,
                      docente=None, df_master=None):
    """
    Calcula tutorías de 2h alrededor de la sincronía:
      - Intersecta ventanas del TIPO y del Excel (si se pasa contexto).
      - Excluye auto-conflictos del docente (con sus propios bloques en ese mismo día).
    """
    tipo_windows = ventanas_tipo_for_day(tipo_docente, dia)
    if not tipo_windows:
        return []

    # Intersección con Excel (si existe)
    excel_windows = []
    if row_context_for_excel is not None:
        eff_all = effective_windows_by_day(row_context_for_excel)
        excel_windows = eff_all.get(dia, [])

    if not excel_windows:
        base_windows = tipo_windows
    else:
        base_windows = []
        for (ti, tf) in tipo_windows:
            for (ei, ef) in excel_windows:
                ok, ii, ff = _intersect(ti, tf, ei, ef)
                if ok:
                    base_windows.append((ii, ff))

    if not base_windows:
        return []

    si = _t_safe(sincronia_inicio)
    if si is None:
        return []

    A_ini = (si - timedelta(hours=1)).strftime(TIME_FMT)
    A_fin = (si + timedelta(hours=1)).strftime(TIME_FMT)
    B_ini = si.strftime(TIME_FMT)
    B_fin = (si + timedelta(hours=2)).strftime(TIME_FMT)

    def encaja(op_ini, op_fin):
        for (v_ini, v_fin) in base_windows:
            if inside_interval(op_ini, v_ini, v_fin) and (_t_safe(op_fin) is not None) and (_t_safe(op_fin) <= _t_safe(v_fin)):
                return True
        return False

    candidatos = []
    if encaja(A_ini, A_fin): candidatos.append(("A", A_ini, A_fin))
    if encaja(B_ini, B_fin): candidatos.append(("B", B_ini, B_fin))

    # Filtrar auto-conflictos del docente (si se proporcionó contexto)
    if docente and df_master is not None:
        filtrados = []
        for (k, ti, tf) in candidatos:
            conf_self, _ = hay_conflicto_self(
                docente=docente, df_master=df_master, dia=dia,
                s_ini=sincronia_inicio, s_fin=(si + timedelta(hours=1)).strftime(TIME_FMT),
                t_ini=ti, t_fin=tf
            )
            if not conf_self:
                filtrados.append((k, ti, tf))
        return filtrados

    return candidatos

# =========================
# UI Helpers
# =========================
def select_with_placeholder(label, options_real, help=None, key=None):
    opts = ["— Selecciona —"] + options_real
    val = st.selectbox(label, options=opts, key=key, help=help)
    return None if val == "— Selecciona —" else val

def record_card(new_row: dict, title="✅ Registro guardado"):
    st.markdown(f"#### {title}")
    c1, c2 = st.columns(2)
    with c1:
        st.markdown(f"**Docente:** {new_row['docente']}")
        st.markdown(f"**Asignatura:** {new_row['asignatura']}")
        st.markdown(f"**Paralelo:** `{new_row['paralelo_codigo']}`")
        st.markdown(f"**Tipo docente:** `{new_row['tipo_docente']}`")
        st.markdown(f"**Ciclo:** {new_row['ciclo']}")
    with c2:
        st.markdown(f"**Día:** {new_row['dia']}")
        st.markdown(f"**Sincronía:** {new_row['sincronía_inicio']}–{new_row['sincronía_fin']} (1h)")
        st.markdown(f"**Tutoría:** {new_row['tutoría_inicio']}–{new_row['tutoría_fin']} (2h)")
        st.caption(f"🕒 {new_row.get('timestamp','')}")

def tidy_table(df: pd.DataFrame) -> pd.DataFrame:
    cols = ["row_id","docente","asignatura","paralelo_codigo","ciclo","dia",
            "sincronía_inicio","sincronía_fin","tutoría_inicio","tutoría_fin","tipo_docente","timestamp"]
    have = [c for c in cols if c in df.columns]
    tidy = df[have].rename(columns={
        "paralelo_codigo":"paralelo",
        "timestamp":"registrado"
    }).sort_values(["ciclo","dia","sincronía_inicio","docente","asignatura","paralelo"])
    return tidy

def build_cronograma(df: pd.DataFrame, ciclo: int | None, highlight_docente: str | None,
                    start="07:00", end="22:00", step=60, paralelo_filter: str | None=None):
    if ciclo is not None:
        df = df[df["ciclo"]==ciclo].copy()
    if paralelo_filter:
        df = df[df["paralelo_codigo"]==paralelo_filter].copy()

    times = time_range(start, end, step)
    grid = pd.DataFrame(index=times, columns=DAYS_FULL)
    grid[:] = ""

    def fill_block(day, start_h, end_h, text):
        s = _t_safe(start_h); e = _t_safe(end_h)
        if s is None or e is None or s >= e:
            return
        cur = s
        while cur < e:
            slot = cur.strftime(TIME_FMT)
            if slot in grid.index and day in grid.columns:
                current = str(grid.loc[slot, day]).strip()
                grid.loc[slot, day] = text if current == "" else (current + "  |  " + text)
            cur += timedelta(minutes=step)

    for _, r in df.iterrows():
        d = r["dia"]
        if d not in DAYS_FULL: continue
        if is_hhmm(str(r["sincronía_inicio"])) and is_hhmm(str(r["sincronía_fin"])):
            txt = label_block(r["docente"], r["asignatura"], r["paralelo_codigo"], "SINC")
            fill_block(d, r["sincronía_inicio"], r["sincronía_fin"], txt)
        if is_hhmm(str(r["tutoría_inicio"])) and is_hhmm(str(r["tutoría_fin"])):
            txt = label_block(r["docente"], r["asignatura"], r["paralelo_codigo"], "TUT")
            fill_block(d, r["tutoría_inicio"], r["tutoría_fin"], txt)

    azul = "#e6f3ff"; verde = "#e8f7e6"; amarillo = "#fff2b2"

    def style_cell(val):
        if not isinstance(val, str) or val.strip() == "": return ""
        rules = []
        txt = val.lower()
        if "sinc" in txt and "tut" in txt:
            rules.append(f"background-image: linear-gradient(180deg, {azul} 50%, {verde} 50%);")
        elif "sinc" in txt:
            rules.append(f"background-color: {azul};")
        elif "tut" in txt:
            rules.append(f"background-color: {verde};")
        if highlight_docente and normalize_key(highlight_docente) in normalize_key(val):
            rules.append(f"outline: 2px solid #f1c40f; background-color: {amarillo}; font-weight: 700;")
        rules.append("border: 1px solid #ddd; padding: 2px;")
        return " ".join(rules)

    return grid.style.applymap(style_cell)

# =========================
# FUNCIONES DASHBOARD
# =========================
def docente_expected_map(df_doc: pd.DataFrame) -> pd.DataFrame:
    cols = ["docente","asignatura","paralelo_codigo","ciclo"]
    base = df_doc[cols].dropna().copy()
    base["docente_norm"] = base["docente"].str.lower()
    base["key"] = base["docente_norm"] + "||" + base["asignatura"].astype(str) + "||" + base["paralelo_codigo"].astype(str) + "||" + base["ciclo"].astype(str)
    return base

def docente_completed_map(df_master: pd.DataFrame) -> pd.DataFrame:
    if df_master.empty:
        return pd.DataFrame(columns=["docente_norm","key"])
    base = df_master[["docente","asignatura","paralelo_codigo","ciclo"]].dropna().copy()
    base["docente_norm"] = base["docente"].str.lower()
    base["key"] = base["docente_norm"] + "||" + base["asignatura"].astype(str) + "||" + base["paralelo_codigo"].astype(str) + "||" + base["ciclo"].astype(str)
    base = base.drop_duplicates(subset=["key"])
    return base[["docente_norm","key"]]

def compute_completion(df_doc: pd.DataFrame, df_master: pd.DataFrame):
    exp = docente_expected_map(df_doc)
    comp = docente_completed_map(df_master)
    docentes = sorted(df_doc["docente"].dropna().unique().tolist())

    rows = []
    for d in docentes:
        dn = d.lower()
        exp_keys = set(exp[exp["docente_norm"]==dn]["key"].tolist())
        comp_keys = set(comp[comp["docente_norm"]==dn]["key"].tolist())
        total_exp = len(exp_keys)
        total_comp = len(exp_keys & comp_keys)
        rows.append({
            "docente": d,
            "esperados": total_exp,
            "completados": total_comp,
            "pendientes": max(total_exp - total_comp, 0),
            "estado": "✅ Completo" if total_exp>0 and total_comp>=total_exp else "⏳ Pendiente"
        })
    df_status = pd.DataFrame(rows).sort_values(["estado","docente"])
    total_docentes = len(docentes)
    completos = (df_status["estado"]=="✅ Completo").sum()
    pendientes = total_docentes - completos
    return df_status, total_docentes, completos, pendientes, exp, comp

def pending_items_for_docente(docente: str, exp_df: pd.DataFrame, comp_df: pd.DataFrame):
    dn = docente.lower()
    exp_keys = exp_df[exp_df["docente_norm"]==dn][["key","asignatura","paralelo_codigo","ciclo"]]
    comp_keys = set(comp_df[comp_df["docente_norm"]==dn]["key"].tolist())
    pend = exp_keys[~exp_keys["key"].isin(comp_keys)][["asignatura","paralelo_codigo","ciclo"]]
    return pend

# =========================
# APP
# =========================
st.set_page_config(page_title="UTPL · Horarios (Registro/Edición/Dashboard)", page_icon="⏰", layout="wide")
st.title("UTPL · Administración de Empresas — Registro de horarios MAD")

ensure_data_dir()
create_docentes_template_if_missing()
create_master_if_missing()

top_left, top_right = st.columns([1,3])
with top_left:
    if st.button("🔄 Recargar datos"):
        st.cache_data.clear()

df_doc = load_docentes()
df_master = load_master()

st.markdown("---")

tab_reg, tab_edit, tab_dash, tab_admin = st.tabs(["📝 Registrar", "✏️ Editar", "📊 Dashboard", "🛠️ Admin"])


# =====================================================
# ===================== REGISTRAR =====================
# =====================================================
with tab_reg:
    st.caption("Completa los pasos en orden. La app sugiere horarios válidos y previene cruces de SINCRONÍA automáticamente. Además, tus propios bloques (sincr. y tutoría) nunca pueden cruzarse entre sí.")

    # Paso 1: Docente
    st.subheader("1) Docente")
    docentes_list = sorted([d for d in df_doc["docente"].dropna().unique().tolist() if d.strip()])
    docente_input = select_with_placeholder("Selecciona tu nombre", docentes_list, key="docente_select_reg")
    rows_docente = pd.DataFrame()
    if not docente_input:
        st.info("Selecciona tu nombre para continuar.")
    else:
        docente_key = normalize_key(docente_input)
        rows_docente_all = df_doc[df_doc["docente_key"]==docente_key].copy()
        if rows_docente_all.empty:
            st.error("No se encontraron filas para tu nombre en docentes.xlsx.")
        else:
            # ocultar asignaturas/paralelos ya registrados
            mask_me = (df_master["docente"].str.lower()==docente_input.lower())
            completed = set()
            if not df_master.empty and mask_me.any():
                for _, r in df_master[mask_me].iterrows():
                    cyc = int(r["ciclo"]) if pd.notna(r["ciclo"]) else None
                    completed.add((str(r["asignatura"]), str(r["paralelo_codigo"]), cyc))

            def is_pending(row):
                cyc = int(row["ciclo"]) if pd.notna(row["ciclo"]) else None
                return (str(row["asignatura"]), str(row["paralelo_codigo"]), cyc) not in completed

            rows_docente = rows_docente_all[rows_docente_all.apply(is_pending, axis=1)].copy()

            total_asignados = len(rows_docente_all)
            total_pendientes = len(rows_docente)
            total_hechos = total_asignados - total_pendientes
            if total_pendientes == 0:
                st.success(f"✅ {docente_input}: ya registraste **todos** tus horarios ({total_hechos}/{total_asignados}).")
            elif total_hechos > 0:
                st.info(f"Progreso: {total_hechos}/{total_asignados}. Solo verás **paralelos pendientes**.")
                done_df = rows_docente_all[~rows_docente_all.apply(is_pending, axis=1)][["asignatura","paralelo_codigo","ciclo"]]
                st.caption("Ya registrados:")
                st.dataframe(done_df.reset_index(drop=True), use_container_width=True)

    st.markdown("---")

    # Paso 2: Asignatura
    st.subheader("2) Asignatura")
    asignaturas = sorted(rows_docente["asignatura"].dropna().unique().tolist()) if not rows_docente.empty else []
    asignatura_sel = select_with_placeholder("Selecciona tu asignatura (pendiente)", asignaturas, key="asig_select_reg")
    if not docente_input:
        st.info("Selecciona tu nombre para continuar.")
    elif not asignaturas:
        st.info("No hay asignaturas pendientes.")
    elif not asignatura_sel:
        st.info("Selecciona tu asignatura para continuar.")

    st.markdown("---")

    # Paso 3: Paralelo
    st.subheader("3) Paralelo (código)")
    paralelo_codigo_sel, row_base, tipo_docente_val, ciclo_val = None, None, None, None
    if docente_input and asignatura_sel:
        sub_df = rows_docente[rows_docente["asignatura"]==asignatura_sel].copy()
        paralelos_cod = sorted([p for p in sub_df["paralelo_codigo"].dropna().unique().tolist() if p.strip()])
        paralelo_codigo_sel = select_with_placeholder("Selecciona el paralelo (pendiente)", paralelos_cod, key="paralelo_select_reg")
        if paralelo_codigo_sel:
            rb = sub_df[sub_df["paralelo_codigo"]==paralelo_codigo_sel]
            if rb.empty:
                st.error("No se encontró la fila de ese paralelo.")
            else:
                row_base = rb.iloc[0]
                tipo_docente_val = str(row_base["tipo_docente"])
                ciclo_val = int(row_base["ciclo"]) if "ciclo" in row_base else 1
                # IMPORTANTE: forzar que el contexto lleve el nombre del docente (para self-conflict)
                row_base = row_base.to_dict()
                row_base["docente"] = docente_input
                eff = effective_windows_by_day(row_base)
                dias_txt = ", ".join([f"{d}({'; '.join([a+'–'+b for (a,b) in eff[d]])})" for d in eff])
                st.success(f"**Ciclo:** {ciclo_val} | **Tipo:** `{tipo_docente_val}` | **Ventanas efectivas:** {dias_txt}")
    else:
        st.info("Selecciona asignatura para ver paralelos.")

    st.markdown("---")

    # Paso 4: Sincronía
    st.subheader("4) Sincronía (1 hora)")
    sincronia_pick, sinc_opts = None, []
    if row_base is not None:
        sinc_opts = sugerir_sincronia(row_base, df_master)
        if sinc_opts:
            etiquetas = [f"{d} {ini}–{fin}" for (d, ini, fin) in sinc_opts]
            idx = st.selectbox("Elige una sincronía (saltos de 60')", options=list(range(len(etiquetas))),
                               format_func=lambda i: etiquetas[i], key="sinc_select_reg")
            dia_sel, sinc_ini_sel, sinc_fin_sel = sinc_opts[idx]
            sincronia_pick = (dia_sel, sinc_ini_sel, sinc_fin_sel)
        else:
            st.warning("No hay sincronías disponibles con tu configuración.")
    else:
        st.info("Selecciona docente, asignatura y paralelo para ver sincronías.")

    st.markdown("---")

    # Paso 5: Tutoría
    st.subheader("5) Tutoría (2 horas)")
    tut_pick = None
    if sincronia_pick and tipo_docente_val:
        dia_sel, sinc_ini_sel, _ = sincronia_pick
        tut_opts = tutorias_posibles(
            tipo_docente_val, dia_sel, sinc_ini_sel,
            row_context_for_excel=row_base, docente=docente_input, df_master=df_master
        )
        if tut_opts:
            labels_tut = [f"Opción {k}: {ti}–{tf}" for (k, ti, tf) in tut_opts]
            idx_tut = st.selectbox("Elige tutoría (bloques de 60')", options=list(range(len(labels_tut))),
                                   format_func=lambda i: labels_tut[i], key="tut_select_reg")
            k, tut_ini, tut_fin = tut_opts[idx_tut]
            tut_pick = (k, tut_ini, tut_fin)
        else:
            st.warning("No hay tutorías válidas con esa sincronía. Prueba otra sincronía.")
    else:
        st.info("Selecciona una sincronía para ver tutorías válidas.")

    st.markdown("---")

    # Paso 6: Validaciones
    st.subheader("6) Validaciones")
    conflict_global, msg_global = False, ""
    conflict_self,  msg_self  = False, ""
    if docente_input:
        total_paralelos_doc = len(df_doc[df_doc["docente_key"]==normalize_key(docente_input)])
        req_sinc = sincronias_requeridas(total_paralelos_doc)
        ya_tiene = (df_master["docente"].str.lower()==docente_input.lower()).sum()
        st.caption(f"Sincronías requeridas/semana: **{req_sinc}** | Ya registradas: **{ya_tiene}**")
    if row_base is not None and sincronia_pick and tut_pick:
        dia_sel, sinc_ini_sel, sinc_fin_sel = sincronia_pick
        _, tut_ini, tut_fin = tut_pick

        # Global: solo sinc entre asignaturas distintas en mismo ciclo/día
        conflict_global, msg_global = hay_conflicto_sync_global(
            df_master=df_master, ciclo=int(row_base["ciclo"]), dia=dia_sel,
            asignatura=str(row_base["asignatura"]), s_ini=sinc_ini_sel, s_fin=sinc_fin_sel
        )
        # Self: el mismo docente no puede cruzar nada con lo suyo (sinc/tut)
        conflict_self, msg_self = hay_conflicto_self(
            docente=docente_input, df_master=df_master, dia=dia_sel,
            s_ini=sinc_ini_sel, s_fin=sinc_fin_sel, t_ini=tut_ini, t_fin=tut_fin
        )

        if conflict_global:
            st.error(f"❌ Conflicto de sincronía: {msg_global}")
        if conflict_self:
            st.error(f"❌ Conflicto con tu propia agenda: {msg_self}")

        # Regla de al menos una tutoría ≥ 18:00 (si aplica)
        must_late = False
        if tipo_docente_val == "tiempo_completo_6+":
            must_late = True
        elif asignatura_sel:
            n_para_asig = len(rows_docente[rows_docente["asignatura"]==asignatura_sel]) if not rows_docente.empty else 0
            if n_para_asig > 1:
                must_late = True
        already_ok = tutorias_cumple_18_19(df_master, docente_input) or (is_hhmm(tut_ini) and tut_ini >= "18:00")
        if must_late and not already_ok:
            st.warning("⚠️ Debes registrar **al menos una tutoría** a partir de **18:00** (o 19:00).")

    st.markdown("---")

    # Paso 7: Guardar
    st.subheader("7) Guardar registro")
    can_submit = (docente_input and asignatura_sel and paralelo_codigo_sel and sincronia_pick and tut_pick and not conflict_global and not conflict_self)
    save_btn = st.button("✅ Guardar registro", disabled=not can_submit, key="save_reg")
    if not can_submit:
        st.info("Completa los pasos y resuelve advertencias para habilitar el guardado.")

    if save_btn:
        dia_sel, sinc_ini_sel, sinc_fin_sel = sincronia_pick
        _, tut_ini, tut_fin = tut_pick
        # Duplicado exacto
        dup_mask = (
            (df_master["docente"].str.lower()==docente_input.lower()) &
            (df_master["asignatura"]==asignatura_sel) &
            (df_master["paralelo_codigo"]==paralelo_codigo_sel) &
            (df_master["ciclo"]==ciclo_val) &
            (df_master["dia"]==dia_sel) &
            (df_master["sincronía_inicio"]==sinc_ini_sel) &
            (df_master["sincronía_fin"]==sinc_fin_sel) &
            (df_master["tutoría_inicio"]==tut_ini) &
            (df_master["tutoría_fin"]==tut_fin)
        )
        if dup_mask.any():
            st.error("Este registro ya existe exactamente igual. No se guardó.")
        else:
            new_row = {
                "row_id": str(uuid.uuid4()),
                "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                "docente": docente_input,
                "tipo_docente": tipo_docente_val,
                "asignatura": asignatura_sel,
                "paralelo": None,
                "paralelo_codigo": paralelo_codigo_sel,
                "ciclo": int(ciclo_val or 1),
                "dia": dia_sel,
                "sincronía_inicio": sinc_ini_sel,
                "sincronía_fin": sinc_fin_sel,
                "tutoría_inicio": tut_ini,
                "tutoría_fin": tut_fin
            }
            df_master_new = pd.concat([df_master, pd.DataFrame([new_row])], ignore_index=True)
            save_master(df_master_new)
            backup_master(reason="register")
            st.success("¡Registro guardado!")
            record_card(new_row)
            st.cache_data.clear()

    st.markdown("---")

    # Consolidado rápido
    st.subheader("📊 Vista consolidada (por defecto: Cronograma por ciclo)")
    df_master = load_master()
    if df_master.empty:
        st.info("Aún no hay registros en el consolidado.")
    else:
        tab_ciclo, tab_tabla, tab_doc = st.tabs(["📆 Cronograma por ciclo", "📄 Tabla", "🧑‍🏫 Cronograma del docente"])

        with tab_ciclo:
            ciclos_disponibles = sorted([int(c) for c in df_master["ciclo"].dropna().unique().tolist()])
            if ciclos_disponibles:
                ciclo_pick = st.selectbox("Ciclo", options=ciclos_disponibles, key="cron_ciclo_pick_reg")
                paralelos_en_ciclo = sorted(df_master[df_master["ciclo"]==ciclo_pick]["paralelo_codigo"].dropna().unique().tolist())
                paralelo_filter = st.selectbox("Paralelo (opcional)", options=["(Todos)"]+paralelos_en_ciclo, key="cron_paralelo_pick_reg")
                paralelo_val = None if paralelo_filter == "(Todos)" else paralelo_filter
                styled = build_cronograma(df_master, ciclo=ciclo_pick, highlight_docente=docente_input,
                                          start="07:00", end="22:00", step=60, paralelo_filter=paralelo_val)
                st.dataframe(styled, use_container_width=True)
            else:
                st.info("No hay ciclos disponibles.")

        with tab_tabla:
            st.dataframe(tidy_table(df_master), use_container_width=True)
            bytes_xlsx = download_excel_bytes(df_master)
            st.download_button("⬇️ Descargar consolidado (Excel)", data=bytes_xlsx,
                               file_name="horarios_master.xlsx",
                               mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                               key="download_master_reg")

        with tab_doc:
            if not docente_input:
                st.info("Selecciona tu nombre arriba para ver tu cronograma personal.")
            else:
                df_me = df_master[df_master["docente"].str.lower()==docente_input.lower()]
                if df_me.empty:
                    st.info("Aún no tienes registros en el consolidado.")
                else:
                    ciclos_me = sorted([int(c) for c in df_me["ciclo"].dropna().unique().tolist()])
                    ciclo_me_pick = st.selectbox("Ciclo (opcional)", options=["(Todos)"]+ciclos_me, key="doc_ciclo_pick_reg")
                    df_me2 = df_me.copy()
                    if ciclo_me_pick != "(Todos)":
                        df_me2 = df_me2[df_me2["ciclo"]==ciclo_me_pick]
                    paralelos_me = sorted(df_me2["paralelo_codigo"].dropna().unique().tolist())
                    par_me_pick = st.selectbox("Paralelo (opcional)", options=["(Todos)"]+paralelos_me, key="doc_paralelo_pick_reg")
                    par_me_val = None if par_me_pick == "(Todos)" else par_me_pick
                    styled = build_cronograma(df_me2, ciclo=None, highlight_docente=docente_input,
                                              start="07:00", end="22:00", step=60, paralelo_filter=par_me_val)
                    st.dataframe(styled, use_container_width=True)

# =====================================================
# ======================= EDITAR ======================
# =====================================================
with tab_edit:
    st.caption("Edita un horario ya registrado. Se validan: (1) cruces de SINCRONÍA globales entre asignaturas distintas (mismo ciclo/día), y (2) tus propios cruces (sinc y tutoría) en el mismo día.")
    df_master = load_master()

    if df_master.empty:
        st.info("No hay registros para editar.")
    else:
        st.subheader("A) Selección")
        docentes_master = sorted(df_master["docente"].dropna().unique().tolist())
        docente_edit = select_with_placeholder("Docente", docentes_master, key="docente_select_edit")

        if not docente_edit:
            st.info("Selecciona un docente para continuar.")
        else:
            df_me = df_master[df_master["docente"].str.lower()==docente_edit.lower()].copy()
            if df_me.empty:
                st.info("El docente todavía no tiene registros.")
            else:
                col1, col2 = st.columns(2)
                ciclos_me = sorted([int(c) for c in df_me["ciclo"].dropna().unique().tolist()])
                ciclo_f = col1.selectbox("Ciclo (opcional)", options=["(Todos)"]+ciclos_me, key="edit_ciclo_f")
                df_me2 = df_me.copy()
                if ciclo_f != "(Todos)":
                    df_me2 = df_me2[df_me2["ciclo"]==ciclo_f]
                paralelos_me = sorted(df_me2["paralelo_codigo"].dropna().unique().tolist())
                par_f = col2.selectbox("Paralelo (opcional)", options=["(Todos)"]+paralelos_me, key="edit_par_f")
                if par_f != "(Todos)":
                    df_me2 = df_me2[df_me2["paralelo_codigo"]==par_f]

                st.subheader("B) Registro a editar")
                if df_me2.empty:
                    st.info("No hay registros que coincidan con los filtros.")
                else:
                    df_me2 = df_me2.reset_index(drop=True)
                    def label_row(i):
                        r = df_me2.iloc[i]
                        return f"[{r['row_id'][:8]}] {r['asignatura']} ({r['paralelo_codigo']}) · Ciclo {int(r['ciclo'])} · {r['dia']} " \
                               f"SINC {r['sincronía_inicio']}-{r['sincronía_fin']} | TUT {r['tutoría_inicio']}-{r['tutoría_fin']}"
                    idx_row = st.selectbox("Selecciona el registro", options=list(range(len(df_me2))),
                                           format_func=label_row, key="edit_row_pick")
                    row_current = df_me2.iloc[idx_row].to_dict()

                    # Buscar contexto de docentes.xlsx para ese (docente, asignatura, paralelo, ciclo)
                    map_df = df_doc[
                        (df_doc["docente"].str.lower()==docente_edit.lower()) &
                        (df_doc["asignatura"]==row_current["asignatura"]) &
                        (df_doc["paralelo_codigo"]==row_current["paralelo_codigo"]) &
                        (df_doc["ciclo"]==int(row_current["ciclo"]))
                    ]
                    if map_df.empty:
                        # fallback amplio L–V
                        row_ctx = {
                            "docente": row_current["docente"],
                            "tipo_docente": row_current["tipo_docente"],
                            "asignatura": row_current["asignatura"],
                            "paralelo_codigo": row_current["paralelo_codigo"],
                            "ciclo": int(row_current["ciclo"]),
                            "dias_permitidos": "Lunes,Martes,Miércoles,Miércoles,Jueves,Viernes".replace("Miércoles,Miércoles","Miércoles"),
                            "franja_inicio": "07:00",
                            "franja_fin": "22:00"
                        }
                        st.warning("No encontré la fila en docentes.xlsx para este registro; usando configuración por defecto (L–V 07:00–22:00).")
                    else:
                        row_ctx = map_df.iloc[0].to_dict()
                        row_ctx["docente"] = docente_edit  # para self-conflict

                    st.markdown("---")
                    st.subheader("C) Nueva sincronía (1 hora)")
                    df_for_suggest = df_master[df_master["row_id"]!=row_current["row_id"]]
                    sinc_opts = sugerir_sincronia(row_ctx, df_for_suggest)
                    if not sinc_opts:
                        st.error("No hay opciones de sincronía válidas con las ventanas actuales.")
                        st.stop()
                    etiquetas = [f"{d} {ini}–{fin}" for (d, ini, fin) in sinc_opts]
                    try:
                        pre_idx = sinc_opts.index((row_current["dia"], row_current["sincronía_inicio"], row_current["sincronía_fin"]))
                    except ValueError:
                        pre_idx = 0
                    idx_new_sinc = st.selectbox("Elige sincronía", options=list(range(len(etiquetas))),
                                                format_func=lambda i: etiquetas[i], index=pre_idx, key="edit_sinc_pick")
                    new_dia, new_sinc_ini, new_sinc_fin = sinc_opts[idx_new_sinc]

                    st.subheader("D) Nueva tutoría (2 horas)")
                    tut_opts = tutorias_posibles(
                        str(row_ctx.get("tipo_docente","")), new_dia, new_sinc_ini,
                        row_context_for_excel=row_ctx, docente=docente_edit, df_master=df_master[df_master["row_id"]!=row_current["row_id"]]
                    )
                    if not tut_opts:
                        st.error("No hay opciones de tutoría válidas con esa sincronía. Elige otra sincronía.")
                    labels_tut = [f"Opción {k}: {ti}–{tf}" for (k, ti, tf) in tut_opts]
                    pre_tut_idx = 0
                    for i,(k,ti,tf) in enumerate(tut_opts):
                        if ti==row_current["tutoría_inicio"] and tf==row_current["tutoría_fin"] and new_dia==row_current["dia"]:
                            pre_tut_idx = i; break
                    idx_new_tut = st.selectbox("Elige tutoría", options=list(range(len(labels_tut))),
                                               format_func=lambda i: labels_tut[i], index=pre_tut_idx, key="edit_tut_pick")
                    _, new_tut_ini, new_tut_fin = tut_opts[idx_new_tut]

                    st.markdown("---")
                    st.subheader("E) Validaciones y guardado")
                    conflict_global, msg_global = hay_conflicto_sync_global(
                        df_master=df_master, ciclo=int(row_ctx["ciclo"]), dia=new_dia,
                        asignatura=str(row_ctx["asignatura"]), s_ini=new_sinc_ini, s_fin=new_sinc_fin,
                        exclude_row_id=row_current["row_id"]
                    )
                    conflict_self, msg_self = hay_conflicto_self(
                        docente=docente_edit, df_master=df_master, dia=new_dia,
                        s_ini=new_sinc_ini, s_fin=new_sinc_fin, t_ini=new_tut_ini, t_fin=new_tut_fin,
                        exclude_row_id=row_current["row_id"]
                    )
                    if conflict_global:
                        st.error(f"❌ Conflicto de sincronía: {msg_global}")
                    if conflict_self:
                        st.error(f"❌ Conflicto con tu propia agenda: {msg_self}")
                    if not conflict_global and not conflict_self:
                        st.success("Sin conflictos con la nueva configuración.")

                    can_update = not conflict_global and not conflict_self
                    if st.button("💾 Guardar cambios", disabled=not can_update, key="save_edit_btn"):
                        df_upd = df_master.copy()
                        mask = (df_upd["row_id"]==row_current["row_id"])
                        df_upd.loc[mask, ["dia","sincronía_inicio","sincronía_fin","tutoría_inicio","tutoría_fin","timestamp"]] = [
                            new_dia, new_sinc_ini, new_sinc_fin, new_tut_ini, new_tut_fin,
                            datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                        ]
                        save_master(df_upd)
                        backup_master(reason="edit")
                        st.success("Cambios guardados correctamente.")
                        record_card({
                            **row_current,
                            "dia": new_dia,
                            "sincronía_inicio": new_sinc_ini,
                            "sincronía_fin": new_sinc_fin,
                            "tutoría_inicio": new_tut_ini,
                            "tutoría_fin": new_tut_fin,
                            "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                        }, title="✏️ Registro actualizado")
                        st.cache_data.clear()

# =====================================================
# ===================== DASHBOARD =====================
# =====================================================
with tab_dash:
    st.caption("Indicadores, pendientes y un cronograma global con filtros para monitoreo en tiempo real.")

    df_doc = load_docentes()
    df_master = load_master()

    status_df, total_docentes, completos, pendientes, exp_df, comp_df = compute_completion(df_doc, df_master)
    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Docentes (mapa docentes.xlsx)", total_docentes)
    c2.metric("Docentes completos", completos)
    c3.metric("Docentes pendientes", pendientes)
    c4.metric("Registros consolidados", len(df_master))

    st.markdown("### Estado por docente")
    st.dataframe(status_df.reset_index(drop=True), use_container_width=True)

    with st.expander("🔍 Ver pendientes por docente", expanded=False):
        docente_pend = select_with_placeholder("Docente", sorted(df_doc["docente"].unique().tolist()), key="dash_doc_pend")
        if docente_pend:
            pend_df = pending_items_for_docente(docente_pend, exp_df, comp_df)
            if pend_df.empty:
                st.success("Este docente no tiene pendientes. ✅")
            else:
                st.warning(f"Pendientes para **{docente_pend}**:")
                st.dataframe(pend_df.rename(columns={"paralelo_codigo":"paralelo"}), use_container_width=True)

    st.markdown("### Cobertura por ciclo")
    exp_cycle = exp_df.groupby("ciclo").size().rename("esperados").reset_index()
    comp_cycle = comp_df.merge(exp_df[["key","ciclo"]], on="key", how="left").groupby("ciclo").size().rename("completados").reset_index()
    cov = exp_cycle.merge(comp_cycle, on="ciclo", how="left").fillna(0)
    cov["completados"] = cov["completados"].astype(int)
    cov["pendientes"] = (cov["esperados"] - cov["completados"]).clip(lower=0)
    if not cov.empty:
        cov["% avance"] = np.where(
            cov["esperados"] > 0,
            (100.0 * cov["completados"] / cov["esperados"]).round(1),
            0.0
        )
    else:
        cov["% avance"] = []
    if cov.empty:
        st.info("No hay datos de cobertura todavía.")
    else:
        st.dataframe(cov.sort_values("ciclo").reset_index(drop=True), use_container_width=True)

    st.markdown("### Cronograma global con filtros")
    colf1, colf2, colf3, colf4 = st.columns(4)
    ciclos_disponibles = sorted([int(c) for c in df_master["ciclo"].dropna().unique().tolist()])
    ciclo_global = colf1.selectbox("Ciclo", options=["(Todos)"]+ciclos_disponibles, key="dash_ciclo")
    docentes_global = ["(Todos)"] + sorted(df_master["docente"].dropna().unique().tolist())
    docente_gl = colf2.selectbox("Docente", options=docentes_global, key="dash_docente")
    paralelos_all = sorted(df_master["paralelo_codigo"].dropna().unique().tolist())
    paralelo_gl = colf3.selectbox("Paralelo", options=["(Todos)"]+paralelos_all, key="dash_paralelo")
    tipo_opts = ["(Todos)"] + sorted(df_master["tipo_docente"].dropna().unique().tolist())
    tipo_gl = colf4.selectbox("Tipo de docente", options=tipo_opts, key="dash_tipo")

    df_view = df_master.copy()
    if ciclo_global != "(Todos)":
        df_view = df_view[df_view["ciclo"]==ciclo_global]
    if docente_gl != "(Todos)":
        df_view = df_view[df_view["docente"]==docente_gl]
    if paralelo_gl != "(Todos)":
        df_view = df_view[df_view["paralelo_codigo"]==paralelo_gl]
    if tipo_gl != "(Todos)":
        df_view = df_view[df_view["tipo_docente"]==tipo_gl]

    if ciclo_global == "(Todos)":
        st.info("Selecciona un **ciclo** para ver la malla semanal. Mientras tanto, revisa la tabla filtrada.")
        st.dataframe(tidy_table(df_view), use_container_width=True)
    else:
        highlight_name = None if docente_gl == "(Todos)" else docente_gl
        paralelo_val = None if paralelo_gl == "(Todos)" else paralelo_gl
        styled = build_cronograma(df_view, ciclo=ciclo_global, highlight_docente=highlight_name,
                                  start="07:00", end="22:00", step=60, paralelo_filter=paralelo_val)
        st.dataframe(styled, use_container_width=True, height=600)

    st.markdown("### Exportación")
    bytes_xlsx = download_excel_bytes(df_master)
    st.download_button("⬇️ Descargar consolidado (Excel)", data=bytes_xlsx,
                       file_name="horarios_master.xlsx",
                       mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                       key="download_master_dash")


# =====================================================
# ====================== ADMIN ========================
# =====================================================
with tab_admin:
    st.caption("Panel administrador: subir/validar y reemplazar el mapa de docentes (docentes.xlsx).")

    # --- Gate por PIN muy simple ---
    if "admin_ok" not in st.session_state:
        st.session_state["admin_ok"] = False

    if not st.session_state["admin_ok"]:
        with st.form("admin_login"):
            st.write("Acceso protegido por PIN.")
            pin_in = st.text_input("PIN", type="password")
            ok = st.form_submit_button("Entrar")
        if ok:
            if pin_in == ADMIN_PIN:
                st.session_state["admin_ok"] = True
                st.success("Acceso concedido.")
                st.rerun()
            else:
                st.error("PIN incorrecto.")
        st.stop()

    # --- Info del archivo actual ---
    c1, c2 = st.columns(2)
    with c1:
        st.subheader("Estado actual")
        st.write(f"Ruta: `{DOCENTES_XLSX}`")
        st.write(f"Última modificación: {docentes_last_modified_str()}")
        if os.path.exists(DOCENTES_XLSX):
            try:
                cur_df = pd.read_excel(DOCENTES_XLSX, sheet_name=DOCENTES_SHEET, engine="openpyxl")
                st.write("Vista previa (primeras filas):")
                st.dataframe(cur_df.head(10), use_container_width=True)
                st.download_button("⬇️ Descargar docentes.xlsx actual",
                                   data=to_excel_bytes(cur_df, DOCENTES_SHEET),
                                   file_name="docentes_actual.xlsx",
                                   mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")
            except Exception as e:
                st.warning(f"No se pudo leer el docentes.xlsx actual: {e}")

    with c2:
        st.subheader("Backups recientes")
        bks = list_docentes_backups()
        if not bks:
            st.info("Sin backups de docentes aún.")
        else:
            sel = st.selectbox("Selecciona un backup para restaurar", options=["(Ninguno)"]+bks, key="restore_pick")
            if sel != "(Ninguno)":
                full = os.path.join(BACKUP_DIR, sel)
                if st.button("↩️ Restaurar este backup", type="secondary"):
                    lock = FileLock(DOCENTES_LOCK_PATH, timeout=10)
                    with lock:
                        # backup del actual, luego restaurar
                        backup_docentes(reason="before-restore")
                        shutil.copy2(full, DOCENTES_XLSX)
                    st.success(f"Restaurado: {sel}")
                    st.cache_data.clear()
                    st.rerun()

    st.markdown("---")
    st.subheader("Cargar nuevo docentes.xlsx")

    up = st.file_uploader("Sube el archivo Excel (hoja 'docentes')", type=["xlsx"], accept_multiple_files=False)
    if up is not None:
        try:
            # Leer la hoja exacta
            new_df = pd.read_excel(up, sheet_name=DOCENTES_SHEET, engine="openpyxl")
        except ValueError as e:
            st.error(f"El archivo no contiene la hoja '{DOCENTES_SHEET}'. Detalle: {e}")
            st.stop()
        except Exception as e:
            st.error(f"No se pudo leer el archivo: {e}")
            st.stop()

        ok, errs, warns = validate_docentes_df(new_df)
        if errs:
            st.error("Errores de validación:")
            for e in errs: st.write(f"- {e}")
        if warns:
            st.warning("Advertencias:")
            for w in warns: st.write(f"- {w}")

        st.write("Vista previa del archivo subido:")
        st.dataframe(new_df.head(15), use_container_width=True)

        can_apply = ok  # solo permitimos reemplazar si pasa validación estricta
        if st.button("✅ Reemplazar docentes.xlsx", disabled=not can_apply):
            try:
                write_docentes_atomic(new_df)
                st.success("Archivo reemplazado correctamente.")
                # Limpiar caché y recargar dataframes
                st.cache_data.clear()
                st.rerun()
            except Timeout:
                st.error("Archivo en uso. Intenta nuevamente en unos segundos.")
            except Exception as e:
                st.error(f"Error al reemplazar: {e}")

    st.caption("Sugerencia: configura la variable de entorno ADMIN_PIN para no hardcodear el PIN. "
               "Tras reemplazar el archivo, la app limpia cachés y recarga datos automáticamente.")



st.caption("Tips: define **franjas por día** en `docentes.xlsx` (p. ej. `lunes_ini/lunes_fin`). "
           "Si no las pones, L–V usa la franja global. "
           "Sábado SOLO aparece si lo declaras explícitamente en `dias_permitidos`. "
           "Backups automáticos en `data/backups/`. "
           "En Render, configura DATA_DIR=/var/data y usa un disco persistente.")


