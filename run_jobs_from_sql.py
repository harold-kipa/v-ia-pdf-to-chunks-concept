
# run_jobs_from_sql.py
#
# Requisitos:
#   pip install python-dotenv pyodbc
#
# .ENV (ejemplos; usa uno de los dos enfoques)
#   # Opción 1: Cadena completa
#   SQLSERVER_CONN="Driver={ODBC Driver 18 for SQL Server};Server=mi-server.database.windows.net;Database=mi_db;Uid=mi_usuario;Pwd=mi_pass;Encrypt=yes;TrustServerCertificate=no;Connection Timeout=30;"
#
#   # Opción 2: Componentes
#   SQLSERVER_SERVER="mi-server.database.windows.net"
#   SQLSERVER_DB="mi_db"
#   SQLSERVER_USER="mi_usuario"
#   SQLSERVER_PWD="mi_pass"
#   SQLSERVER_DRIVER="ODBC Driver 18 for SQL Server"
#
#   # Comando a ejecutar por cada fila (usa {file_path} como placeholder)
#   # Ejemplo: python process_pdf.py --path "{file_path}"
#   COMMAND_TEMPLATE='python process_pdf.py --path "{file_path}"'
#
# Uso:
#   python run_jobs_from_sql.py
#
# Notas:
#  - La tabla objetivo es dbo.tbl_files y se lee la columna file_path (NVARCHAR/VARCHAR).
#  - Puedes filtrar filas con la variable de entorno WHERE_CLAUSE (opcional), p.ej. WHERE_CLAUSE="WHERE is_active = 1"
#  - Si la tabla no tiene columna id, se ordena por file_path.
#  - Para auditoría, se crea logs/run_jobs.log

import os
import sys
import shlex
import logging
import subprocess
from typing import Iterator

import pyodbc
from dotenv import load_dotenv

# ---------------------- Logging ----------------------
LOG_DIR = os.path.join(os.path.dirname(__file__), "logs")
os.makedirs(LOG_DIR, exist_ok=True)
logging.basicConfig(
    filename=os.path.join(LOG_DIR, "run_jobs.log"),
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
)
console = logging.StreamHandler(sys.stdout)
console.setLevel(logging.INFO)
console.setFormatter(logging.Formatter("%(levelname)s | %(message)s"))
logging.getLogger().addHandler(console)

# ---------------------- ENV --------------------------
load_dotenv()

def _build_conn_str() -> str:
    conn = os.getenv("SQLSERVER_CONN")
    if conn:
        return conn

    server = os.getenv("SQLSERVER_SERVER")
    db = os.getenv("SQLSERVER_DB")
    user = os.getenv("SQLSERVER_USER")
    pwd = os.getenv("SQLSERVER_PWD")
    driver = os.getenv("SQLSERVER_DRIVER", "ODBC Driver 18 for SQL Server")
    if not (server and db and user and pwd):
        raise RuntimeError(
            "Faltan variables .env. Define SQLSERVER_CONN o bien "
            "SQLSERVER_SERVER, SQLSERVER_DB, SQLSERVER_USER, SQLSERVER_PWD."
        )
    # Encrypt recomendado para Azure SQL (puedes ajustar TrustServerCertificate si es on‑prem)
    return (
        f"Driver={{{driver}}};"
        f"Server={server};"
        f"Database={db};"
        f"Uid={user};"
        f"Pwd={pwd};"
        "Encrypt=yes;TrustServerCertificate=no;Connection Timeout=30;"
    )

# ---------------------- DB ---------------------------
def yield_file_paths(batch_size: int = 200):
    """
    Devuelve file_path en lotes para no cargar todo en memoria.
    Respeta opcionalmente WHERE_CLAUSE de .env (sin 'WHERE' si prefieres escribir todo).
    """
    conn_str = _build_conn_str()
    where_clause = os.getenv("WHERE_CLAUSE", "").strip()
    # Normaliza WHERE_CLAUSE
    if where_clause and not where_clause.lower().startswith("where"):
        where_clause = "WHERE " + where_clause

    # Intentamos ordenar por id si existe; si falla, ordenamos por file_path
    order_by_candidates = ["id", "file_id", "created_at", "file_path"]
    ordered = False

    with pyodbc.connect(conn_str) as cn:
        cn.fast_executemany = False
        cursor = cn.cursor()
        for order_col in order_by_candidates:
            try:
                base_query = f"SELECT file_path FROM dbo.tbl_files {where_clause} ORDER BY {order_col}"
                cursor.execute(base_query)
                ordered = True
                break
            except pyodbc.Error:
                cn.rollback()
                cursor = cn.cursor()  # reset cursor
                continue

        if not ordered:
            # Último intento sin ORDER BY
            base_query = f"SELECT file_path FROM dbo.tbl_files {where_clause}"
            cursor.execute(base_query)

        while True:
            rows = cursor.fetchmany(batch_size)
            if not rows:
                break
            for (file_path,) in rows:
                if file_path is None:
                    continue
                yield str(file_path)

# ---------------------- Exec -------------------------
def run_command_for_file(command_template: str, file_path: str) -> int:
    """Reemplaza {file_path} y ejecuta el comando. Devuelve returncode."""
    cmd_str = command_template.format(file_path=file_path)
    args = shlex.split(cmd_str)
    logging.info(f"→ Ejecutando: {cmd_str}")
    try:
        result = subprocess.run(args, check=False, capture_output=True, text=True)
        if result.stdout:
            logging.info(f"[STDOUT] {result.stdout.strip()}")
        if result.stderr:
            logging.warning(f"[STDERR] {result.stderr.strip()}")
        return result.returncode
    except FileNotFoundError:
        logging.exception("No se encontró el ejecutable/interpretador del comando.")
        return 127
    except Exception:
        logging.exception("Error inesperado ejecutando el comando.")
        return 1

# ---------------------- Main -------------------------
def main() -> int:
    command_template = os.getenv("COMMAND_TEMPLATE")
    if not command_template:
        logging.error(
            "Define COMMAND_TEMPLATE en .env, por ejemplo:\n"
            'COMMAND_TEMPLATE=python process_pdf.py --path "{file_path}"'
        )
        return 2

    total = 0
    ok = 0
    fail = 0

    for fp in yield_file_paths():
        total += 1
        rc = run_command_for_file(command_template, fp)
        if rc == 0:
            ok += 1
        else:
            fail += 1

    logging.info(f"Procesadas: {total} | OK: {ok} | FALLAS: {fail}")
    return 0 if fail == 0 else 1

if __name__ == "__main__":
    raise SystemExit(main())
