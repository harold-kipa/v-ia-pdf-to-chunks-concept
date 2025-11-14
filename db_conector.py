import pyodbc
from dotenv import load_dotenv
import os


def db_conection(file_id):
    load_dotenv()
    USER = os.getenv("SQLSERVER_USER")
    PASS = os.getenv("SQLSERVER_PASS")
    HOST = os.getenv("SQLSERVER_HOST")
    DB   = os.getenv("SQLSERVER_DB")
    SCHEMA = os.getenv("SQLSERVER_SCHEMA")
    

    conectionText = 'DRIVER={ODBC Driver 17 for SQL Server};'+ f'SERVER={HOST};DATABASE={DB};UID={USER};PWD={PASS}'

    conn = pyodbc.connect(conectionText)

    # Crear cursor
    cursor = conn.cursor()

    # Consulta SQL
    query = f"SELECT file_path FROM {SCHEMA}.tbl_files WHERE file_id = ?"

    # Ejecutar consulta
    cursor.execute(query, (file_id,))

    # Obtener resultado
    row = cursor.fetchone()

    if row:
        print("Registro encontrado:")
        cleaned_row = [str(col).strip() if isinstance(col, str) else col for col in row]
        print(cleaned_row)
        return cleaned_row[0]
    else:
        print("No se encontró ningún registro con ese file_id.")

    # Cerrar conexión
    cursor.close()
    conn.close()


if __name__ == "__main__":
    print(db_conection(1))
