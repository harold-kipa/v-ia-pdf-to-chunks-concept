import pyodbc
from dotenv import load_dotenv
import os

meses_es = {
    1: "Enero", 2: "Febrero", 3: "Marzo", 4: "Abril",
    5: "Mayo", 6: "Junio", 7: "Julio", 8: "Agosto",
    9: "Septiembre", 10: "Octubre", 11: "Noviembre", 12: "Diciembre"
}


def db_conection(file_id, typeFile):
    load_dotenv()
    USER = os.getenv("SQLSERVER_USER")
    PASS = os.getenv("SQLSERVER_PASS")
    HOST = os.getenv("SQLSERVER_HOST")
    DB   = os.getenv("SQLSERVER_DB")
    SCHEMA = os.getenv("SQLSERVER_SCHEMA")
    

    conectionText = 'DRIVER={ODBC Driver 18 for SQL Server};'+ f'SERVER={HOST};DATABASE={DB};UID={USER};PWD={PASS};Encrypt=yes;TrustServerCertificate=yes;'
    # conectionText = 'DRIVER={ODBC Driver 18 for SQL Server};'+ f'SERVER={HOST};DATABASE={DB};UID={USER};PWD={PASS}' // maquina virtual

    conn = pyodbc.connect(conectionText)

    # Crear cursor
    cursor = conn.cursor()

    # Consulta SQL

    if typeFile == "oc-c":
        query = f"SELECT * FROM {SCHEMA}.tbl_files WHERE file_id = ?"
    elif typeFile == "op":
        query = f"SELECT * FROM {SCHEMA}.tbl_files_op_final WHERE consecutive = ?"
    else:
        raise ValueError("Tipo no reconocido. Usa 'oc-c' o 'op'.")
    # Ejecutar consulta
    cursor.execute(query, (file_id,))

    # Obtener resultado
    rowFiles = cursor.fetchone()

    if rowFiles:
        print("Registro encontrado:")
        cleanedRow = [str(col).strip() if isinstance(col, str) else col for col in rowFiles]
        results = {
            "pdf_name": cleanedRow[3],
            "number": None,
            "year": None,
            "month": None,
            "account_number_homologated": None
        }
        print(cleanedRow[7])
        if typeFile == "op":
            if cleanedRow[7] != None:
                query = f"SELECT * FROM {SCHEMA}.tbl_payments_accounts_relation_final WHERE payments_accounts_relation_id = ?"
                cursor.execute(query, (cleanedRow[7],))
                rowPaymentsAccounts = cursor.fetchone()
                # print(rowPaymentsAccounts[15])
                print(rowPaymentsAccounts)

                query = f"SELECT * FROM {SCHEMA}.tbl_higher_accounts_new WHERE higher_account_id = ?"
                cursor.execute(query, (rowPaymentsAccounts[16],))
                rowHigherAccount = cursor.fetchone()
                print(rowHigherAccount[4])
                print(rowHigherAccount)
                if rowPaymentsAccounts[2] is not None:
                    results = {
                        "pdf_name": cleanedRow[3],
                        "number": int(float(rowPaymentsAccounts[2])),
                        "year": rowPaymentsAccounts[1].year,
                        "month": meses_es[rowPaymentsAccounts[1].month],
                        "account_number_homologated": rowHigherAccount[4]
                    }
        # print(cleanedRow[3])
        print(type(cleanedRow[3]))
        return results
    else:
        print("No se encontró ningún registro con ese id.")

    # Cerrar conexión
    cursor.close()
    conn.close()
if __name__ == "__main__":
    print(db_conection(5706,"op"))
